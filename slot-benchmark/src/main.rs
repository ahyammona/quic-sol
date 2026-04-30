//! slot-benchmark
//!
//! Answers one question: which source streams the LATEST slot number faster?
//!
//! Every time either source reports a new slot, print the current latest slot
//! from each source and the gap between them in slot numbers.
//!
//! quic-sol : notify_only — SlotConfirmed fires straight from gossip (no RPC).
//! websocket: slotSubscribe — one notification per slot from Helius.
//!
//! Output (one row per update from either source):
//!
//!   SOURCE        quic-sol       websocket    GAP  LEADER
//!   quic-sol     416406652       416406648     +4  ✅ quic-sol +4 slots
//!   websocket    416406652       416406651     +1  ✅ quic-sol +1 slot
//!   quic-sol     416406655       416406651     +4  ✅ quic-sol +4 slots
//!
//! Usage:
//!   cargo run -p slot-benchmark -- \
//!     --helius-key YOUR_KEY \
//!     --duration   120

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use quinn::{ClientConfig, Endpoint};
use quinn::crypto::rustls::QuicClientConfig;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "slot-benchmark")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:4433")]
    quic_server: String,

    #[arg(long, default_value = "localhost")]
    quic_server_name: String,

    #[arg(long)]
    helius_key: String,

    #[arg(long, default_value_t = 120)]
    duration: u64,
}

// ── Proto ─────────────────────────────────────────────────────────────────────

mod proto {
    use serde::{Deserialize, Serialize};

    pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

    #[derive(Serialize)]
    pub struct ClientHandshake {
        pub service: &'static str,
        pub api_key: Option<String>,
        pub params:  SlotParams,
    }

    #[derive(Serialize)]
    pub struct SlotParams {
        pub kind:            &'static str,
        pub rpc_url:         &'static str,
        pub filter_programs: Vec<String>,
        pub notify_only:     bool,
    }

    #[derive(Deserialize)]
    pub struct ServerAck {
        pub ok:         bool,
        pub message:    String,
        pub session_id: u64,
    }

    pub fn encode(value: &impl serde::Serialize) -> Vec<u8> {
        let body = serde_json::to_vec(value).expect("serialize");
        let mut out = Vec::with_capacity(4 + body.len());
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }
}

// ── TLS no-verify ─────────────────────────────────────────────────────────────

#[derive(Debug)]
struct NoVerifier;
impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(&self,_:&rustls::pki_types::CertificateDer,_:&[rustls::pki_types::CertificateDer],_:&rustls::pki_types::ServerName,_:&[u8],_:rustls::pki_types::UnixTime)->Result<rustls::client::danger::ServerCertVerified,rustls::Error>{Ok(rustls::client::danger::ServerCertVerified::assertion())}
    fn verify_tls12_signature(&self,_:&[u8],_:&rustls::pki_types::CertificateDer,_:&rustls::DigitallySignedStruct)->Result<rustls::client::danger::HandshakeSignatureValid,rustls::Error>{Ok(rustls::client::danger::HandshakeSignatureValid::assertion())}
    fn verify_tls13_signature(&self,_:&[u8],_:&rustls::pki_types::CertificateDer,_:&rustls::DigitallySignedStruct)->Result<rustls::client::danger::HandshakeSignatureValid,rustls::Error>{Ok(rustls::client::danger::HandshakeSignatureValid::assertion())}
    fn supported_verify_schemes(&self)->Vec<rustls::SignatureScheme>{vec![rustls::SignatureScheme::ECDSA_NISTP256_SHA256,rustls::SignatureScheme::ECDSA_NISTP384_SHA384,rustls::SignatureScheme::ED25519,rustls::SignatureScheme::RSA_PKCS1_SHA256,rustls::SignatureScheme::RSA_PKCS1_SHA384,rustls::SignatureScheme::RSA_PKCS1_SHA512]}
}

fn insecure_endpoint() -> Result<Endpoint> {
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();
    let quic = QuicClientConfig::try_from(tls)?;
    let mut ep = Endpoint::client("[::]:0".parse::<SocketAddr>()?)?;
    ep.set_default_client_config(ClientConfig::new(Arc::new(quic)));
    Ok(ep)
}

// ── QUIC framing ──────────────────────────────────────────────────────────────

async fn read_bytes(recv: &mut quinn::RecvStream, n: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    recv.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn read_raw_frame(recv: &mut quinn::RecvStream) -> Result<serde_json::Value> {
    let hdr = read_bytes(recv, 4).await?;
    let len = u32::from_be_bytes(hdr.try_into().unwrap()) as usize;
    anyhow::ensure!(len <= proto::MAX_FRAME_BYTES, "frame too large: {len}");
    let body = read_bytes(recv, len).await?;
    Ok(serde_json::from_slice(&body)?)
}

async fn read_ack(recv: &mut quinn::RecvStream) -> Result<proto::ServerAck> {
    let hdr = read_bytes(recv, 4).await?;
    let len = u32::from_be_bytes(hdr.try_into().unwrap()) as usize;
    anyhow::ensure!(len <= proto::MAX_FRAME_BYTES, "ack too large");
    let body = read_bytes(recv, len).await?;
    Ok(serde_json::from_slice(&body)?)
}

// ── Update types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source { QuicSol, HeliusWs }

#[derive(Debug)]
struct SlotUpdate {
    source: Source,
    slot:   u64,
}

// ── quic-sol listener ─────────────────────────────────────────────────────────

async fn run_quic_sol(
    server:      String,
    server_name: String,
    tx:          mpsc::Sender<SlotUpdate>,
) -> Result<()> {
    let handshake = proto::ClientHandshake {
        service: "slot",
        api_key: None,
        params: proto::SlotParams {
            kind:            "slot",
            rpc_url:         "",
            filter_programs: vec![],
            notify_only:     true,
        },
    };

    let addr: SocketAddr = server.parse().context("invalid quic address")?;
    let ep   = insecure_endpoint()?;
    let conn = ep.connect(addr, &server_name)?.await.context("QUIC connect")?;
    let (mut send, mut recv) = conn.open_bi().await?;

    send.write_all(&proto::encode(&handshake)).await?;
    send.finish()?;

    let ack = read_ack(&mut recv).await?;
    if !ack.ok {
        anyhow::bail!("quic-sol rejected: {}", ack.message);
    }
    eprintln!("[quic-sol ] connected — session {}", ack.session_id);

    loop {
        match read_raw_frame(&mut recv).await {
            Ok(frame) => {
                if let Some(slot) = frame["slot"].as_u64() {
                    let _ = tx.send(SlotUpdate { source: Source::QuicSol, slot }).await;
                }
            }
            Err(e) => { eprintln!("[quic-sol ] stream ended: {e}"); break; }
        }
    }
    Ok(())
}

// ── Helius WebSocket listener ─────────────────────────────────────────────────

async fn run_helius_ws(helius_key: String, tx: mpsc::Sender<SlotUpdate>) -> Result<()> {
    let url = format!("wss://mainnet.helius-rpc.com/?api-key={}", helius_key);

    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .context("Helius WS connect")?;

    let (mut ws_tx, mut ws_rx) = ws.split();

    let sub = serde_json::json!({
        "jsonrpc": "2.0",
        "id":      1,
        "method":  "slotSubscribe"
    });
    ws_tx.send(Message::Text(sub.to_string().into())).await?;
    eprintln!("[websocket] connected — slotSubscribe");

    while let Some(msg) = ws_rx.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(slot) = v
                        .pointer("/params/result/slot")
                        .and_then(|s| s.as_u64())
                    {
                        let _ = tx.send(SlotUpdate { source: Source::HeliusWs, slot }).await;
                    }
                }
            }
            Ok(Message::Close(_)) => { eprintln!("[websocket] closed"); break; }
            Err(e) => { eprintln!("[websocket] error: {e}"); break; }
            _ => {}
        }
    }
    Ok(())
}

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct State {
    quic_latest: Option<u64>,
    ws_latest:   Option<u64>,
    // summary counters (counted per printed row, i.e. when both sides are known)
    quic_ahead_count: u64,
    ws_ahead_count:   u64,
    tied_count:       u64,
    total_updates:    u64,
    quic_gap_sum:     u64,
    ws_gap_sum:       u64,
    max_quic_gap:     u64,
    max_ws_gap:       u64,
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "warn".into()))
        .init();

    let cli = Cli::parse();

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║     slot-benchmark: quic-sol vs Helius WebSocket         ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  quic-sol  → {} (notify_only — straight from gossip)", cli.quic_server);
    println!("  websocket → wss://mainnet.helius-rpc.com (slotSubscribe)");
    println!("  question  → which source has the higher latest slot number?");
    println!("  duration  → {}s", cli.duration);
    println!();
    println!("  TRIGGERED BY = which source fired this update (not who is winning).");
    println!("  GAP = quic-sol latest minus websocket latest (slots).");
    println!("  Rows only printed on leader change or new peak gap — not every slot.");
    println!();

    let (tx, mut rx) = mpsc::channel::<SlotUpdate>(4096);

    tokio::spawn({
        let tx = tx.clone();
        let s  = cli.quic_server.clone();
        let sn = cli.quic_server_name.clone();
        async move {
            if let Err(e) = run_quic_sol(s, sn, tx).await {
                eprintln!("[quic-sol ] fatal: {e}");
            }
        }
    });

    tokio::spawn({
        let tx  = tx.clone();
        let key = cli.helius_key.clone();
        async move {
            if let Err(e) = run_helius_ws(key, tx).await {
                eprintln!("[websocket] fatal: {e}");
            }
        }
    });

    drop(tx);

    // Track last printed leader so we only print on leader flips or new peak gaps
    let mut last_leader: Option<Source> = None;
    let mut last_gap: i64 = 0;

    let state    = Arc::new(Mutex::new(State::default()));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(cli.duration);

    println!("Streaming… (Ctrl-C to stop early)");
    println!("(Rows printed only when the leader changes or gap reaches a new peak)\n");
    println!("{:<10} {:>16} {:>16} {:>6}  {}",
        "TRIGGERED", "quic-sol", "websocket", "GAP", "LEADER");
    println!("  BY");
    println!("{}", "─".repeat(72));

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { break; }

        let update = match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(u)) => u,
            _ => break,
        };

        let mut s = state.lock().unwrap();

        // Advance latest slot for the source that just fired — never go backwards
        match update.source {
            Source::QuicSol => {
                if s.quic_latest.map_or(true, |prev| update.slot > prev) {
                    s.quic_latest = Some(update.slot);
                }
            }
            Source::HeliusWs => {
                if s.ws_latest.map_or(true, |prev| update.slot > prev) {
                    s.ws_latest = Some(update.slot);
                }
            }
        }

        // Wait until both sides have sent at least one slot before printing
        let (qs, ws) = match (s.quic_latest, s.ws_latest) {
            (Some(q), Some(w)) => (q, w),
            _ => continue,
        };

        let gap: i64 = qs as i64 - ws as i64;

        // Determine current leader
        let current_leader = if gap > 0 {
            Some(Source::QuicSol)
        } else if gap < 0 {
            Some(Source::HeliusWs)
        } else {
            None // tied
        };

        // Only print when:
        //   1. Leader just changed (including flip to/from tied)
        //   2. Gap grew in magnitude (new peak for whoever is leading)
        let leader_changed = current_leader != last_leader;
        let gap_is_new_peak = gap.abs() > last_gap.abs() && current_leader == last_leader;

        if !leader_changed && !gap_is_new_peak {
            // Still same leader, gap not growing — skip this intermediate row
            // but still count it in stats
            match gap {
                0 => { s.tied_count += 1; }
                g if g > 0 => {
                    s.quic_ahead_count += 1;
                    let u = g as u64;
                    s.quic_gap_sum += u;
                    if u > s.max_quic_gap { s.max_quic_gap = u; }
                }
                g => {
                    s.ws_ahead_count += 1;
                    let u = g.unsigned_abs();
                    s.ws_gap_sum += u;
                    if u > s.max_ws_gap { s.max_ws_gap = u; }
                }
            }
            s.total_updates += 1;
            last_gap = gap;
            continue;
        }

        s.total_updates += 1;
        last_leader = current_leader;
        last_gap = gap;

        let triggered_by = match update.source {
            Source::QuicSol => "quic-sol",
            Source::HeliusWs => "websocket",
        };

        let (gap_str, leader_str) = if gap == 0 {
            s.tied_count += 1;
            ("     0".to_string(), "🟰  tied".to_string())
        } else if gap > 0 {
            let u = gap as u64;
            s.quic_ahead_count += 1;
            s.quic_gap_sum     += u;
            if u > s.max_quic_gap { s.max_quic_gap = u; }
            (format!("   +{gap}"), format!("✅ quic-sol +{gap} slot{}", if gap == 1 { "" } else { "s" }))
        } else {
            let u = gap.unsigned_abs();
            s.ws_ahead_count += 1;
            s.ws_gap_sum     += u;
            if u > s.max_ws_gap { s.max_ws_gap = u; }
            (format!("   {gap}"), format!("⚠️  websocket +{u} slot{}", if u == 1 { "" } else { "s" }))
        };

        println!("{:<10} {:>16} {:>16} {:>6}  {}",
            triggered_by, qs, ws, gap_str, leader_str);
    }

    // ── Summary ───────────────────────────────────────────────────────────────

    let s = state.lock().unwrap();
    println!();
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║                    FINAL SUMMARY                        ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  Total updates printed : {}", s.total_updates);
    println!();

    if s.total_updates > 0 {
        let total = s.total_updates as f64;
        println!("  ✅ quic-sol ahead     : {} rows  ({:.1}%)",
            s.quic_ahead_count, s.quic_ahead_count as f64 / total * 100.0);
        println!("  ⚠️  websocket ahead   : {} rows  ({:.1}%)",
            s.ws_ahead_count, s.ws_ahead_count as f64 / total * 100.0);
        println!("  🟰  Tied              : {} rows  ({:.1}%)",
            s.tied_count, s.tied_count as f64 / total * 100.0);
        println!();

        if s.quic_ahead_count > 0 {
            println!("  When quic-sol led:");
            println!("    Avg gap : {:.1} slots", s.quic_gap_sum as f64 / s.quic_ahead_count as f64);
            println!("    Max gap : {} slots", s.max_quic_gap);
        }
        if s.ws_ahead_count > 0 {
            println!("  When websocket led:");
            println!("    Avg gap : {:.1} slots", s.ws_gap_sum as f64 / s.ws_ahead_count as f64);
            println!("    Max gap : {} slots", s.max_ws_gap);
        }
        println!();

        if s.quic_ahead_count > s.ws_ahead_count {
            println!("  ✅ VERDICT: quic-sol streams ahead more often.");
        } else if s.ws_ahead_count > s.quic_ahead_count {
            println!("  ⚠️  VERDICT: WebSocket was ahead more often.");
            println!("     Deploy quic-sol closer to Solana validators (US-East)");
            println!("     to see the real gossip advantage.");
        } else {
            println!("  ➡️  VERDICT: Roughly even from this location.");
        }

        if let (Some(qs), Some(ws)) = (s.quic_latest, s.ws_latest) {
            println!();
            println!("  Final slots  →  quic-sol: {}   websocket: {}", qs, ws);
        }
    } else {
        println!("  ⚠️  No data — check both sources connected and streaming.");
    }

    println!();
    Ok(())
}