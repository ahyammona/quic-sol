//! latency-check
//!
//! The CORRECT way to compare quic-sol vs Helius WebSocket latency.
//!
//! For each slot, records the exact millisecond each source FIRST reports it.
//! When BOTH sources have reported the same slot, prints who was first and
//! by how many milliseconds. Same process = same clock = no clock skew.
//!
//! quic-sol side  : notify_only mode — SlotConfirmed fires straight from gossip,
//!                  zero RPC calls, zero getBlock bottleneck.
//! WebSocket side : slotSubscribe — one notification per slot, lightest possible.
//!
//! Output:
//!   SLOT           DELTA (quic_ms - ws_ms)  WINNER
//!   416301996                    -312ms  ✅ quic-sol faster by 312ms
//!   416301997                     +89ms  ⚠️  websocket faster by 89ms
//!
//! Usage:
//!   cargo run -p latency-check -- \
//!     --helius-key YOUR_KEY \
//!     --duration   120

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
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
#[command(name = "latency-check")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:4433")]
    quic_server: String,

    #[arg(long, default_value = "localhost")]
    quic_server_name: String,

    /// Helius API key
    #[arg(long)]
    helius_key: String,

    /// How many seconds to run
    #[arg(long, default_value_t = 120)]
    duration: u64,
}

// ── Timing ────────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── Events ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Source { QuicSol, WebSocket }

/// First-seen event for one slot from one source.
#[derive(Debug)]
struct SlotSeen {
    source:     Source,
    slot:       u64,
    seen_at_ms: u64,
}

// ── Per-slot race state ───────────────────────────────────────────────────────

#[derive(Default)]
struct SlotRace {
    first_seen: HashMap<Source, u64>,
}

// ── Stats ─────────────────────────────────────────────────────────────────────

struct RaceResult {
    slot:    u64,
    winner:  Source,
    lead_ms: u64,
}

#[derive(Default)]
struct Stats {
    quic_wins:        u64,
    ws_wins:          u64,
    draws:            u64,
    total_races:      u64,
    quic_lead_sum_ms: u64,
    ws_lead_sum_ms:   u64,
    max_quic_lead_ms: u64,
    max_ws_lead_ms:   u64,
}

impl Stats {
    fn record(&mut self, r: &RaceResult) {
        self.total_races += 1;
        if r.lead_ms == 0 {
            self.draws += 1;
        } else if r.winner == Source::QuicSol {
            self.quic_wins        += 1;
            self.quic_lead_sum_ms += r.lead_ms;
            if r.lead_ms > self.max_quic_lead_ms { self.max_quic_lead_ms = r.lead_ms; }
        } else {
            self.ws_wins        += 1;
            self.ws_lead_sum_ms += r.lead_ms;
            if r.lead_ms > self.max_ws_lead_ms { self.max_ws_lead_ms = r.lead_ms; }
        }
    }
}

// ── TLS no-verify for self-signed quic-sol cert ───────────────────────────────

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

async fn read_frame(recv: &mut quinn::RecvStream) -> Result<serde_json::Value> {
    let hdr = read_bytes(recv, 4).await?;
    let len = u32::from_be_bytes(hdr.try_into().unwrap()) as usize;
    anyhow::ensure!(len <= 4 * 1024 * 1024, "frame too large");
    let body = read_bytes(recv, len).await?;
    Ok(serde_json::from_slice(&body)?)
}

// ── quic-sol listener ─────────────────────────────────────────────────────────
//
// notify_only = true: server emits one SlotConfirmed per slot straight from
// gossip. No getBlock, no RPC. We timestamp each frame on arrival here —
// same clock as the WebSocket side.

async fn run_quic_sol(
    server:      String,
    server_name: String,
    tx:          mpsc::Sender<SlotSeen>,
) -> Result<()> {
    let handshake = serde_json::json!({
        "service": "slot",
        "api_key": null,
        "params": {
            "kind":            "slot",
            "rpc_url":         "",
            "filter_programs": [],
            "notify_only":     true
        }
    });

    let addr: SocketAddr = server.parse().context("invalid quic address")?;
    let ep   = insecure_endpoint()?;
    let conn = ep.connect(addr, &server_name)?.await.context("QUIC connect")?;
    let (mut send, mut recv) = conn.open_bi().await?;

    let body = serde_json::to_vec(&handshake)?;
    let mut framed = Vec::with_capacity(4 + body.len());
    framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
    framed.extend_from_slice(&body);
    send.write_all(&framed).await?;
    send.finish()?;

    let hdr     = read_bytes(&mut recv, 4).await?;
    let len     = u32::from_be_bytes(hdr.try_into().unwrap()) as usize;
    let ack_raw = read_bytes(&mut recv, len).await?;
    let ack: serde_json::Value = serde_json::from_slice(&ack_raw)?;
    if !ack["ok"].as_bool().unwrap_or(false) {
        anyhow::bail!("quic-sol rejected: {}", ack["message"]);
    }
    eprintln!("[quic-sol ] connected — session {}", ack["session_id"]);

    loop {
        match read_frame(&mut recv).await {
            Ok(frame) => {
                if let Some(slot) = frame["slot"].as_u64() {
                    let _ = tx.send(SlotSeen {
                        source:     Source::QuicSol,
                        slot,
                        seen_at_ms: now_ms(),
                    }).await;
                }
            }
            Err(e) => {
                eprintln!("[quic-sol ] stream ended: {e}");
                break;
            }
        }
    }
    Ok(())
}

// ── WebSocket listener ────────────────────────────────────────────────────────
//
// slotSubscribe fires exactly once per slot — directly comparable to
// quic-sol's one SlotConfirmed per slot in notify_only mode.

async fn run_websocket(helius_key: String, tx: mpsc::Sender<SlotSeen>) -> Result<()> {
    let url = format!("wss://mainnet.helius-rpc.com/?api-key={}", helius_key);

    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .context("WebSocket connect")?;

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
                    // slotNotification: params.result.slot
                    if let Some(slot) = v
                        .pointer("/params/result/slot")
                        .and_then(|s| s.as_u64())
                    {
                        let _ = tx.send(SlotSeen {
                            source:     Source::WebSocket,
                            slot,
                            seen_at_ms: now_ms(),
                        }).await;
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

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "warn".into()))
        .init();

    let cli = Cli::parse();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║   latency-check: quic-sol vs WebSocket — per-slot race       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("  quic-sol  → {} (notify_only — straight from gossip)", cli.quic_server);
    println!("  websocket → wss://mainnet.helius-rpc.com (slotSubscribe)");
    println!("  duration  → {}s", cli.duration);
    println!();
    println!("  HOW IT WORKS:");
    println!("  • Same process, same clock — no clock skew between sources");
    println!("  • Records exact ms when each source FIRST reports each slot");
    println!("  • Race completes when BOTH sources have reported the same slot");
    println!("  • Lead = ms difference between first and second arrival");
    println!();

    let (tx, mut rx) = mpsc::channel::<SlotSeen>(4096);

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
            if let Err(e) = run_websocket(key, tx).await {
                eprintln!("[websocket] fatal: {e}");
            }
        }
    });

    drop(tx);

    let races: Arc<Mutex<HashMap<u64, SlotRace>>> = Arc::new(Mutex::new(HashMap::new()));
    let stats    = Arc::new(Mutex::new(Stats::default()));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(cli.duration);

    println!("Racing… (Ctrl-C to stop early)\n");
    // DELTA = quic-sol arrival time minus websocket arrival time (milliseconds).
    // Negative = quic-sol arrived FIRST (faster by that many ms).
    // Positive = websocket arrived FIRST (faster by that many ms).
    println!("{:<14} {:>24}  {}", "SLOT", "DELTA (quic_ms - ws_ms)", "WINNER");
    println!("{}", "─".repeat(72));

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { break; }

        let seen = match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(s)) => s,
            _ => break,
        };

        let race_complete = {
            let mut map = races.lock().unwrap();
            let race    = map.entry(seen.slot).or_default();
            race.first_seen.entry(seen.source).or_insert(seen.seen_at_ms);
            race.first_seen.len() == 2
        };

        if race_complete {
            let map  = races.lock().unwrap();
            let race = map.get(&seen.slot).unwrap();

            let quic_ms = *race.first_seen.get(&Source::QuicSol).unwrap();
            let ws_ms   = *race.first_seen.get(&Source::WebSocket).unwrap();

            // Signed delta: negative = quic-sol faster, positive = websocket faster
            let delta_ms: i64 = quic_ms as i64 - ws_ms as i64;

            let result = if quic_ms <= ws_ms {
                RaceResult { slot: seen.slot, winner: Source::QuicSol,   lead_ms: ws_ms - quic_ms }
            } else {
                RaceResult { slot: seen.slot, winner: Source::WebSocket, lead_ms: quic_ms - ws_ms }
            };

            let (delta_str, winner_label) = if delta_ms == 0 {
                ("        0ms".to_string(),
                 "🟰  tied".to_string())
            } else if delta_ms < 0 {
                (format!("    {:+}ms", delta_ms),
                 format!("✅ quic-sol faster by {}ms", delta_ms.unsigned_abs()))
            } else {
                (format!("    {:+}ms", delta_ms),
                 format!("⚠️  websocket faster by {}ms", delta_ms))
            };

            println!("{:<14} {:>24}  {}", result.slot, delta_str, winner_label);

            stats.lock().unwrap().record(&result);
            drop(map);

            races.lock().unwrap().retain(|&s, _| s > seen.slot.saturating_sub(500));
        }
    }

    let s = stats.lock().unwrap();

    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                      FINAL SUMMARY                          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("  Total slots raced   : {}", s.total_races);
    println!();

    if s.total_races > 0 {
        println!("  quic-sol  first     : {} slots  ({:.1}%)",
            s.quic_wins, s.quic_wins as f64 / s.total_races as f64 * 100.0);
        println!("  websocket first     : {} slots  ({:.1}%)",
            s.ws_wins, s.ws_wins as f64 / s.total_races as f64 * 100.0);
        println!("  Draws               : {} slots  ({:.1}%)",
            s.draws, s.draws as f64 / s.total_races as f64 * 100.0);
        println!();

        if s.quic_wins > 0 {
            println!("  When quic-sol won:");
            println!("    Avg lead          : {:.1}ms", s.quic_lead_sum_ms as f64 / s.quic_wins as f64);
            println!("    Max lead          : {}ms", s.max_quic_lead_ms);
        }
        if s.ws_wins > 0 {
            println!("  When websocket won:");
            println!("    Avg lead          : {:.1}ms", s.ws_lead_sum_ms as f64 / s.ws_wins as f64);
            println!("    Max lead          : {}ms", s.max_ws_lead_ms);
        }
        println!();

        if s.quic_wins > s.ws_wins {
            let avg = s.quic_lead_sum_ms as f64 / s.quic_wins.max(1) as f64;
            println!("  ✅ VERDICT: quic-sol is FASTER");
            println!("     Won {:.1}% of slots, avg {:.0}ms ahead when winning",
                s.quic_wins as f64 / s.total_races as f64 * 100.0, avg);
        } else if s.ws_wins > s.quic_wins {
            let avg = s.ws_lead_sum_ms as f64 / s.ws_wins.max(1) as f64;
            println!("  ⚠️  VERDICT: WebSocket arrived first more often.");
            println!("     Avg {:.0}ms ahead when winning.", avg);
            println!("     Deploy quic-sol to US-East to see the real advantage.");
        } else {
            println!("  ➡️  VERDICT: Roughly equal from this location.");
            println!("     Deploy quic-sol to US-East for the true comparison.");
        }
    } else {
        println!("  ⚠️  No completed races.");
        println!("     Both sources must report the same slot for a race to complete.");
        println!("     Check quic-sol server is running and streaming.");
    }

    println!();
    Ok(())
}