//! grpc-benchmark
//!
//! Races confirmed slot arrival between:
//!   • slotIngress — your quic-sol gossip-triggered slot stream
//!   • gRPC        — Yellowstone Geyser slot subscription (Shyft, Helius, etc.)
//!
//! No signature matching. No shared clock. Both stream independently.
//! We watch the latest slot each source reports and print the GAP in slot
//! numbers every time either side advances — same format as slot-benchmark.
//!
//! Usage:
//!   cargo run -p grpc-benchmark -- \
//!     --endpoint    https://grpc.ny.shyft.to \
//!     --x-token     YOUR_TOKEN \
//!     --quic-server 127.0.0.1:4433 \
//!
//!   (--rpc-url is optional — notify_only mode skips getBlock entirely)

use std::{net::SocketAddr, sync::{Arc, Mutex}, time::Duration};

use anyhow::{Context, Result};
use backoff::{future::retry, ExponentialBackoff};
use clap::Parser;
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::{GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof,
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterSlots, SubscribeRequestPing,
};

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Clone)]
#[command(name = "grpc-benchmark")]
struct Cli {
    /// gRPC Geyser endpoint e.g. https://grpc.ny.shyft.to
    #[arg(long)]
    endpoint: String,

    /// x-token auth for the gRPC endpoint
    #[arg(long)]
    x_token: String,

    /// quic-sol server address
    #[arg(long, default_value = "127.0.0.1:4433")]
    quic_server: String,

    /// TLS server name for quic-sol self-signed cert
    #[arg(long, default_value = "localhost")]
    quic_server_name: String,

    /// HTTP RPC URL — not used in notify_only mode, can be omitted
    #[arg(long, default_value = "")]
    rpc_url: String,

    /// How long to run in seconds
    #[arg(long, default_value_t = 120)]
    duration: u64,
}

impl Cli {
    async fn connect_grpc(&self) -> Result<GeyserGrpcClient<impl Interceptor>> {
        GeyserGrpcClient::build_from_shared(self.endpoint.clone())?
            .x_token(Some(self.x_token.clone()))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10))
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .max_decoding_message_size(1024 * 1024 * 1024)
            .connect()
            .await
            .map_err(Into::into)
    }

    fn slot_subscribe_request(&self) -> Result<SubscribeRequest> {
        let mut slots = std::collections::HashMap::new();
        slots.insert(
            "bench".to_string(),
            SubscribeRequestFilterSlots { filter_by_commitment: Some(true) },
        );
        Ok(SubscribeRequest {
            slots,
            commitment: Some(CommitmentLevel::Confirmed as i32),
            ..Default::default()
        })
    }
}

// ── Sources ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source { SlotIngress, Grpc }

#[derive(Debug)]
struct Update { source: Source, slot: u64 }

// ── Stats ─────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Stats {
    ingress_latest:  u64,
    grpc_latest:     u64,
    ingress_ahead:   u64,
    grpc_ahead:      u64,
    tied:            u64,
    comparisons:     u64,
    gap_sum:         i64,
    gap_max_ingress: i64,
    gap_max_grpc:    i64,
}

impl Stats {
    fn record(&mut self, u: &Update) -> Option<i64> {
        match u.source {
            Source::SlotIngress => self.ingress_latest = u.slot,
            Source::Grpc        => self.grpc_latest    = u.slot,
        }
        if self.ingress_latest == 0 || self.grpc_latest == 0 { return None; }

        // positive = slotIngress ahead, negative = grpc ahead
        let gap = self.ingress_latest as i64 - self.grpc_latest as i64;
        self.gap_sum     += gap;
        self.comparisons += 1;
        match gap.cmp(&0) {
            std::cmp::Ordering::Greater => {
                self.ingress_ahead += 1;
                if gap > self.gap_max_ingress { self.gap_max_ingress = gap; }
            }
            std::cmp::Ordering::Less => {
                self.grpc_ahead += 1;
                if gap < self.gap_max_grpc { self.gap_max_grpc = gap; }
            }
            std::cmp::Ordering::Equal => { self.tied += 1; }
        }
        Some(gap)
    }
}

// ── gRPC listener ─────────────────────────────────────────────────────────────

async fn run_grpc(cli: Cli, tx: mpsc::Sender<Update>) {
    let result: Result<()> = retry(ExponentialBackoff::default(), || {
        let cli = cli.clone();
        let tx  = tx.clone();
        async move {
            let client = cli.connect_grpc().await
                .map_err(|e| backoff::Error::transient(anyhow::anyhow!(e)))?;
            eprintln!("[grpc] connected");

            let request = cli.slot_subscribe_request()
                .map_err(backoff::Error::Permanent)?;

            grpc_stream(client, request, tx).await
                .map_err(|e| backoff::Error::transient(anyhow::anyhow!(e)))?;

            Ok::<(), backoff::Error<anyhow::Error>>(())
        }
    })
    .await
    .map_err(Into::into);

    if let Err(e) = result { eprintln!("[grpc] fatal: {e:#}"); }
}

async fn grpc_stream(
    mut client: GeyserGrpcClient<impl Interceptor>,
    request:    SubscribeRequest,
    tx:         mpsc::Sender<Update>,
) -> Result<()> {
    let (mut sub_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;
    eprintln!("[grpc] stream opened — waiting for confirmed slots");

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => match msg.update_oneof {
                Some(UpdateOneof::Slot(su)) => {
                    if su.status == CommitmentLevel::Confirmed as i32 {
                        let _ = tx.send(Update { source: Source::Grpc, slot: su.slot }).await;
                    }
                }
                Some(UpdateOneof::Ping(_)) => {
                    sub_tx.send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    }).await?;
                }
                None => { eprintln!("[grpc] empty update — closing"); break; }
                _ => {}
            },
            Err(e) => { eprintln!("[grpc] stream error: {e}"); break; }
        }
    }
    Ok(())
}

// ── quic-sol / slotIngress listener ──────────────────────────────────────────

mod quic_client {
    use anyhow::Result;
    use quinn::{ClientConfig, Endpoint};
    use quinn::crypto::rustls::QuicClientConfig;
    use std::{net::SocketAddr, sync::Arc};

    #[derive(Debug)]
    struct NoVerifier;
    impl rustls::client::danger::ServerCertVerifier for NoVerifier {
        fn verify_server_cert(&self,_:&rustls::pki_types::CertificateDer,_:&[rustls::pki_types::CertificateDer],_:&rustls::pki_types::ServerName,_:&[u8],_:rustls::pki_types::UnixTime)->Result<rustls::client::danger::ServerCertVerified,rustls::Error>{Ok(rustls::client::danger::ServerCertVerified::assertion())}
        fn verify_tls12_signature(&self,_:&[u8],_:&rustls::pki_types::CertificateDer,_:&rustls::DigitallySignedStruct)->Result<rustls::client::danger::HandshakeSignatureValid,rustls::Error>{Ok(rustls::client::danger::HandshakeSignatureValid::assertion())}
        fn verify_tls13_signature(&self,_:&[u8],_:&rustls::pki_types::CertificateDer,_:&rustls::DigitallySignedStruct)->Result<rustls::client::danger::HandshakeSignatureValid,rustls::Error>{Ok(rustls::client::danger::HandshakeSignatureValid::assertion())}
        fn supported_verify_schemes(&self)->Vec<rustls::SignatureScheme>{vec![rustls::SignatureScheme::ECDSA_NISTP256_SHA256,rustls::SignatureScheme::ECDSA_NISTP384_SHA384,rustls::SignatureScheme::ED25519,rustls::SignatureScheme::RSA_PKCS1_SHA256,rustls::SignatureScheme::RSA_PKCS1_SHA384,rustls::SignatureScheme::RSA_PKCS1_SHA512]}
    }

    pub fn insecure_endpoint() -> Result<Endpoint> {
        let tls = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth();
        let quic = QuicClientConfig::try_from(tls)?;
        let mut ep = Endpoint::client("[::]:0".parse::<SocketAddr>()?)?;
        ep.set_default_client_config(ClientConfig::new(Arc::new(quic)));
        Ok(ep)
    }

    pub async fn read_bytes(recv: &mut quinn::RecvStream, n: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; n];
        recv.read_exact(&mut buf).await?;
        Ok(buf)
    }

    pub async fn read_frame(recv: &mut quinn::RecvStream) -> Result<serde_json::Value> {
        let hdr = read_bytes(recv, 4).await?;
        let len = u32::from_be_bytes(hdr.try_into().unwrap()) as usize;
        anyhow::ensure!(len <= 4 * 1024 * 1024, "frame too large");
        let body = read_bytes(recv, len).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub fn encode(value: &serde_json::Value) -> Vec<u8> {
        let body = serde_json::to_vec(value).unwrap();
        let mut out = Vec::with_capacity(4 + body.len());
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }
}

async fn run_quic_sol(
    server:      String,
    server_name: String,
    rpc_url:     String,
    tx:          mpsc::Sender<Update>,
) {
    use quic_client::*;
    use serde_json::json;

    let result: Result<()> = async {
        let handshake = json!({
            "service": "slot", "api_key": null,
            "params": { "kind": "slot", "rpc_url": rpc_url, "filter_programs": [], }
        });

        let addr: SocketAddr = server.parse().context("invalid quic address")?;
        let ep = insecure_endpoint()?;
        let conn: quinn::Connection = tokio::time::timeout(
            Duration::from_secs(5),
            ep.connect(addr, &server_name).context("QUIC endpoint setup")?,
        )
        .await
        .map_err(|_| anyhow::anyhow!("connect timed out"))?
        .context("QUIC connect")?;

        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&encode(&handshake)).await?;
        send.finish()?;

        let hdr  = read_bytes(&mut recv, 4).await?;
        let len  = u32::from_be_bytes(hdr.try_into().unwrap()) as usize;
        let body = read_bytes(&mut recv, len).await?;
        let ack: serde_json::Value = serde_json::from_slice(&body)?;
        if !ack["ok"].as_bool().unwrap_or(false) {
            anyhow::bail!("quic-sol rejected: {}", ack["message"]);
        }
        eprintln!("[quic-sol] connected — session {}", ack["session_id"]);

        loop {
            match read_frame(&mut recv).await {
                Ok(frame) => {
                    if let Some(slot) = frame.get("slot").and_then(|s| s.as_u64()) {
                        let _ = tx.send(Update { source: Source::SlotIngress, slot }).await;
                    }
                }
                Err(e) => { eprintln!("[quic-sol] stream ended: {e}"); break; }
            }
        }
        Ok(())
    }.await;

    if let Err(e) = result { eprintln!("[quic-sol] fatal: {e}"); }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "warn".into()))
        .init();

    let cli = Cli::parse();

    let endpoint_host = cli.endpoint
        .split("://").nth(1)
        .and_then(|h| h.split('/').next())
        .unwrap_or(&cli.endpoint);

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║     grpc-benchmark: quic-sol vs gRPC — slot gap view     ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  slotIngress → {}", cli.quic_server);
    println!("  grpc        → {endpoint_host}");
    println!("  mode        → notify_only (gossip slot, no getBlock)");
    println!("  duration    → {}s", cli.duration);
    println!();

    let (update_tx, mut update_rx) = mpsc::channel::<Update>(4096);

    tokio::spawn({
        let cli2 = cli.clone();
        let tx   = update_tx.clone();
        async move { run_grpc(cli2, tx).await; }
    });

    tokio::spawn({
        let tx  = update_tx.clone();
        let s   = cli.quic_server.clone();
        let sn  = cli.quic_server_name.clone();
        let ru  = cli.rpc_url.clone();
        async move { run_quic_sol(s, sn, ru, tx).await; }
    });

    drop(update_tx);

    let stats    = Arc::new(Mutex::new(Stats::default()));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(cli.duration);

    let mut last_printed_ingress: u64 = 0;
    let mut last_printed_grpc:    u64 = 0;

    println!("Streaming… (Ctrl-C to stop early)\n");
    println!(
        "{:<14}  {:<24}  {:<24}  {}",
        "WINNER", "[slotIngress] slot", "[grpc] slot", "GAP"
    );
    println!("{}", "─".repeat(80));

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { break; }

        let update = match tokio::time::timeout(remaining, update_rx.recv()).await {
            Ok(Some(u)) => u,
            _           => break,
        };

        let slot_changed = match update.source {
            Source::SlotIngress => update.slot > last_printed_ingress,
            Source::Grpc        => update.slot > last_printed_grpc,
        };
        if !slot_changed { continue; }

        match update.source {
            Source::SlotIngress => last_printed_ingress = update.slot,
            Source::Grpc        => last_printed_grpc    = update.slot,
        }

        let gap = {
            let mut s = stats.lock().unwrap();
            s.record(&update)
        };

        let s = stats.lock().unwrap();
        if let Some(gap) = gap {
            let (winner, gap_str) = if gap > 0 {
                ("✅ slotIngress",  format!("+{} slots (~{}ms)", gap, gap * 400))
            } else if gap < 0 {
                ("⚠️  grpc",        format!("+{} slots (~{}ms)", gap.unsigned_abs(), gap.unsigned_abs() * 400))
            } else {
                ("🟰  DRAW",       "tied".to_string())
            };

            println!(
                "{:<14}  {:<24}  {:<24}  {}",
                winner,
                format!("[slotIngress]  {}", s.ingress_latest),
                format!("[grpc] {}", s.grpc_latest),
                gap_str
            );
        }
    }

    let s = stats.lock().unwrap();
    println!();
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║                    FINAL SUMMARY                        ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  slotIngress latest slot : {}", s.ingress_latest);
    println!("  grpc latest slot        : {}", s.grpc_latest);
    println!("  Total comparisons       : {}", s.comparisons);
    println!();

    if s.comparisons > 0 {
        let avg = s.gap_sum as f64 / s.comparisons as f64;
        println!("  ✅ slotIngress ahead : {} times ({:.1}%)", s.ingress_ahead, s.ingress_ahead as f64 / s.comparisons as f64 * 100.0);
        println!("  ⚠️  grpc ahead       : {} times ({:.1}%)", s.grpc_ahead,    s.grpc_ahead    as f64 / s.comparisons as f64 * 100.0);
        println!("  🟰  Draws            : {} times ({:.1}%)", s.tied,          s.tied          as f64 / s.comparisons as f64 * 100.0);
        println!();
        println!("  Avg gap              : {:.2} slots  (+ = slotIngress ahead)", avg);
        println!("  Max slotIngress lead : {} slots (~{}ms)", s.gap_max_ingress, s.gap_max_ingress * 400);
        println!("  Max grpc lead        : {} slots (~{}ms)", s.gap_max_grpc.unsigned_abs(), s.gap_max_grpc.unsigned_abs() * 400);
        println!();

        if avg > 0.5 {
            println!("  ✅ VERDICT: slotIngress is ahead of gRPC");
            println!("     Avg {:.2} slots ahead (~{:.0}ms faster)", avg, avg * 400.0);
        } else if avg < -0.5 {
            println!("  ⚠️  VERDICT: gRPC is ahead of slotIngress");
            println!("     Avg {:.2} slots behind (~{:.0}ms slower)", avg.abs(), avg.abs() * 400.0);
        } else {
            println!("  ➡️  VERDICT: roughly equal — within 1 slot on average");
        }
    } else {
        println!("  ⚠️  No comparisons made — check both sources connected.");
    }

    println!();
    Ok(())
}