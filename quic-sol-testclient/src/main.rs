//! quic-sol-testclient — connect to a running quic-sol-server and print frames.
//!
//! Usage examples:
//!
//!   # Raw log stream
//!   cargo run -p quic-sol-testclient -- logs \
//!     --rpc-url wss://mainnet.helius-rpc.com/?api-key=YOUR_KEY
//!
//!   # Gossip
//!   cargo run -p quic-sol-testclient -- gossip \
//!     --rpc-url https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
//!
//!   # Mempool — subscribe to pre-confirmation transactions
//!   cargo run -p quic-sol-testclient -- mempool
//!
//!   # Mempool filtered to Pump.fun and Jupiter only
//!   cargo run -p quic-sol-testclient -- mempool \
//!     --programs 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P,JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4 \
//!     --min-priority-fee 1000
//!
//!   # ShredStream — decoded shred frames from validator TVU ports
//!   cargo run -p quic-sol-testclient -- shred-stream
//!   cargo run -p quic-sol-testclient -- shred-stream --code-shreds --slots 320000000,320000001

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use quinn::{ClientConfig, Endpoint};
use quinn::crypto::rustls::QuicClientConfig;

// ── Inline wire protocol (mirrors quic-sol-server/src/proto.rs) ──────────────

mod proto {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ClientHandshake {
        pub service: ServiceKind,
        pub api_key: Option<String>,
        pub params: ServiceParams,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ServiceKind {
        Logs,
        Gossip,
        Price,
        Submit,
        Router,
        Mempool,
        ShredStream,
        Slot,
        Optimistic,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    pub enum ServiceParams {
        Logs {
            rpc_url: String,
            program_id: Option<String>,
            commitment: String,
            #[serde(default)] parsed: bool,
            #[serde(default)] events: bool,
        },
        Gossip  { rpc_url: String, watch_pubkeys: Vec<String>, subscribe_cluster_info: bool },
        Price   { pairs: Vec<String> },
        Submit  { rpc_url: String, strategy: String, leader_count: Option<u32> },
        Router  { rpc_url: String, max_streams: Option<u32> },
        Mempool {
            #[serde(default)] filter_programs: Vec<String>,
            #[serde(default)] min_priority_fee: u64,
        },
        ShredStream {
            #[serde(default)] include_code_shreds: bool,
            #[serde(default)] filter_slots: Vec<u64>,
        },
        Slot {
            rpc_url: String,
            #[serde(default)] filter_programs: Vec<String>,
            #[serde(default)] notify_only: bool,
        },
        Optimistic {},
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ServerAck {
        pub ok: bool,
        pub message: String,
        pub session_id: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "frame", rename_all = "snake_case")]
    pub enum ServerFrame {
        Log(LogFrame),
        Event(EventFrame),
        EnrichedTransaction(serde_json::Value),
        GossipLeaderChange   { slot: u64, leader: String, tpu_addr: Option<String>, stake: Option<u64>, version: Option<String> },
        GossipNodeDegraded   { pubkey: String, reason: String },
        GossipNodeRecovered  { pubkey: String },
        GossipNodeJoined     { pubkey: String, stake: u64, gossip_addr: String },
        GossipNodeLeft       { pubkey: String },
        GossipClusterInfo(ClusterSnapshot),
        PriceTick(PriceTick),
        SubmitAck            { signature: String },
        SubmitFailed         { reason: String },
        RpcResponse          { request_id: u64, body: serde_json::Value },
        RpcError             { request_id: u64, message: String },
        GossipTpuCongestion  { leader: String, queue_depth: u32, level: String, recommendation: String },
        GossipLeaderSchedule { upcoming: Vec<SlotLeaderEntry> },
        SlotTransaction(SlotTransaction),
        SlotConfirmed(SlotConfirmed),
        OptimisticVote(OptimisticVote),
        Heartbeat,
        Error { message: String },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SlotTransaction {
        pub slot: u64,
        pub block_time: Option<i64>,
        pub signature: String,
        pub transaction: serde_json::Value,
        pub program_ids: Vec<String>,
        pub fee: u64,
        pub compute_units_consumed: Option<u64>,
        pub priority_fee_micro_lamports: Option<u64>,
        pub log_messages: Vec<String>,
        pub received_at_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SlotConfirmed {
        pub slot: u64,
        pub block_time: Option<i64>,
        pub block_height: Option<u64>,
        pub parent_slot: u64,
        pub tx_count: u32,
        pub tx_emitted: u32,
        pub received_at_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TxEvent { pub program_id: String, pub instruction: String, pub args: Vec<String>, pub depth: u32 }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct EventFrame {
        pub slot: u64, pub signature: String, pub commitment: String,
        pub received_at_ms: u64, pub programs: Vec<String>,
        pub events: Vec<TxEvent>, pub log_count: u32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct LogFrame {
        pub slot: u64, pub signature: String, pub message: String,
        pub program_id: String, pub commitment: String,
        pub received_at_ms: u64, pub log_count: u32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ClusterSnapshot {
        pub active_validators: u32, pub total_stake: u64, pub estimated_tps: f64,
        pub epoch: u64, pub slot_index: u64, pub timestamp_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PriceTick {
        pub pair: String, pub price: f64, pub confidence: f64, pub timestamp_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SlotLeaderEntry {
        pub slot: u64, pub leader: String, pub eta_ms: u64,
        pub tpu_addr: Option<String>, pub stake: Option<u64>, pub version: Option<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct OptimisticVote {
        pub slot:           u64,
        pub slots:          Vec<u64>,
        pub validator:      String,
        pub hash:           String,
        pub root:           Option<u64>,
        pub timestamp:      Option<i64>,
        pub stake_lamports: Option<u64>,
        pub vote_count:     u32,
        pub received_at_ms: u64,
    }

    pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

    pub fn encode_frame<T: serde::Serialize>(value: &T) -> Vec<u8> {
        let body = serde_json::to_vec(value).expect("serialize");
        let mut out = Vec::with_capacity(4 + body.len());
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }
}

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "quic-sol-testclient", about = "Stream frames from a running quic-sol-server")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:4433")]
    server: String,

    #[arg(long, default_value = "localhost")]
    server_name: String,

    #[arg(long)]
    api_key: Option<String>,

    #[arg(long, default_value_t = true)]
    insecure: bool,

    #[command(subcommand)]
    service: Service,
}

#[derive(Subcommand)]
enum Service {
    /// Subscribe to Solana transaction log events (post-execution)
    Logs {
        #[arg(long)] rpc_url: String,
        #[arg(long)] program: Option<String>,
        #[arg(long, default_value = "processed")] commitment: String,
        #[arg(long, default_value_t = false)] parsed: bool,
        #[arg(long, default_value_t = false)] events: bool,
    },
    /// Subscribe to gossip / validator topology events
    Gossip {
        #[arg(long)] rpc_url: String,
        #[arg(long, value_delimiter = ',')] pubkeys: Vec<String>,
        #[arg(long, default_value_t = true)] cluster_info: bool,
    },
    /// Subscribe to price ticks
    Price {
        #[arg(long, value_delimiter = ',', default_value = "SOLUSDC")] pairs: Vec<String>,
    },
    /// Submit a signed transaction via the server's TPU relay
    Submit {
        #[arg(long)] rpc_url: String,
        #[arg(long, default_value = "multi-leader")] strategy: String,
        #[arg(long)] leader_count: Option<u32>,
    },
    /// Proxy JSON-RPC calls through the server
    Router {
        #[arg(long)] rpc_url: String,
        #[arg(long)] max_streams: Option<u32>,
    },
    /// Subscribe to pre-confirmation mempool transactions (TPU ingress)
    /// Subscribe to the fast confirmed-transaction stream (gossip-triggered getBlock).
    /// ~3-6x lower latency than logsSubscribe for the same RPC endpoint.
    Slot {
        /// HTTP RPC endpoint for getBlock calls.
        #[arg(long, default_value = "https://api.mainnet-beta.solana.com")]
        rpc_url: String,

        /// Only show transactions that invoke these program IDs (comma-separated).
        /// Leave empty to receive all transactions.
        #[arg(long, value_delimiter = ',')]
        programs: Vec<String>,
    },
    /// Subscribe to pre-confirmation mempool transactions (TPU ingress)
    Mempool {
        #[arg(long, value_delimiter = ',')] programs: Vec<String>,
        #[arg(long, default_value_t = 0)]  min_priority_fee: u64,
    },
    /// Subscribe to decoded shred frames from validator TVU ports
    ShredStream {
        #[arg(long, default_value_t = false)] code_shreds: bool,
        #[arg(long, value_delimiter = ',')] slots: Vec<u64>,
    },
    /// Test optimistic confirmation gossip — prints slots as they are confirmed
    /// by the validator network via raw UDP gossip. No RPC, no getBlock.
    /// This shows the true raw gossip speed vs websocket/gRPC.
    OptimisticTest,

    /// Stream raw UDP gossip leader-change events directly from the server's
    /// optimistic ingress (entrypoint.mainnet-beta.solana.com:8001).
    /// No RPC, no polling — every line is a real validator vote observed on
    /// the gossip mesh. Prints slot, inter-slot gap, and wall-clock time.
    UdpGossip,
}

// ── TLS: skip-verify for self-signed certs ────────────────────────────────────

#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self, _msg: &[u8], _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self, _msg: &[u8], _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

fn insecure_endpoint() -> Result<Endpoint> {
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();
    let quic_cfg = QuicClientConfig::try_from(tls).context("QuicClientConfig")?;
    let mut ep = Endpoint::client("[::]:0".parse::<SocketAddr>()?)?;
    ep.set_default_client_config(ClientConfig::new(Arc::new(quic_cfg)));
    Ok(ep)
}

// ── Framing ───────────────────────────────────────────────────────────────────

async fn read_exact(recv: &mut quinn::RecvStream, n: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    recv.read_exact(&mut buf).await.context("read_exact")?;
    Ok(buf)
}

async fn read_frame<T: for<'de> serde::Deserialize<'de>>(
    recv: &mut quinn::RecvStream,
) -> Result<T> {
    let hdr = read_exact(recv, 4).await?;
    let len = u32::from_be_bytes(hdr.try_into().unwrap()) as usize;
    anyhow::ensure!(len <= proto::MAX_FRAME_BYTES, "frame too large ({len} bytes)");
    let body = read_exact(recv, len).await?;
    serde_json::from_slice(&body).context("deserialize frame")
}

// ── Printers ──────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn sig_short(sig: &str) -> String {
    if sig.len() >= 16 { format!("{}…{}", &sig[..8], &sig[sig.len()-8..]) }
    else { sig.to_string() }
}

fn shorten_program(id: &str) -> String {
    match id {
        "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA" => "PumpAMM".into(),
        "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"  => "Pump.fun".into(),
        "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4" => "Jupiter-v6".into(),
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" => "Raydium-AMM".into(),
        "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK" => "Raydium-CLMM".into(),
        "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"  => "Raydium-CPMM".into(),
        "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"  => "Orca-Whirlpool".into(),
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"  => "Token".into(),
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"  => "Token-2022".into(),
        "11111111111111111111111111111111"               => "SystemProgram".into(),
        "ComputeBudget111111111111111111111111111111"    => "ComputeBudget".into(),
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL" => "ATA".into(),
        other => {
            if other.len() > 12 { format!("{}…{}", &other[..6], &other[other.len()-4..]) }
            else { other.to_string() }
        }
    }
}

fn print_frame(frame: &proto::ServerFrame) {
    match frame {
        proto::ServerFrame::Heartbeat => println!("  💓 heartbeat"),
        proto::ServerFrame::Error { message } => println!("  ❌ server error: {message}"),

        proto::ServerFrame::Event(ef) => {
            let latency = now_ms().saturating_sub(ef.received_at_ms);
            let progs: Vec<_> = ef.programs.iter().map(|p| shorten_program(p)).collect();
            println!("  ⚡ event  slot={}  latency={}ms  logs={}", ef.slot, latency, ef.log_count);
            println!("       sig={}", sig_short(&ef.signature));
            println!("       programs=[{}]", progs.join(", "));
            for ev in &ef.events {
                let indent = "  ".repeat((ev.depth as usize).saturating_sub(1));
                println!("       {}├─ {} ({})", indent, ev.instruction, shorten_program(&ev.program_id));
                for arg in &ev.args { println!("       {}│  {}", indent, arg); }
            }
        }

        proto::ServerFrame::Log(f) => {
            let latency = now_ms().saturating_sub(f.received_at_ms);
            println!("  📋 log  slot={}  latency={}ms", f.slot, latency);
            println!("       sig={}", sig_short(&f.signature));
            let msg = if f.message.len() > 120 { format!("{}…", &f.message[..120]) } else { f.message.clone() };
            println!("       {msg}");
        }

        proto::ServerFrame::GossipLeaderChange { slot, leader, tpu_addr, stake, version } => {
            let ver = version.as_deref().unwrap_or("?");
            let tpu = tpu_addr.as_deref().unwrap_or("?");
            let sol = stake.map(|s| format!("{:.1}k SOL", s as f64 / 1e12)).unwrap_or("?".into());
            println!("  🔀 leader  slot={slot}  [{ver}]  stake={sol}");
            println!("       identity={leader}");
            println!("       tpu={tpu}");
        }
        proto::ServerFrame::GossipNodeJoined { pubkey, stake, gossip_addr } =>
            println!("  ✅ node joined  pubkey={pubkey}  stake={stake}  addr={gossip_addr}"),
        proto::ServerFrame::GossipNodeLeft { pubkey } =>
            println!("  👋 node left  pubkey={pubkey}"),
        proto::ServerFrame::GossipNodeDegraded { pubkey, reason } =>
            println!("  ⚠️  degraded  pubkey={pubkey}  reason={reason}"),
        proto::ServerFrame::GossipNodeRecovered { pubkey } =>
            println!("  🟢 recovered  pubkey={pubkey}"),
        proto::ServerFrame::GossipClusterInfo(s) =>
            println!("  🌐 cluster  validators={}  tps={:.1}  epoch={}", s.active_validators, s.estimated_tps, s.epoch),
        proto::ServerFrame::GossipTpuCongestion { leader, queue_depth, level, recommendation } => {
            println!("  🚦 congestion  leader={leader}  depth={queue_depth}  level={level}");
            println!("       {recommendation}");
        }
        proto::ServerFrame::GossipLeaderSchedule { upcoming } => {
            println!("  📅 schedule ({} slots):", upcoming.len());
            for e in upcoming.iter().take(5) {
                let tpu = e.tpu_addr.as_deref().unwrap_or("?");
                let ver = e.version.as_deref().unwrap_or("?");
                let sol = e.stake.map(|s| format!("{:.1}k", s as f64 / 1e12)).unwrap_or("?".into());
                let id  = if e.leader.len() > 12 { format!("{}…{}", &e.leader[..8], &e.leader[e.leader.len()-4..]) } else { e.leader.clone() };
                println!("       slot={}  eta={:>6}ms  {}  tpu={}  stake={}  [{}]", e.slot, e.eta_ms, id, tpu, sol, ver);
            }
        }
        proto::ServerFrame::PriceTick(t) =>
            println!("  💲 {}  ${:.4}  conf={:.4}", t.pair, t.price, t.confidence),


        proto::ServerFrame::OptimisticVote(ov) => {
            let age_ms  = now_ms().saturating_sub(ov.received_at_ms);
            let val_short = if ov.validator.len() >= 16 {
                format!("{}…{}", &ov.validator[..8], &ov.validator[ov.validator.len()-4..])
            } else { ov.validator.clone() };
            let hash_short = if ov.hash.len() >= 12 {
                format!("{}…", &ov.hash[..12])
            } else { ov.hash.clone() };

            // Slot range display
            let slot_range = if ov.slots.len() == 1 {
                format!("{}", ov.slots[0])
            } else {
                let lo = ov.slots.iter().min().copied().unwrap_or(ov.slot);
                let hi = ov.slots.iter().max().copied().unwrap_or(ov.slot);
                format!("{lo}..{hi} ({} slots)", ov.slots.len())
            };

            // Stake display
            let stake_str = match ov.stake_lamports {
                Some(lam) => format!("{:.2}M SOL", lam as f64 / 1_000_000_000_000.0),
                None      => "?".to_string(),
            };

            // Timestamp display
            let ts_str = match ov.timestamp {
                Some(ts) => {
                    let dt = std::time::UNIX_EPOCH + std::time::Duration::from_secs(ts.max(0) as u64);
                    let elapsed = std::time::SystemTime::now()
                        .duration_since(dt)
                        .unwrap_or_default()
                        .as_secs();
                    if elapsed < 120 { format!("{}s ago", elapsed) }
                    else             { format!("ts={ts}") }
                }
                None => "no-ts".to_string(),
            };

            // Finality line
            let root_str = match ov.root {
                Some(r) => {
                    let behind = ov.slot.saturating_sub(r);
                    format!("{r} (-{behind} slots finalized)")
                }
                None => "–".to_string(),
            };

            println!("  ⚡ vote  slot={}  validators={}  age={}ms",
                ov.slot, ov.vote_count, age_ms);
            println!("       voted_slots:  {slot_range}");
            println!("       validator:    {val_short}  stake={stake_str}");
            println!("       bank_hash:    {hash_short}");
            println!("       root:         {root_str}");
            println!("       validator_ts: {ts_str}");
        }

        proto::ServerFrame::SlotTransaction(tx) => {
          //  println!("TXN {:#?}", tx);
            println!("Slot {:?}", tx.slot);
        }

        proto::ServerFrame::SlotConfirmed(sc) => {
            let age_ms = now_ms().saturating_sub(sc.received_at_ms);
            if sc.tx_count == 0 && sc.tx_emitted == 0 {
                // notify_only mode — show raw gossip speed
                println!(
                    "  ⚡ optimistic slot {} confirmed  age={}ms  (pure gossip, no RPC)",
                    sc.slot, age_ms
                );
            } else {
                println!(
                    "  📦 slot {} confirmed — {} txs total, {} emitted  age={}ms",
                    sc.slot, sc.tx_count, sc.tx_emitted, age_ms
                );
            }
        }

        other => {
            println!("  📦 {}", serde_json::to_string(other).unwrap_or_default());
        }
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let cli = Cli::parse();

    let (service_kind, params, label) = match &cli.service {
        Service::Logs { rpc_url, program, commitment, parsed, events } => (
            proto::ServiceKind::Logs,
            proto::ServiceParams::Logs {
                rpc_url: rpc_url.clone(),
                program_id: program.clone(),
                commitment: commitment.clone(),
                parsed: *parsed,
                events: *events,
            },
            format!(
                "logs(rpc={}, program={}, events={events})",
                rpc_url.split('?').next().unwrap_or(rpc_url),
                program.as_deref().unwrap_or("*")
            ),
        ),
        Service::Gossip { rpc_url, pubkeys, cluster_info } => (
            proto::ServiceKind::Gossip,
            proto::ServiceParams::Gossip {
                rpc_url: rpc_url.clone(),
                watch_pubkeys: pubkeys.clone(),
                subscribe_cluster_info: *cluster_info,
            },
            format!("gossip(rpc={})", rpc_url.split('?').next().unwrap_or(rpc_url)),
        ),
        Service::Price { pairs } => (
            proto::ServiceKind::Price,
            proto::ServiceParams::Price { pairs: pairs.clone() },
            format!("price(pairs=[{}])", pairs.join(",")),
        ),
        Service::Submit { rpc_url, strategy, leader_count } => (
            proto::ServiceKind::Submit,
            proto::ServiceParams::Submit {
                rpc_url: rpc_url.clone(),
                strategy: strategy.clone(),
                leader_count: *leader_count,
            },
            format!("submit(rpc={})", rpc_url.split('?').next().unwrap_or(rpc_url)),
        ),
        Service::Router { rpc_url, max_streams } => (
            proto::ServiceKind::Router,
            proto::ServiceParams::Router {
                rpc_url: rpc_url.clone(),
                max_streams: *max_streams,
            },
            format!("router(rpc={})", rpc_url.split('?').next().unwrap_or(rpc_url)),
        ),

        Service::Slot { rpc_url, programs } => {
            let filter: Vec<String> = programs.iter()
                .filter(|p| !p.is_empty())
                .cloned()
                .collect();
            (
                proto::ServiceKind::Slot,
                proto::ServiceParams::Slot {
                    rpc_url: rpc_url.clone(),
                    filter_programs: filter.clone(),
                    notify_only: false,
                },
                format!(
                    "slot(rpc={}, programs=[{}])",
                    rpc_url.split('?').next().unwrap_or(rpc_url),
                    if filter.is_empty() { "*".into() } else { filter.join(",") }
                ),
            )
        }

        Service::OptimisticTest => (
            proto::ServiceKind::Slot,
            proto::ServiceParams::Slot {
                rpc_url: String::new(),
                filter_programs: vec![],
                notify_only: true,
            },
            "optimistic(gossip-speed)".to_string(),
        ),

        Service::Mempool { programs, min_priority_fee } => (
            proto::ServiceKind::Mempool,
            proto::ServiceParams::Mempool {
                filter_programs: programs.clone(),
                min_priority_fee: *min_priority_fee,
            },
            format!("mempool(programs=[{}], min_fee={})", programs.join(","), min_priority_fee),
        ),

        Service::ShredStream { code_shreds, slots } => (
            proto::ServiceKind::ShredStream,
            proto::ServiceParams::ShredStream {
                include_code_shreds: *code_shreds,
                filter_slots: slots.clone(),
            },
            format!("shred-stream(code={code_shreds})"),
        ),

        Service::UdpGossip => (
            proto::ServiceKind::Gossip,
            proto::ServiceParams::Gossip {
                rpc_url: String::new(),       // ignored by server — no RPC path
                watch_pubkeys: vec![],
                subscribe_cluster_info: false,
            },
            "udp-gossip(source=entrypoint.mainnet-beta.solana.com:8001)".to_string(),
        ),
    };

    let handshake = proto::ClientHandshake {
        service: service_kind,
        api_key: cli.api_key.clone(),
        params,
    };

    let server_addr: SocketAddr = cli.server.parse()
        .with_context(|| format!("invalid server address: {}", cli.server))?;

    let endpoint = insecure_endpoint()?;

    println!("Connecting to {} ({label}) …", cli.server);

    let conn = endpoint
        .connect(server_addr, &cli.server_name)?
        .await
        .context("QUIC connect")?;

    println!("Connected. Opening stream …");

    let (mut send, mut recv) = conn.open_bi().await.context("open_bi")?;

    let hs_bytes = proto::encode_frame(&handshake);
    send.write_all(&hs_bytes).await.context("send handshake")?;
    send.finish().context("finish send")?;

    let ack: proto::ServerAck = read_frame(&mut recv).await.context("read ServerAck")?;
    if !ack.ok {
        anyhow::bail!("server rejected handshake: {}", ack.message);
    }
    println!("✓ session {} — {}\n", ack.session_id, ack.message);

    // UdpGossip uses its own focused loop — slot + timing only, no RPC.
    if matches!(cli.service, Service::UdpGossip) {
        return run_udp_gossip_loop(&mut recv).await;
    }

    let mut count = 0u64;
    let mut seen_sigs: std::collections::HashSet<String> = std::collections::HashSet::new();

    loop {
        match read_frame::<proto::ServerFrame>(&mut recv).await {
            Ok(frame) => {
                // Dedup log frames by signature client-side
                if let proto::ServerFrame::Log(ref f) = frame {
                    if seen_sigs.contains(&f.signature) { continue; }
                    seen_sigs.insert(f.signature.clone());
                    if seen_sigs.len() > 2000 { seen_sigs.clear(); }
                }
                count += 1;
                print!("[#{count}] ");
                print_frame(&frame);
            }
            Err(e) => {
                eprintln!("\nStream ended after {count} frames: {e}");
                break;
            }
        }
    }

    Ok(())
}
// ── UDP gossip dedicated receive loop ────────────────────────────────────────

async fn run_udp_gossip_loop(recv: &mut quinn::RecvStream) -> Result<()> {
    use std::time::{SystemTime, UNIX_EPOCH};

    fn wall_clock() -> String {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let h = (secs % 86400) / 3600;
        let m = (secs % 3600) / 60;
        let s = secs % 60;
        format!("{h:02}:{m:02}:{s:02} UTC")
    }

    println!("source  : entrypoint.mainnet-beta.solana.com:8001 (raw UDP bincode)");
    println!("path    : validator vote → optimistic.rs → gossip bus → QUIC → here");
    println!("rpc     : none");
    println!("{}", "─".repeat(72));
    println!("{:<12}  {:<12}  {:<10}  {}", "SLOT", "GAP", "TIME", "SOURCE");
    println!("{}", "─".repeat(72));

    let mut slot_count: u64 = 0;
    let mut last_slot: Option<u64> = None;

    loop {
        let frame = match read_frame::<proto::ServerFrame>(recv).await {
            Ok(f) => f,
            Err(e) => {
                eprintln!("\nStream ended after {slot_count} leader-change events: {e}");
                break;
            }
        };

        match frame {
            proto::ServerFrame::GossipLeaderChange { slot, .. } => {
                slot_count += 1;

                let gap = match last_slot {
                    Some(prev) => {
                        let diff = slot.saturating_sub(prev);
                        if diff == 0 {
                            // duplicate or same slot — skip
                            continue;
                        }
                        format!("+{diff} slots")
                    }
                    None => "first".to_string(),
                };

                let time = wall_clock();
                println!(
                    "{:<12}  {:<12}  {:<10}  udp-gossip/optimistic",
                    slot, gap, time
                );

                last_slot = Some(slot);
            }

            proto::ServerFrame::Heartbeat => {
                println!("── heartbeat ── ({slot_count} slots seen so far)");
            }

            // Ignore everything else (cluster info, schedule, etc.)
            _ => {}
        }
    }

    Ok(())
}