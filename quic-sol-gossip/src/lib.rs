//! quic-sol-gossip — Solana Gossip Network Client over QUIC
//!
//! Connects directly to Solana's gossip network and delivers real-time
//! cluster events — leader changes, validator health, cluster-state
//! updates, and NEW: leader-aware scheduling + TPU congestion signals.
//!
//! # Quick start
//! ```no_run
//! use quic_sol_gossip::{GossipClient, GossipEvent};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mut gossip = GossipClient::connect("entrypoint.mainnet-beta.solana.com:8001").await?;
//!     while let Some(event) = gossip.next().await {
//!         match event {
//!             GossipEvent::LeaderChange { slot, leader, .. } => {
//!                 println!("Slot {} leader: {}", slot, leader);
//!             }
//!             GossipEvent::TpuCongestion { leader, queue_depth, recommendation, .. } => {
//!                 println!("TPU {} congestion: {} txns queued — {}", leader, queue_depth, recommendation);
//!             }
//!             GossipEvent::LeaderSchedule { upcoming } => {
//!                 println!("Next {} leaders: {:?}", upcoming.len(), upcoming);
//!             }
//!             _ => {}
//!         }
//!     }
//!     Ok(())
//! }
//! ```

use quic_sol_shared::{quic::create_quic_endpoint, QuicSolError};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A scheduled leader slot — used in LeaderSchedule lookahead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotLeader {
    pub slot: u64,
    pub leader: String,
    /// Estimated time until this slot starts (milliseconds).
    pub eta_ms: u64,
}

/// TPU congestion level.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CongestionLevel {
    Low,
    Moderate,
    High,
    Critical,
}

impl std::fmt::Display for CongestionLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CongestionLevel::Low => write!(f, "low"),
            CongestionLevel::Moderate => write!(f, "moderate"),
            CongestionLevel::High => write!(f, "high"),
            CongestionLevel::Critical => write!(f, "critical"),
        }
    }
}

/// A real-time event sourced from the Solana gossip network.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GossipEvent {
    /// The scheduled leader for a slot has been determined or changed.
    LeaderChange {
        slot: u64,
        leader: String,
        /// How many slots until this leader's turn (0 = current).
        slots_until: u64,
    },

    /// 🆕 Lookahead: upcoming leader schedule for the next N slots.
    /// Use this to pre-route transactions to the right TPU in advance.
    LeaderSchedule {
        upcoming: Vec<SlotLeader>,
    },

    /// 🆕 TPU congestion oracle — pre-block intelligence feed.
    /// Signals queue pressure at a validator's TPU port so you can
    /// time submissions into low-congestion windows.
    TpuCongestion {
        /// Validator pubkey whose TPU is being measured.
        leader: String,
        /// Estimated pending transaction count in the TPU queue.
        queue_depth: u32,
        /// Congestion classification.
        level: CongestionLevel,
        /// Human-readable action recommendation.
        recommendation: String,
    },

    /// A validator has been observed to be unresponsive or degraded.
    NodeDegraded {
        pubkey: String,
        reason: String,
    },

    /// A previously degraded validator has returned to normal operation.
    NodeRecovered {
        pubkey: String,
    },

    /// A new validator has joined the cluster.
    NodeJoined {
        pubkey: String,
        stake: u64,
        gossip_addr: String,
    },

    /// A validator has left or been removed from the active set.
    NodeLeft {
        pubkey: String,
    },

    /// Periodic cluster-health snapshot pushed by the server.
    ClusterInfo(ClusterSnapshot),

    /// Server-initiated heartbeat to keep the connection alive.
    Heartbeat,
}

/// A point-in-time snapshot of cluster health.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    pub active_validators: u32,
    pub total_stake: u64,
    pub estimated_tps: f64,
    pub epoch: u64,
    pub slot_index: u64,
}

// ---------------------------------------------------------------------------
// Wire protocol (internal)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct HandshakeRequest {
    watch_pubkeys: Vec<String>,
    subscribe_cluster_info: bool,
    /// 🆕 Subscribe to leader schedule lookahead events.
    subscribe_leader_schedule: bool,
    /// 🆕 Subscribe to TPU congestion oracle events.
    subscribe_tpu_congestion: bool,
    heartbeat_interval_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
enum ServerFrame {
    Event(GossipEvent),
    Error { message: String },
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

pub struct GossipClientBuilder {
    addr: Option<String>,
    watch_pubkeys: Vec<String>,
    subscribe_cluster_info: bool,
    subscribe_leader_schedule: bool,
    subscribe_tpu_congestion: bool,
    channel_capacity: usize,
    reconnect_delay: Duration,
    max_reconnects: Option<u32>,
    heartbeat_interval: Duration,
}

impl Default for GossipClientBuilder {
    fn default() -> Self {
        GossipClientBuilder {
            addr: None,
            watch_pubkeys: Vec::new(),
            subscribe_cluster_info: true,
            subscribe_leader_schedule: true,
            subscribe_tpu_congestion: true,
            channel_capacity: 1024,
            reconnect_delay: Duration::from_secs(3),
            max_reconnects: None,
            heartbeat_interval: Duration::from_secs(30),
        }
    }
}

impl GossipClientBuilder {
    pub fn new() -> Self { Self::default() }

    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        self.addr = Some(addr.into()); self
    }

    pub fn watch_pubkeys(mut self, pubkeys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.watch_pubkeys = pubkeys.into_iter().map(Into::into).collect(); self
    }

    pub fn subscribe_cluster_info(mut self, yes: bool) -> Self {
        self.subscribe_cluster_info = yes; self
    }

    /// Enable/disable leader schedule lookahead. Default: enabled.
    pub fn subscribe_leader_schedule(mut self, yes: bool) -> Self {
        self.subscribe_leader_schedule = yes; self
    }

    /// Enable/disable TPU congestion oracle. Default: enabled.
    pub fn subscribe_tpu_congestion(mut self, yes: bool) -> Self {
        self.subscribe_tpu_congestion = yes; self
    }

    pub fn channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap; self
    }

    pub fn reconnect_delay(mut self, delay: Duration) -> Self {
        self.reconnect_delay = delay; self
    }

    pub fn max_reconnects(mut self, n: u32) -> Self {
        self.max_reconnects = Some(n); self
    }

    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval; self
    }

    fn parse_addr(&self) -> Result<(String, SocketAddr), QuicSolError> {
        let raw = self.addr.as_deref().ok_or(QuicSolError::InvalidAddress)?;
        let stripped = raw
            .strip_prefix("https://")
            .or_else(|| raw.strip_prefix("http://"))
            .or_else(|| raw.strip_prefix("quic://"))
            .unwrap_or(raw);

        let (host, port) = if let Some(colon) = stripped.rfind(':') {
            let port: u16 = stripped[colon + 1..]
                .parse()
                .map_err(|_| QuicSolError::InvalidAddress)?;
            (&stripped[..colon], port)
        } else {
            (stripped, 8001u16)
        };

        use std::net::ToSocketAddrs;
        let addr = format!("{}:{}", host, port)
            .to_socket_addrs()
            .map_err(QuicSolError::Connection)?
            .next()
            .ok_or(QuicSolError::InvalidAddress)?;

        Ok((host.to_string(), addr))
    }

    pub async fn build(self) -> Result<GossipClient, QuicSolError> {
        let (host, addr) = self.parse_addr()?;
        let handshake = HandshakeRequest {
            watch_pubkeys: self.watch_pubkeys.clone(),
            subscribe_cluster_info: self.subscribe_cluster_info,
            subscribe_leader_schedule: self.subscribe_leader_schedule,
            subscribe_tpu_congestion: self.subscribe_tpu_congestion,
            heartbeat_interval_secs: self.heartbeat_interval.as_secs(),
        };

        let (tx, rx) = mpsc::channel::<GossipEvent>(self.channel_capacity);
        let reconnect_delay = self.reconnect_delay;
        let max_reconnects = self.max_reconnects;

        tokio::spawn(async move {
            let mut attempt: u32 = 0;
            loop {
                if let Some(max) = max_reconnects {
                    if attempt >= max {
                        error!("quic-sol-gossip: exceeded max reconnects ({}), stopping", max);
                        break;
                    }
                }
                if attempt > 0 {
                    warn!("quic-sol-gossip: reconnect attempt {} in {:?}", attempt, reconnect_delay);
                    tokio::time::sleep(reconnect_delay).await;
                }
                match run_gossip_connection(&host, addr, &handshake, &tx).await {
                    Ok(()) => { info!("quic-sol-gossip: server closed stream cleanly"); break; }
                    Err(e) => { warn!("quic-sol-gossip: connection error: {}", e); }
                }
                attempt += 1;
                if tx.is_closed() {
                    debug!("quic-sol-gossip: receiver dropped, exiting");
                    break;
                }
            }
        });

        Ok(GossipClient { receiver: rx })
    }
}

async fn run_gossip_connection(
    host: &str,
    addr: SocketAddr,
    handshake: &HandshakeRequest,
    tx: &mpsc::Sender<GossipEvent>,
) -> Result<(), QuicSolError> {
    let endpoint = create_quic_endpoint().await.map_err(|e| {
        QuicSolError::Quic(format!("failed to create QUIC endpoint: {}", e))
    })?;

    info!("quic-sol-gossip: connecting to {} ({})", host, addr);
    let conn = endpoint
        .connect(addr, host)
        .map_err(|e| QuicSolError::Quic(e.to_string()))?
        .await
        .map_err(|e| QuicSolError::Quic(e.to_string()))?;

    info!("quic-sol-gossip: QUIC connection established");
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| QuicSolError::Quic(e.to_string()))?;

    // Send handshake as length-prefixed JSON.
    let hs_bytes = serde_json::to_vec(handshake).map_err(QuicSolError::Serde)?;
    let len = (hs_bytes.len() as u32).to_be_bytes();
    send.write_all(&len).await.map_err(|e| QuicSolError::Quic(e.to_string()))?;
    send.write_all(&hs_bytes).await.map_err(|e| QuicSolError::Quic(e.to_string()))?;
    send.finish().await.map_err(|e| QuicSolError::Quic(e.to_string()))?;

    info!("quic-sol-gossip: handshake sent, streaming events…");

    loop {
        // Read 4-byte length prefix — quinn 0.11 uses ReadExactError differently
        let mut len_buf = [0u8; 4];
        match recv.read_exact(&mut len_buf).await {
            Ok(()) => {}
            Err(e) => {
                let s = e.to_string();
                if s.contains("Finished") || s.contains("reset") {
                    debug!("quic-sol-gossip: server closed stream");
                    return Ok(());
                }
                return Err(QuicSolError::Quic(format!("read length prefix: {}", e)));
            }
        }

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > 1 << 20 {
            return Err(QuicSolError::Quic(format!("oversized gossip frame: {} bytes", msg_len)));
        }

        let mut buf = vec![0u8; msg_len];
        recv.read_exact(&mut buf)
            .await
            .map_err(|e| QuicSolError::Quic(format!("read frame body: {}", e)))?;

        let frame: ServerFrame = serde_json::from_slice(&buf).map_err(QuicSolError::Serde)?;

        match frame {
            ServerFrame::Event(GossipEvent::Heartbeat) => {
                debug!("quic-sol-gossip: heartbeat");
            }
            ServerFrame::Event(event) => {
                if tx.send(event).await.is_err() {
                    debug!("quic-sol-gossip: consumer gone, closing connection");
                    conn.close(0u32.into(), b"consumer gone");
                    return Ok(());
                }
            }
            ServerFrame::Error { message } => {
                warn!("quic-sol-gossip: server error: {}", message);
                return Err(QuicSolError::Quic(message));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// GossipClient
// ---------------------------------------------------------------------------

pub struct GossipClient {
    receiver: mpsc::Receiver<GossipEvent>,
}

impl GossipClient {
    pub async fn connect(addr: &str) -> Result<Self, QuicSolError> {
        GossipClientBuilder::new().addr(addr).build().await
    }

    pub fn builder() -> GossipClientBuilder {
        GossipClientBuilder::new()
    }

    pub async fn next(&mut self) -> Option<GossipEvent> {
        self.receiver.recv().await
    }

    pub fn try_next(&mut self) -> Option<GossipEvent> {
        self.receiver.try_recv().ok()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_change_serde() {
        let ev = GossipEvent::LeaderChange { slot: 42, leader: "ABC123".into(), slots_until: 0 };
        let json = serde_json::to_string(&ev).unwrap();
        let back: GossipEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, GossipEvent::LeaderChange { slot: 42, .. }));
    }

    #[test]
    fn leader_schedule_serde() {
        let ev = GossipEvent::LeaderSchedule {
            upcoming: vec![
                SlotLeader { slot: 100, leader: "ABC".into(), eta_ms: 400 },
                SlotLeader { slot: 104, leader: "DEF".into(), eta_ms: 2000 },
            ],
        };
        let json = serde_json::to_string(&ev).unwrap();
        let back: GossipEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, GossipEvent::LeaderSchedule { .. }));
    }

    #[test]
    fn tpu_congestion_serde() {
        let ev = GossipEvent::TpuCongestion {
            leader: "PUBKEY123".into(),
            queue_depth: 850,
            level: CongestionLevel::High,
            recommendation: "Delay 200ms or target next leader".into(),
        };
        let json = serde_json::to_string(&ev).unwrap();
        let back: GossipEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, GossipEvent::TpuCongestion { .. }));
    }

    #[test]
    fn node_degraded_serde() {
        let ev = GossipEvent::NodeDegraded { pubkey: "PUBKEY".into(), reason: "timeout".into() };
        let json = serde_json::to_string(&ev).unwrap();
        let back: GossipEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, GossipEvent::NodeDegraded { .. }));
    }

    #[test]
    fn cluster_snapshot_serde() {
        let snap = ClusterSnapshot {
            active_validators: 1800,
            total_stake: 400_000_000_000_000_000,
            estimated_tps: 2500.0,
            epoch: 600,
            slot_index: 1234,
        };
        let ev = GossipEvent::ClusterInfo(snap);
        let json = serde_json::to_string(&ev).unwrap();
        let back: GossipEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, GossipEvent::ClusterInfo(_)));
    }

    #[test]
    fn congestion_level_display() {
        assert_eq!(CongestionLevel::High.to_string(), "high");
        assert_eq!(CongestionLevel::Critical.to_string(), "critical");
    }

    #[test]
    fn builder_defaults() {
        let b = GossipClientBuilder::new();
        assert!(b.addr.is_none());
        assert!(b.subscribe_cluster_info);
        assert!(b.subscribe_leader_schedule);
        assert!(b.subscribe_tpu_congestion);
        assert_eq!(b.channel_capacity, 1024);
    }

    #[tokio::test]
    async fn missing_addr_returns_error() {
        let result = GossipClient::builder().build().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn connect_shorthand_bad_addr_returns_error() {
        let result = GossipClient::connect("not_a_valid_addr").await;
        assert!(result.is_err());
    }
}
