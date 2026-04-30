//! Dynamic peer registry — shared between the gossip bus and the
//! ShredStream / TPU ingressses so that validator addresses discovered
//! via gossip are automatically used without hardcoding.
//!
//! Usage:
//!   1. Create one `PeerRegistry` in `main`.
//!   2. Pass `Arc<PeerRegistry>` to `GossipPeerUpdater::new(gossip_rx, registry)`.
//!   3. Pass the same `Arc<PeerRegistry>` to `ShredStreamIngress` and `TpuIngress`.
//!   4. Both ingressses call `registry.tvu_addrs()` / `registry.tpu_addrs()` on
//!      each receive loop iteration to get the current live set.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info};

use crate::proto::ServerFrame;

// ── Registry ──────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct PeerRegistry {
    /// Validator TVU addresses — shredstream receives shreds from these.
    tvu: RwLock<HashSet<SocketAddr>>,
    /// Validator TPU addresses — mempool: validators forward txns to leaders.
    /// We track these so we know which IPs to *expect* traffic from.
    tpu: RwLock<HashSet<SocketAddr>>,
}

impl PeerRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(PeerRegistry::default())
    }

    /// Seed with static addresses from config (called once at startup).
    pub async fn seed_tvu(&self, addrs: &[String]) {
        let mut set = self.tvu.write().await;
        for a in addrs {
            if let Ok(addr) = a.parse::<SocketAddr>() {
                set.insert(addr);
            }
        }
    }

    /// Add a TVU address discovered via gossip.
    pub async fn add_tvu(&self, addr: SocketAddr) -> bool {
        self.tvu.write().await.insert(addr)
    }

    /// Add a TPU address discovered via gossip.
    pub async fn add_tpu(&self, addr: SocketAddr) -> bool {
        self.tpu.write().await.insert(addr)
    }

    /// Snapshot of current TVU peers.
    pub async fn tvu_addrs(&self) -> HashSet<SocketAddr> {
        self.tvu.read().await.clone()
    }

    /// Snapshot of current TPU peers.
    pub async fn tpu_addrs(&self) -> HashSet<SocketAddr> {
        self.tpu.read().await.clone()
    }

    pub async fn tvu_count(&self) -> usize { self.tvu.read().await.len() }
    pub async fn tpu_count(&self) -> usize { self.tpu.read().await.len() }
}

// ── GossipPeerUpdater ─────────────────────────────────────────────────────────

/// Listens on the gossip broadcast bus and feeds newly discovered
/// validator TPU / TVU addresses into the `PeerRegistry`.
///
/// Spawn this as a background task once at startup.
pub struct GossipPeerUpdater {
    rx:       broadcast::Receiver<ServerFrame>,
    registry: Arc<PeerRegistry>,
}

impl GossipPeerUpdater {
    pub fn new(
        rx:       broadcast::Receiver<ServerFrame>,
        registry: Arc<PeerRegistry>,
    ) -> Self {
        GossipPeerUpdater { rx, registry }
    }

    pub async fn run(mut self) {
        loop {
            let frame = match self.rx.recv().await {
                Ok(f) => f,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    //debug!("peer_updater: lagged {} frames", n);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };

            match &frame {
                // Leader change — now carries both tpu_addr and tvu_addr
                // directly from getClusterNodes (real ports, no heuristic).
                ServerFrame::GossipLeaderChange { tpu_addr, tvu_addr, .. } => {
                    if let Some(addr_str) = tpu_addr {
                        if let Ok(tpu) = addr_str.parse::<SocketAddr>() {
                            if self.registry.add_tpu(tpu).await {
                                // debug!("peer_updater: new TPU peer from leader change: {}", tpu);
                            }
                        }
                    }
                    if let Some(addr_str) = tvu_addr {
                        if let Ok(tvu) = addr_str.parse::<SocketAddr>() {
                            if self.registry.add_tvu(tvu).await {
                            }
                        }
                    }
                }

                // Node joined — no TPU/TVU in this frame, skip port guessing.
                ServerFrame::GossipNodeJoined { .. } => {}

                // Upcoming leader schedule — carries real tpu_addr + tvu_addr
                // for each upcoming slot leader from getClusterNodes.
                ServerFrame::GossipLeaderSchedule { upcoming } => {
                    for entry in upcoming {
                        if let Some(addr_str) = &entry.tpu_addr {
                            if let Ok(tpu) = addr_str.parse::<SocketAddr>() {
                                if self.registry.add_tpu(tpu).await {
                                 //   debug!("peer_updater: new TPU peer from schedule: {}", tpu);
                                }
                            }
                        }
                        if let Some(addr_str) = &entry.tvu_addr {
                            if let Ok(tvu) = addr_str.parse::<SocketAddr>() {
                                if self.registry.add_tvu(tvu).await {
                           //         debug!("peer_updater: new TVU peer from schedule: {}", tvu);
                                }
                            }
                        }
                    }
                }

                _ => {}
            }
        }

       // info!("peer_updater: gossip bus closed, stopping");
    }
}