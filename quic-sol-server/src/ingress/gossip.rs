//! Gossip service — subscribes to the shared gossip broadcast bus and fans out
//! events to the client stream. Real UDP gossip data comes from optimistic.rs
//! which connects directly to Solana validators — no RPC, no polling.

use quinn::SendStream;
use tokio::sync::broadcast;
use tracing::debug;

use crate::error::{Result, ServerError};
use crate::proto::{encode_frame, ServerFrame, ServiceParams};

pub struct GossipService {
    /// Shared broadcast from optimistic.rs + cluster ingress.
    gossip_tx: broadcast::Sender<ServerFrame>,
}

impl GossipService {
    pub fn new(gossip_tx: broadcast::Sender<ServerFrame>) -> Self {
        GossipService { gossip_tx }
    }

    pub async fn serve(
        &self,
        mut send: SendStream,
        params: ServiceParams,
        session_id: u64,
    ) -> Result<()> {
        let (watch_pubkeys, want_cluster_info) = match params {
            ServiceParams::Gossip { watch_pubkeys, subscribe_cluster_info, .. } => {
                (watch_pubkeys, subscribe_cluster_info)
            }
            _ => return Err(ServerError::Protocol("wrong params for gossip service".into())),
        };

        // Subscribe to the shared gossip bus — receives real UDP validator data
        // from optimistic.rs the moment slots are confirmed on-chain.
        let mut rx = self.gossip_tx.subscribe();
        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));

        debug!(session = session_id, "gossip service: streaming started (UDP gossip mode)");

        loop {
            tokio::select! {
                result = rx.recv() => {
                    let frame = match result {
                        Ok(f) => f,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            debug!(session = session_id, "gossip: lagged {} frames, continuing", n);
                            continue;
                        }
                        Err(_) => break,
                    };

                    if !should_forward(&frame, &watch_pubkeys, want_cluster_info) {
                        continue;
                    }

                    let bytes = encode_frame(&frame)
                        .map_err(|e| ServerError::Protocol(e.to_string()))?;
                    send.write_all(&bytes).await
                        .map_err(|e| ServerError::Quic(e.to_string()))?;
                }
                _ = heartbeat.tick() => {
                    let hb = encode_frame(&ServerFrame::Heartbeat)
                        .map_err(|e| ServerError::Protocol(e.to_string()))?;
                    if send.write_all(&hb).await.is_err() {
                        debug!(session = session_id, "gossip: client disconnected on heartbeat");
                        break;
                    }
                }
            }
        }

        debug!(session = session_id, "gossip service: stream ended");
        Ok(())
    }
}

fn should_forward(frame: &ServerFrame, watch: &[String], want_cluster: bool) -> bool {
    match frame {
        ServerFrame::GossipClusterInfo(_)        => want_cluster,
        ServerFrame::GossipLeaderChange { .. }   => true,
        ServerFrame::GossipLeaderSchedule { .. } => true,
        ServerFrame::GossipNodeDegraded { pubkey, .. }
        | ServerFrame::GossipNodeRecovered { pubkey }
        | ServerFrame::GossipNodeLeft { pubkey } => {
            watch.is_empty() || watch.contains(pubkey)
        }
        ServerFrame::GossipNodeJoined { pubkey, .. } => {
            watch.is_empty() || watch.contains(pubkey)
        }
        ServerFrame::GossipTpuCongestion { leader, .. } => {
            watch.is_empty() || watch.contains(leader)
        }
        _ => false,
    }
}