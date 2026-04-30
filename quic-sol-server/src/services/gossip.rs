//! Gossip service — spawns a per-session ClusterIngress using the client-supplied
//! HTTP RPC URL and fans out gossip events to the client stream.

use quinn::SendStream;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::error::{Result, ServerError};
use crate::ingress::cluster::ClusterIngress;
use crate::ingress::rpc::RpcPool;
use crate::metrics::Metrics;
use crate::proto::{encode_frame, ServerFrame, ServiceParams};

pub struct GossipService {
    metrics: Metrics,
}

impl GossipService {
    pub fn new(metrics: Metrics) -> Self {
        GossipService { metrics }
    }

    pub async fn serve(
        &self,
        mut send: SendStream,
        params: ServiceParams,
        session_id: u64,
    ) -> Result<()> {
        let (rpc_url, watch_pubkeys, want_cluster_info) = match params {
            ServiceParams::Gossip { rpc_url, watch_pubkeys, subscribe_cluster_info } => {
                (rpc_url, watch_pubkeys, subscribe_cluster_info)
            }
            _ => return Err(ServerError::Protocol("wrong params for gossip service".into())),
        };

        // Build a per-session HTTP RPC pool from the client-supplied URL.
        // Convert wss:// → https:// (or ws:// → http://) so reqwest can make
        // JSON-RPC calls — the client supplies a WebSocket URL but HTTP is needed here.
        let http_url = rpc_url
            .replacen("wss://", "https://", 1)
            .replacen("ws://", "http://", 1);
        let rpc = RpcPool::new_single(http_url)
            .map_err(|e| ServerError::Protocol(format!("invalid rpc_url: {}", e)))?;

        // mpsc channel: ClusterIngress → this serve loop.
        let (tx, mut rx) = mpsc::channel::<ServerFrame>(256);

        let ingress = ClusterIngress {
            rpc,
            tx,
            metrics: self.metrics.clone(),
        };

        // Spawn the ingress; it will stop automatically when the mpsc sender is dropped
        // (i.e. when `rx` is dropped at the end of this function).
        tokio::spawn(async move {
            if let Err(e) = ingress.run().await {
                tracing::error!("per-session cluster ingress error: {}", e);
            }
        });

        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));

        debug!(session = session_id, "gossip service: streaming started");

        loop {
            tokio::select! {
                maybe_frame = rx.recv() => {
                    let frame = match maybe_frame {
                        Some(f) => f,
                        None => break, // ingress stopped
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