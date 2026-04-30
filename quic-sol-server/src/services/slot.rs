//! Slot service — streams confirmed slot transactions to QUIC clients.
//!
//! This is the fast path for confirmed transactions. Instead of logsSubscribe
//! (400–800ms lag), we watch gossip for slot boundaries and fire getBlock
//! immediately when a slot seals — racing the RPC indexer by ~300–600ms.
//!
//! Client connects with ServiceKind::Slot and ServiceParams::Slot { rpc_url, filter_programs }.
//! Server streams SlotTransaction frames as each slot confirms, then a SlotConfirmed
//! marker so the client knows the slot is complete.

use quinn::SendStream;
use tokio::sync::mpsc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::error::{Result, ServerError};
use crate::ingress::rpc::RpcPool;
use crate::ingress::slot::SlotIngress;
use crate::metrics::Metrics;
use crate::proto::{encode_frame, ServerFrame, ServiceParams};

const INGRESS_CHANNEL_DEPTH: usize = 8_192;

pub struct SlotService {
    /// Shared gossip broadcast bus — SlotIngress subscribes to detect slot boundaries.
    gossip_tx: broadcast::Sender<ServerFrame>,
    metrics: Metrics,
}

impl SlotService {
    pub fn new(gossip_tx: broadcast::Sender<ServerFrame>, metrics: Metrics) -> Self {
        SlotService { gossip_tx, metrics }
    }

    pub async fn serve(
        &self,
        mut send: SendStream,
        params: ServiceParams,
        session_id: u64,
    ) -> Result<()> {
        let (rpc_url, filter_programs) = match params {
            ServiceParams::Slot { rpc_url, filter_programs } => (rpc_url, filter_programs),
            _ => return Err(ServerError::Protocol("wrong params for slot service".into())),
        };

        if rpc_url.is_empty() {
            return Err(ServerError::Protocol(
                "slot service requires rpc_url — provide your HTTP RPC endpoint".into(),
            ));
        }

        info!(
            session  = session_id,
            rpc_url  = %rpc_url,
            filters  = ?filter_programs,
            "slot service: starting"
        );

        let rpc = RpcPool::new_single(rpc_url)
            .map_err(|e| ServerError::Protocol(format!("rpc pool: {}", e)))?;

        let (ingress_tx, mut ingress_rx) = mpsc::channel::<ServerFrame>(INGRESS_CHANNEL_DEPTH);

        let ingress = SlotIngress::new(
            self.gossip_tx.subscribe(),
            ingress_tx,
            rpc,
            self.metrics.clone(),
            filter_programs,
        );

        tokio::spawn(async move {
            if let Err(e) = ingress.run().await {
                warn!("slot ingress task ended: {}", e);
            }
        });

        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));

        loop {
            tokio::select! {
                frame_opt = ingress_rx.recv() => {
                    let frame = match frame_opt {
                        Some(f) => f,
                        None => {
                            debug!(session = session_id, "slot ingress channel closed");
                            break;
                        }
                    };

                    let bytes = encode_frame(&frame)
                        .map_err(|e| ServerError::Protocol(e.to_string()))?;
                    send.write_all(&bytes).await
                        .map_err(|e| ServerError::Quic(e.to_string()))?;
                }

                _ = heartbeat.tick() => {
                    let hb = encode_frame(&ServerFrame::Heartbeat)
                        .map_err(|e| ServerError::Protocol(e.to_string()))?;
                    if send.write_all(&hb).await.is_err() { break; }
                }
            }
        }

        debug!(session = session_id, "slot service: stream ended");
        Ok(())
    }
}