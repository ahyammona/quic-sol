//! Logs service — spawns a per-session LogIngress connected to the client's
//! own RPC endpoint, then filters and forwards frames to the QUIC stream.

use quinn::SendStream;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::error::{Result, ServerError};
use crate::ingress::logs::LogIngress;
use crate::ingress::rpc::RpcPool;
use crate::metrics::Metrics;
use crate::proto::{encode_frame, ServerFrame, ServiceParams};

const INGRESS_CHANNEL_DEPTH: usize = 4_096;

pub struct LogsService {
    metrics: Metrics,
    enrich_enabled: bool,
}

impl LogsService {
    pub fn new(metrics: Metrics, enrich_enabled: bool) -> Self {
        LogsService { metrics, enrich_enabled }
    }

    pub async fn serve(
        &self,
        mut send: SendStream,
        params: ServiceParams,
        session_id: u64,
    ) -> Result<()> {
        let (rpc_url, program_id_filter, commitment_filter, parsed, events_mode) = match params {
            ServiceParams::Logs { rpc_url, program_id, commitment, parsed, events } => {
                (rpc_url, program_id, commitment, parsed, events)
            }
            _ => return Err(ServerError::Protocol("wrong params for logs service".into())),
        };

        if rpc_url.is_empty() {
            return Err(ServerError::Protocol(
                "logs service requires rpc_url — provide your WebSocket RPC endpoint".into(),
            ));
        }

        info!(
            session    = session_id,
            rpc_url    = %rpc_url,
            filter     = ?program_id_filter,
            commitment = %commitment_filter,
            parsed,
            events     = events_mode,
            "logs service: starting per-session ingress"
        );

        // Build a per-session HTTP RPC pool for enrichment (convert wss:// → https://).
        let http_url = rpc_url
            .replacen("wss://", "https://", 1)
            .replacen("ws://", "http://", 1);
        let rpc = RpcPool::new_single(http_url)
            .map_err(|e| ServerError::Protocol(format!("rpc pool: {}", e)))?;

        let (ingress_tx, mut ingress_rx) = mpsc::channel::<ServerFrame>(INGRESS_CHANNEL_DEPTH);

        let ingress = LogIngress::new(
            rpc_url,
            rpc,
            ingress_tx,
            self.metrics.clone(),
            parsed || self.enrich_enabled,  // client --parsed flag enables enrichment regardless of server default
        );

        tokio::spawn(async move {
            if let Err(e) = ingress.run().await {
                warn!("log ingress task ended: {}", e);
            }
        });

        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));

        loop {
            tokio::select! {
                frame_opt = ingress_rx.recv() => {
                    let frame = match frame_opt {
                        Some(f) => f,
                        None => {
                            debug!(session = session_id, "log ingress channel closed");
                            break;
                        }
                    };

                    let should_send = if events_mode {
                        match &frame {
                            ServerFrame::Event(ef) => {
                                program_id_filter.as_ref()
                                    .map(|pid| ef.programs.contains(pid))
                                    .unwrap_or(true)
                            }
                            _ => false,
                        }
                    } else if parsed {
                        match &frame {
                            ServerFrame::EnrichedTransaction(tx) => {
                                program_id_filter.as_ref()
                                    .map(|pid| tx.programs.contains(pid))
                                    .unwrap_or(true)
                            }
                            _ => false,
                        }
                    } else {
                        match &frame {
                            ServerFrame::Log(log) => {
                                let pid_ok = program_id_filter.as_ref()
                                    .map(|pid| &log.program_id == pid)
                                    .unwrap_or(true);
                                let commit_ok = log.commitment == commitment_filter;
                                pid_ok && commit_ok
                            }
                            _ => false,
                        }
                    };

                    if !should_send { continue; }

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

        debug!(session = session_id, "logs service: stream ended");
        Ok(())
    }
}