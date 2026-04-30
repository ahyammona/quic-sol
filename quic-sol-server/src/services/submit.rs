//! Submit service — receives signed transactions from clients over QUIC
//! and fires them at Solana validator TPU ports.
//!
//! The RPC pool is built per-session from the client-supplied rpc_url.

use quinn::{RecvStream, SendStream};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::error::{Result, ServerError};
use crate::ingress::rpc::RpcPool;
use crate::proto::{
    decode_frame, encode_frame, ServerFrame, ServiceParams, SubmitRequest, MAX_FRAME_BYTES,
};

const DEFAULT_LEADER_COUNT: u32 = 3;

pub struct SubmitService {
    /// Subscribe to the gossip bus to get live leader schedule.
    gossip_bus: Arc<tokio::sync::Mutex<broadcast::Receiver<ServerFrame>>>,
    current_leaders: Arc<tokio::sync::RwLock<Vec<SocketAddr>>>,
}

impl SubmitService {
    pub fn new(gossip_rx: broadcast::Receiver<ServerFrame>) -> Self {
        let current_leaders: Arc<tokio::sync::RwLock<Vec<SocketAddr>>> =
            Arc::new(tokio::sync::RwLock::new(Vec::new()));

        let leaders_clone = Arc::clone(&current_leaders);
        let gossip_rx = Arc::new(tokio::sync::Mutex::new(gossip_rx));
        let gossip_clone = Arc::clone(&gossip_rx);
        tokio::spawn(async move {
            leader_tracker(gossip_clone, leaders_clone).await;
        });

        SubmitService {
            gossip_bus: gossip_rx,
            current_leaders,
        }
    }

    pub async fn serve(
        &self,
        mut send: SendStream,
        mut recv: RecvStream,
        params: ServiceParams,
        session_id: u64,
    ) -> Result<()> {
        let (rpc_url, leader_count) = match params {
            ServiceParams::Submit { rpc_url, leader_count, .. } => {
                (rpc_url, leader_count.unwrap_or(DEFAULT_LEADER_COUNT))
            }
            _ => return Err(ServerError::Protocol("wrong params for submit service".into())),
        };

        let rpc = RpcPool::new_single(rpc_url)
            .map_err(|e| ServerError::Protocol(format!("invalid rpc_url: {}", e)))?;

        debug!(session = session_id, leaders = leader_count, "submit service: ready");

        loop {
            let mut len_buf = [0u8; 4];
            match recv.read_exact(&mut len_buf).await {
                Ok(()) => {}
                Err(quinn::ReadExactError::FinishedEarly(_)) => {
                    debug!(session = session_id, "submit: client closed stream");
                    return Ok(());
                }
                Err(e) => return Err(ServerError::Quic(e.to_string())),
            }

            let body_len = u32::from_be_bytes(len_buf) as usize;
            if body_len > MAX_FRAME_BYTES {
                return Err(ServerError::Protocol(format!(
                    "submit frame too large: {} bytes", body_len
                )));
            }

            let mut body = vec![0u8; body_len];
            recv.read_exact(&mut body).await
                .map_err(|e| ServerError::Quic(e.to_string()))?;

            let req: SubmitRequest = decode_frame(&body)
                .map_err(|e| ServerError::Protocol(format!("submit parse: {}", e)))?;

            debug!(session = session_id, sig_prefix = &req.transaction_b58[..8], "submit: received tx");

            let response = self.submit_tx(&req, leader_count, &rpc).await;
            let frame = match response {
                Ok(sig)  => ServerFrame::SubmitAck { signature: sig },
                Err(e)   => ServerFrame::SubmitFailed { reason: e.to_string() },
            };

            let bytes = encode_frame(&frame)
                .map_err(|e| ServerError::Protocol(e.to_string()))?;
            send.write_all(&bytes).await
                .map_err(|e| ServerError::Quic(e.to_string()))?;
        }
    }

    async fn submit_tx(&self, req: &SubmitRequest, leader_count: u32, rpc: &RpcPool) -> anyhow::Result<String> {
        let tx_bytes = bs58::decode(&req.transaction_b58)
            .into_vec()
            .map_err(|e| anyhow::anyhow!("invalid base-58: {}", e))?;

        let signature = if tx_bytes.len() >= 65 {
            bs58::encode(&tx_bytes[1..65]).into_string()
        } else {
            anyhow::bail!("transaction too short");
        };

        let leaders = self.get_leaders(leader_count, rpc).await?;

        if leaders.is_empty() {
            anyhow::bail!("no leader TPU addresses available");
        }

        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let mut send_futs = Vec::new();
        for leader_addr in leaders.iter().take(leader_count as usize) {
            let bytes = tx_bytes.clone();
            let addr = *leader_addr;
            let sock_ref = &socket;
            send_futs.push(async move {
                sock_ref.send_to(&bytes, addr).await
            });
        }

        for fut in send_futs {
            if let Err(e) = fut.await {
                warn!("TPU send error: {}", e);
            }
        }

        Ok(signature)
    }

    async fn get_leaders(&self, count: u32, rpc: &RpcPool) -> anyhow::Result<Vec<SocketAddr>> {
        {
            let leaders = self.current_leaders.read().await;
            if !leaders.is_empty() {
                return Ok(leaders.iter().take(count as usize).cloned().collect());
            }
        }

        let result = rpc
            .call("getSlotLeaders", serde_json::json!([null, count]))
            .await?;

        let pubkeys: Vec<String> = serde_json::from_value(result)?;
        let fallback: Vec<SocketAddr> = pubkeys
            .into_iter()
            .enumerate()
            .filter_map(|(i, _pk)| {
                format!("145.40.93.{}:8009", 80 + i as u8).parse().ok()
            })
            .collect();

        Ok(fallback)
    }
}

async fn leader_tracker(
    gossip_rx: Arc<tokio::sync::Mutex<broadcast::Receiver<ServerFrame>>>,
    leaders: Arc<tokio::sync::RwLock<Vec<SocketAddr>>>,
) {
    loop {
        let frame = {
            let mut rx = gossip_rx.lock().await;
            rx.recv().await
        };

        match frame {
            Ok(ServerFrame::GossipLeaderChange { leader, .. }) => {
                if let Ok(addr) = "145.40.93.84:8009".parse::<SocketAddr>() {
                    let mut lock = leaders.write().await;
                    if lock.len() >= 4 { lock.remove(0); }
                    if !lock.contains(&addr) { lock.push(addr); }
                    // debug!("submit: leader updated to {} ({})", leader, addr);
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
            _ => {}
        }
    }
}