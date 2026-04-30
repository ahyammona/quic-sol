//! Router service — multiplexes JSON-RPC requests from a single client QUIC
//! stream across an RPC pool and returns responses keyed by request_id.
//!
//! The RPC pool is built per-session from the client-supplied rpc_url.

use quinn::{RecvStream, SendStream};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing::debug;

use crate::error::{Result, ServerError};
use crate::ingress::rpc::RpcPool;
use crate::proto::{
    decode_frame, encode_frame, RpcRequest, ServerFrame, ServiceParams, MAX_FRAME_BYTES,
};

const MAX_CONCURRENT: u32 = 200;

pub struct RouterService;

impl RouterService {
    pub fn new() -> Self {
        RouterService
    }

    pub async fn serve(
        &self,
        send: SendStream,
        mut recv: RecvStream,
        params: ServiceParams,
        session_id: u64,
    ) -> Result<()> {
        let (rpc_url, max_streams) = match params {
            ServiceParams::Router { rpc_url, max_streams } => {
                (rpc_url, max_streams.unwrap_or(MAX_CONCURRENT).min(MAX_CONCURRENT))
            }
            _ => return Err(ServerError::Protocol("wrong params for router service".into())),
        };

        let rpc = RpcPool::new_single(rpc_url)
            .map_err(|e| ServerError::Protocol(format!("invalid rpc_url: {}", e)))?;

        debug!(session = session_id, max_streams, "router service: ready");

        let sem = Arc::new(Semaphore::new(max_streams as usize));
        let (resp_tx, mut resp_rx) = mpsc::channel::<ServerFrame>(max_streams as usize * 2);

        let writer_handle = tokio::spawn(async move {
            let mut send = send;
            while let Some(frame) = resp_rx.recv().await {
                let bytes = encode_frame(&frame)?;
                send.write_all(&bytes).await
                    .map_err(|e| ServerError::Quic(e.to_string()))?;
            }
            Ok::<_, ServerError>(())
        });

        loop {
            let mut len_buf = [0u8; 4];
            match recv.read_exact(&mut len_buf).await {
                Ok(()) => {}
                Err(quinn::ReadExactError::FinishedEarly(_)) => {
                    debug!(session = session_id, "router: client closed stream");
                    break;
                }
                Err(e) => return Err(ServerError::Quic(e.to_string())),
            }

            let body_len = u32::from_be_bytes(len_buf) as usize;
            if body_len > MAX_FRAME_BYTES {
                return Err(ServerError::Protocol(format!(
                    "router frame too large: {} bytes", body_len
                )));
            }

            let mut body = vec![0u8; body_len];
            recv.read_exact(&mut body).await
                .map_err(|e| ServerError::Quic(e.to_string()))?;

            let rpc_req: RpcRequest = decode_frame(&body)
                .map_err(|e| ServerError::Protocol(format!("rpc parse: {}", e)))?;

            let request_id = rpc_req.request_id;
            let permit = Arc::clone(&sem).acquire_owned().await
                .map_err(|_| ServerError::Protocol("semaphore closed".into()))?;

            let rpc = rpc.clone();
            let tx  = resp_tx.clone();

            tokio::spawn(async move {
                let _permit = permit;
                let frame = match rpc.call(&rpc_req.method, rpc_req.params).await {
                    Ok(result) => ServerFrame::RpcResponse { request_id, body: result },
                    Err(e)     => ServerFrame::RpcError { request_id, message: e.to_string() },
                };
                let _ = tx.send(frame).await;
            });
        }

        drop(resp_tx);
        let _ = writer_handle.await;

        debug!(session = session_id, "router service: stream ended");
        Ok(())
    }
}