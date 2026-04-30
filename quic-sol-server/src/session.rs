//! Per-connection session manager.

use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use quinn::Connection;
use tracing::{debug, info, warn};

use crate::error::{Result, ServerError};
use crate::metrics::Metrics;
use crate::proto::{
    ClientHandshake, ServerAck, ServerFrame, ServiceKind,
    decode_frame, encode_frame, MAX_FRAME_BYTES,
};
use crate::services::{
    gossip::GossipService,
    logs::LogsService,
    price::PriceService,
    router::RouterService,
    slot::SlotService,
    submit::SubmitService,
};

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct SessionManager {
    gossip_svc: Arc<GossipService>,
    logs_svc:   Arc<LogsService>,
    price_svc:  Arc<PriceService>,
    submit_svc: Arc<SubmitService>,
    router_svc: Arc<RouterService>,
    slot_svc:   Arc<SlotService>,
    api_keys:   Vec<String>,
    metrics:    Metrics,
}

impl SessionManager {
    pub fn new(
        gossip_svc: GossipService,
        logs_svc:   LogsService,
        price_svc:  PriceService,
        submit_svc: SubmitService,
        router_svc: RouterService,
        slot_svc:   SlotService,
        api_keys:   Vec<String>,
        metrics:    Metrics,
    ) -> Self {
        SessionManager {
            gossip_svc: Arc::new(gossip_svc),
            logs_svc:   Arc::new(logs_svc),
            price_svc:  Arc::new(price_svc),
            submit_svc: Arc::new(submit_svc),
            router_svc: Arc::new(router_svc),
            slot_svc:   Arc::new(slot_svc),
            api_keys,
            metrics,
        }
    }

    pub async fn handle_connection(&self, conn: Connection) -> Result<()> {
        let remote = conn.remote_address();
        self.metrics.connections_active.fetch_add(1, Ordering::Relaxed);
        let result = self.run_connection_loop(&conn).await;
        self.metrics.connections_active.fetch_sub(1, Ordering::Relaxed);
        debug!(peer = %remote, "connection closed");
        result
    }

    async fn run_connection_loop(&self, conn: &Connection) -> Result<()> {
        loop {
            let (send, mut recv) = conn.accept_bi().await
                .map_err(|e| ServerError::Quic(e.to_string()))?;

            let session_id = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);

            let mut len_buf = [0u8; 4];
            recv.read_exact(&mut len_buf).await
                .map_err(|e| ServerError::Protocol(format!("handshake length: {}", e)))?;

            let body_len = u32::from_be_bytes(len_buf) as usize;
            if body_len > MAX_FRAME_BYTES {
                return Err(ServerError::Protocol(format!(
                    "handshake frame too large: {} bytes", body_len
                )));
            }

            let mut body = vec![0u8; body_len];
            recv.read_exact(&mut body).await
                .map_err(|e| ServerError::Protocol(format!("handshake body: {}", e)))?;

            let handshake: ClientHandshake = decode_frame(&body)
                .map_err(|e| ServerError::Protocol(format!("handshake parse: {}", e)))?;

            // Authenticate
            if !self.api_keys.is_empty() {
                let key = handshake.api_key.as_deref().unwrap_or("");
                if !self.api_keys.iter().any(|k| k == key) {
                    let err_frame = encode_frame(&ServerFrame::Error {
                        message: "invalid API key".into(),
                    }).unwrap_or_default();
                    let mut s = send;
                    let _ = s.write_all(&err_frame).await;
                    let _ = s.finish();
                    warn!(session = session_id, service = ?handshake.service, "authentication failed");
                    return Err(ServerError::Auth("invalid API key".into()));
                }
            }

            // Send ACK
            let ack = ServerAck {
                ok: true,
                message: format!("connected to {:?}", handshake.service),
                session_id,
            };
            let ack_bytes = encode_frame(&ack)
                .map_err(|e| ServerError::Protocol(format!("ack encode: {}", e)))?;

            let mut send = send;
            send.write_all(&ack_bytes).await
                .map_err(|e| ServerError::Quic(e.to_string()))?;

            info!(
                session = session_id,
                service = ?handshake.service,
                peer    = %conn.remote_address(),
                "session started"
            );

            self.metrics.sessions_total.fetch_add(1, Ordering::Relaxed);

            let service = handshake.service.clone();
            let gossip  = Arc::clone(&self.gossip_svc);
            let logs    = Arc::clone(&self.logs_svc);
            let price   = Arc::clone(&self.price_svc);
            let submit  = Arc::clone(&self.submit_svc);
            let router  = Arc::clone(&self.router_svc);
            let slot    = Arc::clone(&self.slot_svc);
            let params  = handshake.params;
            let metrics = self.metrics.clone();

            tokio::spawn(async move {
                let res = match service {
                    ServiceKind::Gossip      => gossip.serve(send, params, session_id).await,
                    ServiceKind::Logs        => logs.serve(send, params, session_id).await,
                    ServiceKind::Price       => price.serve(send, params, session_id).await,
                    ServiceKind::Submit      => submit.serve(send, recv, params, session_id).await,
                    ServiceKind::Router      => router.serve(send, recv, params, session_id).await,
                    ServiceKind::Slot        => slot.serve(send, params, session_id).await,
                    // These variants exist in proto for wire compat but have no server impl
                    ServiceKind::Mempool | ServiceKind::ShredStream => {
                        Err(ServerError::Protocol("service not enabled on this server".into()))
                    }
                };
                metrics.sessions_total.fetch_sub(1, Ordering::Relaxed);
                if let Err(e) = res {
                    debug!(session = session_id, "session ended: {}", e);
                }
            });
        }
    }
}