//! QUIC listener — accepts incoming connections, performs TLS + auth,
//! then hands each connection to the [`SessionManager`].

use anyhow::{Context, Result};
use quinn::{Endpoint, ServerConfig as QuinnServerConfig};
use quinn::crypto::rustls::QuicServerConfig;
use rustls::ServerConfig as RustlsServerConfig;
use std::{net::SocketAddr, sync::Arc};
use tracing::{debug, info, warn};

use crate::config::TlsConfig;
use crate::session::SessionManager;

pub struct QuicListener {
    endpoint: Endpoint,
    session_mgr: Arc<SessionManager>,
}

impl QuicListener {
    pub async fn new(
        bind: SocketAddr,
        tls: TlsConfig,
        session_mgr: SessionManager,
    ) -> Result<Self> {
        let rustls_cfg = build_tls_config(&tls)
            .context("building TLS config")?;

        // Quinn 0.11 requires wrapping rustls ServerConfig in QuicServerConfig
        let quic_server_cfg = QuicServerConfig::try_from(rustls_cfg)
            .map_err(|e| anyhow::anyhow!("QuicServerConfig: {}", e))?;
        let mut quinn_cfg = QuinnServerConfig::with_crypto(Arc::new(quic_server_cfg));

        // Keep streaming connections alive: send QUIC PING every 10s,
        // allow up to 5 minutes of idle before closing.
        let mut transport = quinn::TransportConfig::default();
        transport.keep_alive_interval(Some(std::time::Duration::from_secs(10)));
        transport.max_idle_timeout(Some(
            quinn::VarInt::from_u32(300_000).into() // 5 minutes in ms
        ));
        quinn_cfg.transport_config(Arc::new(transport));

        let endpoint = Endpoint::server(quinn_cfg, bind)
            .context("binding QUIC endpoint")?;

        info!("QUIC endpoint bound on {}", bind);

        Ok(QuicListener {
            endpoint,
            session_mgr: Arc::new(session_mgr),
        })
    }

    /// Accept connections in a loop. Returns only on fatal error or shutdown.
    pub async fn run(self) -> Result<()> {
        loop {
            let conn = match self.endpoint.accept().await {
                Some(incoming) => {
                    match incoming.await {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("connection setup failed: {}", e);
                            continue;
                        }
                    }
                }
                None => {
                    info!("QUIC endpoint closed");
                    break;
                }
            };

            let remote = conn.remote_address();
            info!(peer = %remote, "new QUIC connection");

            let mgr = Arc::clone(&self.session_mgr);
            tokio::spawn(async move {
                if let Err(e) = mgr.handle_connection(conn).await {
                    debug!(peer = %remote, "connection ended: {}", e);
                }
            });
        }
        Ok(())
    }
}

// ── TLS setup ─────────────────────────────────────────────────────────────────

fn build_tls_config(cfg: &TlsConfig) -> Result<RustlsServerConfig> {
    if cfg.self_signed || (cfg.cert_path.is_none() && cfg.key_path.is_none()) {
        warn!("using self-signed TLS certificate — do not use in production");
        return make_self_signed();
    }

    let cert_path = cfg.cert_path.as_ref().context("cert_path required")?;
    let key_path  = cfg.key_path.as_ref().context("key_path required")?;

    let cert_pem = std::fs::read(cert_path)
        .with_context(|| format!("reading cert {}", cert_path.display()))?;
    let key_pem  = std::fs::read(key_path)
        .with_context(|| format!("reading key {}", key_path.display()))?;

    let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("parsing certificate PEM")?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .context("parsing private key PEM")?
        .context("no private key found")?;

    RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building rustls server config")
}

fn make_self_signed() -> Result<RustlsServerConfig> {
    // rcgen 0.13 API: generate_simple_self_signed returns CertifiedKey
    let certified = rcgen::generate_simple_self_signed(vec!["quic-sol-server".into()])
        .context("generating self-signed cert")?;

    let cert_der = rustls::pki_types::CertificateDer::from(
        certified.cert.der().to_vec()
    );
    let key_der = rustls::pki_types::PrivateKeyDer::try_from(
        certified.key_pair.serialize_der()
    ).map_err(|e| anyhow::anyhow!("key conversion: {}", e))?;

    RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .context("building self-signed rustls config")
}