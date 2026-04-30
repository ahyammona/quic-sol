// QUIC helpers — quinn 0.11 + rustls 0.23 + webpki-roots 0.26

use quinn::{ClientConfig, Endpoint};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::RootCertStore;
use std::sync::Arc;

pub async fn create_quic_endpoint() -> Result<Endpoint, Box<dyn std::error::Error + Send + Sync>> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    // Quinn 0.11 requires wrapping rustls config in QuicClientConfig
    let quic_client_cfg = QuicClientConfig::try_from(client_tls)?;
    let client_config = ClientConfig::new(Arc::new(quic_client_cfg));

    let mut endpoint = Endpoint::client("[::]:0".parse()?)?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}