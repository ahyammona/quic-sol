//! Server-wide error type.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("QUIC transport error: {0}")]
    Quic(String),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Client disconnected")]
    Disconnected,

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Upstream error: {0}")]
    Upstream(String),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ServerError>;