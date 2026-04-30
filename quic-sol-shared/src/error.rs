use thiserror::Error;

#[derive(Error, Debug)]
pub enum QuicSolError {
    #[error("Invalid address")]
    InvalidAddress,
    #[error("Connection error: {0}")]
    Connection(#[from] std::io::Error),
    #[error("QUIC error: {0}")]
    Quic(String),
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}