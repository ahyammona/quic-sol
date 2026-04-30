//! quic-sol-logs — Reliable Solana program log streaming over QUIC
//!
//! Connects to a quic-sol log-streaming endpoint, applies server-side filters,
//! and delivers [`Log`] events to the caller with built-in backpressure and
//! transparent reconnection.
//!
//! # Example
//! ```no_run
//! use quic_sol_logs::{LogStream, LogFilter, CommitmentLevel};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mut stream = LogStream::builder()
//!         .endpoint("https://stream.quic-sol.com")
//!         .filter(LogFilter::program("YourProgramIdHere"))
//!         .commitment(CommitmentLevel::Confirmed)
//!         .connect()
//!         .await?;
//!
//!     while let Some(log) = stream.next().await? {
//!         println!("[{}] {}: {}", log.slot, log.signature, log.message);
//!     }
//!     Ok(())
//! }
//! ```

use quic_sol_shared::{quic::create_quic_endpoint, QuicSolError};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Filter applied on the server side before events are streamed to the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogFilter {
    /// Only stream logs emitted by this program ID (base-58 pubkey string).
    Program(String),
    /// Stream every log (no filtering — high volume).
    All,
}

impl LogFilter {
    /// Convenience constructor for the common program-filter case.
    pub fn program(id: impl Into<String>) -> Self {
        LogFilter::Program(id.into())
    }
}

/// Solana commitment level requested for log delivery.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CommitmentLevel {
    /// Logs for transactions that have reached "confirmed" cluster commitment.
    Confirmed,
    /// Logs for transactions that have reached "finalized" cluster commitment.
    Finalized,
}

/// A single program log event delivered over the stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    /// The slot in which this transaction was processed.
    pub slot: u64,
    /// The base-58 transaction signature.
    pub signature: String,
    /// The log message string (e.g. `"Program log: ..."`)
    pub message: String,
    /// The program that emitted this log (base-58 pubkey).
    pub program_id: String,
    /// Commitment level at which this event was observed.
    pub commitment: CommitmentLevel,
}

// ---------------------------------------------------------------------------
// Wire protocol (internal)
// ---------------------------------------------------------------------------

/// Handshake message sent to the server immediately after the QUIC stream opens.
#[derive(Debug, Serialize, Deserialize)]
struct SubscribeRequest {
    filter: LogFilter,
    commitment: CommitmentLevel,
}

/// Framed message received from the server.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ServerMessage {
    Log(Log),
    Heartbeat,
    Error { message: String },
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for [`LogStream`].
pub struct LogStreamBuilder {
    endpoint: Option<String>,
    filter: LogFilter,
    commitment: CommitmentLevel,
    channel_capacity: usize,
    reconnect_delay: Duration,
    max_reconnects: Option<u32>,
}

impl Default for LogStreamBuilder {
    fn default() -> Self {
        LogStreamBuilder {
            endpoint: None,
            filter: LogFilter::All,
            commitment: CommitmentLevel::Confirmed,
            channel_capacity: 1_000,
            reconnect_delay: Duration::from_secs(2),
            max_reconnects: None,
        }
    }
}

impl LogStreamBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// QUIC endpoint URL, e.g. `"https://stream.quic-sol.com"`.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Server-side filter applied to the log stream.
    pub fn filter(mut self, filter: LogFilter) -> Self {
        self.filter = filter;
        self
    }

    /// Commitment level for log delivery.
    pub fn commitment(mut self, commitment: CommitmentLevel) -> Self {
        self.commitment = commitment;
        self
    }

    /// Internal channel capacity (backpressure buffer). Default: 1000.
    pub fn channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }

    /// Delay between reconnection attempts. Default: 2 s.
    pub fn reconnect_delay(mut self, delay: Duration) -> Self {
        self.reconnect_delay = delay;
        self
    }

    /// Maximum number of reconnection attempts (`None` = unlimited).
    pub fn max_reconnects(mut self, n: u32) -> Self {
        self.max_reconnects = Some(n);
        self
    }

    /// Resolve the endpoint URL to a `(host, SocketAddr)` pair.
    fn parse_endpoint(&self) -> Result<(String, std::net::SocketAddr), QuicSolError> {
        let raw = self.endpoint.as_deref().ok_or(QuicSolError::InvalidAddress)?;
        let stripped = raw
            .strip_prefix("https://")
            .or_else(|| raw.strip_prefix("http://"))
            .unwrap_or(raw);

        let (host, port) = if let Some(colon) = stripped.rfind(':') {
            let port: u16 = stripped[colon + 1..]
                .parse()
                .map_err(|_| QuicSolError::InvalidAddress)?;
            (&stripped[..colon], port)
        } else {
            (stripped, 4433u16)
        };

        use std::net::ToSocketAddrs;
        let addr = format!("{}:{}", host, port)
            .to_socket_addrs()
            .map_err(QuicSolError::Connection)?
            .next()
            .ok_or(QuicSolError::InvalidAddress)?;

        Ok((host.to_string(), addr))
    }

    /// Connect to the log-streaming server and return a [`LogStream`].
    pub async fn connect(self) -> Result<LogStream, QuicSolError> {
        let (host, addr) = self.parse_endpoint()?;
        let subscribe_req = SubscribeRequest {
            filter: self.filter.clone(),
            commitment: self.commitment,
        };

        let (tx, rx) = mpsc::channel::<Result<Log, QuicSolError>>(self.channel_capacity);
        let reconnect_delay = self.reconnect_delay;
        let max_reconnects = self.max_reconnects;

        tokio::spawn(async move {
            let mut attempt: u32 = 0;

            loop {
                if let Some(max) = max_reconnects {
                    if attempt >= max {
                        error!("quic-sol-logs: exceeded max reconnects ({}), stopping", max);
                        break;
                    }
                }

                if attempt > 0 {
                    warn!(
                        "quic-sol-logs: reconnect attempt {} in {:?}",
                        attempt, reconnect_delay
                    );
                    tokio::time::sleep(reconnect_delay).await;
                }

                match run_connection(&host, addr, &subscribe_req, &tx).await {
                    Ok(()) => {
                        info!("quic-sol-logs: server closed stream cleanly");
                        break;
                    }
                    Err(e) => {
                        warn!("quic-sol-logs: connection error: {}", e);
                    }
                }

                attempt += 1;

                if tx.is_closed() {
                    debug!("quic-sol-logs: receiver dropped, exiting background task");
                    break;
                }
            }
        });

        Ok(LogStream { receiver: rx })
    }
}

/// Establish one QUIC connection, send the subscribe request, and pump
/// incoming [`ServerMessage`]s into `tx`.
async fn run_connection(
    host: &str,
    addr: std::net::SocketAddr,
    subscribe_req: &SubscribeRequest,
    tx: &mpsc::Sender<Result<Log, QuicSolError>>,
) -> Result<(), QuicSolError> {
    let endpoint = create_quic_endpoint().await.map_err(|e| {
        QuicSolError::Quic(format!("failed to create QUIC endpoint: {}", e))
    })?;

    info!("quic-sol-logs: connecting to {} ({})", host, addr);

    let conn = endpoint
        .connect(addr, host)
        .map_err(|e| QuicSolError::Quic(e.to_string()))?
        .await
        .map_err(|e| QuicSolError::Quic(e.to_string()))?;

    info!("quic-sol-logs: QUIC connection established");

    // Open a bidirectional stream: send subscribe request, receive log events.
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| QuicSolError::Quic(e.to_string()))?;

    // Write subscribe request as length-prefixed JSON.
    let req_bytes = serde_json::to_vec(subscribe_req).map_err(QuicSolError::Serde)?;
    let len = (req_bytes.len() as u32).to_be_bytes();
    send.write_all(&len)
        .await
        .map_err(|e| QuicSolError::Quic(e.to_string()))?;
    send.write_all(&req_bytes)
        .await
        .map_err(|e| QuicSolError::Quic(e.to_string()))?;
    send.finish()
        .await
        .map_err(|e| QuicSolError::Quic(e.to_string()))?;

    info!("quic-sol-logs: subscribed, waiting for events…");

    loop {
        let mut len_buf = [0u8; 4];
        match recv.read_exact(&mut len_buf).await {
            Ok(()) => {}
            Err(quinn::ReadExactError::FinishedEarly) => {
                debug!("quic-sol-logs: server closed receive stream");
                return Ok(());
            }
            Err(e) => {
                return Err(QuicSolError::Quic(format!("read length prefix: {}", e)));
            }
        }

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        // Sanity cap at 1 MiB per frame.
        if msg_len > 1 << 20 {
            return Err(QuicSolError::Quic(format!("oversized frame: {} bytes", msg_len)));
        }

        let mut buf = vec![0u8; msg_len];
        recv.read_exact(&mut buf)
            .await
            .map_err(|e| QuicSolError::Quic(format!("read frame body: {}", e)))?;

        let msg: ServerMessage = serde_json::from_slice(&buf).map_err(QuicSolError::Serde)?;

        match msg {
            ServerMessage::Log(log) => {
                if tx.send(Ok(log)).await.is_err() {
                    debug!("quic-sol-logs: consumer gone, closing connection");
                    conn.close(0u32.into(), b"consumer gone");
                    return Ok(());
                }
            }
            ServerMessage::Heartbeat => {
                debug!("quic-sol-logs: heartbeat");
            }
            ServerMessage::Error { message } => {
                warn!("quic-sol-logs: server error: {}", message);
                let _ = tx.send(Err(QuicSolError::Quic(message.clone()))).await;
                return Err(QuicSolError::Quic(message));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// LogStream
// ---------------------------------------------------------------------------

/// An async stream of Solana program log events delivered over QUIC.
///
/// Created via [`LogStream::builder`]. Backed by an internal channel with
/// configurable capacity for backpressure. The background QUIC connection
/// reconnects automatically on transient failures.
pub struct LogStream {
    receiver: mpsc::Receiver<Result<Log, QuicSolError>>,
}

impl LogStream {
    /// Create a [`LogStreamBuilder`] to configure the connection.
    pub fn builder() -> LogStreamBuilder {
        LogStreamBuilder::new()
    }

    /// Receive the next log event.
    ///
    /// Returns:
    /// - `Ok(Some(log))` — a log event is available.
    /// - `Ok(None)` — the server closed the stream cleanly.
    /// - `Err(e)` — a server error was reported; background task will reconnect.
    pub async fn next(&mut self) -> Result<Option<Log>, QuicSolError> {
        match self.receiver.recv().await {
            Some(Ok(log)) => Ok(Some(log)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_program_constructor() {
        let f = LogFilter::program("So11111111111111111111111111111111111111112");
        assert!(matches!(f, LogFilter::Program(_)));
    }

    #[test]
    fn log_roundtrip_serde() {
        let log = Log {
            slot: 99,
            signature: "sig".into(),
            message: "Program log: hello".into(),
            program_id: "prog123".into(),
            commitment: CommitmentLevel::Finalized,
        };
        let json = serde_json::to_string(&log).unwrap();
        let back: Log = serde_json::from_str(&json).unwrap();
        assert_eq!(back.slot, 99);
        assert_eq!(back.commitment, CommitmentLevel::Finalized);
    }

    #[test]
    fn server_message_heartbeat_serde() {
        let hb = ServerMessage::Heartbeat;
        let json = serde_json::to_string(&hb).unwrap();
        let back: ServerMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, ServerMessage::Heartbeat));
    }

    #[test]
    fn builder_defaults() {
        let b = LogStreamBuilder::new();
        assert!(b.endpoint.is_none());
        assert_eq!(b.commitment, CommitmentLevel::Confirmed);
        assert_eq!(b.channel_capacity, 1_000);
    }

    #[tokio::test]
    async fn missing_endpoint_returns_error() {
        let result = LogStream::builder().connect().await;
        assert!(result.is_err());
    }
}
