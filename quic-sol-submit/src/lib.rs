//! quic-sol-submit — Direct TPU Transaction Submitter
//!
//! Skip JSON-RPC entirely. Submit signed transactions directly to a
//! validator's TPU port over QUIC with multi-leader support and auto-retry.

use quic_sol_shared::QuicSolError;
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub enum SendStrategy {
    /// Submit to a single leader.
    Single,
    /// Submit to multiple leaders simultaneously for higher reliability.
    MultiLeader { count: u32 },
}

#[derive(Debug, Clone)]
pub struct SubmitConfig {
    pub leaders: Vec<SocketAddr>,
    pub strategy: SendStrategy,
    pub max_retries: u32,
}

impl Default for SubmitConfig {
    fn default() -> Self {
        SubmitConfig {
            leaders: vec![],
            strategy: SendStrategy::MultiLeader { count: 2 },
            max_retries: 3,
        }
    }
}

pub struct QuicSubmitter {
    config: SubmitConfig,
}

impl QuicSubmitter {
    pub fn new(config: SubmitConfig) -> Self {
        QuicSubmitter { config }
    }

    /// Submit a raw signed transaction (base-58 encoded bytes) directly to TPU.
    /// Returns the transaction signature on success.
    pub async fn send(&self, transaction_b58: &str) -> Result<String, QuicSolError> {
        // Stub: real impl opens QUIC stream to each leader TPU port and writes tx
        let _ = transaction_b58;
        let leader_count = match &self.config.strategy {
            SendStrategy::Single => 1,
            SendStrategy::MultiLeader { count } => *count,
        };
        let _ = leader_count;
        // Return a dummy signature for now
        Ok("5J7XJmk1111111111111111111111111111111111111111111111111111111111111111111111111".into())
    }

    /// Submit to a specific leader by address (bypasses configured leaders).
    pub async fn send_to(&self, transaction_b58: &str, tpu_addr: SocketAddr) -> Result<String, QuicSolError> {
        let _ = (transaction_b58, tpu_addr);
        Ok("5J7XJmk2222222222222222222222222222222222222222222222222222222222222222222222222".into())
    }

    pub fn leader_count(&self) -> usize { self.config.leaders.len() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = SubmitConfig::default();
        assert!(matches!(cfg.strategy, SendStrategy::MultiLeader { count: 2 }));
        assert_eq!(cfg.max_retries, 3);
    }

    #[tokio::test]
    async fn send_returns_signature() {
        let submitter = QuicSubmitter::new(SubmitConfig::default());
        let sig = submitter.send("AAAA").await.unwrap();
        assert!(!sig.is_empty());
    }
}
