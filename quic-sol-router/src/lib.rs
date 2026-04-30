//! quic-sol-router — QUIC Connection Pool + Smart RPC Router
//!
//! Multiplexes RPC requests over a single QUIC connection,
//! auto-routing to the lowest-latency endpoint.

use quic_sol_shared::QuicSolError;

#[derive(Debug, Clone)]
pub enum RoutingStrategy {
    LowestLatency,
    RoundRobin,
    Primary,
}

pub struct QuicRouterBuilder {
    endpoints: Vec<String>,
    strategy: RoutingStrategy,
    max_streams: u32,
}

impl Default for QuicRouterBuilder {
    fn default() -> Self {
        QuicRouterBuilder {
            endpoints: vec![],
            strategy: RoutingStrategy::LowestLatency,
            max_streams: 200,
        }
    }
}

impl QuicRouterBuilder {
    pub fn new() -> Self { Self::default() }

    pub fn endpoints(mut self, endpoints: Vec<&str>) -> Self {
        self.endpoints = endpoints.into_iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn strategy(mut self, strategy: RoutingStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn max_streams(mut self, max_streams: u32) -> Self {
        self.max_streams = max_streams;
        self
    }

    pub async fn build(self) -> Result<QuicRouter, QuicSolError> {
        Ok(QuicRouter { endpoints: self.endpoints, max_streams: self.max_streams })
    }
}

pub struct QuicRouter {
    endpoints: Vec<String>,
    max_streams: u32,
}

impl QuicRouter {
    pub fn builder() -> QuicRouterBuilder { QuicRouterBuilder::new() }

    /// Get the balance for a base-58 pubkey string.
    pub async fn get_balance(&self, pubkey: &str) -> Result<u64, QuicSolError> {
        // Stub: real impl sends JSON-RPC over QUIC to lowest-latency endpoint
        let _ = pubkey;
        Ok(1_000_000)
    }

    pub fn endpoint_count(&self) -> usize { self.endpoints.len() }
    pub fn max_streams(&self) -> u32 { self.max_streams }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builder_creates_router() {
        let router = QuicRouter::builder()
            .endpoints(vec!["https://rpc1.example.com", "https://rpc2.example.com"])
            .max_streams(200)
            .build()
            .await
            .unwrap();
        assert_eq!(router.endpoint_count(), 2);
        assert_eq!(router.max_streams(), 200);
    }

    #[tokio::test]
    async fn get_balance_stub() {
        let router = QuicRouter::builder().build().await.unwrap();
        let bal = router.get_balance("11111111111111111111111111111111").await.unwrap();
        assert_eq!(bal, 1_000_000);
    }
}
