//! RPC pool — maintains connections to multiple Solana RPC endpoints
//! and routes requests to the one with lowest current latency.

use anyhow::Result;
use serde_json::Value;
use std::{sync::Arc, time::{Duration, Instant}};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::metrics::Metrics;

const CALL_TIMEOUT_SECS: u64 = 10;
const LARGE_CALL_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone)]
struct Endpoint {
    url: String,
    latency_ms: f64,
    healthy: bool,
}

#[derive(Clone)]
pub struct RpcPool(Arc<Inner>);

struct Inner {
    endpoints: RwLock<Vec<Endpoint>>,
    client: reqwest::Client,
    client_large: reqwest::Client,
}

impl RpcPool {
    /// Full pool with background health-check task — used for multi-endpoint configs.
    pub async fn new(urls: Vec<String>, metrics: Metrics) -> Result<Self> {
        let endpoints: Vec<Endpoint> = urls
            .into_iter()
            .map(|url| Endpoint { url, latency_ms: 100.0, healthy: true })
            .collect();

        if endpoints.is_empty() {
            anyhow::bail!("RPC pool requires at least one endpoint");
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(CALL_TIMEOUT_SECS))
            .gzip(true)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;

        let client_large = reqwest::Client::builder()
            .timeout(Duration::from_secs(LARGE_CALL_TIMEOUT_SECS))
            .gzip(true)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;

        let pool = RpcPool(Arc::new(Inner {
            endpoints: RwLock::new(endpoints),
            client,
            client_large,
        }));

        let pool_clone = pool.clone();
        tokio::spawn(async move {
            pool_clone.health_loop(metrics).await;
        });

        Ok(pool)
    }

    /// Lightweight single-endpoint pool — no background task, used for per-session pools.
    pub fn new_single(url: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(CALL_TIMEOUT_SECS))
            .gzip(true)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;

        let client_large = reqwest::Client::builder()
            .timeout(Duration::from_secs(LARGE_CALL_TIMEOUT_SECS))
            .gzip(true)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;

        Ok(RpcPool(Arc::new(Inner {
            endpoints: RwLock::new(vec![Endpoint {
                url,
                latency_ms: 100.0,
                healthy: true,
            }]),
            client,
            client_large,
        })))
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.call_inner(method, params, false).await
    }

    pub async fn call_large(&self, method: &str, params: Value) -> Result<Value> {
        self.call_inner(method, params, true).await
    }

    async fn call_inner(&self, method: &str, params: Value, large: bool) -> Result<Value> {
        let url = self.pick_url().await?;

        let t0 = Instant::now();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        });

        let client = if large { &self.0.client_large } else { &self.0.client };

        let resp = client
            .post(&url)
            .json(&body)
            .send()
            .await?
            .json::<Value>()
            .await?;

        let elapsed = t0.elapsed().as_millis() as f64;
        self.update_latency(&url, elapsed).await;

        if let Some(err) = resp.get("error") {
            anyhow::bail!("RPC error: {}", err);
        }

        Ok(resp["result"].clone())
    }

    async fn pick_url(&self) -> Result<String> {
        let endpoints = self.0.endpoints.read().await;
        endpoints
            .iter()
            .filter(|e| e.healthy)
            .min_by(|a, b| a.latency_ms.partial_cmp(&b.latency_ms).unwrap())
            .map(|e| e.url.clone())
            .ok_or_else(|| anyhow::anyhow!("no healthy RPC endpoints"))
    }

    pub async fn latency_snapshot(&self) -> Vec<(String, f64, bool)> {
        let endpoints = self.0.endpoints.read().await;
        endpoints
            .iter()
            .map(|e| (e.url.clone(), e.latency_ms, e.healthy))
            .collect()
    }

    async fn update_latency(&self, url: &str, latency_ms: f64) {
        let mut endpoints = self.0.endpoints.write().await;
        if let Some(ep) = endpoints.iter_mut().find(|e| e.url == url) {
            ep.latency_ms = 0.2 * latency_ms + 0.8 * ep.latency_ms;
        }
    }

    async fn health_loop(&self, metrics: Metrics) {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        loop {
            interval.tick().await;
            let urls: Vec<String> = {
                let eps = self.0.endpoints.read().await;
                eps.iter().map(|e| e.url.clone()).collect()
            };
            for url in &urls {
                let healthy = self.probe(url).await;
                let mut eps = self.0.endpoints.write().await;
                if let Some(ep) = eps.iter_mut().find(|e| &e.url == url) {
                    if ep.healthy != healthy {
                        if healthy {
                            info!("RPC endpoint recovered: {}", url);
                        } else {
                            warn!("RPC endpoint degraded: {}", url);
                            metrics.upstream_reconnects.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        ep.healthy = healthy;
                    }
                    debug!(
                        url = %url,
                        latency_ms = ep.latency_ms,
                        healthy = ep.healthy,
                        "RPC endpoint status"
                    );
                }
            }

            let snapshot = self.latency_snapshot().await;
            for (url, latency, healthy) in &snapshot {
                let short_url = url.split('?').next().unwrap_or(url);
                info!(
                    "RPC latency: {} — {:.1}ms [{}]",
                    short_url,
                    latency,
                    if *healthy { "healthy" } else { "degraded" }
                );
            }
        }
    }

    async fn probe(&self, url: &str) -> bool {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "getHealth", "params": []
        });
        match self.0.client.post(url).json(&body).send().await {
            Ok(r) => r.status().is_success(),
            Err(_) => false,
        }
    }
}