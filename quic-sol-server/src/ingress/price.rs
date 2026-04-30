//! Price ingress — maintains concurrent WebSocket connections to Binance,
//! Coinbase, and OKX, normalises ticks, and publishes to the broadcast bus.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::config::PriceConfig;
use crate::metrics::Metrics;
use crate::proto::{PriceSource, PriceTick, ServerFrame};

pub struct PriceIngress {
    cfg: PriceConfig,
    tx: broadcast::Sender<ServerFrame>,
    metrics: Metrics,
}

impl PriceIngress {
    pub fn new(cfg: PriceConfig, tx: broadcast::Sender<ServerFrame>, metrics: Metrics) -> Self {
        PriceIngress { cfg, tx, metrics }
    }

    pub async fn run(self) -> Result<()> {
        // Run all three exchange connections concurrently.
        let binance_task = {
            let tx = self.tx.clone();
            let url = self.cfg.binance_url.clone();
            let pairs = self.cfg.pairs.clone();
            let m = self.metrics.clone();
            tokio::spawn(async move { binance_loop(url, pairs, tx, m).await })
        };
        let coinbase_task = {
            let tx = self.tx.clone();
            let url = self.cfg.coinbase_url.clone();
            let pairs = self.cfg.pairs.clone();
            let m = self.metrics.clone();
            tokio::spawn(async move { coinbase_loop(url, pairs, tx, m).await })
        };
        let okx_task = {
            let tx = self.tx.clone();
            let url = self.cfg.okx_url.clone();
            let pairs = self.cfg.pairs.clone();
            let m = self.metrics.clone();
            tokio::spawn(async move { okx_loop(url, pairs, tx, m).await })
        };

        // If all three fail, return an error.
        let (a, b, c) = tokio::join!(binance_task, coinbase_task, okx_task);
        a??; b??; c??;
        Ok(())
    }
}

// ── Binance ───────────────────────────────────────────────────────────────────

async fn binance_loop(
    url: String,
    pairs: Vec<String>,
    tx: broadcast::Sender<ServerFrame>,
    metrics: Metrics,
) -> Result<()> {
    let mut attempt = 0u32;
    loop {
        if let Err(e) = binance_once(&url, &pairs, &tx, &metrics).await {
            // First failure is ERROR, subsequent are WARN to avoid log spam
            // when Binance is geo-blocked or the network is flaky.
            if attempt == 0 {
          //      error!("binance ingress error: {}", e);
            } else {
            //    warn!("binance ingress error (attempt {}): {}", attempt + 1, e);
            }
            // Exponential backoff: 5s, 10s, 20s, 40s, capped at 60s
            let delay = Duration::from_secs(5 * 2u64.pow(attempt.min(3)));
            tokio::time::sleep(delay).await;
            attempt += 1;
            metrics.upstream_reconnects.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            // Clean disconnect — reset backoff
            attempt = 0;
        }
    }
}

async fn binance_once(
    url: &str,
    pairs: &[String],
    tx: &broadcast::Sender<ServerFrame>,
    metrics: &Metrics,
) -> Result<()> {
    // Build stream URL: e.g. wss://stream.binance.com:9443/stream?streams=solusdc@ticker/btcusdc@ticker
    let streams: Vec<String> = pairs.iter()
        .map(|p| format!("{}@ticker", p.to_lowercase()))
        .collect();
    let full_url = format!("{}?streams={}", url, streams.join("/"));

//    info!("binance ingress: connecting to {}", url);
    let (ws, _) = connect_async(&full_url).await.context("binance WS connect")?;
    let (mut _sink, mut stream) = ws.split();

    while let Some(msg) = stream.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(data) = v.get("data") {
                        let symbol = data["s"].as_str().unwrap_or("").to_uppercase();
                        let price  = data["c"].as_str()
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let vol = data["v"].as_str()
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);

                        if price > 0.0 {
                            publish_tick(tx, metrics, symbol, price, vol, "binance");
                        }
                    }
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }
    Ok(())
}

// ── Coinbase ──────────────────────────────────────────────────────────────────

async fn coinbase_loop(
    url: String,
    pairs: Vec<String>,
    tx: broadcast::Sender<ServerFrame>,
    metrics: Metrics,
) -> Result<()> {
    loop {
        if let Err(e) = coinbase_once(&url, &pairs, &tx, &metrics).await {
            error!("coinbase ingress error: {}", e);
            tokio::time::sleep(Duration::from_secs(5)).await;
            metrics.upstream_reconnects.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

async fn coinbase_once(
    url: &str,
    pairs: &[String],
    tx: &broadcast::Sender<ServerFrame>,
    metrics: &Metrics,
) -> Result<()> {
    info!("coinbase ingress: connecting");
    let (ws, _) = connect_async(url).await.context("coinbase WS connect")?;
    let (mut sink, mut stream) = ws.split();

    // Coinbase product IDs use dashes: SOL-USDC
    let product_ids: Vec<String> = pairs.iter()
        .map(|p| {
            if p.ends_with("USDC") {
                format!("{}-USDC", &p[..p.len()-4])
            } else { p.clone() }
        })
        .collect();

    let subscribe = serde_json::json!({
        "type": "subscribe",
        "product_ids": product_ids,
        "channel": "ticker"
    });
    sink.send(Message::Text(subscribe.to_string())).await
        .context("coinbase subscribe")?;

    while let Some(msg) = stream.next().await {
        if let Ok(Message::Text(text)) = msg {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if v["type"] == "ticker" {
                    let symbol = v["product_id"].as_str()
                        .unwrap_or("")
                        .replace('-', "")
                        .to_uppercase();
                    let price = v["price"].as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let vol = v["volume_24_h"].as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    if price > 0.0 {
                        publish_tick(tx, metrics, symbol, price, vol, "coinbase");
                    }
                }
            }
        }
    }
    Ok(())
}

// ── OKX ───────────────────────────────────────────────────────────────────────

async fn okx_loop(
    url: String,
    pairs: Vec<String>,
    tx: broadcast::Sender<ServerFrame>,
    metrics: Metrics,
) -> Result<()> {
    loop {
        if let Err(e) = okx_once(&url, &pairs, &tx, &metrics).await {
            error!("okx ingress error: {}", e);
            tokio::time::sleep(Duration::from_secs(5)).await;
            metrics.upstream_reconnects.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

async fn okx_once(
    url: &str,
    pairs: &[String],
    tx: &broadcast::Sender<ServerFrame>,
    metrics: &Metrics,
) -> Result<()> {
    info!("okx ingress: connecting");
    let (ws, _) = connect_async(url).await.context("okx WS connect")?;
    let (mut sink, mut stream) = ws.split();

    // OKX inst IDs: SOL-USDC-SPOT
    let inst_ids: Vec<serde_json::Value> = pairs.iter()
        .map(|p| {
            let base = if p.ends_with("USDC") { &p[..p.len()-4] } else { p.as_str() };
            serde_json::json!({ "channel": "tickers", "instId": format!("{}-USDC", base) })
        })
        .collect();

    let subscribe = serde_json::json!({
        "op": "subscribe",
        "args": inst_ids
    });
    sink.send(Message::Text(subscribe.to_string())).await
        .context("okx subscribe")?;

    let mut ping_interval = tokio::time::interval(Duration::from_secs(25));

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            if v["arg"]["channel"] == "tickers" {
                                if let Some(data) = v["data"].as_array().and_then(|a| a.first()) {
                                    let symbol = data["instId"].as_str()
                                        .unwrap_or("")
                                        .replace('-', "")
                                        .replace("SPOT", "")
                                        .to_uppercase();
                                    let price = data["last"].as_str()
                                        .and_then(|s| s.parse::<f64>().ok())
                                        .unwrap_or(0.0);
                                    let vol = data["vol24h"].as_str()
                                        .and_then(|s| s.parse::<f64>().ok())
                                        .unwrap_or(0.0);
                                    if price > 0.0 {
                                        publish_tick(tx, metrics, symbol, price, vol, "okx");
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                let _ = sink.send(Message::Text("ping".into())).await;
            }
        }
    }
    Ok(())
}

// ── Shared helper ─────────────────────────────────────────────────────────────

fn publish_tick(
    tx: &broadcast::Sender<ServerFrame>,
    metrics: &Metrics,
    pair: String,
    price: f64,
    volume_24h: f64,
    exchange: &str,
) {
    use std::sync::atomic::Ordering::Relaxed;

    let tick = PriceTick {
        pair: pair.clone(),
        price,
        confidence: 0.0, // aggregation happens in the price service
        sources: vec![PriceSource {
            exchange: exchange.to_string(),
            price,
            volume_24h,
        }],
        timestamp_ms: unix_ms(),
    };

    if tx.send(ServerFrame::PriceTick(tick)).is_ok() {
        metrics.price_ticks_rx.fetch_add(1, Relaxed);
        metrics.frames_sent.fetch_add(1, Relaxed);
    } else {
        metrics.frames_dropped.fetch_add(1, Relaxed);
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}