//! Price service — aggregates ticks from all exchanges, computes a
//! confidence-weighted mid-price, and pushes to subscribed clients.
//!
//! Aggregation strategy:
//!   1. Collect ticks from the bus for a short window (50 ms).
//!   2. Group by pair, weight each source by 24h volume.
//!   3. Compute volume-weighted average price (VWAP) and spread as confidence.
//!   4. Emit one [`ServerFrame::PriceTick`] per pair per window.

use quinn::SendStream;
use std::collections::HashMap;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::error::{Result, ServerError};
use crate::proto::{encode_frame, PriceTick, ServerFrame, ServiceParams};

/// Aggregation window: collect ticks for this long before emitting.
const AGG_WINDOW_MS: u64 = 50;

pub struct PriceService {
    bus: broadcast::Sender<ServerFrame>,
}

impl PriceService {
    pub fn new(bus: broadcast::Sender<ServerFrame>) -> Self {
        PriceService { bus }
    }

    pub async fn serve(
        &self,
        mut send: SendStream,
        params: ServiceParams,
        session_id: u64,
    ) -> Result<()> {
        let pair_filter: Vec<String> = match params {
            ServiceParams::Price { pairs } => pairs,
            _ => return Err(ServerError::Protocol("wrong params for price service".into())),
        };

        let mut rx = self.bus.subscribe();
        // Buffer: pair → list of raw ticks from individual exchanges
        let mut window: HashMap<String, Vec<PriceTick>> = HashMap::new();
        let agg_interval = std::time::Duration::from_millis(AGG_WINDOW_MS);
        let mut ticker = tokio::time::interval(agg_interval);

        debug!(
            session = session_id,
            pairs   = ?pair_filter,
            "price service: streaming started"
        );

        loop {
            tokio::select! {
                result = rx.recv() => {
                    let frame = match result {
                        Ok(f) => f,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!(session = session_id, "price: lagged {} ticks", n);
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    };

                    if let ServerFrame::PriceTick(tick) = frame {
                        // Filter to subscribed pairs (empty = all).
                        if !pair_filter.is_empty() && !pair_filter.contains(&tick.pair) {
                            continue;
                        }
                        window.entry(tick.pair.clone()).or_default().push(tick);
                    }
                }

                _ = ticker.tick() => {
                    // Aggregate and emit one tick per pair.
                    for (pair, ticks) in window.drain() {
                        if ticks.is_empty() { continue; }
                        let aggregated = aggregate(&pair, ticks);
                        let frame = ServerFrame::PriceTick(aggregated);
                        let bytes = encode_frame(&frame)
                            .map_err(|e| ServerError::Protocol(e.to_string()))?;
                        if send.write_all(&bytes).await.is_err() {
                            debug!(session = session_id, "price: client disconnected");
                            return Ok(());
                        }
                    }
                }
            }
        }

        debug!(session = session_id, "price service: stream ended");
        Ok(())
    }
}

/// Volume-weighted average price across all exchange sources in a window.
fn aggregate(pair: &str, ticks: Vec<PriceTick>) -> PriceTick {
    // Collect all sources (one per tick, may have duplicates from same exchange).
    // For each exchange, take the last tick seen in the window.
    let mut by_exchange: HashMap<String, &PriceTick> = HashMap::new();
    for tick in &ticks {
        for src in &tick.sources {
            by_exchange
                .entry(src.exchange.clone())
                .and_modify(|e| *e = tick)
                .or_insert(tick);
        }
    }

    let sources: Vec<_> = ticks
        .iter()
        .flat_map(|t| t.sources.iter().cloned())
        .collect();

    // Deduplicate sources by exchange, keep last.
    let mut deduped: HashMap<String, crate::proto::PriceSource> = HashMap::new();
    for src in sources {
        deduped.insert(src.exchange.clone(), src);
    }
    let sources: Vec<_> = deduped.into_values().collect();

    // VWAP: Σ(price × vol) / Σ(vol)
    let total_vol: f64 = sources.iter().map(|s| s.volume_24h).sum();
    let vwap = if total_vol > 0.0 {
        sources.iter().map(|s| s.price * s.volume_24h).sum::<f64>() / total_vol
    } else {
        sources.iter().map(|s| s.price).sum::<f64>() / sources.len() as f64
    };

    // Confidence: 1 - (max_price - min_price) / vwap  (1.0 = perfect agreement)
    let prices: Vec<f64> = sources.iter().map(|s| s.price).collect();
    let spread = prices.iter().cloned().fold(f64::NAN, f64::max)
        - prices.iter().cloned().fold(f64::NAN, f64::min);
    let confidence = if vwap > 0.0 { (1.0 - spread / vwap).max(0.0) } else { 0.0 };

    PriceTick {
        pair: pair.to_string(),
        price: vwap,
        confidence,
        sources,
        timestamp_ms: unix_ms(),
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::PriceSource;

    fn make_tick(exchange: &str, price: f64, vol: f64) -> PriceTick {
        PriceTick {
            pair: "SOLUSDC".into(),
            price,
            confidence: 0.0,
            sources: vec![PriceSource {
                exchange: exchange.into(),
                price,
                volume_24h: vol,
            }],
            timestamp_ms: 0,
        }
    }

    #[test]
    fn vwap_weighted_by_volume() {
        let ticks = vec![
            make_tick("binance",  150.0, 1_000_000.0),
            make_tick("coinbase", 152.0,   500_000.0),
            make_tick("okx",      151.0,   500_000.0),
        ];
        let agg = aggregate("SOLUSDC", ticks);
        // Expected: (150*1M + 152*0.5M + 151*0.5M) / 2M = 150.75
        assert!((agg.price - 150.75).abs() < 0.01, "price={}", agg.price);
    }

    #[test]
    fn confidence_perfect_agreement() {
        let ticks = vec![
            make_tick("binance",  150.0, 1_000_000.0),
            make_tick("coinbase", 150.0,   500_000.0),
        ];
        let agg = aggregate("SOLUSDC", ticks);
        assert!((agg.confidence - 1.0).abs() < 1e-9, "confidence={}", agg.confidence);
    }

    #[test]
    fn confidence_spread() {
        // 10% spread → confidence ≈ 0.9
        let ticks = vec![
            make_tick("binance",  100.0, 1_000_000.0),
            make_tick("coinbase", 110.0, 1_000_000.0),
        ];
        let agg = aggregate("SOLUSDC", ticks);
        assert!(agg.confidence < 0.95, "confidence={}", agg.confidence);
    }
}
