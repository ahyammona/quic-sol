//! Lightweight in-process metrics — atomic counters, no external dependency.
//!
//! Exposed on a plain HTTP /metrics endpoint in a separate task (future work).

use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

#[derive(Clone, Debug)]
pub struct Metrics(Arc<Inner>);

#[derive(Debug, Default)]
pub struct Inner {
    pub connections_active:  AtomicU64,
    pub sessions_total:      AtomicU64,
    pub gossip_events_rx:    AtomicU64,
    pub log_events_rx:       AtomicU64,
    pub price_ticks_rx:      AtomicU64,
    pub frames_sent:         AtomicU64,
    pub frames_dropped:      AtomicU64,
    pub upstream_reconnects: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics(Arc::new(Inner::default()))
    }
}

// Expose individual counters directly on Metrics for ergonomics.
impl std::ops::Deref for Metrics {
    type Target = Inner;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl Metrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            connections_active:  self.connections_active.load(Ordering::Relaxed),
            sessions_total:      self.sessions_total.load(Ordering::Relaxed),
            gossip_events_rx:    self.gossip_events_rx.load(Ordering::Relaxed),
            log_events_rx:       self.log_events_rx.load(Ordering::Relaxed),
            price_ticks_rx:      self.price_ticks_rx.load(Ordering::Relaxed),
            frames_sent:         self.frames_sent.load(Ordering::Relaxed),
            frames_dropped:      self.frames_dropped.load(Ordering::Relaxed),
            upstream_reconnects: self.upstream_reconnects.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    pub connections_active:  u64,
    pub sessions_total:      u64,
    pub gossip_events_rx:    u64,
    pub log_events_rx:       u64,
    pub price_ticks_rx:      u64,
    pub frames_sent:         u64,
    pub frames_dropped:      u64,
    pub upstream_reconnects: u64,
}