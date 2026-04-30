//! Cluster ingress — polls Solana RPC to emit real-time cluster events.
//!
//! Each gossip session spawns its own ClusterIngress, using the client-supplied
//! HTTP RPC URL. Frames are sent over an mpsc channel to the session's gossip
//! service instead of a shared broadcast bus.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

use crate::ingress::rpc::RpcPool;
use crate::metrics::Metrics;
use crate::proto::{ClusterSnapshot, ServerFrame, SlotLeaderEntry};

#[derive(Clone, Default)]
struct ValidatorInfo {
    stake: Option<u64>,
    tpu_addr: Option<String>,
    tvu_addr: Option<String>,
    version: Option<String>,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub struct ClusterIngress {
    pub rpc:     RpcPool,
    pub tx:      mpsc::Sender<ServerFrame>,
    pub metrics: Metrics,
}

impl ClusterIngress {
    pub async fn run(self) -> Result<()> {
        let mut attempt = 0u32;
        loop {
            if attempt > 0 {
                let delay = Duration::from_secs(2u64.pow(attempt.min(5)));
                warn!("cluster ingress: reconnect attempt {} in {:?}", attempt, delay);
                tokio::time::sleep(delay).await;
                self.metrics.upstream_reconnects.fetch_add(1, Relaxed);
            }
            if let Err(e) = self.run_once().await {
                error!("cluster ingress error: {}", e);
            }
            // Stop if the receiver is gone (session ended).
            if self.tx.is_closed() {
                debug!("cluster ingress: session ended, stopping");
                return Ok(());
            }
            attempt += 1;
        }
    }

    async fn run_once(&self) -> Result<()> {
        let mut leader_schedule: HashMap<u64, String> = HashMap::new();
        let mut last_epoch: u64 = 0;
        let mut last_slot: u64 = 0;
        let mut last_leader: Option<String> = None;
        let mut validator_info: HashMap<String, ValidatorInfo> = HashMap::new();

        let mut slot_interval     = tokio::time::interval(Duration::from_millis(200));
        let mut cluster_interval  = tokio::time::interval(Duration::from_secs(15));
        let mut schedule_interval = tokio::time::interval(Duration::from_secs(5));

        slot_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        cluster_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        schedule_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!("cluster ingress: starting");

        match self.fetch_epoch_info().await {
            Ok((epoch, slot_index, abs_slot)) => {
                last_epoch = epoch;
                last_slot  = abs_slot;
                if let Ok(sched) = self.fetch_leader_schedule(Some(abs_slot)).await {
                    leader_schedule = sched;
                    info!(epoch, leaders = leader_schedule.len(), "cluster ingress: leader schedule loaded");
                }
            }
            Err(e) => warn!("cluster ingress: warm-up epoch info failed: {}", e),
        }
        match self.fetch_validator_info().await {
            Ok(info) => {
                info!(validators = info.len(), "cluster ingress: validator info loaded");
                validator_info = info;
            }
            Err(e) => warn!("cluster ingress: warm-up validator info failed: {}", e),
        }

        loop {
            // Stop if the session's receiver has been dropped.
            if self.tx.is_closed() {
                return Ok(());
            }

            tokio::select! {
                _ = slot_interval.tick() => {
                    if self.rpc.latency_snapshot().await.iter().all(|(_, _, h)| !h) {
                        continue;
                    }

                    match self.rpc.call("getSlot", serde_json::json!([{"commitment": "processed"}])).await {
                        Ok(v) => {
                            let slot = v.as_u64().unwrap_or(0);
                            if slot > last_slot {
                                last_slot = slot;
                                self.metrics.gossip_events_rx.fetch_add(1, Relaxed);

                                if let Some(leader) = leader_schedule.get(&slot) {
                                    let leader = leader.clone();
                                    if last_leader.as_deref() != Some(&leader) {
                                        last_leader = Some(leader.clone());
                                        let vi = validator_info.get(&leader).cloned().unwrap_or_default();
                                        let frame = ServerFrame::GossipLeaderChange {
                                            slot,
                                            leader,
                                            tpu_addr: vi.tpu_addr,
                                            tvu_addr: vi.tvu_addr,
                                            stake:    vi.stake,
                                            version:  vi.version,
                                        };
                                        if self.tx.send(frame).await.is_ok() {
                                            self.metrics.frames_sent.fetch_add(1, Relaxed);
                                        }
                                    }
                                }

                                let slot_in_epoch = slot % 432_000;
                                let needs_refresh = slot_in_epoch < 100 || leader_schedule.len() < 1000;

                                if needs_refresh {
                                    match self.fetch_epoch_info().await {
                                        Ok((epoch, _, abs_slot)) => {
                                            if epoch != last_epoch || leader_schedule.len() < 1000 {
                                                last_epoch = epoch;
                                                if let Ok(sched) = self.fetch_leader_schedule(Some(abs_slot)).await {
                                                    let count = sched.len();
                                                    leader_schedule = sched;
                                                    info!(epoch, leaders = count, "cluster ingress: leader schedule refreshed");
                                                }
                                            }
                                        }
                                        Err(e) => debug!("cluster ingress: epoch info failed: {}", e),
                                    }
                                }
                            }
                        }
                        Err(e) => debug!("cluster ingress: getSlot failed: {}", e),
                    }
                }

                _ = schedule_interval.tick() => {
                    self.emit_leader_schedule(last_slot, &leader_schedule, &validator_info).await;
                }

                _ = cluster_interval.tick() => {
                    self.emit_cluster_snapshot(&mut validator_info).await;
                }
            }
        }
    }

    async fn fetch_epoch_info(&self) -> Result<(u64, u64, u64)> {
        let info = self.rpc
            .call("getEpochInfo", serde_json::json!([{"commitment": "confirmed"}]))
            .await
            .context("getEpochInfo")?;

        let epoch      = info["epoch"].as_u64().context("epoch")?;
        let slot_index = info["slotIndex"].as_u64().unwrap_or(0);
        let abs_slot   = info["absoluteSlot"].as_u64().unwrap_or(0);
        Ok((epoch, slot_index, abs_slot))
    }

    async fn fetch_leader_schedule(&self, slot: Option<u64>) -> Result<HashMap<u64, String>> {
        let params = match slot {
            Some(s) => serde_json::json!([s, {"commitment": "confirmed"}]),
            None    => serde_json::json!([null, {"commitment": "confirmed"}]),
        };

        let resp = self.rpc
            .call_large("getLeaderSchedule", params)
            .await
            .context("getLeaderSchedule")?;

        let epoch_info = self.rpc
            .call("getEpochInfo", serde_json::json!([{"commitment": "confirmed"}]))
            .await
            .context("getEpochInfo for schedule")?;

        let abs_slot   = epoch_info["absoluteSlot"].as_u64().unwrap_or(0);
        let slot_index = epoch_info["slotIndex"].as_u64().unwrap_or(0);
        let epoch_first_slot = abs_slot.saturating_sub(slot_index);

        let mut schedule: HashMap<u64, String> = HashMap::new();
        if let Some(obj) = resp.as_object() {
            for (pubkey, slots_val) in obj {
                if let Some(slots_arr) = slots_val.as_array() {
                    for slot_val in slots_arr {
                        if let Some(rel_slot) = slot_val.as_u64() {
                            let abs = epoch_first_slot + rel_slot;
                            schedule.insert(abs, pubkey.clone());
                        }
                    }
                }
            }
        }
        Ok(schedule)
    }

    async fn emit_leader_schedule(
        &self,
        current_slot: u64,
        schedule: &HashMap<u64, String>,
        validator_info: &HashMap<String, ValidatorInfo>,
    ) {
        if schedule.is_empty() { return; }

        let mut upcoming: Vec<SlotLeaderEntry> = schedule
            .iter()
            .filter(|(slot, _)| **slot >= current_slot)
            .take(200)
            .map(|(slot, leader)| {
                let slots_away = slot.saturating_sub(current_slot);
                let eta_ms = slots_away * 400;
                let vi = validator_info.get(leader).cloned().unwrap_or_default();
                SlotLeaderEntry {
                    slot: *slot,
                    leader: leader.clone(),
                    eta_ms,
                    tpu_addr: vi.tpu_addr,
                    tvu_addr: vi.tvu_addr,
                    stake:    vi.stake,
                    version:  vi.version,
                }
            })
            .collect();

        upcoming.sort_by_key(|e| e.slot);
        upcoming.truncate(20);

        if upcoming.is_empty() { return; }

        let frame = ServerFrame::GossipLeaderSchedule { upcoming };
        if self.tx.send(frame).await.is_ok() {
            self.metrics.frames_sent.fetch_add(1, Relaxed);
        }
    }

    async fn fetch_validator_info(&self) -> Result<HashMap<String, ValidatorInfo>> {
        let mut info: HashMap<String, ValidatorInfo> = HashMap::new();

        match self.rpc.call_large("getVoteAccounts", serde_json::json!([{"commitment": "confirmed"}])).await {
            Ok(v) => {
                let empty = vec![];
                let current = v["current"].as_array().unwrap_or(&empty);
                for va in current {
                    if let Some(pubkey) = va["nodePubkey"].as_str() {
                        let stake = va["activatedStake"].as_u64();
                        info.entry(pubkey.to_string()).or_default().stake = stake;
                    }
                }
            }
            Err(e) => debug!("fetch_validator_info: getVoteAccounts failed: {}", e),
        }

        match self.rpc.call_large("getClusterNodes", serde_json::json!([])).await {
            Ok(v) => {
                if let Some(nodes) = v.as_array() {
                    for node in nodes {
                        if let Some(pubkey) = node["pubkey"].as_str() {
                            let entry = info.entry(pubkey.to_string()).or_default();
                            if let Some(tpu) = node["tpu"].as_str() {
                                entry.tpu_addr = Some(tpu.to_string());
                            }
                            if let Some(tvu) = node["tvu"].as_str() {
                                entry.tvu_addr = Some(tvu.to_string());
                            }
                            if let Some(ver) = node["version"].as_str() {
                                entry.version = Some(ver.to_string());
                            }
                        }
                    }
                }
            }
            Err(e) => debug!("fetch_validator_info: getClusterNodes failed: {}", e),
        }

        Ok(info)
    }

    async fn emit_cluster_snapshot(&self, validator_info: &mut HashMap<String, ValidatorInfo>) {
        let (epoch, slot_index, _) = match self.fetch_epoch_info().await {
            Ok(v) => v,
            Err(e) => { debug!("cluster snapshot: getEpochInfo failed: {}", e); return; }
        };

        match self.fetch_validator_info().await {
            Ok(fresh) => { *validator_info = fresh; }
            Err(e) => debug!("cluster snapshot: validator info refresh failed: {}", e),
        }

        let active_validators = validator_info.values().filter(|v| v.stake.is_some()).count() as u32;
        let total_stake: u64  = validator_info.values().filter_map(|v| v.stake).sum();

        let estimated_tps = match self.rpc.call("getRecentPerformanceSamples", serde_json::json!([4])).await {
            Ok(v) => {
                let samples = v.as_array().cloned().unwrap_or_default();
                if samples.is_empty() { 0.0 } else {
                    let total_txs: u64  = samples.iter().filter_map(|s| s["numTransactions"].as_u64()).sum();
                    let total_secs: u64 = samples.iter().filter_map(|s| s["samplePeriodSecs"].as_u64()).sum();
                    if total_secs > 0 { total_txs as f64 / total_secs as f64 } else { 0.0 }
                }
            }
            Err(e) => { debug!("cluster snapshot: getRecentPerformanceSamples failed: {}", e); 0.0 }
        };

        let snap = ServerFrame::GossipClusterInfo(ClusterSnapshot {
            active_validators,
            total_stake,
            estimated_tps,
            epoch,
            slot_index,
            timestamp_ms: unix_ms(),
        });

        if self.tx.send(snap).await.is_ok() {
            self.metrics.frames_sent.fetch_add(1, Relaxed);
        }
    }
}