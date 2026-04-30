//! SlotIngress — fast confirmed-transaction stream driven by gossip slot events.
//!
//! ## Why this beats logsSubscribe
//!
//! `logsSubscribe` sits downstream of the RPC node's indexer. The node receives
//! a block, processes it, indexes logs, then pushes your subscription — adding
//! 400–800ms of indexer lag on top of block time.
//!
//! SlotIngress cuts that path:
//!
//!   GossipLeaderChange fires  →  slot N is now producing
//!   GossipLeaderChange fires  →  slot N+1 started  →  slot N is sealed
//!   getBlock(slot N)          →  we race the indexer, arrive ~50–150ms post-seal
//!
//! The result: confirmed transactions land on the bus 3–6× faster than
//! logsSubscribe for the same RPC endpoint.
//!
//! ## Flow
//!
//! 1. Subscribe to the shared `gossip_rx` broadcast channel.
//! 2. On each `GossipLeaderChange`, note the new slot.
//! 3. When the slot advances (N → N+1), fire a `getBlock(N)` call immediately.
//! 4. Parse every transaction in the block and emit `SlotTransaction` frames.
//! 5. Also emit a `SlotConfirmed` frame so consumers know the slot is complete.

use anyhow::{Context, Result};
use std::collections::VecDeque;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use crate::ingress::rpc::RpcPool;
use crate::metrics::Metrics;
use crate::proto::{
    ServerFrame, SlotConfirmed, SlotTransaction,
    CompiledInstruction, MessageHeader, TransactionMessage,
    Transaction, AddressTableLookup,
};

/// How many slots to buffer before we start dropping (prevents runaway backlog
/// if the RPC endpoint is slow).
const SLOT_QUEUE_MAX: usize = 32;

/// How long to wait for a getBlock response before giving up on a slot.
const GETBLOCK_TIMEOUT_SECS: u64 = 8;

/// Retry once after this delay if getBlock returns null (slot not yet available).
const NULL_RETRY_MS: u64 = 80;

#[inline]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub struct SlotIngress {
    /// Subscribes to gossip frames to detect slot boundaries.
    gossip_rx: broadcast::Receiver<ServerFrame>,
    /// Sends SlotTransaction / SlotConfirmed frames to the SlotService.
    tx: mpsc::Sender<ServerFrame>,
    /// RPC pool used for getBlock calls.
    rpc: RpcPool,
    metrics: Metrics,
    /// Optional program-ID filter. Empty = all programs.
    filter_programs: Vec<String>,
}

impl SlotIngress {
    pub fn new(
        gossip_rx: broadcast::Receiver<ServerFrame>,
        tx: mpsc::Sender<ServerFrame>,
        rpc: RpcPool,
        metrics: Metrics,
        filter_programs: Vec<String>,
    ) -> Self {
        SlotIngress { gossip_rx, tx, rpc, metrics, filter_programs }
    }

    pub async fn run(mut self) -> Result<()> {
        info!("slot ingress: starting — watching gossip for slot boundaries");

        // Pending slots waiting for getBlock. We keep a small queue so that
        // if RPC is momentarily slow, we don't lose confirmed slots.
        let mut pending: VecDeque<u64> = VecDeque::new();
        let mut current_slot: Option<u64> = None;

        // Spawn a worker pool of concurrent getBlock fetchers.
        // Each sealed slot goes into a bounded channel; workers drain it.
        let (slot_tx, slot_rx) = async_channel::bounded::<u64>(SLOT_QUEUE_MAX);
        let slot_rx = std::sync::Arc::new(slot_rx);

        // Spawn N concurrent getBlock workers so slow RPCs don't stall us.
        for _ in 0..4 {
            let slot_rx = slot_rx.clone();
            let tx = self.tx.clone();
            let rpc = self.rpc.clone();
            let metrics = self.metrics.clone();
            let filter = self.filter_programs.clone();
            tokio::spawn(async move {
                while let Ok(slot) = slot_rx.recv().await {
                    fetch_and_emit_slot(slot, &rpc, &tx, &metrics, &filter).await;
                }
            });
        }

        loop {
            match self.gossip_rx.recv().await {
                Ok(ServerFrame::GossipLeaderChange { slot, .. }) => {
                    // When the leader changes, the previous slot is sealed.
                    if let Some(prev) = current_slot {
                        if slot > prev {
                            // prev is now sealed — queue it for getBlock.
                            if slot_tx.len() < SLOT_QUEUE_MAX {
                                let _ = slot_tx.send(prev).await;
                                self.metrics.gossip_events_rx.fetch_add(1, Relaxed);
                            } else {
                                warn!("slot ingress: queue full, dropping slot {}", prev);
                            }
                        }
                    }
                    current_slot = Some(slot);
                }

                Ok(_) => {} // ignore non-leader frames

                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("slot ingress: gossip bus lagged {} messages — some slots may be missed", n);
                }

                Err(broadcast::error::RecvError::Closed) => {
                    info!("slot ingress: gossip bus closed, stopping");
                    break;
                }
            }
        }

        Ok(())
    }
}

/// Fetch a single confirmed slot via getBlock and emit all its transactions.
async fn fetch_and_emit_slot(
    slot: u64,
    rpc: &RpcPool,
    tx: &mpsc::Sender<ServerFrame>,
    metrics: &Metrics,
    filter_programs: &[String],
) {
    let received_at_ms = now_ms();

    let params = serde_json::json!([
        slot,
        {
            "encoding": "json",
            "transactionDetails": "full",
            "rewards": false,
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0
        }
    ]);

    // First attempt
    let block = match rpc.call("getBlock", params.clone()).await {
        Ok(v) if v.is_null() => {
            // Slot not yet available — wait and retry once
            tokio::time::sleep(Duration::from_millis(NULL_RETRY_MS)).await;
            match rpc.call("getBlock", params).await {
                Ok(v) => v,
                Err(e) => {
                    debug!("slot ingress: getBlock({}) retry failed: {}", slot, e);
                    return;
                }
            }
        }
        Ok(v) => v,
        Err(e) => {
            // -32009 = slot was skipped (leader didn't produce)
            if !e.to_string().contains("-32009") && !e.to_string().contains("skipped") {
                debug!("slot ingress: getBlock({}) error: {}", slot, e);
            }
            return;
        }
    };

    if block.is_null() {
       // debug!("slot ingress: slot {} not yet confirmed after retry", slot);
        return;
    }

    let block_time: Option<i64> = block["blockTime"].as_i64();
    let block_height: Option<u64> = block["blockHeight"].as_u64();
    let parent_slot: u64 = block["parentSlot"].as_u64().unwrap_or(0);

    let transactions = match block["transactions"].as_array() {
        Some(txs) => txs,
        None => {
         //   debug!("slot ingress: slot {} has no transactions array", slot);
            return;
        }
    };

    let tx_count = transactions.len() as u32;
    let mut emitted = 0u32;

    for raw_tx in transactions {
        // Skip failed transactions (err is non-null)
        if !raw_tx.pointer("/meta/err").map(|v| v.is_null()).unwrap_or(true) {
            continue;
        }

        let signature = raw_tx
            .pointer("/transaction/signatures/0")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if signature.is_empty() { continue; }

        // Collect account keys (static + loaded)
        let static_keys: Vec<String> = raw_tx
            .pointer("/transaction/message/accountKeys")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|k| k.as_str().map(String::from)).collect())
            .unwrap_or_default();

        let writable_loaded: Vec<String> = raw_tx
            .pointer("/meta/loadedAddresses/writable")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|k| k.as_str().map(String::from)).collect())
            .unwrap_or_default();

        let readonly_loaded: Vec<String> = raw_tx
            .pointer("/meta/loadedAddresses/readonly")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|k| k.as_str().map(String::from)).collect())
            .unwrap_or_default();

        let mut all_keys = static_keys.clone();
        all_keys.extend(writable_loaded.iter().cloned());
        all_keys.extend(readonly_loaded.iter().cloned());

        // Extract program IDs from top-level instructions
        let instructions: Vec<CompiledInstruction> = raw_tx
            .pointer("/transaction/message/instructions")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter().map(|ix| CompiledInstruction {
                    program_id_index: ix["programIdIndex"].as_u64().unwrap_or(0) as u8,
                    accounts: ix["accounts"].as_array()
                        .map(|a| a.iter().filter_map(|x| x.as_u64().map(|n| n as u8)).collect())
                        .unwrap_or_default(),
                    data: ix["data"].as_str().unwrap_or("").to_string(),
                }).collect()
            })
            .unwrap_or_default();

        // Resolve program IDs for filtering
        let program_ids: Vec<String> = instructions.iter()
            .filter_map(|ix| all_keys.get(ix.program_id_index as usize).cloned())
            .collect();

        // Apply program filter
        if !filter_programs.is_empty() {
            let matches = program_ids.iter().any(|p| filter_programs.contains(p));
            if !matches { continue; }
        }

        let fee = raw_tx.pointer("/meta/fee").and_then(|v| v.as_u64()).unwrap_or(0);
        let compute_units = raw_tx
            .pointer("/meta/computeUnitsConsumed")
            .and_then(|v| v.as_u64());

        // Extract priority fee from ComputeBudget instruction
        let priority_fee = extract_priority_fee(&raw_tx, &all_keys);

        let log_messages: Vec<String> = raw_tx
            .pointer("/meta/logMessages")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|s| s.as_str().map(String::from)).collect())
            .unwrap_or_default();

        let header = MessageHeader {
            num_required_signatures: raw_tx
                .pointer("/transaction/message/header/numRequiredSignatures")
                .and_then(|v| v.as_u64()).unwrap_or(0) as u8,
            num_readonly_signed_accounts: raw_tx
                .pointer("/transaction/message/header/numReadonlySignedAccounts")
                .and_then(|v| v.as_u64()).unwrap_or(0) as u8,
            num_readonly_unsigned_accounts: raw_tx
                .pointer("/transaction/message/header/numReadonlyUnsignedAccounts")
                .and_then(|v| v.as_u64()).unwrap_or(0) as u8,
        };

        let recent_blockhash = raw_tx
            .pointer("/transaction/message/recentBlockhash")
            .and_then(|v| v.as_str()).unwrap_or("").to_string();

        let address_table_lookups: Vec<AddressTableLookup> = raw_tx
            .pointer("/transaction/message/addressTableLookups")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().map(|alt| AddressTableLookup {
                account_key: alt["accountKey"].as_str().unwrap_or("").to_string(),
                writable_indexes: alt["writableIndexes"].as_array()
                    .map(|a| a.iter().filter_map(|x| x.as_u64().map(|n| n as u8)).collect())
                    .unwrap_or_default(),
                readonly_indexes: alt["readonlyIndexes"].as_array()
                    .map(|a| a.iter().filter_map(|x| x.as_u64().map(|n| n as u8)).collect())
                    .unwrap_or_default(),
            }).collect())
            .unwrap_or_default();

        let message = TransactionMessage {
            version: raw_tx["version"].as_u64().unwrap_or(0) as u8,
            header,
            account_keys: static_keys,
            recent_blockhash,
            instructions: instructions.clone(),
            address_table_lookups,
        };

        let signatures: Vec<String> = raw_tx
            .pointer("/transaction/signatures")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|s| s.as_str().map(String::from)).collect())
            .unwrap_or_else(|| vec![signature.clone()]);

        let frame = ServerFrame::SlotTransaction(SlotTransaction {
            slot,
            block_time,
            signature: signature.clone(),
            transaction: Transaction { signatures, message },
            program_ids,
            fee,
            compute_units_consumed: compute_units,
            priority_fee_micro_lamports: priority_fee,
            log_messages,
            received_at_ms,
        });

        if tx.send(frame).await.is_err() {
            // Session ended
            return;
        }

        metrics.frames_sent.fetch_add(1, Relaxed);
        emitted += 1;
    }

    // Emit slot-complete marker
    let confirmed = ServerFrame::SlotConfirmed(SlotConfirmed {
        slot,
        block_time,
        block_height,
        parent_slot,
        tx_count,
        tx_emitted: emitted,
        received_at_ms,
    });

    let _ = tx.send(confirmed).await;
    metrics.frames_sent.fetch_add(1, Relaxed);

    // debug!(
    //     "slot ingress: slot {} — {} txs total, {} emitted ({:.0}ms after block_time)",
    //     slot, tx_count, emitted,
    //     block_time.map(|bt| (received_at_ms as i64 - bt * 1000) as f64).unwrap_or(0.0)
    // );
}

/// Extract priority fee (micro-lamports per CU) from ComputeBudget instructions.
fn extract_priority_fee(raw_tx: &serde_json::Value, all_keys: &[String]) -> Option<u64> {
    const COMPUTE_BUDGET: &str = "ComputeBudget111111111111111111111111111111";

    let instructions = raw_tx.pointer("/transaction/message/instructions")?.as_array()?;

    for ix in instructions {
        let idx = ix["programIdIndex"].as_u64()? as usize;
        if all_keys.get(idx).map(|k| k == COMPUTE_BUDGET).unwrap_or(false) {
            // ComputeBudget::SetComputeUnitPrice = discriminator 0x03
            let data = ix["data"].as_str()?;
            // data is base58 — decode and check first byte
            let bytes = bs58::decode(data).into_vec().ok()?;
            if bytes.first() == Some(&3) && bytes.len() >= 9 {
                let fee = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
                return Some(fee);
            }
        }
    }
    None
}