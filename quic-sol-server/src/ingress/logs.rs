//! Log ingress — per-session. Subscribes to the client-supplied RPC WebSocket
//! endpoint and forwards frames through an mpsc channel to the session's
//! LogsService. Lifecycle is tied to the session: when the session ends the
//! channel closes and this task exits cleanly.
//!
//! Two modes:
//!   - Raw mode: one LogFrame per log line with received_at_ms for latency.
//!   - Parsed mode: deduplicates by signature, calls getTransaction via RpcPool,
//!     emits one EnrichedTransaction with the full VersionedTransaction parsed.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Semaphore};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::ingress::rpc::RpcPool;
use crate::metrics::Metrics;
use crate::proto::{
    AddressTableLookup, CompiledInstruction, EnrichedTransaction,
    EventFrame, InnerInstruction, InnerInstructions, LoadedAddresses, LogFrame,
    MessageHeader, Reward, Transaction, TransactionMessage,
    TransactionReturnData, TransactionStatusMeta, TransactionTokenBalance,
    TxEvent, UiTokenAmount, ServerFrame,
};

/// Max concurrent getTransaction calls per session.
const MAX_CONCURRENT_ENRICHMENTS: usize = 4;

#[inline]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Pending transaction buffer — accumulates log lines until tx completes.
#[derive(Default)]
struct PendingTx {
    slot: u64,
    logs: Vec<String>,
    programs: Vec<String>,
}

pub struct LogIngress {
    /// Client-supplied WebSocket RPC endpoint (e.g. wss://atlas-mainnet.helius-rpc.com/...).
    rpc_url: String,
    /// HTTP RPC pool used only for optional getTransaction enrichment.
    rpc: RpcPool,
    /// Sends frames to the session's LogsService. When the service drops its
    /// receiver (session ends), sends return Err and the ingress task exits.
    tx: mpsc::Sender<ServerFrame>,
    metrics: Metrics,
    enrich_sem: Arc<Semaphore>,
    enrich_enabled: bool,
}

impl LogIngress {
    pub fn new(
        rpc_url: String,
        rpc: RpcPool,
        tx: mpsc::Sender<ServerFrame>,
        metrics: Metrics,
        enrich_enabled: bool,
    ) -> Self {
        LogIngress {
            rpc_url,
            rpc,
            tx,
            metrics,
            enrich_sem: Arc::new(Semaphore::new(MAX_CONCURRENT_ENRICHMENTS)),
            enrich_enabled,
        }
    }

    pub async fn run(self) -> Result<()> {
        let mut attempt = 0u32;
        loop {
            if attempt > 0 {
                let delay = Duration::from_secs(2u64.pow(attempt.min(5)));
                warn!("log ingress: reconnect attempt {} in {:?}", attempt, delay);
                tokio::time::sleep(delay).await;
                self.metrics.upstream_reconnects.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            match self.run_once().await {
                Ok(()) => {
                    // run_once returns Ok(()) when the session channel closes — exit cleanly.
                    info!("log ingress: session ended, shutting down");
                    return Ok(());
                }
                Err(e) => {
                    error!("log ingress error: {}", e);
                }
            }
            attempt += 1;
        }
    }

    async fn run_once(&self) -> Result<()> {
        info!("log ingress: connecting to {}", self.rpc_url);

        let (ws, _) = connect_async(&self.rpc_url)
            .await
            .context("WebSocket connect failed")?;

        let (mut sink, mut stream) = ws.split();

        let subscribe_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [
                "all",
                { "commitment": "processed" }
            ]
        });

        sink.send(Message::Text(subscribe_msg.to_string()))
            .await
            .context("sending logsSubscribe")?;

        // Wait for the subscription confirmation before entering the main loop.
        // A rejection here is a hard error — don't retry silently.
        match stream.next().await {
            Some(Ok(Message::Text(ack_text))) => {
                let ack: serde_json::Value = serde_json::from_str(&ack_text)
                    .context("parsing logsSubscribe ack")?;
                if let Some(err) = ack.get("error").filter(|v| !v.is_null()) {
                    anyhow::bail!("logsSubscribe rejected by RPC: {}", err);
                }
                info!("log ingress: subscription confirmed (id={})", ack["result"]);
            }
            Some(Ok(Message::Close(_))) | None => {
                anyhow::bail!("WebSocket closed before subscription ack");
            }
            Some(Err(e)) => return Err(e).context("WebSocket error during ack"),
            _ => anyhow::bail!("unexpected message during subscription ack"),
        }

        let pending: std::sync::Arc<tokio::sync::RwLock<HashMap<String, PendingTx>>> =
            std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));

        let mut heartbeat = tokio::time::interval(Duration::from_secs(20));

        loop {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            let received_at_ms = now_ms();
                            // Returns Err if the session channel closed — bubble up to run().
                            self.handle_message(&text, std::sync::Arc::clone(&pending), received_at_ms).await?;
                        }
                        Some(Ok(Message::Ping(p))) => {
                            let _ = sink.send(Message::Pong(p)).await;
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            warn!("log ingress: WebSocket closed");
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            return Err(e).context("WebSocket error");
                        }
                        _ => {}
                    }
                }
                _ = heartbeat.tick() => {
                    sink.send(Message::Ping(vec![]))
                        .await
                        .context("sending ping")?;
                }
            }
        }
    }

    async fn handle_message(
        &self,
        text: &str,
        pending: Arc<tokio::sync::RwLock<HashMap<String, PendingTx>>>,
        received_at_ms: u64,
    ) -> Result<()> {
        use std::sync::atomic::Ordering::Relaxed;

        let Ok(v): Result<serde_json::Value, _> = serde_json::from_str(text) else {
            debug!("log ingress: non-JSON message");
            return Ok(());
        };

        // Skip subscription ack (already consumed above, but guard defensively).
        if v.get("result").is_some() && v["result"].is_number() { return Ok(()); }

        let Some(value) = v.pointer("/params/result/value") else { return Ok(()); };

        let signature = value["signature"].as_str().unwrap_or("").to_string();
        let logs = value["logs"].as_array().cloned().unwrap_or_default();
        let slot = v.pointer("/params/result/context/slot")
            .and_then(|s| s.as_u64())
            .unwrap_or(0);
        let err = !value["err"].is_null();

        if err || signature.is_empty() { return Ok(()); }

        let log_count = logs.len() as u32;

        // ── Raw frames ────────────────────────────────────────────────────────
        for log_line in &logs {
            let message = log_line.as_str().unwrap_or("").to_string();
            let program_id = extract_program_id(&message)
                .unwrap_or_default()
                .to_string();

            let frame = ServerFrame::Log(LogFrame {
                slot,
                signature: signature.clone(),
                message,
                program_id,
                commitment: "processed".into(),
                received_at_ms,
                log_count,
            });

            if self.tx.send(frame).await.is_err() {
                // Receiver dropped — session ended. Signal run() to exit.
                anyhow::bail!("session channel closed");
            }
            self.metrics.frames_sent.fetch_add(1, Relaxed);
            self.metrics.log_events_rx.fetch_add(1, Relaxed);
        }

        // ── Accumulate pending tx buffer ──────────────────────────────────────
        {
            let mut map = pending.write().await;
            let entry = map.entry(signature.clone()).or_insert_with(PendingTx::default);
            if entry.slot == 0 { entry.slot = slot; }
            for log_line in &logs {
                let s = log_line.as_str().unwrap_or("").to_string();
                if s.contains("invoke [1]") {
                    if let Some(pid) = extract_program_id(&s) {
                        if !entry.programs.contains(&pid.to_string()) {
                            entry.programs.push(pid.to_string());
                        }
                    }
                }
                entry.logs.push(s);
            }
        }

        let is_complete = logs.iter().any(|l| {
            let s = l.as_str().unwrap_or("");
            s.starts_with("Program ")
                && !s.contains("invoke [1]")
                && (s.contains(" success") || s.contains(" failed"))
        });

        if is_complete {
            let tx_data = {
                let mut map = pending.write().await;
                map.remove(&signature)
            };

            if let Some(data) = tx_data {
                // ── Always emit EventFrame ────────────────────────────────────
                let events = parse_events(&data.logs);
                let ef = ServerFrame::Event(EventFrame {
                    slot: data.slot,
                    signature: signature.clone(),
                    commitment: "processed".into(),
                    received_at_ms,
                    programs: data.programs.clone(),
                    events,
                    log_count: data.logs.len() as u32,
                });
                if self.tx.send(ef).await.is_err() {
                    anyhow::bail!("session channel closed");
                }
                self.metrics.frames_sent.fetch_add(1, Relaxed);

                // ── EnrichedTransaction — optional, RPC-heavy ─────────────────
                if !self.enrich_enabled { return Ok(()); }

                if self.enrich_sem.available_permits() == 0 {
                    debug!("enrichment queue full, dropping {}", signature);
                    return Ok(());
                }

                let rpc = self.rpc.clone();
                let tx_bus = self.tx.clone();
                let sig = signature.clone();
                let sem = Arc::clone(&self.enrich_sem);
                let metrics = self.metrics.clone();

                tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    if let Some(enriched) = enrich_transaction(&rpc, &sig, data).await {
                        if tx_bus.send(ServerFrame::EnrichedTransaction(enriched)).await.is_ok() {
                            metrics.frames_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                });
            }
        }

        Ok(())
    }
}


async fn enrich_transaction(
    rpc: &RpcPool,
    signature: &str,
    data: PendingTx,
) -> Option<EnrichedTransaction> {
    let t0 = Instant::now();

    let params = serde_json::json!([
        signature,
        {
            "encoding": "json",
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0
        }
    ]);

    let result = match rpc.call("getTransaction", params.clone()).await {
        Err(e) if e.to_string().contains("-32403") || e.to_string().contains("developer plans") => {
            debug!("getTransaction restricted on primary RPC, trying public fallback for {}", signature);
            match RpcPool::new_single("https://api.mainnet-beta.solana.com".to_string()) {
                Ok(fallback) => fallback.call("getTransaction", params).await,
                Err(_) => return None,
            }
        }
        other => other,
    };

    let enrichment_latency_ms = t0.elapsed().as_millis() as u64;

    let tx = match result {
        Ok(v) => v,
        Err(e) => {
            debug!("getTransaction failed for {}: {}", signature, e);
            return None;
        }
    };

    if tx.is_null() { return None; }

    let block_time: Option<i64> = tx.get("blockTime").and_then(|v| v.as_i64());

    let version: u8 = tx.get("version")
        .and_then(|v| v.as_u64())
        .map(|v| v as u8)
        .unwrap_or(0);

    let signatures: Vec<String> = tx
        .pointer("/transaction/signatures")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|s| s.as_str().map(String::from)).collect())
        .unwrap_or_else(|| vec![signature.to_string()]);

    let header = MessageHeader {
        num_required_signatures: tx
            .pointer("/transaction/message/header/numRequiredSignatures")
            .and_then(|v| v.as_u64()).unwrap_or(0) as u8,
        num_readonly_signed_accounts: tx
            .pointer("/transaction/message/header/numReadonlySignedAccounts")
            .and_then(|v| v.as_u64()).unwrap_or(0) as u8,
        num_readonly_unsigned_accounts: tx
            .pointer("/transaction/message/header/numReadonlyUnsignedAccounts")
            .and_then(|v| v.as_u64()).unwrap_or(0) as u8,
    };

    let account_keys: Vec<String> = tx
        .pointer("/transaction/message/accountKeys")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|k| k.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let recent_blockhash = tx
        .pointer("/transaction/message/recentBlockhash")
        .and_then(|v| v.as_str()).unwrap_or("").to_string();

    let instructions: Vec<CompiledInstruction> = tx
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

    let address_table_lookups: Vec<AddressTableLookup> = tx
        .pointer("/transaction/message/addressTableLookups")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter().map(|alt| AddressTableLookup {
                account_key: alt["accountKey"].as_str().unwrap_or("").to_string(),
                writable_indexes: alt["writableIndexes"].as_array()
                    .map(|a| a.iter().filter_map(|x| x.as_u64().map(|n| n as u8)).collect())
                    .unwrap_or_default(),
                readonly_indexes: alt["readonlyIndexes"].as_array()
                    .map(|a| a.iter().filter_map(|x| x.as_u64().map(|n| n as u8)).collect())
                    .unwrap_or_default(),
            }).collect()
        })
        .unwrap_or_default();

    let loaded_addresses = LoadedAddresses {
        writable: tx.pointer("/meta/loadedAddresses/writable")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|k| k.as_str().map(String::from)).collect())
            .unwrap_or_default(),
        readonly: tx.pointer("/meta/loadedAddresses/readonly")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|k| k.as_str().map(String::from)).collect())
            .unwrap_or_default(),
    };

    let message = TransactionMessage { version, header, account_keys, recent_blockhash, instructions, address_table_lookups };
    let transaction = Transaction { signatures, message };

    let err: Option<serde_json::Value> = tx.pointer("/meta/err")
        .filter(|v| !v.is_null()).cloned();

    let fee = tx.pointer("/meta/fee").and_then(|v| v.as_u64()).unwrap_or(0);

    let pre_balances: Vec<u64> = tx.pointer("/meta/preBalances").and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|b| b.as_u64()).collect()).unwrap_or_default();

    let post_balances: Vec<u64> = tx.pointer("/meta/postBalances").and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|b| b.as_u64()).collect()).unwrap_or_default();

    let inner_instructions: Vec<InnerInstructions> = tx
        .pointer("/meta/innerInstructions").and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter().filter_map(|group| {
                let index = group["index"].as_u64()? as u8;
                let instructions = group["instructions"].as_array()
                    .map(|ixs| ixs.iter().map(|ix| InnerInstruction {
                        program_id_index: ix["programIdIndex"].as_u64().unwrap_or(0) as u8,
                        accounts: ix["accounts"].as_array()
                            .map(|a| a.iter().filter_map(|x| x.as_u64().map(|n| n as u8)).collect())
                            .unwrap_or_default(),
                        data: ix["data"].as_str().unwrap_or("").to_string(),
                        stack_height: ix["stackHeight"].as_u64().unwrap_or(2) as u32,
                    }).collect())
                    .unwrap_or_default();
                Some(InnerInstructions { index, instructions })
            }).collect()
        })
        .unwrap_or_default();

    let log_messages: Option<Vec<String>> = tx.pointer("/meta/logMessages").and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|s| s.as_str().map(String::from)).collect());

    let parse_token_balances = |ptr: &str| -> Option<Vec<TransactionTokenBalance>> {
        tx.pointer(ptr).and_then(|v| v.as_array()).map(|arr| {
            arr.iter().map(|b| TransactionTokenBalance {
                account_index: b["accountIndex"].as_u64().unwrap_or(0) as u8,
                mint: b["mint"].as_str().unwrap_or("").to_string(),
                ui_token_amount: UiTokenAmount {
                    amount: b.pointer("/uiTokenAmount/amount").and_then(|v| v.as_str()).unwrap_or("0").to_string(),
                    decimals: b.pointer("/uiTokenAmount/decimals").and_then(|v| v.as_u64()).unwrap_or(0) as u8,
                    ui_amount: b.pointer("/uiTokenAmount/uiAmount").and_then(|v| v.as_f64()),
                    ui_amount_string: b.pointer("/uiTokenAmount/uiAmountString").and_then(|v| v.as_str()).unwrap_or("0").to_string(),
                },
                owner: b["owner"].as_str().unwrap_or("").to_string(),
                program_id: b["programId"].as_str().unwrap_or("").to_string(),
            }).collect()
        })
    };

    let rewards: Option<Vec<Reward>> = tx.pointer("/meta/rewards").and_then(|v| v.as_array())
        .map(|arr| arr.iter().map(|r| Reward {
            pubkey: r["pubkey"].as_str().unwrap_or("").to_string(),
            lamports: r["lamports"].as_i64().unwrap_or(0),
            post_balance: r["postBalance"].as_u64().unwrap_or(0),
            reward_type: r["rewardType"].as_str().map(String::from),
            commission: r["commission"].as_u64().map(|v| v as u8),
        }).collect());

    let return_data: Option<TransactionReturnData> = tx.pointer("/meta/returnData")
        .filter(|v| !v.is_null())
        .map(|rd| TransactionReturnData {
            program_id: rd["programId"].as_str().unwrap_or("").to_string(),
            data: rd["data"].as_array().and_then(|arr| arr.first()).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        });

    let compute_units_consumed: Option<u64> = tx.pointer("/meta/computeUnitsConsumed").and_then(|v| v.as_u64());

    let meta = TransactionStatusMeta {
        err, fee, pre_balances, post_balances, inner_instructions, log_messages,
        pre_token_balances: parse_token_balances("/meta/preTokenBalances"),
        post_token_balances: parse_token_balances("/meta/postTokenBalances"),
        rewards, loaded_addresses, return_data, compute_units_consumed,
    };

    Some(EnrichedTransaction {
        slot: data.slot,
        block_time,
        commitment: "processed".into(),
        signature: signature.to_string(),
        transaction,
        meta,
        programs: data.programs,
        enrichment_latency_ms,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn extract_program_id(log_line: &str) -> Option<&str> {
    let line = log_line.strip_prefix("Program ")?;
    Some(line.split_whitespace().next()?)
}

fn parse_events(logs: &[String]) -> Vec<TxEvent> {
    let mut events: Vec<TxEvent> = Vec::new();
    let mut stack: Vec<(String, String, Vec<String>, u32)> = Vec::new();

    for line in logs {
        if line.starts_with("Program ") && line.contains("invoke [") {
            if let Some(pid) = extract_program_id(line) {
                let depth = line.split("invoke [").nth(1)
                    .and_then(|s| s.split(']').next())
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(1);
                stack.push((pid.to_string(), String::new(), Vec::new(), depth));
            }
            continue;
        }

        if let Some(rest) = line.strip_prefix("Program log: Instruction: ") {
            if let Some(frame) = stack.last_mut() { frame.1 = rest.trim().to_string(); }
            continue;
        }

        if let Some(rest) = line.strip_prefix("Program log: ") {
            if rest.starts_with("Please upgrade") { continue; }
            if let Some(frame) = stack.last_mut() {
                if !frame.1.is_empty() { frame.2.push(rest.trim().to_string()); }
            }
            continue;
        }

        if line.starts_with("Program ")
            && !line.contains("invoke [")
            && (line.contains(" success") || line.contains(" failed"))
        {
            if let Some((pid, instruction, args, depth)) = stack.pop() {
                if !instruction.is_empty() {
                    events.push(TxEvent { program_id: pid, instruction, args, depth });
                }
            }
        }
    }

    events
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_program_id() {
        assert_eq!(
            extract_program_id("Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [1]"),
            Some("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
        );
        assert_eq!(extract_program_id("Program log: hello"), Some("log:"));
        assert_eq!(extract_program_id("something else"), None);
    }

    #[test]
    fn test_is_complete_success() {
        let logs = vec![
            "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA invoke [1]".to_string(),
            "Program log: Instruction: Buy".to_string(),
            "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA success".to_string(),
        ];
        let complete = logs.iter().any(|l| {
            l.starts_with("Program ")
                && !l.contains("invoke [1]")
                && (l.contains(" success") || l.contains(" failed"))
        });
        assert!(complete);
    }

    #[test]
    fn test_is_complete_no_fire_on_invoke() {
        let logs = vec![
            "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA invoke [1]".to_string(),
            "Program log: Instruction: Buy".to_string(),
        ];
        let complete = logs.iter().any(|l| {
            l.starts_with("Program ")
                && !l.contains("invoke [1]")
                && (l.contains(" success") || l.contains(" failed"))
        });
        assert!(!complete);
    }

    #[test]
    fn test_now_ms_is_recent() {
        assert!(now_ms() > 1_704_067_200_000);
    }
}