//! Wire protocol shared between server and all quic-sol client libraries.
//!
//! Every QUIC stream starts with a client-sent [`ClientHandshake`] that
//! identifies which service the client wants and any filter parameters.
//! The server replies with a [`ServerAck`] (success) or [`ServerError`],
//! then begins streaming [`ServerFrame`]s.
//!
//! Framing: 4-byte big-endian length prefix followed by JSON body.

use serde::{Deserialize, Serialize};

// ── Handshake ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientHandshake {
    pub service: ServiceKind,
    pub api_key: Option<String>,
    pub params: ServiceParams,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceKind {
    Logs,
    Gossip,
    Price,
    Submit,
    Router,
    /// Subscribe to the pre-confirmation mempool (TPU ingress).
    Mempool,
    /// Subscribe to the raw decoded shred stream (ShredStream ingress).
    ShredStream,
    /// Subscribe to the fast confirmed-transaction stream (gossip-triggered getBlock).
    /// ~3-6x lower latency than logsSubscribe for the same RPC endpoint.
    Slot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServiceParams {
    Logs {
        /// WebSocket RPC URL the server will connect to on behalf of this session.
        rpc_url: String,
        program_id: Option<String>,
        commitment: String,
        #[serde(default)]
        parsed: bool,
        #[serde(default)]
        events: bool,
    },
    Gossip {
        /// HTTP RPC URL used for this session's cluster ingress.
        /// e.g. https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
        rpc_url: String,
        watch_pubkeys: Vec<String>,
        subscribe_cluster_info: bool,
    },
    Price {
        pairs: Vec<String>,
    },
    Submit {
        /// HTTP RPC URL used for leader lookup fallback.
        rpc_url: String,
        strategy: String,
        leader_count: Option<u32>,
    },
    Router {
        /// HTTP RPC URL the server will proxy requests through for this session.
        rpc_url: String,
        max_streams: Option<u32>,
    },
    /// Mempool subscription params.
    Mempool {
        /// Optional program-ID filter. Only forward entries that invoke these
        /// programs. Empty = forward all.
        #[serde(default)]
        filter_programs: Vec<String>,
        /// Minimum priority fee (micro-lamports) to forward. 0 = no filter.
        #[serde(default)]
        min_priority_fee: u64,
    },
    /// ShredStream subscription params.
    ShredStream {
        /// If true, also receive code (parity) shred metadata frames.
        #[serde(default)]
        include_code_shreds: bool,
        /// If non-empty, only forward shreds for these slots.
        /// Empty = forward all.
        #[serde(default)]
        filter_slots: Vec<u64>,
    },
    /// Slot subscription params — fast confirmed-transaction stream.
    Slot {
        /// HTTP RPC endpoint used for getBlock calls.
        /// e.g. https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
        /// Ignored when notify_only = true.
        #[serde(default)]
        rpc_url: String,
        /// Optional program-ID filter. Only emit transactions that invoke
        /// at least one of these programs. Empty = emit all transactions.
        #[serde(default)]
        filter_programs: Vec<String>,
    },
}

// ── Server responses ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerAck {
    pub ok: bool,
    pub message: String,
    pub session_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum ServerFrame {
    Log(LogFrame),
    Event(EventFrame),
    EnrichedTransaction(EnrichedTransaction),

    GossipLeaderChange {
        slot: u64,
        leader: String,
        tpu_addr: Option<String>,
        tvu_addr: Option<String>,
        stake: Option<u64>,
        version: Option<String>,
    },
    GossipNodeDegraded  { pubkey: String, reason: String },
    GossipNodeRecovered { pubkey: String },
    GossipNodeJoined    { pubkey: String, stake: u64, gossip_addr: String },
    GossipNodeLeft      { pubkey: String },
    GossipClusterInfo(ClusterSnapshot),
    GossipTpuCongestion {
        leader: String,
        queue_depth: u32,
        level: String,
        recommendation: String,
    },
    GossipLeaderSchedule { upcoming: Vec<SlotLeaderEntry> },

    PriceTick(PriceTick),

    SubmitAck    { signature: String },
    SubmitFailed { reason: String },

    RpcResponse { request_id: u64, body: serde_json::Value },
    RpcError    { request_id: u64, message: String },

    // ── Slot frames (SlotIngress) ─────────────────────────────────────────────

    /// A confirmed transaction from a sealed slot, emitted as soon as
    /// getBlock returns. Arrives 3–6× faster than logsSubscribe.
    SlotTransaction(SlotTransaction),

    /// Emitted once per slot after all SlotTransaction frames for that slot.
    /// Use this as a "slot complete" marker to know when you have all txs.
    SlotConfirmed(SlotConfirmed),

    // ── Shred frames (ShredStream ingress) ────────────────────────────────────

    /// A decoded data shred received from a validator TVU port.
    ///
    /// `payload` contains the raw entry bytes for this shred position.
    /// Consecutive data shreds within the same `slot` / `fec_set_index`
    /// can be reassembled (after erasure recovery with code shreds) into
    /// full `Entry` objects (bincode-encoded `Vec<Entry>`).
    ShredEntry {
        /// UDP source address of the validator that sent this shred.
        from_addr: String,
        /// Wall-clock time of reception (ms since Unix epoch).
        received_at_ms: u64,
        /// Slot number this shred belongs to.
        slot: u64,
        /// Shred index within the slot.
        index: u32,
        /// Cluster-wide shred version (changes after hard forks).
        version: u16,
        /// FEC set index (group of shreds that can recover each other).
        fec_set_index: u32,
        /// Slot - parent_slot (how many slots were skipped before this one).
        parent_offset: u16,
        /// True if this is the final data shred in the slot.
        last_in_slot: bool,
        /// True if this is the last data shred in its FEC set.
        last_in_fec_set: bool,
        /// Declared payload byte count (the valid prefix of `payload`).
        payload_size: u16,
        /// Raw entry data. May be partial — combine with other shreds in the
        /// same FEC set using Reed-Solomon to recover missing pieces.
        payload: Vec<u8>,
        /// Shred type label: "legacy_data" | "merkle_data".
        shred_type: String,
    },

    /// Metadata for a code (Reed-Solomon parity) shred.
    ///
    /// Emitted only when the session's `include_code_shreds` param is true.
    /// Code shreds don't carry entry data — they exist to recover missing
    /// data shreds. Most consumers don't need these.
    ShredCode {
        from_addr: String,
        received_at_ms: u64,
        slot: u64,
        index: u32,
        version: u16,
        fec_set_index: u32,
        /// Shred type label: "legacy_code" | "merkle_code".
        shred_type: String,
    },

    // ── Mempool frame (TPU ingress) ───────────────────────────────────────────

    /// A pre-confirmation transaction received on the TPU port.
    ///
    /// These arrive before the transaction is included in a block. The
    /// signature, fee_payer, and program list are parsed from the wire format.
    /// The raw transaction is not re-emitted to avoid bandwidth overhead —
    /// use the `Submit` service to forward your own transactions.
    MempoolEntry {
        /// First transaction signature (base-58). Unique transaction identifier.
        signature: String,
        /// Fee-payer pubkey (base-58).
        fee_payer: String,
        /// All program IDs invoked by this transaction.
        program_ids: Vec<String>,
        /// Priority fee set via `ComputeBudget::SetComputeUnitPrice`
        /// (micro-lamports per compute unit). `None` if no ComputeBudget ix.
        priority_fee_micro_lamports: Option<u64>,
        /// Compute unit limit set via `ComputeBudget::SetComputeUnitLimit`.
        /// `None` if no ComputeBudget ix (default limit applies).
        compute_unit_limit: Option<u32>,
        /// Raw byte size of the transaction on the wire.
        byte_len: u32,
        /// Wall-clock time of reception (ms since Unix epoch).
        received_at_ms: u64,
        /// UDP source address ("ip:port") of the forwarder.
        from_addr: String,
    },

    // ── Legacy raw shred (kept for backwards compat) ──────────────────────────

    /// Raw undecoded shred bytes. Superseded by `ShredEntry` / `ShredCode`
    /// but kept so existing consumers don't break.
    #[deprecated(note = "use ShredEntry / ShredCode instead")]
    RawShred {
        from_addr: String,
        received_at_ms: u64,
        /// Byte length of the shred payload (always <= 1228 for valid shreds).
        len: u32,
        /// Raw binary payload — verbatim bytes off the wire.
        data: Vec<u8>,
    },

    Heartbeat,
    Error { message: String },
}

// ── Supporting types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogFrame {
    pub slot: u64,
    pub signature: String,
    pub message: String,
    pub program_id: String,
    pub commitment: String,
    pub received_at_ms: u64,
    pub log_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxEvent {
    pub program_id: String,
    pub instruction: String,
    pub args: Vec<String>,
    pub depth: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventFrame {
    pub slot: u64,
    pub signature: String,
    pub commitment: String,
    pub received_at_ms: u64,
    pub programs: Vec<String>,
    pub events: Vec<TxEvent>,
    pub log_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InnerInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: String,
    pub stack_height: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InnerInstructions {
    pub index: u8,
    pub instructions: Vec<InnerInstruction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressTableLookup {
    pub account_key: String,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedAddresses {
    pub writable: Vec<String>,
    pub readonly: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMessage {
    pub version: u8,
    pub header: MessageHeader,
    pub account_keys: Vec<String>,
    pub recent_blockhash: String,
    pub instructions: Vec<CompiledInstruction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address_table_lookups: Vec<AddressTableLookup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub signatures: Vec<String>,
    pub message: TransactionMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiTokenAmount {
    pub amount: String,
    pub decimals: u8,
    pub ui_amount: Option<f64>,
    pub ui_amount_string: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionTokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: UiTokenAmount,
    pub owner: String,
    pub program_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reward {
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: Option<String>,
    pub commission: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionReturnData {
    pub program_id: String,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStatusMeta {
    pub err: Option<serde_json::Value>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inner_instructions: Vec<InnerInstructions>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub post_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub rewards: Option<Vec<Reward>>,
    pub loaded_addresses: LoadedAddresses,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedTransaction {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub commitment: String,
    pub signature: String,
    pub transaction: Transaction,
    pub meta: TransactionStatusMeta,
    pub programs: Vec<String>,
    pub enrichment_latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    pub active_validators: u32,
    pub total_stake: u64,
    pub estimated_tps: f64,
    pub epoch: u64,
    pub slot_index: u64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceTick {
    pub pair: String,
    pub price: f64,
    pub confidence: f64,
    pub sources: Vec<PriceSource>,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceSource {
    pub exchange: String,
    pub price: f64,
    pub volume_24h: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubmitRequest {
    pub transaction_b58: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub request_id: u64,
    pub method: String,
    pub params: serde_json::Value,
}

/// A confirmed transaction from a sealed slot.
/// Emitted by SlotIngress as soon as getBlock returns for that slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotTransaction {
    /// Slot this transaction was confirmed in.
    pub slot: u64,
    /// Unix timestamp of the block (seconds). None if not available.
    pub block_time: Option<i64>,
    /// First transaction signature (base-58).
    pub signature: String,
    /// Full transaction (message + signatures).
    pub transaction: Transaction,
    /// All program IDs invoked (resolved from account key table).
    pub program_ids: Vec<String>,
    /// Transaction fee in lamports.
    pub fee: u64,
    /// Compute units consumed, if reported.
    pub compute_units_consumed: Option<u64>,
    /// Priority fee in micro-lamports per CU (from ComputeBudget ix), if set.
    pub priority_fee_micro_lamports: Option<u64>,
    /// Raw log messages from the transaction.
    pub log_messages: Vec<String>,
    /// Wall-clock time this server received the block (ms since Unix epoch).
    pub received_at_ms: u64,
}

/// Emitted once per slot after all SlotTransaction frames for that slot.
/// Use as a "slot complete" marker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotConfirmed {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub parent_slot: u64,
    /// Total number of transactions in the block (including failed).
    pub tx_count: u32,
    /// Number of SlotTransaction frames actually emitted (after filters).
    pub tx_emitted: u32,
    /// Wall-clock time this server received the block (ms since Unix epoch).
    pub received_at_ms: u64,
}

pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

pub fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let body = serde_json::to_vec(value)?;
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

pub fn decode_frame<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, serde_json::Error> {
    serde_json::from_slice(body)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotLeaderEntry {
    pub tpu_addr: Option<String>,
    pub tvu_addr: Option<String>,
    pub stake: Option<u64>,
    pub version: Option<String>,
    pub slot: u64,
    pub leader: String,
    pub eta_ms: u64,
}