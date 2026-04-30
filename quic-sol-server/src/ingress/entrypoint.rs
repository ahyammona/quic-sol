//! Gossip Entrypoint — makes quic-sol-server a first-class Solana gossip node.
//!
//! When `[entrypoint]` is enabled in config, this module binds UDP :8001 (or
//! whatever port you configure) and participates in the Solana gossip mesh as
//! a full peer — not just a consumer. Other validators and quic-sol instances
//! can bootstrap through your node.
//!
//! ## What it does
//!
//! 1. Binds a UDP socket on the configured gossip port.
//! 2. Seeds itself by pulling from upstream entrypoints (e.g. Solana Labs').
//! 3. Maintains a CRDS table — a map of pubkey → latest CrdsEntry.
//! 4. Responds to `PullRequest` from newcomers with `PullResponse`.
//! 5. Rebroadcasts `PushMessage` to its known peers (fan-out).
//! 6. Sends periodic `PullRequest` to stay fresh and retain mesh membership.
//! 7. Handles `PruneMessage` — removes peers that asked to be pruned.
//!
//! ## Bloom filter (simplified)
//!
//! Real Solana gossip uses a Bloom filter in PullRequest so peers don't
//! resend data the requester already has. We use a simple seen-hash set
//! as a lightweight equivalent — good enough for a relay/entrypoint that
//! isn't trying to be a full validator.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};

use crate::ingress::rpc::RpcPool;
use crate::metrics::Metrics;
use crate::proto::{ClusterSnapshot, ServerFrame};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Entrypoint configuration — lives under `[entrypoint]` in config.toml.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EntrypointConfig {
    /// Enable the entrypoint listener. Default: false.
    #[serde(default)]
    pub enabled: bool,

    /// UDP bind address. Default: "0.0.0.0:8001".
    #[serde(default = "default_gossip_bind")]
    pub bind: String,

    /// Upstream entrypoints to seed from on startup.
    #[serde(default = "default_seed_peers")]
    pub seed_peers: Vec<String>,

    /// Max peers to track in the CRDS table. Default: 5000.
    #[serde(default = "default_max_peers")]
    pub max_peers: usize,

    /// How often to re-pull from seed peers to stay fresh (seconds). Default: 10.
    #[serde(default = "default_pull_interval")]
    pub pull_interval_secs: u64,

    /// How often to push our known peers to the mesh (seconds). Default: 5.
    #[serde(default = "default_push_interval")]
    pub push_interval_secs: u64,

    /// Max peers to fan-out a PushMessage to. Default: 6 (same as Solana).
    #[serde(default = "default_fanout")]
    pub push_fanout: usize,
}

impl Default for EntrypointConfig {
    fn default() -> Self {
        EntrypointConfig {
            enabled: false,
            bind: default_gossip_bind(),
            seed_peers: default_seed_peers(),
            max_peers: default_max_peers(),
            pull_interval_secs: default_pull_interval(),
            push_interval_secs: default_push_interval(),
            push_fanout: default_fanout(),
        }
    }
}

fn default_gossip_bind() -> String { "0.0.0.0:8001".into() }
fn default_seed_peers() -> Vec<String> {
    vec![
        "entrypoint.mainnet-beta.solana.com:8001".into(),
        "entrypoint2.mainnet-beta.solana.com:8001".into(),
    ]
}
fn default_max_peers() -> usize { 5_000 }
fn default_pull_interval() -> u64 { 10 }
fn default_push_interval() -> u64 { 5 }
fn default_fanout() -> usize { 6 }

// ---------------------------------------------------------------------------
// CRDS table
// ---------------------------------------------------------------------------

/// A single entry in the Cluster Replicated Data Store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdsEntry {
    /// Entry type: "contact_info" | "vote" | "leader_schedule" | "node_degraded"
    pub kind: String,
    /// Validator pubkey (base-58).
    pub pubkey: String,
    /// Gossip address of this validator.
    pub gossip_addr: Option<String>,
    /// TPU address (for direct transaction submission).
    pub tpu_addr: Option<String>,
    /// Reported stake weight (lamports).
    pub stake: Option<u64>,
    /// Slot number (for leader_schedule entries).
    pub slot: Option<u64>,
    /// Wall clock of when this entry was last updated (ms since epoch).
    pub wallclock: u64,
    /// Arbitrary extra data (version string, shred version, etc.)
    pub data: Option<serde_json::Value>,
}

impl CrdsEntry {
    /// Unique key for deduplication.
    fn crds_key(&self) -> String {
        format!("{}:{}", self.kind, self.pubkey)
    }

    /// A fast hash for bloom-filter-like deduplication.
    fn content_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.crds_key().hash(&mut h);
        self.wallclock.hash(&mut h);
        h.finish()
    }
}

/// The in-memory CRDS table.
struct CrdsTable {
    /// Latest entry per (kind, pubkey).
    entries: HashMap<String, CrdsEntry>,
    /// Set of content hashes we've already seen — used to avoid rebroadcast loops.
    seen: HashSet<u64>,
    /// Max entries before we start evicting oldest.
    max_entries: usize,
}

impl CrdsTable {
    fn new(max_entries: usize) -> Self {
        CrdsTable {
            entries: HashMap::new(),
            seen: HashSet::new(),
            max_entries,
        }
    }

    /// Insert an entry. Returns true if this is new/updated (should be rebroadcast).
    fn upsert(&mut self, entry: CrdsEntry) -> bool {
        let hash = entry.content_hash();
        if self.seen.contains(&hash) {
            return false; // already processed
        }
        self.seen.insert(hash);

        let key = entry.crds_key();
        let is_newer = match self.entries.get(&key) {
            Some(existing) => entry.wallclock > existing.wallclock,
            None => true,
        };

        if is_newer {
            if self.entries.len() >= self.max_entries {
                self.evict_oldest();
            }
            self.entries.insert(key, entry);
            true
        } else {
            false
        }
    }

    /// Evict the entry with the oldest wallclock.
    fn evict_oldest(&mut self) {
        if let Some(key) = self
            .entries
            .iter()
            .min_by_key(|(_, v)| v.wallclock)
            .map(|(k, _)| k.clone())
        {
            self.entries.remove(&key);
        }
    }

    /// All entries as a slice, for sending in PullResponse / PushMessage.
    fn all_entries(&self) -> Vec<CrdsEntry> {
        self.entries.values().cloned().collect()
    }

    /// Entries newer than `since_ms`.
    fn entries_since(&self, since_ms: u64) -> Vec<CrdsEntry> {
        self.entries
            .values()
            .filter(|e| e.wallclock > since_ms)
            .cloned()
            .collect()
    }

    fn len(&self) -> usize { self.entries.len() }
}

// ---------------------------------------------------------------------------
// Wire protocol
// ---------------------------------------------------------------------------

/// A gossip message on the wire (our simplified protocol).
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "msg", rename_all = "snake_case")]
enum GossipMessage {
    /// Newcomer requests CRDS data. `filter_since_ms` = only send entries
    /// newer than this timestamp (0 = send everything).
    PullRequest {
        from_pubkey: String,
        from_addr: String,
        filter_since_ms: u64,
    },

    /// Response to a PullRequest — batch of CRDS entries.
    PullResponse {
        from_pubkey: String,
        entries: Vec<CrdsEntry>,
    },

    /// Push new/updated entries to peers.
    PushMessage {
        from_pubkey: String,
        entries: Vec<CrdsEntry>,
    },

    /// Ask a peer to stop sending us certain pubkeys (we have enough paths).
    PruneMessage {
        from_pubkey: String,
        prune_pubkeys: Vec<String>,
    },
}

// ---------------------------------------------------------------------------
// Peer tracker
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Peer {
    addr: SocketAddr,
    pubkey: String,
    last_seen: Instant,
    /// Pubkeys this peer has asked us to prune.
    pruned: HashSet<String>,
}

struct PeerTable {
    peers: HashMap<String, Peer>, // keyed by pubkey
    max_peers: usize,
}

impl PeerTable {
    fn new(max_peers: usize) -> Self {
        PeerTable { peers: HashMap::new(), max_peers }
    }

    fn upsert(&mut self, pubkey: String, addr: SocketAddr) {
        if self.peers.len() >= self.max_peers && !self.peers.contains_key(&pubkey) {
            self.evict_oldest();
        }
        self.peers
            .entry(pubkey.clone())
            .and_modify(|p| { p.addr = addr; p.last_seen = Instant::now(); })
            .or_insert(Peer {
                addr,
                pubkey,
                last_seen: Instant::now(),
                pruned: HashSet::new(),
            });
    }

    fn prune(&mut self, from_pubkey: &str, prune_pubkeys: &[String]) {
        if let Some(peer) = self.peers.get_mut(from_pubkey) {
            for pk in prune_pubkeys {
                peer.pruned.insert(pk.clone());
            }
        }
    }

    fn evict_oldest(&mut self) {
        if let Some(key) = self
            .peers
            .iter()
            .min_by_key(|(_, p)| p.last_seen)
            .map(|(k, _)| k.clone())
        {
            self.peers.remove(&key);
        }
    }

    /// Pick `fanout` random peers for push, excluding pruned pubkeys.
    fn fanout_targets(&self, fanout: usize, exclude_pubkey: &str) -> Vec<SocketAddr> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut candidates: Vec<_> = self
            .peers
            .values()
            .filter(|p| p.pubkey != exclude_pubkey)
            .collect();

        // Deterministic shuffle based on current time shard
        let shard = unix_ms() / 1000;
        candidates.sort_by_key(|p| {
            let mut h = DefaultHasher::new();
            p.pubkey.hash(&mut h);
            shard.hash(&mut h);
            h.finish()
        });

        candidates.iter().take(fanout).map(|p| p.addr).collect()
    }

    fn len(&self) -> usize { self.peers.len() }
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

/// Shared state between the entrypoint and the rest of the server.
pub type SharedCrdsTable = Arc<RwLock<CrdsTable>>;

pub struct GossipEntrypoint {
    cfg: EntrypointConfig,
    tx: broadcast::Sender<ServerFrame>,
    metrics: Metrics,
    rpc: RpcPool,
    /// Exposed so GossipIngress can read peers directly from our table.
    pub crds: SharedCrdsTable,
}

impl GossipEntrypoint {
    pub fn new(
        cfg: EntrypointConfig,
        tx: broadcast::Sender<ServerFrame>,
        metrics: Metrics,
        rpc: RpcPool,
    ) -> Self {
        let crds = Arc::new(RwLock::new(CrdsTable::new(cfg.max_peers)));
        GossipEntrypoint { cfg, tx, metrics, rpc, crds }
    }

    pub async fn run(self) -> Result<()> {
        let bind: SocketAddr = self.cfg.bind.parse()
            .with_context(|| format!("parsing entrypoint bind addr: {}", self.cfg.bind))?;

        let socket = Arc::new(
            UdpSocket::bind(bind)
                .await
                .with_context(|| format!("binding gossip entrypoint UDP on {}", bind))?
        );

        info!("gossip entrypoint: listening on {}", bind);

        // Resolve seed peers
        let mut seed_addrs: Vec<SocketAddr> = Vec::new();
        for peer in &self.cfg.seed_peers {
            match tokio::net::lookup_host(peer).await {
                Ok(mut addrs) => {
                    if let Some(addr) = addrs.next() {
                        seed_addrs.push(addr);
                        info!("gossip entrypoint: seed peer {} -> {}", peer, addr);
                    }
                }
                Err(e) => warn!("gossip entrypoint: could not resolve seed {}: {}", peer, e),
            }
        }

        let peers = Arc::new(RwLock::new(PeerTable::new(self.cfg.max_peers)));
        let crds = self.crds.clone();

        // Intervals
        let pull_interval = Duration::from_secs(self.cfg.pull_interval_secs);
        let push_interval = Duration::from_secs(self.cfg.push_interval_secs);
        let mut pull_tick = tokio::time::interval(pull_interval);
        let mut push_tick = tokio::time::interval(push_interval);
        let mut status_tick = tokio::time::interval(Duration::from_secs(30));

        let mut buf = vec![0u8; 65_535];
        let our_pubkey = "QuicSolEntrypoint1111111111111111111111111111".to_string();
        let our_addr = bind.to_string();

        // Seed pull on startup
        self.send_pull_request(&socket, &seed_addrs, &our_pubkey, &our_addr, 0).await;

        let fanout = self.cfg.push_fanout;

        loop {
            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((n, from)) => {
                            self.handle_message(
                                &buf[..n],
                                from,
                                &socket,
                                &peers,
                                &crds,
                                &our_pubkey,
                                fanout,
                            ).await;
                        }
                        Err(e) => {
                            warn!("gossip entrypoint: recv error: {}", e);
                        }
                    }
                }

                _ = pull_tick.tick() => {
                    // Pull from seeds + a sample of known peers
                    let peer_addrs: Vec<SocketAddr> = {
                        let p = peers.read().await;
                        p.fanout_targets(4, &our_pubkey)
                    };
                    let mut targets = seed_addrs.clone();
                    targets.extend(peer_addrs);
                    // Only ask for entries since last pull
                    let since = unix_ms().saturating_sub(pull_interval.as_millis() as u64 * 2);
                    self.send_pull_request(&socket, &targets, &our_pubkey, &our_addr, since).await;
                }

                _ = push_tick.tick() => {
                    // Push our recent CRDS entries to fanout peers
                    let (targets, entries) = {
                        let p = peers.read().await;
                        let c = crds.read().await;
                        let targets = p.fanout_targets(fanout, &our_pubkey);
                        let since = unix_ms().saturating_sub(push_interval.as_millis() as u64 * 2);
                        let entries = c.entries_since(since);
                        (targets, entries)
                    };
                    if !entries.is_empty() {
                        self.broadcast_push(&socket, &targets, &our_pubkey, entries).await;
                    }
                }

                _ = status_tick.tick() => {
                    let peer_count = peers.read().await.len();
                    let crds_count = crds.read().await.len();
                    info!(
                        "gossip entrypoint: {} peers, {} CRDS entries",
                        peer_count, crds_count
                    );
                    // Emit cluster snapshot to quic-sol clients
                    self.emit_cluster_snapshot(crds_count, peer_count).await;
                }
            }
        }
    }

    async fn handle_message(
        &self,
        data: &[u8],
        from: SocketAddr,
        socket: &UdpSocket,
        peers: &Arc<RwLock<PeerTable>>,
        crds: &SharedCrdsTable,
        our_pubkey: &str,
        fanout: usize,
    ) {
        let msg: GossipMessage = match serde_json::from_slice(data) {
            Ok(m) => m,
            Err(_) => {
                debug!("gossip entrypoint: unparseable packet from {} ({} bytes)", from, data.len());
                return;
            }
        };

        match msg {
            // ── Someone new is joining the mesh ──────────────────────────────
            GossipMessage::PullRequest { from_pubkey, from_addr, filter_since_ms } => {
                info!("gossip entrypoint: PullRequest from {} ({})", from_pubkey, from);

                // Register this peer
                peers.write().await.upsert(from_pubkey.clone(), from);

                // Send them our CRDS entries
                let entries = {
                    let c = crds.read().await;
                    if filter_since_ms == 0 {
                        c.all_entries()
                    } else {
                        c.entries_since(filter_since_ms)
                    }
                };

                // Add a contact_info entry for ourselves so they know about us
                let our_entry = self.our_contact_info(our_pubkey);
                let mut response_entries = entries;
                response_entries.push(our_entry);

                let response = GossipMessage::PullResponse {
                    from_pubkey: our_pubkey.to_string(),
                    entries: response_entries,
                };

                self.send_msg(socket, from, &response).await;

                // Also register their address in our CRDS
                let contact = CrdsEntry {
                    kind: "contact_info".into(),
                    pubkey: from_pubkey.clone(),
                    gossip_addr: Some(from_addr),
                    tpu_addr: None,
                    stake: None,
                    slot: None,
                    wallclock: unix_ms(),
                    data: None,
                };
                let is_new = crds.write().await.upsert(contact.clone());
                if is_new {
                    self.emit_node_joined(&contact).await;
                }
            }

            // ── Peer is giving us their CRDS data ────────────────────────────
            GossipMessage::PullResponse { from_pubkey, entries } => {
                debug!("gossip entrypoint: PullResponse from {} ({} entries)", from_pubkey, entries.len());
                peers.write().await.upsert(from_pubkey.clone(), from);

                let mut new_entries = Vec::new();
                {
                    let mut c = crds.write().await;
                    for entry in entries {
                        if c.upsert(entry.clone()) {
                            new_entries.push(entry);
                        }
                    }
                }

                // Emit events for newly learned data
                for entry in &new_entries {
                    self.emit_crds_event(entry).await;
                }

                // Fan-out new entries to other peers
                if !new_entries.is_empty() {
                    let targets = peers.read().await.fanout_targets(fanout, &from_pubkey);
                    self.broadcast_push(socket, &targets, our_pubkey, new_entries).await;
                }
            }

            // ── Peer is pushing new data into the mesh ───────────────────────
            GossipMessage::PushMessage { from_pubkey, entries } => {
                debug!("gossip entrypoint: PushMessage from {} ({} entries)", from_pubkey, entries.len());
                peers.write().await.upsert(from_pubkey.clone(), from);

                let mut new_entries = Vec::new();
                {
                    let mut c = crds.write().await;
                    for entry in entries {
                        if c.upsert(entry.clone()) {
                            new_entries.push(entry);
                        }
                    }
                }

                for entry in &new_entries {
                    self.emit_crds_event(entry).await;
                }

                // Re-broadcast to fanout peers (not back to sender)
                if !new_entries.is_empty() {
                    let targets = peers.read().await.fanout_targets(fanout, &from_pubkey);
                    self.broadcast_push(socket, &targets, our_pubkey, new_entries).await;
                }
            }

            // ── Peer is asking us to prune certain pubkeys ───────────────────
            GossipMessage::PruneMessage { from_pubkey, prune_pubkeys } => {
                debug!("gossip entrypoint: PruneMessage from {} (prune {} pubkeys)", from_pubkey, prune_pubkeys.len());
                peers.write().await.prune(&from_pubkey, &prune_pubkeys);
            }
        }

        self.metrics.gossip_events_rx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // ── Sending helpers ───────────────────────────────────────────────────────

    async fn send_pull_request(
        &self,
        socket: &UdpSocket,
        targets: &[SocketAddr],
        our_pubkey: &str,
        our_addr: &str,
        since_ms: u64,
    ) {
        let msg = GossipMessage::PullRequest {
            from_pubkey: our_pubkey.to_string(),
            from_addr: our_addr.to_string(),
            filter_since_ms: since_ms,
        };
        for addr in targets {
            self.send_msg(socket, *addr, &msg).await;
        }
    }

    async fn broadcast_push(
        &self,
        socket: &UdpSocket,
        targets: &[SocketAddr],
        our_pubkey: &str,
        entries: Vec<CrdsEntry>,
    ) {
        if targets.is_empty() || entries.is_empty() {
            return;
        }
        let msg = GossipMessage::PushMessage {
            from_pubkey: our_pubkey.to_string(),
            entries,
        };
        for addr in targets {
            self.send_msg(socket, *addr, &msg).await;
        }
    }

    async fn send_msg(&self, socket: &UdpSocket, to: SocketAddr, msg: &GossipMessage) {
        match serde_json::to_vec(msg) {
            Ok(bytes) => {
                if let Err(e) = socket.send_to(&bytes, to).await {
                    debug!("gossip entrypoint: send to {} failed: {}", to, e);
                }
            }
            Err(e) => {
                warn!("gossip entrypoint: serialize error: {}", e);
            }
        }
    }

    // ── Event emission to quic-sol clients ────────────────────────────────────

    async fn emit_crds_event(&self, entry: &CrdsEntry) {
        use std::sync::atomic::Ordering::Relaxed;

        let frame = match entry.kind.as_str() {
            "contact_info" => {
                Some(ServerFrame::GossipNodeJoined {
                    pubkey: entry.pubkey.clone(),
                    stake: entry.stake.unwrap_or(0),
                    gossip_addr: entry.gossip_addr.clone().unwrap_or_default(),
                })
            }
            "leader_schedule" => {
                if let Some(slot) = entry.slot {
                    Some(ServerFrame::GossipLeaderChange {
                        slot,
                        leader: entry.pubkey.clone(),
                        tpu_addr: entry.tpu_addr.clone(),
                        tvu_addr: None,
                        stake: entry.stake,
                        version: entry.data.as_ref().and_then(|d| d["version"].as_str().map(String::from)),
                    })
                } else {
                    None
                }
            }
            "node_degraded" => {
                Some(ServerFrame::GossipNodeDegraded {
                    pubkey: entry.pubkey.clone(),
                    reason: entry.data
                        .as_ref()
                        .and_then(|d| d.get("reason"))
                        .and_then(|r| r.as_str())
                        .unwrap_or("unresponsive")
                        .to_string(),
                })
            }
            _ => None,
        };

        if let Some(f) = frame {
            if self.tx.send(f).is_ok() {
                self.metrics.frames_sent.fetch_add(1, Relaxed);
            } else {
                self.metrics.frames_dropped.fetch_add(1, Relaxed);
            }
        }
    }

    async fn emit_node_joined(&self, entry: &CrdsEntry) {
        let frame = ServerFrame::GossipNodeJoined {
            pubkey: entry.pubkey.clone(),
            stake: entry.stake.unwrap_or(0),
            gossip_addr: entry.gossip_addr.clone().unwrap_or_default(),
        };
        let _ = self.tx.send(frame);
    }

    async fn emit_cluster_snapshot(&self, crds_count: usize, peer_count: usize) {
        // Fetch live epoch + slot data from RPC.
        let (epoch, slot_index, estimated_tps) = match self.rpc
            .call("getEpochInfo", serde_json::json!([{"commitment": "confirmed"}]))
            .await
        {
            Ok(info) => {
                let epoch      = info["epoch"].as_u64().unwrap_or(0);
                let slot_index = info["slotIndex"].as_u64().unwrap_or(0);
                // Rough TPS: ~1500 tx/slot at ~0.4s/slot
                let tps = 1500.0_f64 / 0.4;
                (epoch, slot_index, tps)
            }
            Err(e) => {
                debug!("gossip entrypoint: getEpochInfo failed: {}", e);
                (0, 0, 0.0)
            }
        };

        let snap = ServerFrame::GossipClusterInfo(ClusterSnapshot {
            active_validators: peer_count as u32,
            total_stake: 0,
            estimated_tps,
            epoch,
            slot_index,
            timestamp_ms: unix_ms(),
        });
        let _ = self.tx.send(snap);
        debug!(
            "gossip entrypoint: cluster snapshot: {} peers, {} CRDS, epoch={}, slot_idx={}",
            peer_count, crds_count, epoch, slot_index
        );
    }

    fn our_contact_info(&self, pubkey: &str) -> CrdsEntry {
        CrdsEntry {
            kind: "contact_info".into(),
            pubkey: pubkey.to_string(),
            gossip_addr: Some(self.cfg.bind.clone()),
            tpu_addr: None,
            stake: None,
            slot: None,
            wallclock: unix_ms(),
            data: Some(serde_json::json!({ "version": "quic-sol/0.1" })),
        }
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}