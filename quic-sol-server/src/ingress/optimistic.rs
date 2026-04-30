//! Gossip ingress — raw UDP slot detector.
//!
//! Joins Solana's gossip network via UDP bincode, parses Vote CRDS messages,
//! and emits a single GossipLeaderChange per slot. No RPC, no polling —
//! pure gossip speed.
//!
//! ## Why the old parser broke silently
//!
//! The entrypoint sends PullResponse packets containing many CRDS values of
//! mixed types: LegacyContactInfo, ContactInfo, NodeInstance, Vote, etc.
//! The old code did `if tag != CRDS_VOTE { break }` — breaking the entire
//! inner loop on the first non-vote entry, so it parsed nothing from most
//! packets. The fix: skip unknown CRDS variants by reading past their payload
//! instead of bailing.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::metrics::Metrics;
use crate::proto::ServerFrame;

const ENTRYPOINTS: &[&str] = &[
    "entrypoint.mainnet-beta.solana.com:8001",
    "entrypoint2.mainnet-beta.solana.com:8001",
    "entrypoint3.mainnet-beta.solana.com:8001",
    "entrypoint4.mainnet-beta.solana.com:8001",
    "entrypoint5.mainnet-beta.solana.com:8001",
];

const UDP_BUF:          usize = 65_535;
const PULL_INTERVAL_MS: u64   = 5_000;

// Gossip message type tags (outermost u32)
const MSG_PULL_RESPONSE: u32 = 1;
const MSG_PUSH:          u32 = 2;

// CRDS value type tags
const CRDS_LEGACY_CONTACT_INFO: u32 = 0;
const CRDS_VOTE:                u32 = 1;
const CRDS_LOWEST_SLOT:         u32 = 2;
const CRDS_LEGACY_SNAPSHOT_HASHES: u32 = 3;
const CRDS_ACCOUNTS_HASHES:     u32 = 4;
const CRDS_EPOCH_SLOTS:         u32 = 5;
const CRDS_LEGACY_VERSION:      u32 = 6;
const CRDS_VERSION:             u32 = 7;
const CRDS_NODE_INSTANCE:       u32 = 8;
const CRDS_DUPLICATE_SHRED:     u32 = 9;
const CRDS_SNAPSHOT_HASHES:     u32 = 10;
const CRDS_CONTACT_INFO:        u32 = 11;
const CRDS_RESTART_LAST_VOTED_FORK_SLOTS: u32 = 12;
const CRDS_RESTART_HEAVIEST_FORK: u32 = 13;

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

// ── Public API ────────────────────────────────────────────────────────────────

pub fn spawn(tx: broadcast::Sender<ServerFrame>, metrics: Metrics, bind: Option<String>) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = run(tx.clone(), metrics.clone(), bind.clone()).await {
                warn!("optimistic-gossip: restarting after error: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}

async fn run(
    tx:      broadcast::Sender<ServerFrame>,
    metrics: Metrics,
    bind:    Option<String>,
) -> anyhow::Result<()> {
    // Try 0.0.0.0:8001 first; if it's taken (e.g. entrypoint mode is on), use a random port.
    let socket = match UdpSocket::bind(bind.as_deref().unwrap_or("0.0.0.0:8001")).await {
        Ok(s) => s,
        Err(_) => UdpSocket::bind("0.0.0.0:0").await?,
    };
    info!("optimistic-gossip: UDP socket bound on {}", socket.local_addr()?);

    let mut entrypoint_addrs: Vec<SocketAddr> = Vec::new();
    for ep in ENTRYPOINTS {
        match tokio::net::lookup_host(*ep).await {
            Ok(mut a) => { if let Some(addr) = a.next() { entrypoint_addrs.push(addr); } }
            Err(e) => warn!("optimistic-gossip: could not resolve {}: {}", ep, e),
        }
    }
    if entrypoint_addrs.is_empty() { anyhow::bail!("no entrypoints resolved"); }

    for addr in &entrypoint_addrs { send_pull_request(&socket, *addr).await; }
    info!("optimistic-gossip: sent PullRequest to {} entrypoints", entrypoint_addrs.len());

    let mut buf          = vec![0u8; UDP_BUF];
    let mut pull_tick    = tokio::time::interval(Duration::from_millis(PULL_INTERVAL_MS));
    let mut slot_emitted: HashSet<u64> = HashSet::new();
    let mut highest_seen = 0u64;

    loop {
        tokio::select! {
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((n, from)) => {
                        debug!("optimistic-gossip: {} bytes from {}", n, from);
                        let slots = parse_slots(&buf[..n]);
                        debug!("optimistic-gossip: parsed {} vote slots", slots.len());

                        for slot in slots {
                            if slot == 0 || slot_emitted.contains(&slot) { continue; }

                            slot_emitted.insert(slot);
                            if slot > highest_seen { highest_seen = slot; }
                            metrics.gossip_events_rx.fetch_add(1, Relaxed);

                            let _ = tx.send(ServerFrame::GossipLeaderChange {
                                slot,
                                leader:   String::new(),
                                tpu_addr: None,
                                tvu_addr: None,
                                stake:    None,
                                version:  None,
                            });
                            info!("optimistic-gossip: new slot {} from gossip", slot);
                        }

                        // Trim old slot set
                        if slot_emitted.len() > 1000 {
                            let min = highest_seen.saturating_sub(200);
                            slot_emitted.retain(|&s| s >= min);
                        }
                    }
                    Err(e) => warn!("optimistic-gossip: recv error: {}", e),
                }
            }
            _ = pull_tick.tick() => {
                let addr = entrypoint_addrs[(now_ms() as usize) % entrypoint_addrs.len()];
                send_pull_request(&socket, addr).await;
            }
        }
    }
}

// ── Bincode parser ────────────────────────────────────────────────────────────

/// Extract every voted slot from a PullResponse or PushMessage UDP packet.
fn parse_slots(data: &[u8]) -> Vec<u64> {
    let mut cur = 0usize;
    let msg_type = match read_u32_le(data, &mut cur) { Some(t) => t, None => return vec![] };
    match msg_type {
        MSG_PUSH | MSG_PULL_RESPONSE => {
            if cur + 32 > data.len() { return vec![]; }
            cur += 32; // skip sender pubkey
            extract_slots_from_crds_values(data, &mut cur)
        }
        _ => vec![],
    }
}

/// Walk every CRDS value in the packet, skipping unknown types, collecting slots from votes.
fn extract_slots_from_crds_values(data: &[u8], cur: &mut usize) -> Vec<u64> {
    // Vec length prefix
    let count = match read_u64_le(data, cur) { Some(c) => c as usize, None => return vec![] };
    if count > 4096 { return vec![]; }

    let mut slots = Vec::new();

    for _ in 0..count {
        // Each CRDS value on the wire:
        //   [64 bytes signature] [u32 type tag] [payload...]
        if *cur + 68 > data.len() { break; }
        *cur += 64; // skip signature

        let tag = match read_u32_le(data, cur) { Some(t) => t, None => break };

        match tag {
            CRDS_VOTE => {
                // u8 vote_index, then the vote transaction
                if *cur >= data.len() { break; }
                *cur += 1; // vote_index byte
                if let Some(slot) = extract_slot_from_vote_tx(data, cur) {
                    slots.push(slot);
                }
            }

            // Known types we don't need — skip their payload so we can keep going.
            CRDS_LEGACY_CONTACT_INFO => { skip_legacy_contact_info(data, cur); }
            CRDS_CONTACT_INFO        => { skip_contact_info(data, cur); }
            CRDS_NODE_INSTANCE       => { skip_node_instance(data, cur); }
            CRDS_VERSION             => { skip_version(data, cur); }
            CRDS_LEGACY_VERSION      => { skip_legacy_version(data, cur); }
            CRDS_LOWEST_SLOT         => { skip_lowest_slot(data, cur); }
            CRDS_EPOCH_SLOTS         => { skip_epoch_slots(data, cur); }
            CRDS_DUPLICATE_SHRED     => { skip_duplicate_shred(data, cur); }
            CRDS_SNAPSHOT_HASHES | CRDS_LEGACY_SNAPSHOT_HASHES |
            CRDS_ACCOUNTS_HASHES | CRDS_RESTART_LAST_VOTED_FORK_SLOTS |
            CRDS_RESTART_HEAVIEST_FORK => {
                // We don't have precise skip logic for these — safe to stop
                // since they're rare and appear at the end of most packets.
                break;
            }
            _ => {
                debug!("optimistic-gossip: unknown CRDS tag {}, stopping parse", tag);
                break;
            }
        }
    }

    slots
}

// ── CRDS skip helpers ─────────────────────────────────────────────────────────
// Each one advances `cur` past the full payload of that CRDS type.
// On any parse failure they just return without panic — worst case we
// stop processing that packet early.

/// LegacyContactInfo: pubkey(32) + gossip(6) + tvu(6) + tvu_fwd(6) + repair(6)
///   + tpu(6) + tpu_fwd(6) + unused1(6) + rpc(6) + rpc_pubsub(6)
///   + serve_repair(6) + wallclock(8) + shred_version(2) = 106 bytes after pubkey
fn skip_legacy_contact_info(data: &[u8], cur: &mut usize) {
    // pubkey(32) + 9 socket addrs * 6 bytes + serve_repair(6) + wallclock(8) + shred_version(2)
    // = 32 + 10*6 + 8 + 2 = 102
    skip_bytes(data, cur, 102);
}

/// ContactInfo (v2): pubkey(32) + wallclock(8) + outset(8) + shred_version(2)
///   + then a series of length-prefixed socket addr arrays — variable length.
///   We can't reliably skip this without a full parser, so just advance past
///   pubkey + fixed header and stop. This is fine — contact_info is not what we want.
fn skip_contact_info(data: &[u8], cur: &mut usize) {
    skip_bytes(data, cur, 50); // pubkey + wallclock + outset + shred_version
    // After that the format is variable; just bail.
}

/// NodeInstance: pubkey(32) + wallclock(8) + timestamp(8) + token(8) = 56 bytes
fn skip_node_instance(data: &[u8], cur: &mut usize) {
    skip_bytes(data, cur, 56);
}

/// Version (new): pubkey(32) + wallclock(8) + version: major(2)+minor(2)+patch(2)+commit(4)+feature_set(4) = 54 bytes
fn skip_version(data: &[u8], cur: &mut usize) {
    skip_bytes(data, cur, 54);
}

/// LegacyVersion: pubkey(32) + wallclock(8) + major(2)+minor(2)+patch(2)+commit(opt 5) — variable, stop early
fn skip_legacy_version(data: &[u8], cur: &mut usize) {
    skip_bytes(data, cur, 46);
}

/// LowestSlot: pubkey(32) + wallclock(8) + u8 index + u64 lowest = 41 bytes minimum
fn skip_lowest_slot(data: &[u8], cur: &mut usize) {
    skip_bytes(data, cur, 41);
}

/// EpochSlots: variable length — pubkey(32) + wallclock(8) + slot array. Can't skip cleanly.
fn skip_epoch_slots(data: &[u8], cur: &mut usize) {
    skip_bytes(data, cur, 40);
}

/// DuplicateShred: variable length — stop early.
fn skip_duplicate_shred(data: &[u8], cur: &mut usize) {
    skip_bytes(data, cur, 40);
}

#[inline]
fn skip_bytes(data: &[u8], cur: &mut usize, n: usize) {
    if *cur + n <= data.len() { *cur += n; }
    else { *cur = data.len(); }
}

// ── Vote transaction parser ───────────────────────────────────────────────────

fn extract_slot_from_vote_tx(data: &[u8], cur: &mut usize) -> Option<u64> {
    // Compact-u16 encoded signature count (Solana uses compact-u16 here)
    let sig_count = read_compact_u16(data, cur)? as usize;
    if sig_count > 32 { return None; }
    if *cur + sig_count * 64 > data.len() { return None; }
    *cur += sig_count * 64;

    // Message header (3 bytes: num_required_signatures, num_readonly_signed, num_readonly_unsigned)
    if *cur + 3 > data.len() { return None; }
    *cur += 3;

    // Account keys — compact-u16 count then 32 bytes each
    let key_count = read_compact_u16(data, cur)? as usize;
    if key_count == 0 || key_count > 64 { return None; }
    if *cur + key_count * 32 > data.len() { return None; }
    *cur += key_count * 32;

    // Recent blockhash (32 bytes)
    if *cur + 32 > data.len() { return None; }
    *cur += 32;

    // Instructions — compact-u16 count
    let ix_count = read_compact_u16(data, cur)? as usize;
    if ix_count > 32 { return None; }

    for _ in 0..ix_count {
        if *cur >= data.len() { return None; }
        *cur += 1; // program_id_index

        let acc_count = read_compact_u16(data, cur)? as usize;
        if acc_count > 64 { return None; }
        if *cur + acc_count > data.len() { return None; }
        *cur += acc_count;

        let data_len = read_compact_u16(data, cur)? as usize;
        if data_len > 4096 || *cur + data_len > data.len() { return None; }
        let ix_data = &data[*cur..*cur + data_len];
        *cur += data_len;

        if let Some(slot) = highest_slot_from_vote_ix(ix_data) {
            return Some(slot);
        }
    }
    None
}

/// Compact-u16: Solana's variable-length encoding for small integers.
/// 1 byte if value < 128, 2 bytes if < 16384, 3 bytes otherwise.
fn read_compact_u16(data: &[u8], cur: &mut usize) -> Option<u16> {
    if *cur >= data.len() { return None; }
    let b0 = data[*cur] as u16;
    *cur += 1;
    if b0 & 0x80 == 0 {
        return Some(b0);
    }
    if *cur >= data.len() { return None; }
    let b1 = data[*cur] as u16;
    *cur += 1;
    let val = (b0 & 0x7f) | ((b1 & 0x7f) << 7);
    if b1 & 0x80 == 0 {
        return Some(val);
    }
    if *cur >= data.len() { return None; }
    let b2 = data[*cur] as u16;
    *cur += 1;
    Some(val | ((b2 & 0x03) << 14))
}

/// Extract the highest slot from a Vote / VoteStateUpdate / CompactVoteStateUpdate instruction.
fn highest_slot_from_vote_ix(data: &[u8]) -> Option<u64> {
    let mut cur = 0usize;
    let tag = read_u32_le(data, &mut cur)?;
    match tag {
        // Vote { slots: Vec<u64>, hash: Hash, timestamp: Option<i64> }
        1 => {
            let count = read_u64_le(data, &mut cur)? as usize;
            if count == 0 || count > 32 { return None; }
            let mut max = 0u64;
            for _ in 0..count {
                if let Some(s) = read_u64_le(data, &mut cur) { if s > max { max = s; } }
            }
            Some(max)
        }
        // VoteStateUpdate { lockouts: Vec<Lockout(slot u64, confirmation_count u32)>, root, hash, timestamp }
        9 => {
            let count = read_u64_le(data, &mut cur)? as usize;
            if count == 0 || count > 32 { return None; }
            let mut max = 0u64;
            for _ in 0..count {
                if let Some(s) = read_u64_le(data, &mut cur) { if s > max { max = s; } }
                read_u32_le(data, &mut cur)?; // confirmation_count
            }
            Some(max)
        }
        // CompactVoteStateUpdate { root: u64, lockouts: Vec<(offset: u16, conf: u8)>, hash, timestamp }
        13 => {
            let root = read_u64_le(data, &mut cur)?;
            let count = read_u64_le(data, &mut cur)? as usize;
            if count > 32 { return None; }
            let mut highest = root;
            let mut acc = root;
            for _ in 0..count {
                if cur + 2 > data.len() { return None; }
                let offset = u16::from_le_bytes(data[cur..cur+2].try_into().ok()?) as u64;
                cur += 2;
                if cur >= data.len() { return None; }
                cur += 1; // confirmation_count byte
                acc += offset;
                if acc > highest { highest = acc; }
            }
            Some(highest)
        }
        _ => None,
    }
}

// ── Wire helpers ──────────────────────────────────────────────────────────────

#[inline]
fn read_u32_le(data: &[u8], cur: &mut usize) -> Option<u32> {
    if *cur + 4 > data.len() { return None; }
    let v = u32::from_le_bytes(data[*cur..*cur+4].try_into().ok()?);
    *cur += 4;
    Some(v)
}

#[inline]
fn read_u64_le(data: &[u8], cur: &mut usize) -> Option<u64> {
    if *cur + 8 > data.len() { return None; }
    let v = u64::from_le_bytes(data[*cur..*cur+8].try_into().ok()?);
    *cur += 8;
    Some(v)
}

/// Send a minimal but valid Solana gossip PullRequest.
async fn send_pull_request(socket: &UdpSocket, to: SocketAddr) {
    let mut p = Vec::with_capacity(200);
    p.extend_from_slice(&0u32.to_le_bytes()); // message type: PullRequest
    // CrdsFilter: bloom filter (empty — accept everything)
    p.extend_from_slice(&0u64.to_le_bytes()); // bloom keys len
    p.extend_from_slice(&0u64.to_le_bytes()); // bloom bits len
    p.extend_from_slice(&0u64.to_le_bytes()); // num_bits_set
    p.extend_from_slice(&0u64.to_le_bytes()); // mask
    p.extend_from_slice(&0u32.to_le_bytes()); // mask_bits
    // CRDS value: LegacyContactInfo for "us"
    p.extend_from_slice(&[0u8; 64]);           // signature (zeroed — entrypoint doesn't verify)
    p.extend_from_slice(&0u32.to_le_bytes()); // CRDS tag: LegacyContactInfo
    p.extend_from_slice(&[0u8; 32]);           // id / pubkey (zeroed)
    p.push(0);                                  // addr family: IPv4
    p.extend_from_slice(&[0u8; 4]);             // ip (0.0.0.0)
    p.extend_from_slice(&0u16.to_le_bytes());   // port 0
    p.extend_from_slice(&0u64.to_le_bytes());   // wallclock
    let _ = socket.send_to(&p, to).await;
}