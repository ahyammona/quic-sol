# quic-sol-server

> The relay server that powers the quic-sol client libraries. A single Rust binary that speaks QUIC to your application and speaks Solana's native protocols upstream.

---

## What it does

`quic-sol-server` sits between your application and the raw Solana network. It maintains the expensive upstream connections — gossip mesh participation, RPC WebSocket subscriptions, CEX WebSocket feeds — and fans the data out to many clients simultaneously over QUIC with TLS 1.3, API-key auth, and per-connection backpressure.

```
                        ┌─────────────────────────────────────────┐
  Solana validators ────▶  gossip ingress                         │
  Solana RPC WS     ────▶  log ingress        broadcast bus       │
  Binance/CB/OKX    ────▶  price ingress   ──────────────────▶   │
  RPC pool          ────▶  rpc proxy                              │
                        │         ▼                               │
                        │   service modules                       │
                        │   (filter, aggregate, route)            │
                        │         ▼                               │
                        │   QUIC listener (TLS 1.3 + auth)       │
                        └──────────┬──────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼               ▼
              Rust client   Python client   TS client
              (quic-sol-*)  (pyo3 bindings) (napi-rs)
```

**Five services, one binary, one port.**

---

## Architecture

### Ingress adapters (upstream → bus)

| Adapter | Source | Protocol |
|---|---|---|
| `gossip` | Solana validators | UDP CRDS push/pull |
| `logs` | Solana RPC | `logsSubscribe` WebSocket |
| `price` | Binance, Coinbase, OKX | CEX WebSocket feeds |
| `rpc` | Multiple RPC endpoints | HTTP JSON-RPC + EMA latency routing |

Each adapter runs in its own Tokio task, reconnects automatically on failure, and publishes typed events to a `tokio::sync::broadcast` channel (the "bus"). The bus is the only coupling between ingress and services — adapters and services never call each other directly.

### Service modules (bus → client streams)

| Service | What it does |
|---|---|
| `gossip` | Filters gossip events by pubkey, fans out to subscribed clients |
| `logs` | Filters log events by program ID + commitment, fans out |
| `price` | Aggregates raw CEX ticks (VWAP + confidence), pushes per 50 ms window |
| `submit` | Decodes signed transactions, routes to leader TPU ports via UDP |
| `router` | Proxies JSON-RPC requests to the latency-sorted RPC pool, up to 200 concurrent streams per connection |

### QUIC listener

One `quinn::Endpoint` accepts all connections. Each accepted connection runs a loop that:
1. Accepts bidirectional QUIC streams (one per service subscription).
2. Reads the `ClientHandshake` frame (service type, API key, params).
3. Validates the API key (if configured).
4. Sends a `ServerAck` with a session ID.
5. Dispatches to the appropriate service module in a new Tokio task.

Multiple service streams can be multiplexed over one QUIC connection — a single connection can receive logs *and* gossip events simultaneously.

### Wire protocol

All frames use a 4-byte big-endian length prefix followed by a JSON body. The same framing is used in both directions and is shared with the quic-sol client libraries.

```
┌─────────────────────────────────────────┐
│ 4 bytes: body length (big-endian u32)   │
├─────────────────────────────────────────┤
│ N bytes: JSON body                      │
└─────────────────────────────────────────┘
```

Key message types:

```rust
// Client → server (first frame on every stream)
ClientHandshake { service: ServiceKind, api_key: Option<String>, params: ServiceParams }

// Server → client (immediately after handshake)
ServerAck { ok: bool, message: String, session_id: u64 }

// Server → client (streaming)
ServerFrame::Log(LogFrame)
ServerFrame::GossipLeaderChange { slot, leader }
ServerFrame::GossipNodeDegraded { pubkey, reason }
ServerFrame::GossipClusterInfo(ClusterSnapshot)
ServerFrame::PriceTick(PriceTick)       // VWAP-aggregated, confidence-scored
ServerFrame::SubmitAck { signature }
ServerFrame::RpcResponse { request_id, body }
ServerFrame::Heartbeat
```

---

## Building

```bash
# Clone the monorepo
git clone https://github.com/your-org/quic-sol
cd quic-sol

# Build the server binary
cargo build --release -p quic-sol-server

# The binary lands at:
./target/release/quic-sol-server
```

**Requirements:** Rust 1.75+ (async fn in traits stable), Tokio 1.x.

---

## Running

### Dev mode (self-signed TLS, no auth)

```bash
./quic-sol-server
# Uses config.toml defaults: binds 0.0.0.0:4433, self-signed cert, no API key required.
```

### Production mode

```bash
./quic-sol-server \
  --config /etc/quic-sol/config.toml \
  --bind 0.0.0.0:4433
```

### Docker

```dockerfile
FROM rust:1.75 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p quic-sol-server

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/quic-sol-server /usr/local/bin/
COPY config.toml /etc/quic-sol/config.toml
EXPOSE 4433/udp
CMD ["quic-sol-server", "--config", "/etc/quic-sol/config.toml"]
```

```bash
docker build -t quic-sol-server .
docker run -p 4433:4433/udp \
  -e QUIC_SOL_RPC_URL=wss://api.mainnet-beta.solana.com \
  quic-sol-server
```

---

## Configuration

The server reads `config.toml` by default. Every field can be overridden via CLI flag or environment variable.

```toml
# Network
bind              = "0.0.0.0:4433"
rpc_url           = "wss://api.mainnet-beta.solana.com"
gossip_entrypoint = "entrypoint.mainnet-beta.solana.com:8001"
rpc_endpoints     = [
  "https://api.mainnet-beta.solana.com",
  "https://rpc.ankr.com/solana",
]

# TLS — production
[tls]
cert_path   = "/etc/quic-sol/cert.pem"
key_path    = "/etc/quic-sol/key.pem"
self_signed = false

# TLS — dev (auto-generates a self-signed cert, logs a warning)
[tls]
self_signed = true

# Authentication
[auth]
api_keys = ["sk_live_abc123", "sk_live_def456"]
# Empty list = no auth (dev only)

# Price feeds
[price]
binance_url  = "wss://stream.binance.com:9443/stream"
coinbase_url = "wss://advanced-trade-ws.coinbase.com"
okx_url      = "wss://ws.okx.com:8443/ws/v5/public"
pairs        = ["SOLUSDC", "BTCUSDC", "ETHUSDC"]

# Tuning
[tuning]
client_buffer       = 512   # per-client event buffer depth (events)
max_connections     = 1000  # max simultaneous QUIC connections
gossip_reconnect_ms = 2000  # delay between gossip reconnect attempts
```

### Environment variable overrides

| Variable | Config key | Example |
|---|---|---|
| `QUIC_SOL_BIND` | `bind` | `0.0.0.0:4433` |
| `QUIC_SOL_RPC_URL` | `rpc_url` | `wss://api.mainnet-beta.solana.com` |
| `QUIC_SOL_GOSSIP` | `gossip_entrypoint` | `entrypoint.mainnet-beta.solana.com:8001` |

---

## Connecting a client

The quic-sol client libraries connect to this server automatically. Point them at your server's address:

```rust
// quic-sol-logs
let mut stream = LogStream::builder()
    .endpoint("https://your-server.example.com:4433")
    .filter(LogFilter::program("YourProgramIdHere"))
    .commitment(CommitmentLevel::Confirmed)
    .connect()
    .await?;

// quic-sol-gossip
let mut gossip = GossipClient::connect("your-server.example.com:4433").await?;

// quic-sol-price
let mut feed = PriceFeed::builder()
    .endpoint("https://your-server.example.com:4433")
    .pairs(vec![Pair::SolUsdc])
    .connect()
    .await?;
```

For the managed hosted endpoints (globally distributed, no infra headache), get an API key at **quic-sol.dev**.

---

## Service deep-dives

### Gossip service

The gossip ingress joins Solana's CRDS (Cluster Replicated Data Store) gossip network by binding a local UDP socket and exchanging pull/push messages with the configured entrypoint. It parses incoming CRDS entries and emits:

- `GossipLeaderChange` — fired when the leader schedule is updated for an upcoming slot. Clients receive this typically 400–800 ms before the slot begins.
- `GossipNodeDegraded` — fired when a validator stops responding to gossip for more than one slot.
- `GossipNodeRecovered` — fired when a degraded validator returns.
- `GossipNodeJoined` — fired when a new contact_info entry appears in CRDS.
- `GossipClusterInfo` — periodic cluster health snapshot (active validators, total stake, estimated TPS, epoch).

Clients can filter to specific validator pubkeys to reduce noise.

### Logs service

Maintains a single `logsSubscribe` WebSocket to the configured RPC. All program logs flow through the server; each client subscribes to a filtered slice by program ID and commitment level. This is more efficient than each client maintaining its own WebSocket — one upstream connection serves thousands of clients.

**Backpressure:** if a client's QUIC stream is full (consumer too slow), the service drops the oldest events from that client's buffer rather than blocking the bus. Dropped event counts are tracked in metrics.

### Price service

Raw ticks arrive from Binance, Coinbase, and OKX concurrently. The price service collects them into 50 ms aggregation windows, computes a volume-weighted average price (VWAP) across all sources, and derives a confidence score from the cross-exchange spread:

```
confidence = 1 - (max_price - min_price) / vwap
```

A confidence of 1.0 means all exchanges are in perfect agreement. A confidence of 0.9 means a 10% spread. Clients can gate on confidence before trading.

### Submit service

Clients send base-58 encoded signed transactions. The server:
1. Decodes the transaction and extracts the signature.
2. Resolves the current slot leader's TPU port from the gossip CRDS store.
3. Sends the transaction to N upcoming leaders concurrently via raw UDP (same protocol validators use internally).
4. Returns the signature immediately — does not poll for confirmation (use `logsSubscribe` or `getSignatureStatuses` for that).

Direct TPU submission skips the JSON-RPC `sendTransaction` round-trip, which typically saves 50–150 ms.

### Router service

Proxies JSON-RPC calls to the latency-sorted RPC pool. Each client connection gets up to 200 concurrent in-flight requests multiplexed over its single QUIC connection — replacing the HTTP/1.1 limitation of one request per TCP connection.

The pool maintains an exponential moving average (EMA, α=0.2) of response time for each endpoint and always routes to the lowest-latency healthy node. A background health-check task pings all endpoints every 15 seconds.

---

## Observability

The server emits structured logs via `tracing`. Set `RUST_LOG` to control verbosity:

```bash
RUST_LOG=info                        # default
RUST_LOG=quic_sol_server=debug       # verbose server logs
RUST_LOG=quic_sol_server=trace       # per-frame tracing (very noisy)
```

In-process metrics are tracked as atomic counters and exposed via the `Metrics` struct. A Prometheus `/metrics` HTTP endpoint is on the roadmap.

| Metric | Description |
|---|---|
| `connections_active` | Current open QUIC connections |
| `sessions_total` | Current active service sessions |
| `gossip_events_rx` | Total gossip packets received from upstream |
| `log_events_rx` | Total log events received from RPC |
| `price_ticks_rx` | Total price ticks received from CEX feeds |
| `frames_sent` | Total frames written to client streams |
| `frames_dropped` | Frames dropped due to slow clients |
| `upstream_reconnects` | Total upstream reconnection events |

---

## Security

**TLS:** All client connections use TLS 1.3 via `rustls`. In production, supply a certificate signed by a trusted CA. Dev mode auto-generates a self-signed certificate (clients must skip verification or trust it explicitly).

**Authentication:** API keys are validated on the `ClientHandshake` frame, before any data flows. Invalid keys receive an error frame and the stream is closed. The key comparison is constant-time to prevent timing attacks (production hardening item: use `subtle::ConstantTimeEq`).

**Resource limits:** `max_connections` caps the total number of concurrent QUIC connections. Per-client stream buffers are bounded by `client_buffer`. The router service caps concurrency at 200 in-flight RPC requests per connection.

---

## Deployment notes

- **UDP port:** QUIC runs over UDP. Make sure port 4433 (or your configured port) is open for UDP traffic, not just TCP.
- **Gossip firewall:** The gossip ingress sends and receives UDP packets on a random ephemeral port. If your server is behind NAT or a strict firewall, ensure outbound UDP to Solana validators is permitted.
- **TLS cert rotation:** Restart the server to pick up a new certificate. Zero-downtime rotation (QUIC connection migration) is a future roadmap item.
- **Single binary:** All five services run in one process. There is no service discovery or inter-process communication — everything communicates through Tokio channels.

---

## Roadmap

- [ ] Prometheus `/metrics` HTTP endpoint
- [ ] CRDS binary parsing (full gossip protocol, not simplified JSON)
- [ ] TPU contact info resolution from CRDS store (removes placeholder addresses)
- [ ] mTLS client authentication (alternative to API keys)
- [ ] Zero-downtime certificate rotation
- [ ] Rate limiting per API key
- [ ] `quic-sol-server health` CLI subcommand
- [ ] Kubernetes Helm chart

---

## License

Apache 2.0 — see [LICENSE](../LICENSE)
