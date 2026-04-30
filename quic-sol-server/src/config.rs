//! Server configuration — loaded from a TOML file and overridable via CLI flags.

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};

use crate::ingress::entrypoint::EntrypointConfig;

#[derive(Parser, Debug)]
#[command(name = "quic-sol-server", about = "quic-sol relay server")]
struct Cli {
    #[arg(long, short, default_value = "config.toml")]
    config: PathBuf,

    #[arg(long, env = "QUIC_SOL_BIND")]
    bind: Option<SocketAddr>,

    #[arg(long, env = "QUIC_SOL_GOSSIP")]
    gossip: Option<String>,

    #[arg(long, env = "QUIC_SOL_ENTRYPOINT")]
    entrypoint: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub gossip_entrypoint: String,

    #[serde(default)]
    pub tls: TlsConfig,

    #[serde(default)]
    pub auth: AuthConfig,

    #[serde(default)]
    pub price: PriceConfig,

    #[serde(default)]
    pub tuning: TuningConfig,

    #[serde(default)]
    pub entrypoint: EntrypointConfig,

    #[serde(default)]
    pub enrich_transactions: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TlsConfig {
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    #[serde(default = "default_true")]
    pub self_signed: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        TlsConfig { cert_path: None, key_path: None, self_signed: true }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AuthConfig {
    #[serde(default)]
    pub api_keys: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PriceConfig {
    #[serde(default = "default_binance_url")]
    pub binance_url: String,
    #[serde(default = "default_coinbase_url")]
    pub coinbase_url: String,
    #[serde(default = "default_okx_url")]
    pub okx_url: String,
    #[serde(default = "default_pairs")]
    pub pairs: Vec<String>,
}

impl Default for PriceConfig {
    fn default() -> Self {
        PriceConfig {
            binance_url:  default_binance_url(),
            coinbase_url: default_coinbase_url(),
            okx_url:      default_okx_url(),
            pairs:        default_pairs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TuningConfig {
    #[serde(default = "default_client_buf")]
    pub client_buffer: usize,
    #[serde(default = "default_max_conn")]
    pub max_connections: usize,
    #[serde(default = "default_gossip_delay")]
    pub gossip_reconnect_ms: u64,
}

impl Default for TuningConfig {
    fn default() -> Self {
        TuningConfig {
            client_buffer:       default_client_buf(),
            max_connections:     default_max_conn(),
            gossip_reconnect_ms: default_gossip_delay(),
        }
    }
}

fn default_true()         -> bool   { true }
fn default_client_buf()   -> usize  { 512 }
fn default_max_conn()     -> usize  { 1000 }
fn default_gossip_delay() -> u64    { 2000 }
fn default_binance_url()  -> String { "wss://stream.binance.com:9443/stream".into() }
fn default_coinbase_url() -> String { "wss://advanced-trade-ws.coinbase.com".into() }
fn default_okx_url()      -> String { "wss://ws.okx.com:8443/ws/v5/public".into() }
fn default_pairs() -> Vec<String> {
    vec!["SOLUSDC".into(), "BTCUSDC".into(), "ETHUSDC".into()]
}

impl ServerConfig {
    pub fn load() -> Result<Self> {
        let cli = Cli::parse();

        let mut cfg: ServerConfig = if cli.config.exists() {
            let raw = std::fs::read_to_string(&cli.config)
                .with_context(|| format!("reading {}", cli.config.display()))?;
            toml::from_str(&raw)
                .with_context(|| format!("parsing {}", cli.config.display()))?
        } else {
            ServerConfig {
                bind:                "0.0.0.0:4433".parse().unwrap(),
                gossip_entrypoint:   "entrypoint.mainnet-beta.solana.com:8001".into(),
                tls:                 TlsConfig::default(),
                auth:                AuthConfig::default(),
                price:               PriceConfig::default(),
                tuning:              TuningConfig::default(),
                entrypoint:          EntrypointConfig::default(),
                enrich_transactions: false,
            }
        };

        if let Some(bind) = cli.bind   { cfg.bind = bind; }
        if let Some(g)    = cli.gossip { cfg.gossip_entrypoint = g; }
        if cli.entrypoint              { cfg.entrypoint.enabled = true; }

        Ok(cfg)
    }
}