//! quic-sol-server — entry point

mod config;
mod error;
mod listener;
mod proto;
mod ingress;
mod services;
mod session;
mod metrics;

use anyhow::Result;
use tracing::{info, error};
use tracing_subscriber::{EnvFilter, fmt};

use config::ServerConfig;
use listener::QuicListener;
use ingress::{
    cluster::ClusterIngress,
    entrypoint::GossipEntrypoint,
    peer_registry::{PeerRegistry, GossipPeerUpdater},
    price::PriceIngress,
};
use services::{
    gossip::GossipService,
    logs::LogsService,
    price::PriceService,
    router::RouterService,
    slot::SlotService,
    submit::SubmitService,
};
use session::SessionManager;
use metrics::Metrics;

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,quic_sol_server=debug")),
        )
        .with_target(true)
        .compact()
        .init();

    let cfg = ServerConfig::load()?;

    info!(
        bind            = %cfg.bind,
        gossip_entry    = %cfg.gossip_entrypoint,
        entrypoint_mode = cfg.entrypoint.enabled,
        "quic-sol-server starting"
    );

    let metrics = Metrics::new();

    // ── Broadcast buses ───────────────────────────────────────────────────────
    let (gossip_tx, _) = tokio::sync::broadcast::channel(4_096);
    let (price_tx,  _) = tokio::sync::broadcast::channel(8_192);

    // ── Shared peer registry ──────────────────────────────────────────────────
    let registry = PeerRegistry::new();
    {
        let updater = GossipPeerUpdater::new(gossip_tx.subscribe(), registry.clone());
        tokio::spawn(async move { updater.run().await });
        info!("peer registry: GossipPeerUpdater spawned");
    }

    // ── Server-side ClusterIngress → feeds gossip_tx with leader/schedule frames
    {
        let (cluster_tx, mut cluster_rx) =
            tokio::sync::mpsc::channel::<crate::proto::ServerFrame>(1_024);

        let gossip_tx_clone = gossip_tx.clone();
        tokio::spawn(async move {
            while let Some(frame) = cluster_rx.recv().await {
                let _ = gossip_tx_clone.send(frame);
            }
        });

        let rpc = ingress::rpc::RpcPool::new_single(
            "https://mainnet.helius-rpc.com/?api-key=4738eb60-3a1d-43e7-bc70-19ed66553b08".into()
        )?;
        let cluster_ingress = ClusterIngress { rpc, tx: cluster_tx, metrics: metrics.clone() };
        tokio::spawn(async move {
            if let Err(e) = cluster_ingress.run().await {
                error!("cluster ingress failed: {}", e);
            }
        });
        info!("cluster ingress: spawned");
    }

    // ── Optional: full gossip entrypoint ──────────────────────────────────────
    if cfg.entrypoint.enabled {
        let rpc = ingress::rpc::RpcPool::new_single(
            "https://mainnet.helius-rpc.com/?api-key=4738eb60-3a1d-43e7-bc70-19ed66553b08".into()
        )?;
        let ep = GossipEntrypoint::new(cfg.entrypoint.clone(), gossip_tx.clone(), metrics.clone(), rpc);
        tokio::spawn(async move {
            if let Err(e) = ep.run().await { error!("gossip entrypoint failed: {}", e); }
        });
    }

    // ── Optimistic confirmation gossip ingress ───────────────────────────────
    // Connects to real Solana validators via raw UDP bincode gossip.
    // Emits GossipLeaderChange immediately when a slot gets optimistic
    // confirmation — no RPC, no indexer lag, pure gossip speed.
    {
        ingress::optimistic::spawn(
            gossip_tx.clone(),
            metrics.clone(),
            Some("0.0.0.0:8001".into()), // bind on well-known gossip port so validators can reach us
        );
        info!("optimistic-gossip ingress: spawned");
    }

    // ── Price ingress ─────────────────────────────────────────────────────────
    {
        let price_ingress = PriceIngress::new(cfg.price.clone(), price_tx.clone(), metrics.clone());
        tokio::spawn(async move {
            if let Err(e) = price_ingress.run().await { error!("price ingress failed: {}", e); }
        });
    }

    // ── Session services ──────────────────────────────────────────────────────
    let gossip_svc = GossipService::new(gossip_tx.clone());
    let logs_svc   = LogsService::new(metrics.clone(), cfg.enrich_transactions);
    let price_svc  = PriceService::new(price_tx.clone());
    let submit_svc = SubmitService::new(gossip_tx.subscribe());
    let router_svc = RouterService::new();
    let slot_svc   = SlotService::new(gossip_tx.clone(), metrics.clone());

    let session_mgr = SessionManager::new(
        gossip_svc,
        logs_svc,
        price_svc,
        submit_svc,
        router_svc,
        slot_svc,
        cfg.auth.api_keys.clone(),
        metrics.clone(),
    );

    // ── QUIC listener ─────────────────────────────────────────────────────────
    let listener = QuicListener::new(cfg.bind, cfg.tls, session_mgr).await?;
    info!("QUIC listener ready on {}", cfg.bind);

    tokio::select! {
        res = listener.run() => {
            if let Err(e) = res { error!("listener error: {}", e); }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl-C, shutting down");
        }
    }

    info!("quic-sol-server stopped");
    Ok(())
}