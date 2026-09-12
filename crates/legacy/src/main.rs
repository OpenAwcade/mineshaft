use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use mineshaft_legacy::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let platform = Platform::current();
    info!("Starting Mineshaft on {}", platform.name());

    let client_id = 42069;
    let relay_addr: SocketAddr = "127.0.0.1:19999".parse()?;
    let local_mc_addr: SocketAddr = "127.0.0.1:19132".parse()?;

    info!("Initializing MineshaftBridge for client_id={}", client_id);
    let bridge = Arc::new(MineshaftBridge::new(client_id, relay_addr, local_mc_addr).await?);

    // Subscribe to events
    let mut server_events = bridge.subscribe_local_server();
    tokio::spawn(async move {
        while let Ok(event) = server_events.recv().await {
            match event {
                LocalServerEvent::ServerStarted(ident) => {
                    info!(">>> Local Minecraft server hosting: '{}' ({})", ident.server_name, ident.version_name);
                }
                LocalServerEvent::ServerUpdated(ident) => {
                    info!(">>> Local Minecraft server updated: '{}' (players: {}/{})", ident.server_name, ident.player_count, ident.max_player_count);
                }
                LocalServerEvent::ServerStopped => {
                    info!(">>> Local Minecraft server stopped");
                }
            }
        }
    });

    let mut tunnel_events = bridge.subscribe_tunnel_events();
    tokio::spawn(async move {
        while let Ok(event) = tunnel_events.recv().await {
            match event {
                TunnelEvent::ClientConnecting(cid) => {
                    info!(">>> Remote peer connecting to our world: client_id={}", cid);
                }
                TunnelEvent::ClientDisconnected(cid) => {
                    info!(">>> Remote peer disconnected: client_id={}", cid);
                }
                TunnelEvent::PlayersActive(peers) => {
                    if !peers.is_empty() {
                        info!(">>> Active connected peers: {:?}", peers);
                    }
                }
            }
        }
    });

    // Spawn background workers
    bridge.spawn_background_tasks();

    info!("Mineshaft engine running. Press Ctrl+C to terminate.");
    signal::ctrl_c().await?;
    info!("Shutting down Mineshaft...");
    bridge.stop();

    Ok(())
}
