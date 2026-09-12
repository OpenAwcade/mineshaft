use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

use mineshaft_legacy::platform::Platform;
use mineshaft_legacy::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to initialize tracing subscriber");

    let platform = Platform::current();
    info!("Starting Mineshaft Client on {}", platform.name());

    let my_session_id: u32 = 555123;
    let relay_addr: SocketAddr = "127.0.0.1:19999".parse()?;
    let local_mc_addr: SocketAddr = "127.0.0.1:19132".parse()?;

    info!(
        "Initializing MineshaftBridge (Session: {}, Relay: {})",
        my_session_id, relay_addr
    );
    let bridge = Arc::new(MineshaftBridge::new(my_session_id, relay_addr, local_mc_addr).await?);

    // 1. Subscribe to local world detection events
    let mut local_events = bridge.subscribe_local_server();
    tokio::spawn(async move {
        while let Ok(ev) = local_events.recv().await {
            match ev {
                LocalServerEvent::ServerStarted(ident) => {
                    info!(
                        "[Local Game] Hosting detected: '{}' (Version: {})",
                        ident.server_name, ident.version_name
                    );
                }
                LocalServerEvent::ServerUpdated(ident) => {
                    info!(
                        "[Local Game] Updated: '{}' ({}/{} players)",
                        ident.server_name, ident.player_count, ident.max_player_count
                    );
                }
                LocalServerEvent::ServerStopped => {
                    info!("[Local Game] Stopped hosting");
                }
            }
        }
    });

    // 2. Subscribe to remote peer tunnel events
    let mut tunnel_events = bridge.subscribe_tunnel_events();
    tokio::spawn(async move {
        while let Ok(ev) = tunnel_events.recv().await {
            match ev {
                TunnelEvent::ClientConnecting(peer_id) => {
                    info!("[Tunnel] Peer connecting: session_id={}", peer_id);
                }
                TunnelEvent::ClientDisconnected(peer_id) => {
                    info!("[Tunnel] Peer disconnected: session_id={}", peer_id);
                }
                TunnelEvent::PlayersActive(peers) => {
                    if !peers.is_empty() {
                        info!("[Tunnel] Active connected peers: {:?}", peers);
                    }
                }
            }
        }
    });

    // 3. Register remote friend worlds from network presence
    info!("Injecting remote network worlds into local Minecraft LAN list...");

    let friend1 = bridge
        .add_server(
            2001,
            0xDEADBEEF0001,
            relay_addr,
            "MCPE;Alex's Survival Realm;776;1.21.60;2;8;998811;Sub;Survival;1;19132;19133;",
        )
        .await?;
    info!(
        "Added Alex's Realm -> Ephemeral Port: {}",
        friend1.ephemeral_port
    );

    let friend2 = bridge
        .add_server(
            2002,
            0xDEADBEEF0002,
            relay_addr,
            "MCPE;Steve's Creative Build;776;1.21.60;4;12;998822;Sub;Creative;2;19132;19133;",
        )
        .await?;
    info!(
        "Added Steve's Build -> Ephemeral Port: {}",
        friend2.ephemeral_port
    );

    // 4. Start background virtual multi-socket forwarders & synthetic pong burst engine
    bridge.spawn_background_tasks();
    info!(
        "Synthetic Pong loop running: Advertisements broadcasted to Minecraft client at {}",
        local_mc_addr
    );

    info!("Mineshaft Client active. Open Minecraft -> 'Friends' tab to see injected LAN worlds.");
    info!("Press Ctrl+C to terminate.");
    signal::ctrl_c().await?;

    info!("Shutting down Mineshaft Client...");
    bridge.stop();

    Ok(())
}
