use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::broadcast;
use tracing::{Level, debug, info};
use tracing_subscriber::FmtSubscriber;

use mineshaft_legacy::platform::{Platform, create_reusable_udp_socket};
use mineshaft_legacy::protocol::{
    BedrockIdentifier, build_unconnected_pong, is_unconnected_ping, parse_unconnected_ping,
};
use mineshaft_server::relay::{RelayConfig, RelayServer};

/// Hardcoded dummy Minecraft Bedrock servers to demonstrate status query responses
#[derive(Clone, Debug)]
struct DummyServer {
    name: &'static str,
    guid: u64,
    protocol_version: u32,
    version_name: &'static str,
    player_count: u32,
    max_players: u32,
    sub_title: &'static str,
    game_mode: &'static str,
    port: u16,
}

const DUMMY_SERVERS: &[DummyServer] = &[
    DummyServer {
        name: "Mineshaft SMP [Survival]",
        guid: 0xDEADBEEF0001,
        protocol_version: 776,
        version_name: "1.21.60",
        player_count: 4,
        max_players: 10,
        sub_title: "Pure Vanilla Community",
        game_mode: "Survival",
        port: 19132,
    },
    DummyServer {
        name: "Mineshaft SkyBlock Hub",
        guid: 0xDEADBEEF0002,
        protocol_version: 776,
        version_name: "1.21.60",
        player_count: 12,
        max_players: 25,
        sub_title: "Custom Islands & Economy",
        game_mode: "Survival",
        port: 19134,
    },
    DummyServer {
        name: "Mineshaft Bedrock MiniGames",
        guid: 0xDEADBEEF0003,
        protocol_version: 776,
        version_name: "1.21.60",
        player_count: 8,
        max_players: 16,
        sub_title: "Bedwars & Parkour Arena",
        game_mode: "Adventure",
        port: 19136,
    },
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to initialize tracing subscriber");

    let platform = Platform::current();
    info!("Starting Mineshaft Server on {}", platform.name());

    // 1. Start Mineshaft Relay Core
    let relay_addr: SocketAddr = "0.0.0.0:19999".parse()?;
    let relay = Arc::new(
        RelayServer::bind(RelayConfig {
            bind_addr: relay_addr,
            session_timeout: Duration::from_secs(35),
            cleanup_interval: Duration::from_secs(10),
        })
        .await?,
    );

    let (shutdown_tx, _) = broadcast::channel(16);
    let relay_runner = relay.clone();
    let relay_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        relay_runner.run(relay_shutdown).await;
    });
    info!("Mineshaft Core Relay listening on {}", relay_addr);

    // 2. Start Bedrock LAN Query Responder with Dummy Server List
    let query_addr: SocketAddr = "0.0.0.0:19132".parse()?;
    let query_socket = match create_reusable_udp_socket(query_addr) {
        Ok(sock) => {
            info!("Bedrock LAN Query Responder listening on {}", query_addr);
            Some(Arc::new(sock))
        }
        Err(e) => {
            info!(
                "Port 19132 occupied or restricted ({}). Falling back to non-exclusive query listener.",
                e
            );
            let fallback: SocketAddr = "0.0.0.0:0".parse()?;
            Some(Arc::new(create_reusable_udp_socket(fallback)?))
        }
    };

    if let Some(sock) = query_socket {
        let mut query_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut buf = [0u8; 1500];
            loop {
                tokio::select! {
                    _ = query_shutdown.recv() => break,
                    recv_res = sock.recv_from(&mut buf) => {
                        if let Ok((len, client_addr)) = recv_res {
                            if len > 0 && is_unconnected_ping(&buf[..len]) {
                                if let Ok((ping_ts, _client_guid)) = parse_unconnected_ping(&buf[..len]) {
                                    debug!("Received Bedrock ping from {}", client_addr);

                                    // Respond with each dummy server from the hardcoded list
                                    for dummy in DUMMY_SERVERS {
                                        let ident = BedrockIdentifier {
                                            edition: "MCPE".into(),
                                            server_name: dummy.name.into(),
                                            protocol_version: dummy.protocol_version,
                                            version_name: dummy.version_name.into(),
                                            player_count: dummy.player_count,
                                            max_player_count: dummy.max_players,
                                            server_guid: dummy.guid,
                                            sub_name: dummy.sub_title.into(),
                                            game_mode: dummy.game_mode.into(),
                                            game_mode_numeric: 1,
                                            port_ipv4: dummy.port,
                                            port_ipv6: dummy.port,
                                            extra: Vec::new(),
                                        };

                                        if let Ok(pong) = build_unconnected_pong(ping_ts, dummy.guid, &ident.serialize_to_string()) {
                                            let _ = sock.send_to(&pong, client_addr).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    info!("=== Active Dummy Servers ===");
    for (i, dummy) in DUMMY_SERVERS.iter().enumerate() {
        info!(
            "  [#{}] '{}' (Port: {}, Players: {}/{})",
            i + 1,
            dummy.name,
            dummy.port,
            dummy.player_count,
            dummy.max_players
        );
    }
    info!("============================");

    info!("Server running. Press Ctrl+C to stop.");
    signal::ctrl_c().await?;
    info!("Shutting down Mineshaft Server...");
    let _ = shutdown_tx.send(());

    Ok(())
}
