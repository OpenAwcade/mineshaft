//! Mineshaft node client — runs 24/7 alongside the game.
//!
//! - Watches UDP 7551: if the game holds it and *listens* (hosting a world),
//!   registers the world with the rendezvous server; if the game is only
//!   browsing (Friends tab), pulls the host list and advertises entries into
//!   the game via the ephemeral-advertiser path.
//! - Forwards "player clicked server X" to the rendezvous server.
//! - Keeps ONE persistent TCP connection to the rendezvous server.

use std::time::Duration;

use mineshaft_core::net::{ClientMessage, ServerMessage, read_message, write_message};
use mineshaft_core::{
    AdvertisedServer, DiscoveryConfig, DiscoveryEvent, DiscoveryService, ServerAdvertisement,
    platform,
};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:25000";
const LOOP_INTERVAL: Duration = Duration::from_secs(2);

/// Resolve the rendezvous server address.
///
/// Priority: `--server <addr>` flag > positional arg > `MINESHAFT_SERVER`
/// env var > default.
fn resolve_server_addr() -> String {
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--server") {
        if let Some(addr) = args.get(pos + 1) {
            return addr.clone();
        }
    }
    if let Some(addr) = args.iter().skip(1).find(|a| !a.starts_with('-')) {
        return addr.clone();
    }
    if let Ok(addr) = std::env::var("MINESHAFT_SERVER") {
        if !addr.is_empty() {
            return addr;
        }
    }
    DEFAULT_SERVER_ADDR.to_string()
}

/// What the local game is doing with UDP 7551 right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GameState {
    /// Game not running or port free.
    Closed,
    /// Game holds 7551 and is only browsing (Friends tab).
    Browsing,
    /// Game holds 7551 and is listening as a host (world open to LAN).
    Hosting,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let server_addr = resolve_server_addr();
    info!(
        "mineshaft node starting, rendezvous server: {}",
        server_addr
    );

    // Ephemeral advertiser — never binds 7551 itself.
    let discovery = DiscoveryService::new(DiscoveryConfig::default()).await?;
    discovery.start().await;

    let my_network_id: u64 = rand::random();
    info!("our network id: {:#x}", my_network_id);

    // Forward "player clicked a server" events to the rendezvous server.
    let mut discovery_events = discovery.subscribe();
    let (join_tx, mut join_rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
    tokio::spawn(async move {
        loop {
            match discovery_events.recv().await {
                Ok(DiscoveryEvent::ConnectRequest {
                    target_sender_id, ..
                }) => {
                    info!("player is trying to connect to {:#x}", target_sender_id);
                    let _ = join_tx.send(target_sender_id);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!("missed {} discovery events", n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let mut server: Option<TcpStream> = None;
    let mut hosting_registered = false;
    let mut advertised_count = 0usize;

    loop {
        // keep one persistent connection to the rendezvous server
        if server.is_none() {
            match TcpStream::connect(&server_addr).await {
                Ok(stream) => {
                    info!("connected to rendezvous server {}", server_addr);
                    server = Some(stream);
                }
                Err(e) => {
                    warn!("rendezvous connect failed: {}; retrying", e);
                    tokio::time::sleep(LOOP_INTERVAL).await;
                    continue;
                }
            }
        }

        match detect_game_state().await {
            GameState::Closed => {
                if hosting_registered {
                    info!("game closed; unregistering host");
                    send_or_drop(
                        &mut server,
                        &ClientMessage::UnregisterHost {
                            sender_id: my_network_id,
                        },
                    )
                    .await;
                    hosting_registered = false;
                }
                if advertised_count > 0 {
                    discovery.replace_all(vec![]);
                    advertised_count = 0;
                }
                debug!("game closed, 7551 free");
            }
            GameState::Hosting => {
                if !hosting_registered {
                    info!("game is hosting a world (7551 listening) — registering with server");
                    let mut ad = ServerAdvertisement::default();
                    ad.server_name = "local player".to_string();
                    ad.level_name = "local world".to_string();
                    ad.session_id = format!("{:016x}", my_network_id);
                    send_or_drop(
                        &mut server,
                        &ClientMessage::RegisterHost(AdvertisedServer {
                            sender_id: my_network_id,
                            data: ad,
                        }),
                    )
                    .await;
                    info!("host registered as {:#x}", my_network_id);
                    hosting_registered = true;
                }
                if advertised_count > 0 {
                    discovery.replace_all(vec![]);
                    advertised_count = 0;
                }
            }
            GameState::Browsing => {
                if hosting_registered {
                    info!("user left world, now browsing — unregistering host");
                    send_or_drop(
                        &mut server,
                        &ClientMessage::UnregisterHost {
                            sender_id: my_network_id,
                        },
                    )
                    .await;
                    hosting_registered = false;
                }
                debug!("user browsing; requesting host list");
                match fetch_host_list(&mut server).await {
                    Ok(hosts) => {
                        if hosts.len() != advertised_count {
                            info!("advertising {} server(s) into the game", hosts.len());
                        } else {
                            debug!("refreshing {} advertised server(s)", hosts.len());
                        }
                        discovery.replace_all(hosts);
                        advertised_count = discovery.advertised().len();
                    }
                    Err(e) => warn!("host list fetch failed: {}", e),
                }
            }
        }

        // flush pending join intents to the rendezvous server
        while let Ok(target) = join_rx.try_recv() {
            info!(
                "telling server we want to join {:#x}; server will relay to the host",
                target
            );
            send_or_drop(
                &mut server,
                &ClientMessage::ConnectRequest {
                    target_sender_id: target,
                    joiner_network_id: my_network_id,
                },
            )
            .await;
        }

        tokio::time::sleep(LOOP_INTERVAL).await;
    }
}

/// Send a message over the persistent connection; drop it on failure so the
/// next loop iteration reconnects.
async fn send_or_drop(server: &mut Option<TcpStream>, msg: &ClientMessage) {
    if let Some(stream) = server.as_mut() {
        if write_message(stream, msg).await.is_err() {
            *server = None;
        }
    }
}

/// 7551 bound → game is up. Bound + answers a discovery request → hosting.
/// (Browsing clients broadcast but never answer.)
async fn detect_game_state() -> GameState {
    match platform::udp_port_bound(7551) {
        Ok(true) => {
            if probe_host_listening().await {
                GameState::Hosting
            } else {
                GameState::Browsing
            }
        }
        Ok(false) => GameState::Closed,
        Err(e) => {
            warn!("7551 probe failed: {}", e);
            GameState::Closed
        }
    }
}

/// Sends a real NetherNet discovery request to the local game and reports
/// whether a valid response came back within a short window.
async fn probe_host_listening() -> bool {
    use nethernet::protocol::packet::discovery::{self, RequestPacket};

    let Ok(socket) = tokio::net::UdpSocket::bind("0.0.0.0:0").await else {
        return false;
    };
    let probe_id: u64 = rand::random();
    let Ok(request) = discovery::marshal(&RequestPacket, probe_id) else {
        return false;
    };
    if socket.send_to(&request, "127.0.0.1:7551").await.is_err() {
        return false;
    }

    let mut buf = vec![0u8; 2048];
    let result = tokio::time::timeout(Duration::from_millis(300), socket.recv_from(&mut buf)).await;
    match result {
        Ok(Ok((n, _))) => discovery::unmarshal(&buf[..n]).is_ok(),
        _ => false,
    }
}

async fn fetch_host_list(
    server: &mut Option<TcpStream>,
) -> mineshaft_core::Result<Vec<AdvertisedServer>> {
    let stream = server
        .as_mut()
        .ok_or(mineshaft_core::CoreError::InvalidState("not connected"))?;
    write_message(stream, &ClientMessage::ListHosts).await?;
    match read_message::<ServerMessage, _>(stream).await? {
        Some(ServerMessage::HostList(hosts)) => Ok(hosts),
        Some(ServerMessage::Error(e)) => Err(mineshaft_core::CoreError::Other(e)),
        other => {
            warn!("unexpected server response: {:?}", other);
            Ok(vec![])
        }
    }
}
