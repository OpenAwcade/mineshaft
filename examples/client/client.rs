//! Mineshaft node client — runs 24/7 alongside the game.
//!
//! Signal-forwarding architecture: the two games establish WebRTC directly
//! with each other. We only shuttle the discovery signaling messages
//! (CONNECTREQUEST/CONNECTRESPONSE/CANDIDATEADD) between the local game and
//! the remote host over the rendezvous server.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mineshaft_core::net::{ClientMessage, ServerMessage, read_message, write_message};
use mineshaft_core::{
    AdvertisedServer, DiscoveryConfig, DiscoveryService, ServerAdvertisement, platform,
};
use nethernet::Signaling as _;
use nethernet::protocol::packet::discovery::{self, MessagePacket};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:25000";
const LOOP_INTERVAL: Duration = Duration::from_secs(2);

/// Resolve the rendezvous server address.
/// Priority: `--server <addr>` flag > positional arg > `MINESHAFT_SERVER` env > default.
fn resolve_server_addr() -> String {
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--server")
        && let Some(addr) = args.get(pos + 1)
    {
        return addr.clone();
    }
    if let Some(addr) = args.iter().skip(1).find(|a| !a.starts_with('-')) {
        return addr.clone();
    }
    if let Ok(addr) = std::env::var("MINESHAFT_SERVER")
        && !addr.is_empty()
    {
        return addr;
    }
    DEFAULT_SERVER_ADDR.to_string()
}

/// What the local game is doing with UDP 7551 right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GameState {
    Closed,
    Browsing,
    Hosting,
}

type SharedWriter = Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>;
type SharedOptionWriter = Arc<tokio::sync::Mutex<Option<SharedWriter>>>;

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

    let my_network_id: u64 = rand::random();
    info!("our network id: {:#x}", my_network_id);

    let discovery = DiscoveryService::new(my_network_id, DiscoveryConfig::default()).await?;
    discovery.start().await;
    let signaling = discovery.signaling();

    // connection_id -> (game network id, advertised target id) for joiner-side
    // response injection
    let join_conns: Arc<tokio::sync::Mutex<HashMap<u64, (u64, u64)>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    // network id of our locally hosted game (hosting mode)
    let local_game_id: Arc<tokio::sync::Mutex<Option<u64>>> =
        Arc::new(tokio::sync::Mutex::new(None));
    // current server connection writer, shared with tasks
    let server_writer: SharedOptionWriter = Arc::new(tokio::sync::Mutex::new(None));

    // --- task: forward local game's signaling to the server ---
    {
        let signaling = signaling.clone();
        let server_writer = server_writer.clone();
        let join_conns = join_conns.clone();
        let local_game_id = local_game_id.clone();
        tokio::spawn(async move {
            let mut signals = signaling.signals();
            while let Some(signal) = signals.next().await {
                let sender: u64 = match signal.network_id.parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if sender == my_network_id {
                    continue;
                }

                let conn = signal.connection_id;
                let line = signal.to_string();
                let kind = line.split(' ').next().unwrap_or("?").to_string();

                // Is this from our local game? While browsing, offers come from
                // the game's 7551 socket and target an advertised server.
                let target = signaling.join_target(conn).await;
                let from_local_game = target.is_some()
                    || local_game_id
                        .lock()
                        .await
                        .map(|id| id == sender)
                        .unwrap_or(false);

                if !from_local_game {
                    continue;
                }

                let target_id = if let Some(t) = target {
                    // joiner side: remember who the game clicked + the game's id
                    join_conns.lock().await.insert(conn, (sender, t));
                    t
                } else {
                    // host side: local game answering; server routes by conn id
                    0
                };

                info!(
                    "forwarding {} (conn {}) from game {:#x} to server",
                    kind, conn, sender
                );
                let writer = server_writer.lock().await.clone();
                if let Some(writer) = writer {
                    let mut guard = writer.lock().await;
                    if write_message(
                        &mut *guard,
                        &ClientMessage::Signal {
                            target_sender_id: target_id,
                            connection_id: conn,
                            joiner_network_id: my_network_id,
                            data: line,
                        },
                    )
                    .await
                    .is_err()
                    {
                        warn!("failed to forward signal to server");
                    }
                }
            }
        });
    }

    // --- main loop: mode detection + server connection ---
    let mut server: Option<SharedWriter> = None;
    let server_incoming: SharedIncoming = Arc::new(tokio::sync::Mutex::new(None));
    let mut hosting_registered = false;
    let mut advertised_count = 0usize;

    loop {
        if server.is_none() {
            match TcpStream::connect(&server_addr).await {
                Ok(stream) => {
                    // Signaling frames are small and latency-sensitive; disable Nagle.
                    let _ = stream.set_nodelay(true);
                    info!("connected to rendezvous server {}", server_addr);
                    let (mut read_half, write_half) = stream.into_split();
                    let write_half = Arc::new(tokio::sync::Mutex::new(write_half));
                    *server_writer.lock().await = Some(write_half.clone());

                    let join_conns = join_conns.clone();
                    let local_game_id = local_game_id.clone();
                    let signaling = signaling.clone();
                    let (incoming_tx, incoming_rx) =
                        tokio::sync::mpsc::unbounded_channel::<ServerMessage>();
                    tokio::spawn(async move {
                        loop {
                            match read_message::<ServerMessage, _>(&mut read_half).await {
                                Ok(Some(msg @ ServerMessage::HostList(_))) => {
                                    let _ = incoming_tx.send(msg);
                                }
                                Ok(Some(ServerMessage::Signal {
                                    connection_id,
                                    joiner_network_id,
                                    data,
                                })) => {
                                    let kind = data.split(' ').next().unwrap_or("?");
                                    info!(
                                        "injecting {} (conn {}) from remote into local game",
                                        kind, connection_id
                                    );
                                    inject_signal(
                                        &signaling,
                                        &join_conns,
                                        &local_game_id,
                                        connection_id,
                                        joiner_network_id,
                                        data,
                                    )
                                    .await;
                                }
                                Ok(Some(ServerMessage::IncomingJoin {
                                    joiner_network_id,
                                    session_id,
                                })) => {
                                    info!(
                                        "joiner {:#x} signaling into our world (session {})",
                                        joiner_network_id, session_id
                                    );
                                }
                                Ok(Some(ServerMessage::JoinAccepted { session_id })) => {
                                    debug!("server noted session {}", session_id);
                                }
                                Ok(Some(ServerMessage::Registered { sender_id })) => {
                                    debug!("server acked registration {:#x}", sender_id);
                                }
                                Ok(Some(ServerMessage::Error(e))) => warn!("server error: {}", e),
                                Ok(None) => {
                                    warn!("rendezvous server closed the connection");
                                    break;
                                }
                                Err(e) => {
                                    warn!("rendezvous read error: {}", e);
                                    break;
                                }
                            }
                        }
                    });

                    server = Some(write_half);
                    *server_incoming.lock().await = Some(incoming_rx);
                }
                Err(e) => {
                    warn!("rendezvous connect failed: {}; retrying", e);
                    tokio::time::sleep(LOOP_INTERVAL).await;
                    continue;
                }
            }
        }

        match detect_game_state(&discovery).await {
            (GameState::Closed, _) => {
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
                    *local_game_id.lock().await = None;
                }
                if advertised_count > 0 {
                    discovery.replace_all(vec![]);
                    advertised_count = 0;
                }
                debug!("game closed, 7551 free");
            }
            (GameState::Hosting, world) => {
                if let Some((game_id, _)) = &world {
                    *local_game_id.lock().await = Some(*game_id);
                }
                if !hosting_registered {
                    let ad = world.map(|(_, ad)| ad).unwrap_or_else(|| {
                        warn!("hosting but probe gave no data; using defaults");
                        let mut ad = ServerAdvertisement::default();
                        ad.session_id = format!("{:016x}", my_network_id);
                        ad
                    });
                    info!(
                        "game is hosting '{}'/'{}' ({} players, mode {}) — registering with server",
                        ad.server_name, ad.level_name, ad.player_count, ad.game_type
                    );
                    send_or_drop(
                        &mut server,
                        &ClientMessage::RegisterHost(AdvertisedServer {
                            sender_id: my_network_id,
                            data: ad,
                        }),
                    )
                    .await;
                    hosting_registered = true;
                }
                if advertised_count > 0 {
                    discovery.replace_all(vec![]);
                    advertised_count = 0;
                }
            }
            (GameState::Browsing, _) => {
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
                    *local_game_id.lock().await = None;
                }
                debug!("user browsing; requesting host list");
                match fetch_host_list(&mut server, &server_incoming).await {
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

        tokio::time::sleep(LOOP_INTERVAL).await;
    }
}

/// Inject a remote signaling message into the local game as a discovery
/// MessagePacket.
async fn inject_signal(
    signaling: &Arc<nethernet::LanSignaling>,
    join_conns: &Arc<tokio::sync::Mutex<HashMap<u64, (u64, u64)>>>,
    local_game_id: &Arc<tokio::sync::Mutex<Option<u64>>>,
    connection_id: u64,
    joiner_network_id: u64,
    data: String,
) {
    // Who should the game think this is from, and who receives it?
    let (sender, recipient) = if let Some(game_id) = *local_game_id.lock().await {
        // we are hosting: present the joiner as the sender, our game receives
        (joiner_network_id, game_id)
    } else if let Some((game_id, target)) = join_conns.lock().await.get(&connection_id).copied() {
        // we are joining: present the advertised server as the sender
        (target, game_id)
    } else {
        warn!("no route for signal conn {}", connection_id);
        return;
    };

    let packet = MessagePacket::new(recipient, data);
    match discovery::marshal(&packet, sender) {
        Ok(bytes) => {
            if let Err(e) = signaling.socket().send_to(&bytes, "127.0.0.1:7551").await {
                warn!("failed to inject signal into game: {}", e);
            }
        }
        Err(e) => warn!("failed to marshal signal: {}", e),
    }
}

type SharedIncoming =
    Arc<tokio::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<ServerMessage>>>>;

/// Send a message over the persistent connection; drop it on failure so the
/// next loop iteration reconnects.
async fn send_or_drop(server: &mut Option<SharedWriter>, msg: &ClientMessage) {
    let failed = if let Some(stream) = server.as_ref() {
        let mut guard = stream.lock().await;
        write_message(&mut *guard, msg).await.is_err()
    } else {
        false
    };
    if failed {
        *server = None;
    }
}

/// 7551 bound → game is up. Bound + answers a discovery request → hosting.
async fn detect_game_state(
    discovery: &Arc<DiscoveryService>,
) -> (GameState, Option<(u64, ServerAdvertisement)>) {
    match platform::udp_port_bound(7551) {
        Ok(true) => {
            if let Some(world) = probe_host_server_data(discovery).await {
                (GameState::Hosting, Some(world))
            } else {
                (GameState::Browsing, None)
            }
        }
        Ok(false) => (GameState::Closed, None),
        Err(e) => {
            warn!("7551 probe failed: {}", e);
            (GameState::Closed, None)
        }
    }
}

/// Sends a real NetherNet discovery request to the local game through the
/// shared signaling socket. The response is handled by LanSignaling itself,
/// which records the game's address (needed for connect) and ServerData.
async fn probe_host_server_data(
    discovery: &Arc<DiscoveryService>,
) -> Option<(u64, ServerAdvertisement)> {
    use nethernet::protocol::packet::discovery::{self, RequestPacket};

    let probe_id: u64 = rand::random();
    let request = discovery::marshal(&RequestPacket, probe_id).ok()?;
    discovery
        .signaling()
        .socket()
        .send_to(&request, "127.0.0.1:7551")
        .await
        .ok()?;

    tokio::time::sleep(Duration::from_millis(250)).await;
    let servers = discovery.signaling().discover().await;
    let (game_id, sd) = servers.iter().next()?;
    Some((
        *game_id,
        ServerAdvertisement {
            server_name: sd.server_name.clone(),
            level_name: sd.level_name.clone(),
            game_type: sd.game_type,
            player_count: sd.player_count,
            max_player_count: sd.max_player_count,
            editor_world: sd.editor_world,
            hardcore: sd.hardcore,
            flag_a: sd.flag_a,
            flag_b: sd.flag_b,
            session_id: sd.session_id.clone(),
            transport_layer: sd.transport_layer,
            connection_type: sd.connection_type,
        },
    ))
}

async fn fetch_host_list(
    server: &mut Option<SharedWriter>,
    incoming: &SharedIncoming,
) -> mineshaft_core::Result<Vec<AdvertisedServer>> {
    let stream = server
        .as_ref()
        .ok_or(mineshaft_core::CoreError::InvalidState("not connected"))?
        .clone();
    {
        let mut guard = stream.lock().await;
        write_message(&mut *guard, &ClientMessage::ListHosts).await?;
    }
    // response arrives via the reader task channel
    let mut guard = incoming.lock().await;
    let rx = guard
        .as_mut()
        .ok_or(mineshaft_core::CoreError::InvalidState("no reader channel"))?;
    match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
        Ok(Some(ServerMessage::HostList(hosts))) => Ok(hosts),
        Ok(Some(_)) => Ok(vec![]),
        Ok(None) => Err(mineshaft_core::CoreError::InvalidState("reader closed")),
        Err(_) => Err(mineshaft_core::CoreError::Other(
            "list response timeout".into(),
        )),
    }
}
