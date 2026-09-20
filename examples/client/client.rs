//! Mineshaft node client — runs 24/7 alongside the game.
//!
//! Signal-forwarding architecture: the two games establish WebRTC directly
//! with each other. We only shuttle the discovery signaling messages
//! (CONNECTREQUEST/CONNECTRESPONSE/CANDIDATEADD) between the local game and
//! the remote host over the rendezvous server.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::StreamExt;
use mineshaft_core::net::{ClientMessage, ServerMessage, read_message, write_message};
use mineshaft_core::{
    AdvertisedServer, DiscoveryConfig, DiscoveryService, ServerAdvertisement, platform,
};
use nethernet_tokio::protocol::packet::discovery::{self, MessagePacket, Packets};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:25000";
const LOOP_INTERVAL: Duration = Duration::from_secs(2);
/// How often we ping the rendezvous server. Keeps NAT and port-forwarder
/// state alive on otherwise idle connections (a hosting client sends nothing
/// else) and drives liveness detection on both ends.
const PING_INTERVAL: Duration = Duration::from_secs(15);
/// Reconnect when no message (e.g. a pong) has arrived for this long.
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(45);

/// Maximum remembered join connections. Entries are inserted when the local
/// game sends an offer and live for the rest of the process; a game that
/// hammers offers would otherwise grow this map forever.
const MAX_JOIN_CONNS: usize = 1024;

/// Buffer between the connection reader task and the main loop. Only
/// `HostList` responses flow through here; a full buffer means the main loop
/// is stuck, so the reader drops extras instead of accumulating them.
const INCOMING_QUEUE_CAPACITY: usize = 16;

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

// The workload is timers plus small UDP/TCP messages; a single-threaded
// runtime avoids paying for one worker-thread stack per CPU core, which
// matters for a process that runs 24/7 next to the game.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug")),
        )
        .init();

    let server_addr = resolve_server_addr();
    info!(
        "mineshaft node starting, rendezvous server: {}",
        server_addr
    );

    #[cfg(target_os = "windows")]
    if let Err(e) = platform::ensure_loopback_exempt() {
        warn!(
            "Minecraft loopback exemption is missing: {}. Run PowerShell as \
             Administrator once with: CheckNetIsolation LoopbackExempt -a \
             -n=Microsoft.MinecraftUWP_8wekyb3d8bbwe",
            e
        );
    }

    let my_network_id: u64 = rand::random();
    info!("our network id: {:#x}", my_network_id);

    let discovery_config = {
        #[cfg(target_os = "windows")]
        {
            // Minecraft for Windows is a UWP app. Keep the signaling socket
            // on loopback so both directions use 127.0.0.1 explicitly.
            let mut config = DiscoveryConfig::default();
            config.bind_addr = "127.0.0.1:0"
                .parse()
                .expect("valid Windows loopback bind address");
            config
        }
        #[cfg(not(target_os = "windows"))]
        {
            DiscoveryConfig::default()
        }
    };
    let discovery = DiscoveryService::new(my_network_id, discovery_config).await?;
    discovery.start().await;
    let signaling = discovery.signaling();

    // connection_id -> (game network id, advertised target id) for joiner-side
    // response injection
    let join_conns: Arc<DashMap<u64, (u64, u64)>> = Arc::new(DashMap::new());
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
                let hosted_game = local_game_id
                    .lock()
                    .await
                    .map(|id| id == sender)
                    .unwrap_or(false);
                let from_local_game = hosted_game || target.is_some();

                if !from_local_game {
                    continue;
                }

                let target_id = if hosted_game {
                    // host side: server routes back through the connection map
                    0
                } else if let Some(t) = target {
                    // joiner side: remember who the game clicked + the game's id
                    if join_conns.len() >= MAX_JOIN_CONNS {
                        warn!("join connection map full; dropping oldest entry");
                        if let Some(oldest) = join_conns.iter().next().map(|e| *e.key()) {
                            join_conns.remove(&oldest);
                        }
                    }
                    join_conns.insert(conn, (sender, t));
                    t
                } else {
                    unreachable!("from_local_game implies a target or hosted game")
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
    let mut registered_advertisement: Option<ServerAdvertisement> = None;
    let mut advertised_count = 0usize;
    // Tracks whether the reader task of the current connection is alive; a
    // dead reader means the connection is gone even if writes still succeed.
    let reader_alive = Arc::new(AtomicBool::new(false));
    // Last time any message arrived from the server; pong replies to our
    // pings keep this fresh. A stale timestamp means the connection died
    // silently (e.g. NAT/port-forwarder idle drop) even though the reader
    // task is still blocked on a read.
    let last_seen = Arc::new(std::sync::Mutex::new(Instant::now()));
    let mut last_ping = Instant::now();
    // Abort handle of the current connection's reader task, so a dead
    // connection's reader does not linger after a reconnect.
    let mut reader_task: Option<tokio::task::AbortHandle> = None;

    loop {
        // The connection was dropped somewhere below: make sure the dead
        // reader task and the shared writer are cleaned up before reconnecting.
        if server.is_none()
            && let Some(handle) = reader_task.take()
        {
            handle.abort();
            *server_writer.lock().await = None;
        }
        if server.is_some() {
            let reader_dead = !reader_alive.load(Ordering::Relaxed);
            let silent = last_seen.lock().unwrap().elapsed() > LIVENESS_TIMEOUT;
            if reader_dead || silent {
                if silent && !reader_dead {
                    warn!(
                        "no message from rendezvous server for {:?}; reconnecting",
                        LIVENESS_TIMEOUT
                    );
                } else {
                    warn!("rendezvous connection lost; reconnecting");
                }
                server = None;
                continue;
            }
        }
        if server.is_none() {
            match TcpStream::connect(&server_addr).await {
                Ok(stream) => {
                    // Signaling frames are small and latency-sensitive; disable Nagle.
                    let _ = stream.set_nodelay(true);
                    // Detect silently dropped connections even while idle.
                    let _ = mineshaft_core::net::set_tcp_keepalive(&stream);
                    info!("connected to rendezvous server {}", server_addr);
                    let (mut read_half, write_half) = stream.into_split();
                    let write_half = Arc::new(tokio::sync::Mutex::new(write_half));
                    *server_writer.lock().await = Some(write_half.clone());

                    let join_conns = join_conns.clone();
                    let local_game_id = local_game_id.clone();
                    let signaling = signaling.clone();
                    let reader_alive = reader_alive.clone();
                    let last_seen = last_seen.clone();
                    let (incoming_tx, incoming_rx) =
                        tokio::sync::mpsc::channel::<ServerMessage>(INCOMING_QUEUE_CAPACITY);
                    reader_alive.store(true, Ordering::Relaxed);
                    *last_seen.lock().unwrap() = Instant::now();
                    last_ping = Instant::now();
                    let task = tokio::spawn(async move {
                        loop {
                            let msg = match read_message::<ServerMessage, _>(&mut read_half).await {
                                Ok(Some(msg)) => msg,
                                Ok(None) => {
                                    warn!("rendezvous server closed the connection");
                                    break;
                                }
                                Err(e) => {
                                    warn!("rendezvous read error: {}", e);
                                    break;
                                }
                            };
                            *last_seen.lock().unwrap() = Instant::now();
                            match msg {
                                msg @ ServerMessage::HostList(_) => {
                                    // Main loop is the only consumer; if it is
                                    // stuck the extra responses are worthless.
                                    let _ = incoming_tx.try_send(msg);
                                }
                                ServerMessage::Signal {
                                    connection_id,
                                    joiner_network_id,
                                    data,
                                } => {
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
                                ServerMessage::IncomingJoin {
                                    joiner_network_id,
                                    session_id,
                                } => {
                                    info!(
                                        "joiner {:#x} signaling into our world (session {})",
                                        joiner_network_id, session_id
                                    );
                                }
                                ServerMessage::JoinAccepted { session_id } => {
                                    debug!("server noted session {}", session_id);
                                }
                                ServerMessage::Registered { sender_id } => {
                                    debug!("server acked registration {:#x}", sender_id);
                                }
                                ServerMessage::Pong => {
                                    debug!("pong from rendezvous server");
                                }
                                ServerMessage::Error(e) => warn!("server error: {}", e),
                            }
                        }
                        reader_alive.store(false, Ordering::Relaxed);
                    });
                    reader_task = Some(task.abort_handle());

                    server = Some(write_half);
                    *server_incoming.lock().await = Some(incoming_rx);
                    // The server drops our host registration when the old
                    // connection dies, so register again on the next
                    // Hosting detection below.
                    hosting_registered = false;
                    registered_advertisement = None;
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
                    registered_advertisement = None;
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
                let ad = world.map(|(_, ad)| ad).unwrap_or_else(|| {
                    warn!("hosting but probe gave no data; using defaults");
                    let mut ad = ServerAdvertisement::default();
                    ad.session_id = format!("{:016x}", my_network_id);
                    ad
                });
                let host_changed =
                    !hosting_registered || registered_advertisement.as_ref() != Some(&ad);
                if host_changed {
                    if advertised_count > 0 {
                        discovery.replace_all(vec![]);
                        advertised_count = 0;
                    }
                    discovery.update_host(my_network_id, ad.clone());
                    let update_kind = if hosting_registered {
                        "updated"
                    } else {
                        "registering"
                    };
                    info!(
                        "game is hosting '{}'/'{}' ({} players, mode {}) — {} with server",
                        ad.server_name, ad.level_name, ad.player_count, ad.game_type, update_kind
                    );
                    let message = AdvertisedServer {
                        sender_id: my_network_id,
                        data: ad.clone(),
                    };
                    let client_message = if hosting_registered {
                        ClientMessage::UpdateHost(message)
                    } else {
                        ClientMessage::RegisterHost(message)
                    };
                    send_or_drop(&mut server, &client_message).await;
                    hosting_registered = server.is_some();
                    registered_advertisement = hosting_registered.then_some(ad);
                }
            }
            (GameState::Browsing, _) => {
                // Clear this before handling any remote signaling. A stale game
                // id from a previous world would make inject_signal treat this
                // node as a host and deliver joiner-side answers to the wrong
                // recipient.
                *local_game_id.lock().await = None;
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
                    registered_advertisement = None;
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
                    Err(e) => {
                        // A failed fetch usually means the connection is dead;
                        // drop it so the next iteration reconnects immediately
                        // instead of waiting for the liveness timeout.
                        warn!("host list fetch failed: {}; reconnecting", e);
                        server = None;
                    }
                }
            }
        }

        // Heartbeat: keeps the connection alive through NATs/port forwarders
        // and lets both sides detect a dead connection even while hosting.
        if server.is_some() && last_ping.elapsed() >= PING_INTERVAL {
            send_or_drop(&mut server, &ClientMessage::Ping).await;
            last_ping = Instant::now();
        }

        tokio::time::sleep(LOOP_INTERVAL).await;
    }
}

/// Inject a remote signaling message into the local game as a discovery
/// MessagePacket.
async fn inject_signal(
    signaling: &Arc<nethernet_tokio::LanSignaling>,
    join_conns: &Arc<DashMap<u64, (u64, u64)>>,
    local_game_id: &Arc<tokio::sync::Mutex<Option<u64>>>,
    connection_id: u64,
    joiner_network_id: u64,
    data: String,
) {
    // Who should the game think this is from, and who receives it?
    let (sender, recipient) =
        if let Some((game_id, target)) = join_conns.get(&connection_id).map(|e| *e) {
            // we are joining: present the advertised server as the sender
            (target, game_id)
        } else if let Some(game_id) = *local_game_id.lock().await {
            // we are hosting: present the joiner as the sender, our game receives
            (joiner_network_id, game_id)
        } else {
            warn!("no route for signal conn {}", connection_id);
            return;
        };

    let packet = MessagePacket::new(recipient, data);
    match discovery::encode(&Packets::Message(packet), sender) {
        Ok(bytes) => {
            if !send_to_local_game(&signaling.socket(), &bytes).await {
                warn!("signal injection reached no local game socket");
            }
        }
        Err(e) => warn!("failed to marshal signal: {}", e),
    }
}

/// Sends `bytes` to the local game's discovery port on the loopback family
/// the socket supports. A socket can only address its own family: the
/// default `0.0.0.0` bind can never reach `[::1]` (EAFNOSUPPORT), so the
/// cross-family target is skipped rather than logged as a failure.
/// Returns true if at least one datagram was handed to the OS.
async fn send_to_local_game(socket: &tokio::net::UdpSocket, bytes: &[u8]) -> bool {
    let targets: &[&str] = match socket.local_addr() {
        Ok(addr) if addr.is_ipv6() => &["[::1]:7551"],
        _ => &["127.0.0.1:7551"],
    };
    let mut sent = false;
    for target in targets {
        match socket.send_to(bytes, target).await {
            Ok(_) => sent = true,
            Err(e) => debug!("local game send to {} failed: {}", target, e),
        }
    }
    sent
}

type SharedIncoming = Arc<tokio::sync::Mutex<Option<tokio::sync::mpsc::Receiver<ServerMessage>>>>;

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
    use nethernet_tokio::protocol::packet::discovery::{RequestPacket, encode};

    let signaling = discovery.signaling();
    // Only consider responses to this probe; stale entries from an earlier
    // game instance must not be mistaken for the current world.
    signaling.clear_discovered().await;
    let probe_id: u64 = rand::random();
    let request = encode(&Packets::Request(RequestPacket), probe_id).ok()?;
    if !send_to_local_game(&signaling.socket(), &request).await {
        return None;
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    let servers = signaling.discover().await;
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
            protocol_version: sd.protocol_version,
            game_version: sd.game_version.clone(),
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
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(mineshaft_core::CoreError::Other(
                "list response timeout".into(),
            ));
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ServerMessage::HostList(hosts))) => return Ok(hosts),
            Ok(Some(ServerMessage::Pong)) => continue,
            Ok(Some(_)) => continue,
            Ok(None) => {
                return Err(mineshaft_core::CoreError::InvalidState("reader closed"));
            }
            Err(_) => {
                return Err(mineshaft_core::CoreError::Other(
                    "list response timeout".into(),
                ));
            }
        }
    }
}
