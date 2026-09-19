//! Rendezvous server — the always-on remote node.
//!
//! Keeps the registry of hosting clients, hands the host list to browsing
//! clients, and relays join requests from joiners to hosts.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use mineshaft_core::CoreError;
use mineshaft_core::net::{
    ClientMessage, ServerMessage, read_message, set_tcp_keepalive, write_message,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Default listen address for the rendezvous server.
const BIND_ADDR: &str = "0.0.0.0:25000";

/// Close connections that have been completely silent for this long.
///
/// Clients send [`ClientMessage::Ping`] every few seconds even while hosting,
/// so a connection silent for this long is dead (e.g. a NAT or
/// port-forwarder idle drop that never delivered a FIN/RST).
const CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(45);

/// Maximum queued outbound messages per client. Signaling frames are small
/// and flow at human pace; a full queue means the client has stopped reading
/// (dead TCP connection), so further sends are dropped rather than buffered
/// without bound on this 24/7 process.
const CLIENT_QUEUE_CAPACITY: usize = 64;

/// Maximum relay sessions remembered at once. Session keys are
/// client-controlled (`connection_id` in `Signal`), so without a cap a
/// misbehaving client could grow the map forever.
const MAX_SESSIONS: usize = 4096;

/// A connected client with an optional registered host entry.
struct ClientHandle {
    tx: mpsc::Sender<ServerMessage>,
}

#[derive(Default)]
struct Registry {
    /// All connected clients by socket address.
    clients: DashMap<SocketAddr, ClientHandle>,
    /// sender_id -> addr of the client hosting it.
    hosts: DashMap<u64, SocketAddr>,
    /// sender_id -> full advertised record (for serving HostList).
    host_records: DashMap<u64, mineshaft_core::AdvertisedServer>,
    /// session_id -> (joiner addr, host addr) relay pairing.
    sessions: DashMap<u64, (SocketAddr, SocketAddr)>,
    /// next relay session id
    next_session_id: AtomicU64,
}

// The workload is timers plus small JSON frames over TCP; a single-threaded
// runtime avoids paying for one worker-thread stack per CPU core.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let bind = std::env::args()
        .nth(1)
        .unwrap_or_else(|| BIND_ADDR.to_string());
    let listener = TcpListener::bind(&bind).await?;
    info!("rendezvous server listening on {}", bind);

    let registry = Arc::new(Registry {
        next_session_id: AtomicU64::new(1),
        ..Registry::default()
    });

    loop {
        let (stream, addr) = listener.accept().await?;
        // Signaling frames are small and latency-sensitive; disable Nagle.
        let _ = stream.set_nodelay(true);
        // Detect silently dropped connections even when no traffic flows.
        let _ = set_tcp_keepalive(&stream);
        info!("client connected: {}", addr);
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, addr, registry.clone()).await {
                match e {
                    CoreError::FrameTooLarge(len) => info!(
                        "client {} sent an oversized frame ({} bytes); closing \
                         (most likely not a mineshaft peer)",
                        addr, len
                    ),
                    other => warn!("client {} error: {}", addr, other),
                }
            }
            registry.clients.remove(&addr);
            // Relay sessions involving this client are dead too.
            registry
                .sessions
                .retain(|_, (joiner, host)| *joiner != addr && *host != addr);
            let orphaned: Vec<u64> = registry
                .hosts
                .iter()
                .filter(|entry| *entry.value() == addr)
                .map(|entry| *entry.key())
                .collect();
            for id in orphaned {
                registry.hosts.remove(&id);
                registry.host_records.remove(&id);
                info!("host {:#x} unregistered (client {} left)", id, addr);
            }
            info!("client disconnected: {}", addr);
        });
    }
}

async fn handle_client(
    stream: TcpStream,
    addr: SocketAddr,
    registry: Arc<Registry>,
) -> mineshaft_core::Result<()> {
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(CLIENT_QUEUE_CAPACITY);
    registry.clients.insert(addr, ClientHandle { tx });

    let (mut read_half, mut write_half) = stream.into_split();
    // If the writer dies the connection is unusable: wake the read loop so
    // the whole connection is torn down instead of lingering half-open with
    // messages silently accumulating in the channel.
    let writer_dead = Arc::new(tokio::sync::Notify::new());
    let writer = tokio::spawn({
        let writer_dead = writer_dead.clone();
        async move {
            while let Some(msg) = rx.recv().await {
                if write_message(&mut write_half, &msg).await.is_err() {
                    break;
                }
            }
            writer_dead.notify_one();
        }
    });

    loop {
        let msg: Option<ClientMessage> = tokio::select! {
            _ = writer_dead.notified() => {
                warn!("client {} writer failed; closing connection", addr);
                break;
            }
            result = tokio::time::timeout(CLIENT_IDLE_TIMEOUT, read_message(&mut read_half)) => {
                match result {
                    Ok(Ok(msg)) => msg,
                    Ok(Err(e)) => {
                        writer.abort();
                        return Err(e);
                    }
                    Err(_) => {
                        info!("client {} silent for {:?}; closing connection", addr, CLIENT_IDLE_TIMEOUT);
                        break;
                    }
                }
            }
        };
        let Some(msg) = msg else { break };

        match msg {
            ClientMessage::Ping => {
                send_to(&registry, addr, ServerMessage::Pong);
            }
            ClientMessage::RegisterHost(server) => {
                info!(
                    "client {} registered host '{}'/'{}' ({:#x})",
                    addr, server.data.server_name, server.data.level_name, server.sender_id
                );
                registry.hosts.insert(server.sender_id, addr);
                registry.host_records.insert(server.sender_id, server.clone());
                send_to(
                    &registry,
                    addr,
                    ServerMessage::Registered {
                        sender_id: server.sender_id,
                    },
                );
            }
            ClientMessage::UpdateHost(server) => {
                let updated =
                    if registry.hosts.get(&server.sender_id).as_deref() == Some(&addr) {
                        registry.host_records.insert(server.sender_id, server.clone());
                        true
                    } else {
                        false
                    };
                if updated {
                    info!(
                        "client {} updated host '{}'/'{}' ({} players, {:#x})",
                        addr,
                        server.data.server_name,
                        server.data.level_name,
                        server.data.player_count,
                        server.sender_id
                    );
                } else {
                    warn!(
                        "client {} attempted to update unregistered host {:#x}",
                        addr, server.sender_id
                    );
                }
            }
            ClientMessage::UnregisterHost { sender_id } => {
                info!("client {} unregistered host {:#x}", addr, sender_id);
                if registry.hosts.get(&sender_id).as_deref() == Some(&addr) {
                    registry.hosts.remove(&sender_id);
                    registry.host_records.remove(&sender_id);
                }
            }
            ClientMessage::ListHosts => {
                let hosts: Vec<mineshaft_core::AdvertisedServer> = registry
                    .host_records
                    .iter()
                    .map(|entry| entry.value().clone())
                    .collect();
                info!(
                    "client {} requested host list ({} entries)",
                    addr,
                    hosts.len()
                );
                send_to(&registry, addr, ServerMessage::HostList(hosts));
            }
            ClientMessage::ConnectRequest {
                target_sender_id,
                joiner_network_id,
            } => {
                let host_addr = registry.hosts.get(&target_sender_id).map(|r| *r);
                match host_addr {
                    Some(host_addr) => {
                        let session_id = registry.next_session_id.fetch_add(1, Ordering::Relaxed);
                        if registry.sessions.len() < MAX_SESSIONS {
                            registry.sessions.insert(session_id, (addr, host_addr));
                        }
                        info!(
                            "client {} wants to join {:#x} -> session {} (host {})",
                            addr, target_sender_id, session_id, host_addr
                        );
                        send_to(
                            &registry,
                            host_addr,
                            ServerMessage::IncomingJoin {
                                joiner_network_id,
                                session_id,
                            },
                        );
                        send_to(&registry, addr, ServerMessage::JoinAccepted { session_id });
                    }
                    None => {
                        warn!(
                            "client {} requested unknown host {:#x}",
                            addr, target_sender_id
                        );
                        send_to(
                            &registry,
                            addr,
                            ServerMessage::Error(format!("no such host: {:#x}", target_sender_id)),
                        );
                    }
                }
            }
            ClientMessage::Signal {
                target_sender_id,
                connection_id,
                joiner_network_id,
                data,
            } => {
                let kind = data.split(' ').next().unwrap_or("?").to_string();
                // joiner -> host: target_sender_id identifies the host
                // host -> joiner: target_sender_id == 0, route via session map
                let dest = {
                    if target_sender_id != 0 {
                        let host = registry.hosts.get(&target_sender_id).map(|r| *r);
                        if let Some(host) = host {
                            if registry.sessions.len() < MAX_SESSIONS
                                || registry.sessions.contains_key(&connection_id)
                            {
                                registry.sessions.insert(connection_id, (addr, host));
                            }
                            info!(
                                "signal {} conn {} from {} -> host {} (target {:#x})",
                                kind, connection_id, addr, host, target_sender_id
                            );
                        }
                        host
                    } else {
                        let joiner = registry.sessions.get(&connection_id).map(|r| r.0);
                        if let Some(j) = joiner {
                            debug!(
                                "signal {} conn {} from host -> joiner {}",
                                kind, connection_id, j
                            );
                        }
                        joiner
                    }
                };
                match dest {
                    Some(dest) => {
                        send_to(
                            &registry,
                            dest,
                            ServerMessage::Signal {
                                connection_id,
                                joiner_network_id,
                                data,
                            },
                        );
                    }
                    None => warn!("no route for signal conn {} from {}", connection_id, addr),
                }
            }
        }
    }

    writer.abort();
    Ok(())
}

/// Queue a message for a client. When the client's queue is full the client
/// has stopped reading (dead connection that the idle timeout will reap), so
/// the message is dropped instead of growing memory without bound.
fn send_to(registry: &Arc<Registry>, addr: SocketAddr, msg: ServerMessage) {
    if let Some(client) = registry.clients.get(&addr)
        && client.tx.try_send(msg).is_err()
    {
        warn!("client {} outbound queue full; dropping message", addr);
    }
}
