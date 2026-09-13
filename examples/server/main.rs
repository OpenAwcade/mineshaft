//! Rendezvous server — the always-on remote node.
//!
//! Keeps the registry of hosting clients, hands the host list to browsing
//! clients, and relays join requests from joiners to hosts.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use mineshaft_core::net::{ClientMessage, ServerMessage, read_message, write_message};
use mineshaft_core::{AdvertisedServer, ServerAdvertisement};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tracing::{info, warn};

/// Default listen address for the rendezvous server.
const BIND_ADDR: &str = "0.0.0.0:25000";

/// A connected client with an optional registered host entry.
struct ClientHandle {
    tx: mpsc::UnboundedSender<ServerMessage>,
}

#[derive(Default)]
struct Registry {
    /// All connected clients by socket address.
    clients: HashMap<SocketAddr, ClientHandle>,
    /// sender_id -> addr of the client hosting it.
    hosts: HashMap<u64, SocketAddr>,
    /// sender_id -> full advertised record (for serving HostList).
    host_records: HashMap<u64, mineshaft_core::AdvertisedServer>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let bind = std::env::args().nth(1).unwrap_or_else(|| BIND_ADDR.to_string());
    let listener = TcpListener::bind(&bind).await?;
    info!("rendezvous server listening on {}", bind);

    let registry = Arc::new(Mutex::new(Registry::default()));

    // Permanent dummy host so browsing clients always see something.
    {
        let mut ad = ServerAdvertisement::default();
        ad.server_name = "mineshaft dummy".to_string();
        ad.level_name = "Dummy World".to_string();
        ad.player_count = 1;
        ad.max_player_count = 8;
        ad.session_id = "00000000deadbeef".to_string();
        let dummy = AdvertisedServer {
            sender_id: 0x00000000deadbeef,
            data: ad,
        };
        let mut reg = registry.lock().await;
        reg.host_records.insert(dummy.sender_id, dummy);
        info!("dummy host {:#x} seeded (permanent)", 0xdeadbeefu64);
    }

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("client connected: {}", addr);
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, addr, registry.clone()).await {
                warn!("client {} error: {}", addr, e);
            }
            let mut reg = registry.lock().await;
            reg.clients.remove(&addr);
            let orphaned: Vec<u64> = reg
                .hosts
                .iter()
                .filter(|(_, a)| **a == addr)
                .map(|(id, _)| *id)
                .collect();
            for id in orphaned {
                reg.hosts.remove(&id);
                reg.host_records.remove(&id);
                info!("host {:#x} unregistered (client {} left)", id, addr);
            }
            info!("client disconnected: {}", addr);
        });
    }
}

async fn handle_client(
    stream: TcpStream,
    addr: SocketAddr,
    registry: Arc<Mutex<Registry>>,
) -> mineshaft_core::Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMessage>();
    registry
        .lock()
        .await
        .clients
        .insert(addr, ClientHandle { tx });

    let (mut read_half, mut write_half) = stream.into_split();
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write_message(&mut write_half, &msg).await.is_err() {
                break;
            }
        }
    });

    loop {
        let msg: Option<ClientMessage> = match read_message(&mut read_half).await {
            Ok(msg) => msg,
            Err(e) => {
                writer.abort();
                return Err(e);
            }
        };
        let Some(msg) = msg else { break };

        match msg {
            ClientMessage::RegisterHost(server) => {
                info!(
                    "client {} registered host '{}'/'{}' ({:#x})",
                    addr, server.data.server_name, server.data.level_name, server.sender_id
                );
                {
                    let mut reg = registry.lock().await;
                    reg.hosts.insert(server.sender_id, addr);
                    reg.host_records.insert(server.sender_id, server.clone());
                }
                send_to(&registry, addr, ServerMessage::Registered {
                    sender_id: server.sender_id,
                })
                .await;
            }
            ClientMessage::UnregisterHost { sender_id } => {
                info!("client {} unregistered host {:#x}", addr, sender_id);
                {
                    let mut reg = registry.lock().await;
                    reg.hosts.remove(&sender_id);
                    reg.host_records.remove(&sender_id);
                }
            }
            ClientMessage::ListHosts => {
                let hosts: Vec<mineshaft_core::AdvertisedServer> = registry
                    .lock()
                    .await
                    .host_records
                    .values()
                    .cloned()
                    .collect();
                info!("client {} requested host list ({} entries)", addr, hosts.len());
                send_to(&registry, addr, ServerMessage::HostList(hosts)).await;
            }
            ClientMessage::ConnectRequest {
                target_sender_id,
                joiner_network_id,
            } => {
                let host_addr = registry.lock().await.hosts.get(&target_sender_id).copied();
                match host_addr {
                    Some(host_addr) => {
                        info!(
                            "client {} wants to join {:#x} (relaying to host {})",
                            addr, target_sender_id, host_addr
                        );
                        send_to(
                            &registry,
                            host_addr,
                            ServerMessage::IncomingJoin { joiner_network_id },
                        )
                        .await;
                    }
                    None => {
                        warn!(
                            "client {} requested unknown host {:#x}",
                            addr, target_sender_id
                        );
                        send_to(
                            &registry,
                            addr,
                            ServerMessage::Error(format!(
                                "no such host: {:#x}",
                                target_sender_id
                            )),
                        )
                        .await;
                    }
                }
            }
        }
    }

    writer.abort();
    Ok(())
}

async fn send_to(registry: &Arc<Mutex<Registry>>, addr: SocketAddr, msg: ServerMessage) {
    if let Some(client) = registry.lock().await.clients.get(&addr) {
        let _ = client.tx.send(msg);
    }
}
