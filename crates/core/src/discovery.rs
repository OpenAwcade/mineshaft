//! Discovery service built on the ephemeral-advertiser model proven on-device.
//!
//! The local game owns UDP 7551 while the Friends tab is open. Instead of
//! competing for that port, mineshaft advertises by sending ServerData v6
//! response packets from an ephemeral socket directly to the game's client
//! socket (`127.0.0.1:7551`, `[::1]:7551`, etc.) at a steady heartbeat.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use nethernet::protocol::packet::discovery::{self, MessagePacket, ResponsePacket, ServerData};
use nethernet::{LanConfig, LanSignaling};
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{DiscoveryConfig, ServerAdvertisement};
use crate::error::{CoreError, Result};

/// One advertised server entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AdvertisedServer {
    /// Sender/network ID used in the discovery response.
    pub sender_id: u64,
    /// Server metadata encoded into the response.
    pub data: ServerAdvertisement,
}

/// Shared handle to the discovery service.
pub type SharedDiscoveryService = Arc<DiscoveryService>;

/// Something arrived on the advertiser socket from the game.
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    /// The game wants to join one of our advertised servers; the payload is
    /// the raw WebRTC negotiation message (`CONNECTREQUEST <conn_id> <sdp>`).
    ConnectRequest {
        /// The advertised sender/network ID the player clicked.
        target_sender_id: u64,
        /// Raw message data as sent by the game.
        data: String,
    },
}

/// Advertises server entries into the local game and owns the LAN signaling
/// lifecycle for negotiation.
pub struct DiscoveryService {
    config: DiscoveryConfig,
    advertised: RwLock<HashMap<u64, ServerAdvertisement>>,
    socket: Arc<UdpSocket>,
    events: broadcast::Sender<DiscoveryEvent>,
    cancel: CancellationToken,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl DiscoveryService {
    /// Create a service using the platform socket factory.
    pub async fn new(config: DiscoveryConfig) -> Result<SharedDiscoveryService> {
        let socket = crate::platform::create_udp_socket(config.bind_addr)
            .map_err(|e| CoreError::Platform(e.to_string()))?;
        Self::with_socket(config, socket).await
    }

    /// Create a service from a pre-bound socket.
    pub async fn with_socket(
        config: DiscoveryConfig,
        socket: std::net::UdpSocket,
    ) -> Result<SharedDiscoveryService> {
        socket.set_nonblocking(true)?;
        let socket = Arc::new(UdpSocket::from_std(socket)?);
        let (events, _) = broadcast::channel(64);
        Ok(Arc::new(Self {
            config,
            advertised: RwLock::new(HashMap::new()),
            socket,
            events,
            cancel: CancellationToken::new(),
            task: tokio::sync::Mutex::new(None),
        }))
    }

    /// Register an advertised server entry.
    pub fn advertise(&self, sender_id: u64, data: ServerAdvertisement) {
        self.advertised
            .write()
            .expect("advertised registry poisoned")
            .insert(sender_id, data);
    }

    /// Remove an advertised server entry.
    pub fn unadvertise(&self, sender_id: u64) {
        self.advertised
            .write()
            .expect("advertised registry poisoned")
            .remove(&sender_id);
    }

    /// Replace the advertised set atomically.
    pub fn replace_all(&self, entries: Vec<AdvertisedServer>) {
        let mut guard = self.advertised.write().expect("advertised registry poisoned");
        guard.clear();
        for entry in entries {
            guard.insert(entry.sender_id, entry.data);
        }
    }

    /// Snapshot currently advertised entries.
    pub fn advertised(&self) -> Vec<AdvertisedServer> {
        self.advertised
            .read()
            .expect("advertised registry poisoned")
            .iter()
            .map(|(sender_id, data)| AdvertisedServer {
                sender_id: *sender_id,
                data: data.clone(),
            })
            .collect()
    }

    /// Start the heartbeat advertisement loop.
    pub async fn start(self: &Arc<Self>) {
        let mut task = self.task.lock().await;
        if task.is_some() {
            return;
        }

        let service = self.clone();
        let cancel = self.cancel.clone();
        let interval = self.config.heartbeat_interval;
        let targets = self.config.local_targets.clone();

        // heartbeat sender
        *task = Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        service.broadcast_once(&targets).await;
                    }
                }
            }
        }));

        // inbound listener: catches the game's CONNECTREQUEST messages
        let service = self.clone();
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    result = service.socket.recv_from(&mut buf) => {
                        match result {
                            Ok((n, from)) => service.handle_inbound(&buf[..n], from).await,
                            Err(e) => tracing::trace!("advertiser recv error: {}", e),
                        }
                    }
                }
            }
        });
    }

    /// Subscribe to inbound discovery events (e.g. the game clicking a server).
    pub fn subscribe(&self) -> broadcast::Receiver<DiscoveryEvent> {
        self.events.subscribe()
    }

    async fn handle_inbound(&self, data: &[u8], from: SocketAddr) {
        let Ok((packet, sender_id)) = discovery::unmarshal(data) else {
            tracing::trace!("ignoring unparseable packet from {}", from);
            return;
        };
        if let Some(message) = packet.as_any().downcast_ref::<MessagePacket>() {
            if message.data.starts_with("CONNECTREQUEST") {
                tracing::info!(
                    "game wants to join advertised server {:#x} (from {})",
                    message.recipient_id,
                    from
                );
                let _ = self.events.send(DiscoveryEvent::ConnectRequest {
                    target_sender_id: message.recipient_id,
                    data: message.data.clone(),
                });
            } else {
                tracing::debug!(
                    "message packet from {} (sender {:#x}): {}",
                    from,
                    sender_id,
                    message.data
                );
            }
        }
    }

    /// Stop the heartbeat loop.
    pub async fn stop(&self) {
        self.cancel.cancel();
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }

    /// Build the NetherNet LAN signaling stack used for WebRTC negotiation.
    ///
    /// The signaling socket is separate from the advertiser socket so the
    /// negotiation path can keep working even if the advertiser is restarted.
    pub async fn build_signaling(&self, network_id: u64) -> Result<LanSignaling> {
        let config = LanConfig {
            broadcast_address: None,
            ..LanConfig::default()
        };
        Ok(LanSignaling::with_config(network_id, self.config.bind_addr, config).await?)
    }

    async fn broadcast_once(&self, targets: &[SocketAddr]) {
        let entries = self.advertised();
        for entry in entries {
            let packet = match build_response_packet(&entry) {
                Ok(packet) => packet,
                Err(e) => {
                    tracing::warn!("failed to marshal discovery response: {}", e);
                    continue;
                }
            };

            for target in targets {
                if let Err(e) = self.socket.send_to(&packet, target).await {
                    tracing::trace!("advertisement send to {} failed: {}", target, e);
                }
            }
        }
    }
}

fn build_response_packet(entry: &AdvertisedServer) -> Result<Vec<u8>> {
    let data = ServerData {
        server_name: entry.data.server_name.clone(),
        level_name: entry.data.level_name.clone(),
        game_type: entry.data.game_type,
        player_count: entry.data.player_count,
        max_player_count: entry.data.max_player_count,
        editor_world: entry.data.editor_world,
        hardcore: entry.data.hardcore,
        flag_a: entry.data.flag_a,
        flag_b: entry.data.flag_b,
        session_id: entry.data.session_id.clone(),
        transport_layer: entry.data.transport_layer,
        connection_type: entry.data.connection_type,
    };

    let response = ResponsePacket::new(data.marshal()?);
    Ok(discovery::marshal(&response, entry.sender_id)?)
}

impl Drop for DiscoveryService {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
