//! Discovery service built on the ephemeral-advertiser model proven on-device.
//!
//! The local game owns UDP 7551 while the Friends tab is open. Instead of
//! competing for that port, mineshaft advertises by sending ServerData v6
//! response packets from an ephemeral socket directly to the game's client
//! socket (`127.0.0.1:7551`, `[::1]:7551`, etc.) at a steady heartbeat.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use nethernet_tokio::protocol::NetherCodec;
use nethernet_tokio::protocol::packet::discovery::{self, Packets, ResponsePacket, ServerData};
use nethernet_tokio::{LanConfig, LanSignaling};
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

/// Advertises server entries into the local game and owns the LAN signaling
/// lifecycle for negotiation.
pub struct DiscoveryService {
    config: DiscoveryConfig,
    advertised: RwLock<HashMap<u64, CachedAdvertisement>>,
    signaling: Arc<LanSignaling>,
    cancel: CancellationToken,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

/// An advertised server together with its pre-marshaled discovery response,
/// rebuilt only when the registry changes rather than on every heartbeat.
struct CachedAdvertisement {
    data: ServerAdvertisement,
    packet: Arc<[u8]>,
}

impl DiscoveryService {
    /// Create a service using the platform socket factory.
    pub async fn new(network_id: u64, config: DiscoveryConfig) -> Result<SharedDiscoveryService> {
        let socket = crate::platform::create_udp_socket(config.bind_addr)
            .map_err(|e| CoreError::Platform(e.to_string()))?;
        Self::with_socket(network_id, config, socket).await
    }

    /// Create a service from a pre-bound socket.
    pub async fn with_socket(
        network_id: u64,
        config: DiscoveryConfig,
        socket: std::net::UdpSocket,
    ) -> Result<SharedDiscoveryService> {
        socket.set_nonblocking(true)?;
        let socket = tokio::net::UdpSocket::from_std(socket)?;
        let signaling = LanSignaling::with_socket(
            network_id,
            socket,
            LanConfig {
                broadcast_address: None,
                // We advertise servers whose sender ids are not our own
                // network id, and the game addresses CONNECTREQUESTs to them.
                accept_any_recipient: true,
                ..LanConfig::default()
            },
        )
        .await?;
        let signaling = Arc::new(signaling);
        Ok(Arc::new(Self {
            config,
            advertised: RwLock::new(HashMap::new()),
            signaling,
            cancel: CancellationToken::new(),
            task: tokio::sync::Mutex::new(None),
        }))
    }

    /// The shared LAN signaling stack (game offers/signals arrive here).
    pub fn signaling(&self) -> Arc<LanSignaling> {
        self.signaling.clone()
    }

    /// Register an advertised server entry.
    pub fn advertise(&self, sender_id: u64, data: ServerAdvertisement) {
        match build_response_packet(sender_id, &data) {
            Ok(packet) => {
                self.advertised
                    .write()
                    .expect("advertised registry poisoned")
                    .insert(sender_id, CachedAdvertisement { data, packet });
            }
            Err(e) => {
                tracing::warn!("failed to marshal discovery response: {}", e);
            }
        }
    }

    /// Update an advertised host and rebuild its discovery response packet.
    pub fn update_host(&self, sender_id: u64, data: ServerAdvertisement) {
        self.advertise(sender_id, data);
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
        let mut guard = self
            .advertised
            .write()
            .expect("advertised registry poisoned");
        // Drop entries that are no longer advertised
        guard.retain(|sender_id, _| entries.iter().any(|e| e.sender_id == *sender_id));
        for entry in entries {
            // Skip re-marshaling when the advertisement has not changed
            if let Some(existing) = guard.get(&entry.sender_id)
                && existing.data == entry.data
            {
                continue;
            }
            match build_response_packet(entry.sender_id, &entry.data) {
                Ok(packet) => {
                    guard.insert(
                        entry.sender_id,
                        CachedAdvertisement {
                            data: entry.data,
                            packet,
                        },
                    );
                }
                Err(e) => {
                    tracing::warn!("failed to marshal discovery response: {}", e);
                }
            }
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
                data: data.data.clone(),
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
    }

    /// Stop the heartbeat loop.
    pub async fn stop(&self) {
        self.cancel.cancel();
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }

    async fn broadcast_once(&self, targets: &[SocketAddr]) {
        let packets: Vec<Arc<[u8]>> = self
            .advertised
            .read()
            .expect("advertised registry poisoned")
            .values()
            .map(|entry| entry.packet.clone())
            .collect();
        let socket = self.signaling.socket();
        for packet in packets {
            for target in targets {
                if let Err(e) = socket.send_to(&packet, target).await {
                    tracing::trace!("advertisement send to {} failed: {}", target, e);
                }
            }
        }
    }
}

fn build_response_packet(sender_id: u64, data: &ServerAdvertisement) -> Result<Arc<[u8]>> {
    let server_data = ServerData {
        server_name: data.server_name.clone(),
        level_name: data.level_name.clone(),
        game_type: data.game_type,
        player_count: data.player_count,
        max_player_count: data.max_player_count,
        editor_world: data.editor_world,
        hardcore: data.hardcore,
        flag_a: data.flag_a,
        flag_b: data.flag_b,
        session_id: data.session_id.clone(),
        transport_layer: data.transport_layer,
        connection_type: data.connection_type,
        protocol_version: data.protocol_version,
        game_version: data.game_version.clone(),
    };

    let mut application_data = Vec::with_capacity(server_data.size_hint());
    server_data
        .serialize(&mut application_data)
        .map_err(nethernet_tokio::NetherError::from)?;

    let response = ResponsePacket::new(application_data);
    let packet = discovery::encode(&Packets::Response(response), sender_id)
        .map_err(nethernet_tokio::NetherError::from)?;
    Ok(packet.into())
}

impl Drop for DiscoveryService {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
