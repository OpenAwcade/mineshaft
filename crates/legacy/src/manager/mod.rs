use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{RwLock, watch};
use tracing::{info, warn};

use crate::protocol::{BedrockIdentifier, build_unconnected_pong};

#[derive(Debug)]
pub struct RemoteServer {
    pub target_client_id: u32,
    pub target_raknet_id: u64,
    pub relay_addr: SocketAddr,
    pub raw_identifier: String,
    pub parsed_identifier: BedrockIdentifier,
    pub ephemeral_port: u16,
    pub local_socket: Arc<UdpSocket>,
    /// Tracks the local Minecraft client address communicating with this ephemeral port
    pub last_client_addr: Arc<RwLock<Option<SocketAddr>>>,
    shutdown_tx: watch::Sender<bool>,
}

impl RemoteServer {
    /// Builds a synthetic RakNet ID_UNCONNECTED_PONG packet for this remote server,
    /// with the advertised port replaced by this server's ephemeral port.
    pub fn build_synthetic_pong(
        &self,
        timestamp: u64,
    ) -> Result<Vec<u8>, crate::protocol::PacketError> {
        let rewritten_ident = self
            .parsed_identifier
            .clone()
            .with_ephemeral_port(self.ephemeral_port)
            .with_guid(self.target_raknet_id);

        let ident_str = rewritten_ident.serialize_to_string();
        build_unconnected_pong(timestamp, self.target_raknet_id, &ident_str)
    }

    pub(crate) fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

pub struct ServerManager {
    /// Ordered map equivalent to C++ std::map<uint32_t, ServerEntry> (at 0x180002740)
    servers: RwLock<BTreeMap<u32, Arc<RemoteServer>>>,
    /// Port multiplexing table mapping ephemeral_port -> target_client_id
    port_map: RwLock<HashMap<u16, u32>>,
    default_bind_ip: RwLock<IpAddr>,
}

impl Default for ServerManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerManager {
    pub fn new() -> Self {
        Self {
            servers: RwLock::new(BTreeMap::new()),
            port_map: RwLock::new(HashMap::new()),
            default_bind_ip: RwLock::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
        }
    }

    pub async fn set_bind_ip(&self, ip: IpAddr) {
        let mut bind_ip = self.default_bind_ip.write().await;
        *bind_ip = ip;
    }

    pub async fn get_bind_ip(&self) -> IpAddr {
        *self.default_bind_ip.read().await
    }

    /// Adds a remote friend's server, allocating a local ephemeral UDP socket.
    pub async fn add_server(
        &self,
        target_client_id: u32,
        target_raknet_id: u64,
        relay_addr: SocketAddr,
        identifier: &str,
    ) -> Result<Arc<RemoteServer>, Box<dyn std::error::Error + Send + Sync>> {
        let bind_ip = *self.default_bind_ip.read().await;
        let bind_socket_addr = SocketAddr::new(bind_ip, 0);

        // Allocate OS ephemeral port
        let socket = UdpSocket::bind(bind_socket_addr).await?;
        let local_addr = socket.local_addr()?;
        let ephemeral_port = local_addr.port();
        let (shutdown_tx, _) = watch::channel(false);

        // Parse Bedrock identifier
        let parsed = BedrockIdentifier::parse(identifier).unwrap_or_else(|_| BedrockIdentifier {
            edition: "MCPE".into(),
            server_name: format!("Friend World ({})", target_client_id),
            protocol_version: 776,
            version_name: "1.21.60".into(),
            player_count: 1,
            max_player_count: 8,
            server_guid: target_raknet_id,
            sub_name: "".into(),
            game_mode: "Survival".into(),
            game_mode_numeric: 1,
            port_ipv4: ephemeral_port,
            port_ipv6: ephemeral_port,
            extra: Vec::new(),
        });

        let remote_server = Arc::new(RemoteServer {
            target_client_id,
            target_raknet_id,
            relay_addr,
            raw_identifier: identifier.to_string(),
            parsed_identifier: parsed,
            ephemeral_port,
            local_socket: Arc::new(socket),
            last_client_addr: Arc::new(RwLock::new(None)),
            shutdown_tx,
        });

        let replaced = {
            let mut servers = self.servers.write().await;
            let mut port_map = self.port_map.write().await;

            let replaced = servers.insert(target_client_id, remote_server.clone());
            if let Some(old) = &replaced {
                port_map.remove(&old.ephemeral_port);
            }
            port_map.insert(ephemeral_port, target_client_id);
            replaced
        };

        if let Some(old) = replaced {
            old.shutdown();
            info!(
                "Replaced existing server: client={}, old_port={}",
                target_client_id, old.ephemeral_port
            );
        }

        info!(
            "add socket: client={}, port={}, world='{}'",
            target_client_id, ephemeral_port, remote_server.parsed_identifier.server_name
        );

        Ok(remote_server)
    }

    /// Removes a server and closes its local socket.
    /// Equivalent to C++ 0x180005140 / mineshaft.removeServer(...)
    pub async fn remove_server(&self, target_client_id: u32) -> Option<Arc<RemoteServer>> {
        let mut servers = self.servers.write().await;
        let mut port_map = self.port_map.write().await;

        let removed = if let Some(server) = servers.remove(&target_client_id) {
            port_map.remove(&server.ephemeral_port);
            info!(
                "remove socket: client={}, port={}",
                target_client_id, server.ephemeral_port
            );
            Some(server)
        } else {
            warn!(
                "remove_server called for non-existent client={}",
                target_client_id
            );
            None
        };

        drop(port_map);
        drop(servers);
        if let Some(server) = &removed {
            server.shutdown();
        }
        removed
    }

    pub async fn get_server(&self, target_client_id: u32) -> Option<Arc<RemoteServer>> {
        self.servers.read().await.get(&target_client_id).cloned()
    }

    pub async fn get_by_port(&self, port: u16) -> Option<Arc<RemoteServer>> {
        let port_map = self.port_map.read().await;
        let client_id = port_map.get(&port).copied()?;
        self.servers.read().await.get(&client_id).cloned()
    }

    pub async fn all_servers(&self) -> Vec<Arc<RemoteServer>> {
        self.servers.read().await.values().cloned().collect()
    }

    pub async fn count(&self) -> usize {
        self.servers.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_manager_add_and_remove() {
        let manager = ServerManager::new();
        let relay: SocketAddr = "127.0.0.1:19999".parse().unwrap();
        let ident = "MCPE;Ahmet's World;776;1.21.60;1;5;987654321;Sub;Survival;1;19132;19133;";

        let server1 = manager
            .add_server(1001, 987654321, relay, ident)
            .await
            .unwrap();
        assert_eq!(server1.target_client_id, 1001);
        assert_ne!(server1.ephemeral_port, 0);

        let by_port = manager.get_by_port(server1.ephemeral_port).await;
        assert!(by_port.is_some());
        assert_eq!(by_port.unwrap().target_client_id, 1001);

        // Verify synthetic pong generation
        let pong = server1.build_synthetic_pong(555).unwrap();
        let (_, _, pong_str) = crate::protocol::parse_unconnected_pong(&pong).unwrap();
        assert!(pong_str.contains(&format!(
            ";{};{};",
            server1.ephemeral_port, server1.ephemeral_port
        )));

        // Remove server
        let removed = manager.remove_server(1001).await;
        assert!(removed.is_some());
        assert_eq!(manager.count().await, 0);
        assert!(manager.get_by_port(server1.ephemeral_port).await.is_none());
    }
}
