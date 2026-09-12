use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::info;

use crate::detector::{LocalServerEvent, ServerDetector, ServerDetectorConfig};
use crate::manager::{RemoteServer, ServerManager};
use crate::platform::PlatformNetwork;
use crate::pong::{PongEngine, PongEngineConfig};
use crate::tunnel::{AccessControl, TunnelConfig, TunnelEngine, TunnelEvent};

pub struct MineshaftBridge {
    pub client_id: u32,
    pub relay_addr: SocketAddr,
    pub network: RwLock<PlatformNetwork>,
    pub server_manager: Arc<ServerManager>,
    pub access_control: Arc<RwLock<AccessControl>>,
    pub detector: Arc<ServerDetector>,
    pub pong_engine: Arc<PongEngine>,
    pub tunnel: Arc<TunnelEngine>,
    shutdown_tx: broadcast::Sender<()>,
}

impl MineshaftBridge {
    pub async fn new(
        client_id: u32,
        relay_addr: SocketAddr,
        local_minecraft_addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let network = PlatformNetwork::new();
        network.ensure_windows_loopback_exempt().await?;

        let server_manager = Arc::new(ServerManager::new());
        let access_control = Arc::new(RwLock::new(AccessControl::new()));

        let detector_config = ServerDetectorConfig {
            target_addr: local_minecraft_addr,
            ..Default::default()
        };
        let detector = Arc::new(ServerDetector::bind(detector_config).await?);

        let pong_config = PongEngineConfig {
            broadcast_targets: vec![local_minecraft_addr],
            ..Default::default()
        };
        let pong_engine = Arc::new(PongEngine::new(pong_config, server_manager.clone()).await?);

        let tunnel_config = TunnelConfig {
            my_client_id: client_id,
            relay_addr,
            local_minecraft_addr,
            ..Default::default()
        };
        let tunnel = Arc::new(TunnelEngine::bind(tunnel_config, server_manager.clone(), access_control.clone()).await?);
        let (shutdown_tx, _) = broadcast::channel(16);

        Ok(Self {
            client_id,
            relay_addr,
            network: RwLock::new(network),
            server_manager,
            access_control,
            detector,
            pong_engine,
            tunnel,
            shutdown_tx,
        })
    }

    /// Adds a friend's world to the virtual LAN list.
    /// Equivalent to mineshaft.addServer(...)
    pub async fn add_server(
        &self,
        target_client_id: u32,
        target_raknet_id: u64,
        relay_addr: SocketAddr,
        identifier: &str,
    ) -> Result<Arc<RemoteServer>, Box<dyn std::error::Error + Send + Sync>> {
        let remote = self.server_manager.add_server(target_client_id, target_raknet_id, relay_addr, identifier).await?;
        self.tunnel.spawn_client_ephemeral_forwarder(remote.clone(), self.shutdown_tx.subscribe());
        Ok(remote)
    }

    /// Removes a friend's world from virtual LAN.
    /// Equivalent to mineshaft.removeServer(...)
    pub async fn remove_server(&self, target_client_id: u32) -> Option<Arc<RemoteServer>> {
        self.server_manager.remove_server(target_client_id).await
    }

    /// Blocks a player from joining the local world.
    pub async fn block_player(&self, client_id: u32) {
        let mut ac = self.access_control.write().await;
        ac.block_player(client_id);
    }

    /// Unblocks a player.
    pub async fn unblock_player(&self, client_id: u32) {
        let mut ac = self.access_control.write().await;
        ac.unblock_player(client_id);
    }

    /// Sets reflector IP (WinDivert / virtual IP support on Windows).
    pub async fn set_reflector_address(&self, ip: IpAddr) {
        let mut net = self.network.write().await;
        net.set_reflector(ip);
        self.server_manager.set_bind_ip(ip).await;
    }

    pub fn subscribe_local_server(&self) -> broadcast::Receiver<LocalServerEvent> {
        self.detector.subscribe()
    }

    pub fn subscribe_tunnel_events(&self) -> broadcast::Receiver<TunnelEvent> {
        self.tunnel.subscribe()
    }

    /// Spawns background tasks for detector, pong engine, and tunnel.
    pub fn spawn_background_tasks(self: &Arc<Self>) {
        let det = self.detector.clone();
        let det_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            det.run(det_rx).await;
        });

        let pong = self.pong_engine.clone();
        let pong_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            pong.run(pong_rx).await;
        });

        let tun = self.tunnel.clone();
        let tun_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            tun.run(tun_rx).await;
        });

        info!("MineshaftBridge all background tasks running");
    }

    /// Stops all background tasks.
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(());
    }
}
