pub mod access;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{RwLock, broadcast, watch};
use tracing::{debug, error, info};

use crate::manager::{RemoteServer, ServerManager};
use crate::protocol::MineshaftHeader;
pub use access::AccessControl;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TunnelEvent {
    ClientConnecting(u32),
    ClientDisconnected(u32),
    PlayersActive(Vec<u32>),
}

#[derive(Clone, Debug)]
pub struct TunnelConfig {
    pub my_client_id: u32,
    pub relay_addr: SocketAddr,
    pub local_minecraft_addr: SocketAddr,
    pub bind_addr: SocketAddr,
    pub keepalive_interval: Duration,
    pub session_timeout: Duration,
}

impl Default for TunnelConfig {
    fn default() -> Self {
        Self {
            my_client_id: 0,
            relay_addr: "127.0.0.1:19999".parse().unwrap(),
            local_minecraft_addr: "127.0.0.1:19132".parse().unwrap(),
            bind_addr: "0.0.0.0:0".parse().unwrap(),
            keepalive_interval: Duration::from_secs(25),
            session_timeout: Duration::from_secs(30),
        }
    }
}

/// Represents an active incoming peer connection to our hosted Minecraft server
struct HostPeerSession {
    #[allow(dead_code)]
    peer_client_id: u32,
    /// Dedicated socket talking to local Minecraft server on 19132 so Minecraft differentiates players
    proxy_socket: Arc<UdpSocket>,
    last_activity: Instant,
    shutdown_tx: watch::Sender<bool>,
}

pub struct TunnelEngine {
    config: TunnelConfig,
    relay_socket: Arc<UdpSocket>,
    server_manager: Arc<ServerManager>,
    access_control: Arc<RwLock<AccessControl>>,
    host_sessions: RwLock<HashMap<u32, HostPeerSession>>,
    event_tx: broadcast::Sender<TunnelEvent>,
}

impl TunnelEngine {
    pub async fn bind(
        config: TunnelConfig,
        server_manager: Arc<ServerManager>,
        access_control: Arc<RwLock<AccessControl>>,
    ) -> Result<Self, std::io::Error> {
        if config.keepalive_interval.is_zero() || config.session_timeout.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "tunnel intervals must be greater than zero",
            ));
        }

        let relay_socket = UdpSocket::bind(config.bind_addr).await?;
        let (event_tx, _) = broadcast::channel(64);

        Ok(Self {
            config,
            relay_socket: Arc::new(relay_socket),
            server_manager,
            access_control,
            host_sessions: RwLock::new(HashMap::new()),
            event_tx,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TunnelEvent> {
        self.event_tx.subscribe()
    }

    /// Sends the 8-byte handshake [ ClientId, ClientId ] to the relay server.
    pub async fn send_handshake(&self) -> Result<(), std::io::Error> {
        let hs = MineshaftHeader::handshake(self.config.my_client_id);
        let bytes = hs.to_bytes();
        self.relay_socket
            .send_to(&bytes, self.config.relay_addr)
            .await?;
        debug!(
            "Sent relay handshake for client {}",
            self.config.my_client_id
        );
        Ok(())
    }

    /// Starts a forwarding loop for a remote friend's ephemeral socket.
    /// When local Minecraft sends datagrams to the ephemeral port, we encapsulate
    /// them with [ target_client_id, my_client_id ] and send via relay.
    pub fn spawn_client_ephemeral_forwarder(
        self: &Arc<Self>,
        remote: Arc<RemoteServer>,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        let engine = self.clone();
        let mut remote_shutdown = remote.shutdown_receiver();
        tokio::spawn(async move {
            if *remote_shutdown.borrow() {
                return;
            }

            let mut buf = [0u8; 4096];
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        debug!("Ephemeral forwarder for client {} shutting down", remote.target_client_id);
                        break;
                    }
                    _ = remote_shutdown.changed() => {
                        debug!("Ephemeral forwarder for client {} removed", remote.target_client_id);
                        break;
                    }
                    res = remote.local_socket.recv_from(&mut buf) => {
                        match res {
                            Ok((len, mc_addr)) => {
                                // Remember Minecraft client's source address for return traffic
                                {
                                    let mut last = remote.last_client_addr.write().await;
                                    *last = Some(mc_addr);
                                }

                                // Encapsulate: [ target_client_id, my_client_id ] + payload
                                let header = MineshaftHeader::new(remote.target_client_id, engine.config.my_client_id);
                                let packet = header.encapsulate(&buf[..len]);

                                if let Err(e) = engine.relay_socket.send_to(&packet, remote.relay_addr).await {
                                    error!("Failed to forward packet to relay for client {}: {}", remote.target_client_id, e);
                                }
                            }
                            Err(e) => {
                                error!("Error receiving from ephemeral socket {}: {}", remote.ephemeral_port, e);
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    /// Spawns a dedicated return forwarder for an incoming host session.
    /// When local Minecraft (19132) replies to this proxy socket, we encapsulate
    /// the reply with [ peer_client_id, my_client_id ] and send to relay.
    fn spawn_host_return_forwarder(
        self: &Arc<Self>,
        #[allow(dead_code)] peer_client_id: u32,
        proxy_socket: Arc<UdpSocket>,
        mut session_shutdown: watch::Receiver<bool>,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        let engine = self.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                tokio::select! {
                    _ = shutdown.recv() => break,
                    _ = session_shutdown.changed() => break,
                    res = proxy_socket.recv_from(&mut buf) => {
                        match res {
                            Ok((len, _mc_server_addr)) => {
                                let header = MineshaftHeader::new(peer_client_id, engine.config.my_client_id);
                                let packet = header.encapsulate(&buf[..len]);

                                if let Err(e) = engine.relay_socket.send_to(&packet, engine.config.relay_addr).await {
                                    error!("Failed to send host reply to relay for peer {}: {}", peer_client_id, e);
                                }
                            }
                            Err(e) => {
                                debug!("Host proxy socket recv error for peer {}: {}", peer_client_id, e);
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    /// Handles an incoming datagram from the relay server.
    async fn handle_relay_datagram(
        self: &Arc<Self>,
        data: &[u8],
        shutdown_rx: &broadcast::Sender<()>,
    ) {
        let (header, payload) = match MineshaftHeader::decode(data) {
            Ok(res) => res,
            Err(e) => {
                debug!("Received malformed relay packet: {}", e);
                return;
            }
        };

        if header.target_session_id != self.config.my_client_id {
            debug!(
                "Ignoring relay packet addressed to session {}",
                header.target_session_id
            );
            return;
        }

        // If payload is empty, this is a handshake/keepalive pong
        if payload.is_empty() {
            debug!(
                "Received keepalive pong from relay for client {}",
                header.sender_session_id
            );
            return;
        }

        let sender_id = header.sender_session_id;

        // Check if this is a reply to one of our joined client worlds
        if let Some(remote_server) = self.server_manager.get_server(sender_id).await {
            let last_mc_addr = *remote_server.last_client_addr.read().await;
            if let Some(mc_addr) = last_mc_addr {
                if let Err(e) = remote_server.local_socket.send_to(payload, mc_addr).await {
                    error!("Failed to forward relay packet to Minecraft client: {}", e);
                }
            }
            return;
        }

        // Otherwise, we are the HOST and a remote peer is joining/playing in our world!
        // 1. Check access control
        {
            let ac = self.access_control.read().await;
            if !ac.is_allowed(sender_id) {
                debug!(
                    "Dropping packet from blocked/unauthorized player {}",
                    sender_id
                );
                return;
            }
        }

        // 2. Get or create host session for this peer
        let proxy_socket = {
            let mut sessions = self.host_sessions.write().await;
            if let Some(session) = sessions.get_mut(&sender_id) {
                session.last_activity = Instant::now();
                session.proxy_socket.clone()
            } else {
                // New peer connecting!
                info!("New remote player connecting: client={}", sender_id);
                let _ = self.event_tx.send(TunnelEvent::ClientConnecting(sender_id));

                match UdpSocket::bind("127.0.0.1:0").await {
                    Ok(sock) => {
                        let sock = Arc::new(sock);
                        let (session_shutdown_tx, session_shutdown_rx) = watch::channel(false);
                        let session = HostPeerSession {
                            peer_client_id: sender_id,
                            proxy_socket: sock.clone(),
                            last_activity: Instant::now(),
                            shutdown_tx: session_shutdown_tx,
                        };
                        sessions.insert(sender_id, session);
                        self.spawn_host_return_forwarder(
                            sender_id,
                            sock.clone(),
                            session_shutdown_rx,
                            shutdown_rx.subscribe(),
                        );
                        sock
                    }
                    Err(e) => {
                        error!(
                            "Failed to allocate host proxy socket for peer {}: {}",
                            sender_id, e
                        );
                        return;
                    }
                }
            }
        };

        // 3. Forward raw RakNet payload to local Minecraft server
        if let Err(e) = proxy_socket
            .send_to(payload, self.config.local_minecraft_addr)
            .await
        {
            error!(
                "Failed to forward peer packet to local Minecraft server: {}",
                e
            );
        }
    }

    /// Main runner for the Tunnel Engine:
    /// - Relay keep-alive loop (every 25s)
    /// - Relay datagram receiver & dispatcher
    /// - Session timeout cleanup
    pub async fn run(self: Arc<Self>, mut shutdown: broadcast::Receiver<()>) {
        info!(
            "TunnelEngine started for client_id={}",
            self.config.my_client_id
        );

        let _ = self.send_handshake().await;

        let mut keepalive_timer = tokio::time::interval(self.config.keepalive_interval);
        let mut cleanup_timer = tokio::time::interval(Duration::from_secs(10));
        let mut buf = [0u8; 4096];
        let (internal_shutdown_tx, _) = broadcast::channel(16);

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("TunnelEngine shutting down");
                    let _ = internal_shutdown_tx.send(());
                    break;
                }
                _ = keepalive_timer.tick() => {
                    let _ = self.send_handshake().await;
                }
                _ = cleanup_timer.tick() => {
                    let mut sessions = self.host_sessions.write().await;
                    let timeout = self.config.session_timeout;
                    let now = Instant::now();

                    sessions.retain(|client_id, session| {
                        if now.duration_since(session.last_activity) > timeout {
                            info!("Player session timed out: client={}", client_id);
                            let _ = self.event_tx.send(TunnelEvent::ClientDisconnected(*client_id));
                            let _ = session.shutdown_tx.send(true);
                            false
                        } else {
                            true
                        }
                    });

                    let active: Vec<u32> = sessions.keys().copied().collect();
                    let _ = self.event_tx.send(TunnelEvent::PlayersActive(active));
                }
                recv_res = self.relay_socket.recv_from(&mut buf) => {
                    match recv_res {
                        Ok((len, _from_relay)) => {
                            self.handle_relay_datagram(&buf[..len], &internal_shutdown_tx).await;
                        }
                        Err(e) => {
                            error!("Relay socket recv error: {}", e);
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tunnel_client_and_host_routing() {
        let server_manager = Arc::new(ServerManager::new());
        let access_control = Arc::new(RwLock::new(AccessControl::new()));

        // Mock Relay server socket
        let mock_relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let relay_addr = mock_relay.local_addr().unwrap();

        let config = TunnelConfig {
            my_client_id: 100,
            relay_addr,
            local_minecraft_addr: "127.0.0.1:19132".parse().unwrap(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            keepalive_interval: Duration::from_secs(60),
            session_timeout: Duration::from_secs(60),
        };

        let tunnel = Arc::new(
            TunnelEngine::bind(config, server_manager.clone(), access_control.clone())
                .await
                .unwrap(),
        );

        // Test handshake
        tunnel.send_handshake().await.unwrap();
        let mut buf = [0u8; 64];
        let (len, from) = mock_relay.recv_from(&mut buf).await.unwrap();
        assert_eq!(len, 8);
        let (hs, _) = MineshaftHeader::decode(&buf[..len]).unwrap();
        assert_eq!(hs.target_session_id, 100);
        assert_eq!(hs.sender_session_id, 100);

        // Add remote server (Host Client ID 200)
        let remote = server_manager
            .add_server(
                200,
                99999,
                relay_addr,
                "MCPE;Test;776;1.21.60;1;8;99999;Sub;Survival;1;19132;19133;",
            )
            .await
            .unwrap();

        let (shutdown_tx, _) = broadcast::channel(1);
        tunnel.spawn_client_ephemeral_forwarder(remote.clone(), shutdown_tx.subscribe());

        // Simulate local Minecraft client sending packet to ephemeral socket
        let mc_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ephemeral_target = SocketAddr::new("127.0.0.1".parse().unwrap(), remote.ephemeral_port);
        mc_client
            .send_to(b"Minecraft Packet Data", ephemeral_target)
            .await
            .unwrap();

        // Check that relay receives encapsulated packet: [ 200, 100 ] + payload
        let (len, _) = mock_relay.recv_from(&mut buf).await.unwrap();
        let (header, payload) = MineshaftHeader::decode(&buf[..len]).unwrap();
        assert_eq!(header.target_session_id, 200);
        assert_eq!(header.sender_session_id, 100);
        assert_eq!(payload, b"Minecraft Packet Data");

        // Now simulate relay sending reply back: [ 100, 200 ] + "Server Reply"
        let reply_header = MineshaftHeader::new(100, 200);
        let reply_pkt = reply_header.encapsulate(b"Server Reply");
        mock_relay.send_to(&reply_pkt, from).await.unwrap();

        // Let tunnel process datagram
        let (internal_shutdown, _) = broadcast::channel(1);
        tunnel
            .handle_relay_datagram(&reply_pkt, &internal_shutdown)
            .await;

        // Minecraft client should receive raw "Server Reply" on its socket!
        let mut mc_buf = [0u8; 64];
        let (len, _) = mc_client.recv_from(&mut mc_buf).await.unwrap();
        assert_eq!(&mc_buf[..len], b"Server Reply");
    }
}
