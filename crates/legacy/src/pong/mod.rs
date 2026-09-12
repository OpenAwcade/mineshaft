use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::manager::ServerManager;
use crate::protocol::{is_unconnected_ping, parse_unconnected_ping};

#[derive(Clone, Debug)]
pub struct PongEngineConfig {
    /// Addresses to send periodic synthetic pongs to (e.g. local Minecraft client or LAN broadcast)
    pub broadcast_targets: Vec<SocketAddr>,
    /// Address to listen on for incoming LAN pings (default 0.0.0.0:19132 if available)
    pub listen_addr: Option<SocketAddr>,
    /// Outgoing socket bind address
    pub bind_addr: SocketAddr,
    /// Interval between periodic pong bursts
    pub burst_interval: Duration,
}

impl Default for PongEngineConfig {
    fn default() -> Self {
        Self {
            broadcast_targets: vec!["127.0.0.1:19132".parse().unwrap()],
            listen_addr: Some("0.0.0.0:19132".parse().unwrap()),
            bind_addr: "0.0.0.0:0".parse().unwrap(),
            burst_interval: Duration::from_millis(1000),
        }
    }
}

pub struct PongEngine {
    config: PongEngineConfig,
    manager: Arc<ServerManager>,
    sender_socket: Arc<UdpSocket>,
}

impl PongEngine {
    pub async fn new(
        config: PongEngineConfig,
        manager: Arc<ServerManager>,
    ) -> Result<Self, std::io::Error> {
        if config.burst_interval.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "pong burst interval must be greater than zero",
            ));
        }

        let sender = UdpSocket::bind(config.bind_addr).await?;
        let _ = sender.set_broadcast(true);

        Ok(Self {
            config,
            manager,
            sender_socket: Arc::new(sender),
        })
    }

    /// Sends a synthetic pong for each registered server to the specified target address.
    /// Matches C++ 0x180004d20 iteration loop.
    pub async fn send_pong_burst_to(&self, target: SocketAddr) -> usize {
        let servers = self.manager.all_servers().await;
        if servers.is_empty() {
            return 0;
        }

        let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
            return 0;
        };
        let now = now.as_millis() as u64;

        let mut sent_count = 0;
        for server in servers {
            match server.build_synthetic_pong(now) {
                Ok(pong_bytes) => {
                    if let Err(e) = self.sender_socket.send_to(&pong_bytes, target).await {
                        debug!("Failed to send synthetic pong to {}: {}", target, e);
                    } else {
                        sent_count += 1;
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to build synthetic pong for client {}: {}",
                        server.target_client_id, e
                    );
                }
            }
        }
        sent_count
    }

    /// Sends synthetic pongs to all configured broadcast/unicast targets.
    pub async fn broadcast_burst(&self) -> usize {
        let mut total = 0;
        for target in &self.config.broadcast_targets {
            total += self.send_pong_burst_to(*target).await;
        }
        total
    }

    /// Runs the pong engine loop (both passive listening for pings and periodic bursts).
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        info!(
            "PongEngine starting with {} broadcast targets",
            self.config.broadcast_targets.len()
        );

        // Try to bind listener socket for incoming Minecraft UNCONNECTED_PING (0x01)
        let listener_socket = if let Some(addr) = self.config.listen_addr {
            match UdpSocket::bind(addr).await {
                Ok(s) => {
                    info!(
                        "PongEngine listening for incoming Minecraft pings on {}",
                        addr
                    );
                    Some(Arc::new(s))
                }
                Err(e) => {
                    debug!(
                        "Could not bind pong listener on {} ({}); relying on periodic broadcast",
                        addr, e
                    );
                    None
                }
            }
        } else {
            None
        };

        let this = self.clone();
        let mut listener_shutdown = shutdown.resubscribe();

        // Spawn ping responder task if listener socket bound successfully
        if let Some(listener) = listener_socket {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                loop {
                    tokio::select! {
                        _ = listener_shutdown.recv() => break,
                        res = listener.recv_from(&mut buf) => {
                            match res {
                                Ok((len, client_addr)) => {
                                    if len > 0
                                        && is_unconnected_ping(&buf[..len])
                                        && parse_unconnected_ping(&buf[..len]).is_ok()
                                    {
                                        debug!("Received UNCONNECTED_PING from Minecraft client at {}", client_addr);
                                        this.send_pong_burst_to(client_addr).await;
                                    }
                                }
                                Err(e) => {
                                    debug!("Listener recv error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        }

        // Periodic pong burst loop (0x180004d20)
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("PongEngine shutting down");
                    break;
                }
                _ = sleep(self.config.burst_interval) => {
                    let sent = self.broadcast_burst().await;
                    if sent > 0 {
                        debug!("Broadcasted {} synthetic pongs to LAN", sent);
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
    async fn test_pong_engine_burst() {
        let manager = Arc::new(ServerManager::new());
        let relay: SocketAddr = "127.0.0.1:19999".parse().unwrap();

        // Add 2 mock servers
        let s1 = manager
            .add_server(
                1001,
                11111,
                relay,
                "MCPE;Ahmet's World;776;1.21.60;1;5;11111;Sub;Survival;1;19132;19133;",
            )
            .await
            .unwrap();

        let s2 = manager
            .add_server(
                1002,
                22222,
                relay,
                "MCPE;Mehmet's World;776;1.21.60;3;8;22222;Sub;Survival;1;19132;19133;",
            )
            .await
            .unwrap();

        // Create receiver mock socket pretending to be Minecraft
        let mc_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mc_addr = mc_client.local_addr().unwrap();

        let config = PongEngineConfig {
            broadcast_targets: vec![mc_addr],
            listen_addr: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            burst_interval: Duration::from_millis(500),
        };

        let engine = PongEngine::new(config, manager.clone()).await.unwrap();
        let sent = engine.send_pong_burst_to(mc_addr).await;
        assert_eq!(sent, 2);

        // Receive two pongs
        let mut received_ports = Vec::new();
        let mut buf = [0u8; 1024];

        for _ in 0..2 {
            let (len, _) = mc_client.recv_from(&mut buf).await.unwrap();
            let (_, _, ident_str) = crate::protocol::parse_unconnected_pong(&buf[..len]).unwrap();
            let ident = crate::protocol::BedrockIdentifier::parse(&ident_str).unwrap();
            received_ports.push(ident.port_ipv4);
        }

        assert!(received_ports.contains(&s1.ephemeral_port));
        assert!(received_ports.contains(&s2.ephemeral_port));
    }
}
