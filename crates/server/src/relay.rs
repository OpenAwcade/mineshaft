use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{RwLock, broadcast};
use tracing::{debug, error, info};

use crate::protocol::MineshaftHeader;
use crate::session::SessionRegistry;

#[derive(Clone, Debug)]
pub struct RelayConfig {
    pub bind_addr: SocketAddr,
    pub session_timeout: Duration,
    pub cleanup_interval: Duration,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:19999".parse().unwrap(),
            session_timeout: Duration::from_secs(35),
            cleanup_interval: Duration::from_secs(10),
        }
    }
}

pub struct RelayServer {
    config: RelayConfig,
    socket: Arc<UdpSocket>,
    pub registry: Arc<SessionRegistry>,
    /// Blocked pairs: (Host Session ID, Blocked Client Session ID)
    blocked_peers: Arc<RwLock<HashSet<(u32, u32)>>>,
}

impl RelayServer {
    pub async fn bind(config: RelayConfig) -> Result<Self, std::io::Error> {
        if config.session_timeout.is_zero() || config.cleanup_interval.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "relay intervals must be greater than zero",
            ));
        }

        let socket = UdpSocket::bind(config.bind_addr).await?;
        let local_addr = socket.local_addr()?;
        info!("Mineshaft RelayServer bound on {}", local_addr);

        Ok(Self {
            config,
            socket: Arc::new(socket),
            registry: Arc::new(SessionRegistry::new()),
            blocked_peers: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.socket.local_addr()
    }

    pub async fn block_peer(&self, host_id: u32, peer_id: u32) {
        let mut blocked = self.blocked_peers.write().await;
        blocked.insert((host_id, peer_id));
        info!("Host {} blocked peer {}", host_id, peer_id);
    }

    pub async fn unblock_peer(&self, host_id: u32, peer_id: u32) {
        let mut blocked = self.blocked_peers.write().await;
        blocked.remove(&(host_id, peer_id));
        info!("Host {} unblocked peer {}", host_id, peer_id);
    }

    pub async fn is_blocked(&self, host_id: u32, peer_id: u32) -> bool {
        let blocked = self.blocked_peers.read().await;
        blocked.contains(&(host_id, peer_id))
    }

    pub async fn run(self: Arc<Self>, mut shutdown: broadcast::Receiver<()>) {
        info!(
            "RelayServer processing loop active on {}",
            self.config.bind_addr
        );

        let mut buf = [0u8; 4096];
        let mut cleanup_timer = tokio::time::interval(self.config.cleanup_interval);

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("RelayServer shutting down");
                    break;
                }
                _ = cleanup_timer.tick() => {
                    let pruned = self.registry.prune_stale(self.config.session_timeout).await;
                    if !pruned.is_empty() {
                        debug!("Pruned {} idle sessions", pruned.len());
                    }
                }
                recv_res = self.socket.recv_from(&mut buf) => {
                    match recv_res {
                        Ok((len, sender_addr)) => {
                            if len < crate::protocol::MINESHAFT_HEADER_LEN {
                                continue;
                            }

                            let (header, payload) = match MineshaftHeader::decode(&buf[..len]) {
                                Ok(res) => res,
                                Err(e) => {
                                    debug!("Malformed packet from {}: {}", sender_addr, e);
                                    continue;
                                }
                            };

                            let sender_id = header.sender_session_id;
                            let target_id = header.target_session_id;
                            let is_handshake = header.is_handshake() && payload.is_empty();

                            // Only handshakes may register or migrate an endpoint. This
                            // prevents an unsolicited data packet from hijacking a session.
                            let registered_addr = self.registry.get_addr(sender_id).await;
                            if !is_handshake && registered_addr != Some(sender_addr) {
                                debug!(
                                    "Dropping packet for session {} from unregistered endpoint {}",
                                    sender_id, sender_addr
                                );
                                continue;
                            }
                            if is_handshake {
                                self.registry.register_or_refresh(sender_id, sender_addr).await;
                            }

                            // Case 1: Keep-alive / Handshake ping [ S, S ]
                            if is_handshake {
                                debug!("Handshake/Keepalive from session {} ({})", sender_id, sender_addr);
                                let pong = MineshaftHeader::handshake(sender_id).to_bytes();
                                let _ = self.socket.send_to(&pong, sender_addr).await;
                                continue;
                            }

                            // Case 2: Routing to target peer
                            // Check moderation / blocking
                            if self.is_blocked(target_id, sender_id).await {
                                debug!("Packet from {} to {} dropped (blocked by host)", sender_id, target_id);
                                continue;
                            }

                            if let Some(target_addr) = self.registry.get_addr(target_id).await {
                                if let Err(e) = self.socket.send_to(&buf[..len], target_addr).await {
                                    error!("Failed forwarding datagram to session {} ({}): {}", target_id, target_addr, e);
                                }
                            } else {
                                debug!("Target session {} not registered or offline; dropping datagram", target_id);
                            }
                        }
                        Err(e) => {
                            error!("Relay socket recv_from error: {}", e);
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
    async fn test_relay_routing_between_peers() {
        let config = RelayConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            session_timeout: Duration::from_secs(30),
            cleanup_interval: Duration::from_secs(10),
        };

        let relay = Arc::new(RelayServer::bind(config).await.unwrap());
        let relay_addr = relay.local_addr().unwrap();

        let (shutdown_tx, _) = broadcast::channel(1);
        let relay_task = relay.clone();
        tokio::spawn(async move {
            relay_task.run(shutdown_tx.subscribe()).await;
        });

        // Peer A (Host: 100)
        let peer_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        // Peer B (Client: 200)
        let peer_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // Register both peers with handshakes
        let hs_a = MineshaftHeader::handshake(100).to_bytes();
        peer_a.send_to(&hs_a, relay_addr).await.unwrap();

        let hs_b = MineshaftHeader::handshake(200).to_bytes();
        peer_b.send_to(&hs_b, relay_addr).await.unwrap();

        // Drain keepalive pongs from relay
        let mut drain_buf = [0u8; 64];
        let (len, _) =
            tokio::time::timeout(Duration::from_millis(500), peer_a.recv_from(&mut drain_buf))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(len, 8);
        let (len, _) =
            tokio::time::timeout(Duration::from_millis(500), peer_b.recv_from(&mut drain_buf))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(len, 8);

        // Peer B sends packet to Peer A via Relay
        let pkt_to_a = MineshaftHeader::new(100, 200).encapsulate(b"Hello Host");
        peer_b.send_to(&pkt_to_a, relay_addr).await.unwrap();

        let mut a_buf = [0u8; 1024];
        let (len, _) =
            tokio::time::timeout(Duration::from_millis(500), peer_a.recv_from(&mut a_buf))
                .await
                .unwrap()
                .unwrap();
        let (header, payload) = MineshaftHeader::decode(&a_buf[..len]).unwrap();
        assert_eq!(header.target_session_id, 100);
        assert_eq!(header.sender_session_id, 200);
        assert_eq!(payload, b"Hello Host");

        // Peer A replies back to Peer B
        let reply_to_b = MineshaftHeader::new(200, 100).encapsulate(b"Welcome Client");
        peer_a.send_to(&reply_to_b, relay_addr).await.unwrap();

        let mut b_buf = [0u8; 1024];
        let (len, _) =
            tokio::time::timeout(Duration::from_millis(500), peer_b.recv_from(&mut b_buf))
                .await
                .unwrap()
                .unwrap();
        let (header, payload) = MineshaftHeader::decode(&b_buf[..len]).unwrap();
        assert_eq!(header.target_session_id, 200);
        assert_eq!(header.sender_session_id, 100);
        assert_eq!(payload, b"Welcome Client");
    }
}
