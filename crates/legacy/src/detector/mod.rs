use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, watch};
use tokio::time::sleep;
use tracing::{debug, info};

use crate::protocol::{BedrockIdentifier, build_unconnected_ping, parse_unconnected_pong};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalServerEvent {
    ServerStarted(BedrockIdentifier),
    ServerUpdated(BedrockIdentifier),
    ServerStopped,
}

#[derive(Clone, Debug)]
pub struct ServerDetectorConfig {
    pub target_addr: SocketAddr,
    pub bind_addr: SocketAddr,
    pub probe_interval: Duration,
    pub timeout: Duration,
}

impl Default for ServerDetectorConfig {
    fn default() -> Self {
        Self {
            target_addr: "127.0.0.1:19132".parse().unwrap(),
            bind_addr: "0.0.0.0:0".parse().unwrap(),
            probe_interval: Duration::from_millis(1500),
            timeout: Duration::from_secs(5),
        }
    }
}

pub struct ServerDetector {
    config: ServerDetectorConfig,
    socket: Arc<UdpSocket>,
    event_tx: broadcast::Sender<LocalServerEvent>,
    current_server: Arc<watch::Sender<Option<BedrockIdentifier>>>,
}

impl ServerDetector {
    pub async fn bind(config: ServerDetectorConfig) -> Result<Self, std::io::Error> {
        if config.probe_interval.is_zero() || config.timeout.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "detector intervals must be greater than zero",
            ));
        }

        let socket = UdpSocket::bind(config.bind_addr).await?;
        let _ = socket.set_broadcast(true);
        let (event_tx, _) = broadcast::channel(32);
        let (current_server_tx, _) = watch::channel(None);

        Ok(Self {
            config,
            socket: Arc::new(socket),
            event_tx,
            current_server: Arc::new(current_server_tx),
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<LocalServerEvent> {
        self.event_tx.subscribe()
    }

    pub fn current_server(&self) -> Option<BedrockIdentifier> {
        self.current_server.borrow().clone()
    }

    pub async fn probe_once(
        &self,
    ) -> Result<Option<BedrockIdentifier>, Box<dyn std::error::Error + Send + Sync>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?
            .as_millis() as u64;

        let ping = build_unconnected_ping(now, 0x1234567890abcdef)?;
        self.socket.send_to(&ping, self.config.target_addr).await?;

        let mut buf = [0u8; 2048];
        let res =
            tokio::time::timeout(Duration::from_millis(500), self.socket.recv_from(&mut buf)).await;

        match res {
            Ok(Ok((len, from_addr))) if from_addr == self.config.target_addr => {
                if let Ok((timestamp, _guid, msg)) = parse_unconnected_pong(&buf[..len]) {
                    if timestamp != now {
                        return Ok(None);
                    }
                    if let Ok(ident) = BedrockIdentifier::parse(&msg) {
                        return Ok(Some(ident));
                    }
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        info!("ServerDetector started probing {}", self.config.target_addr);

        let mut last_seen: Option<Instant> = None;
        let mut last_ident: Option<BedrockIdentifier> = None;
        let mut last_probe_timestamp: Option<u64> = None;
        let mut recv_buf = [0u8; 2048];

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("ServerDetector shutting down");
                    break;
                }
                _ = sleep(self.config.probe_interval) => {
                    // Send unconnected ping probe
                    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(duration) => duration.as_millis() as u64,
                        Err(_) => continue,
                    };

                    if let Ok(ping_pkt) = build_unconnected_ping(now, 0x1234567890abcdef) {
                        if self.socket.send_to(&ping_pkt, self.config.target_addr).await.is_ok() {
                            last_probe_timestamp = Some(now);
                        }
                    }

                    // Check timeout
                    if let Some(seen) = last_seen {
                        if seen.elapsed() > self.config.timeout {
                            last_seen = None;
                            last_ident = None;
                            last_probe_timestamp = None;
                            let _ = self.current_server.send(None);
                            let _ = self.event_tx.send(LocalServerEvent::ServerStopped);
                            info!("Local Minecraft server stopped or stopped responding");
                        }
                    }
                }
                recv_res = self.socket.recv_from(&mut recv_buf) => {
                    if let Ok((len, from_addr)) = recv_res {
                        if from_addr != self.config.target_addr {
                            continue;
                        }
                        if let Ok((timestamp, _guid, msg)) = parse_unconnected_pong(&recv_buf[..len]) {
                            if last_probe_timestamp != Some(timestamp) {
                                continue;
                            }
                            debug!("Received pong from {}: {}", from_addr, msg);
                            if let Ok(ident) = BedrockIdentifier::parse(&msg) {
                                last_seen = Some(Instant::now());
                                let prev = last_ident.take();
                                let is_new = prev.is_none();
                                let changed = match &prev {
                                    Some(p) => p != &ident,
                                    None => true,
                                };

                                if is_new {
                                    info!("Local Minecraft world detected: '{}' (version {})", ident.server_name, ident.version_name);
                                    let _ = self.event_tx.send(LocalServerEvent::ServerStarted(ident.clone()));
                                } else if changed {
                                    info!("Local Minecraft world updated: '{}' (players: {}/{})", ident.server_name, ident.player_count, ident.max_player_count);
                                    let _ = self.event_tx.send(LocalServerEvent::ServerUpdated(ident.clone()));
                                }

                                let _ = self.current_server.send(Some(ident.clone()));
                                last_ident = Some(ident);
                            }
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
    use crate::protocol::build_unconnected_pong;

    #[tokio::test]
    async fn test_detector_probe_and_detection() {
        // Mock a Minecraft Bedrock server socket
        let mock_server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = mock_server.local_addr().unwrap();

        let config = ServerDetectorConfig {
            target_addr: server_addr,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            probe_interval: Duration::from_millis(200),
            timeout: Duration::from_millis(800),
        };

        let detector = Arc::new(ServerDetector::bind(config).await.unwrap());

        // Spawn mock server responder
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            while let Ok((len, client_addr)) = mock_server.recv_from(&mut buf).await {
                if len > 0 {
                    let (timestamp, _) =
                        crate::protocol::parse_unconnected_ping(&buf[..len]).unwrap();
                    let pong = build_unconnected_pong(
                        timestamp,
                        99999,
                        "MCPE;Test Detection World;776;1.21.60;2;8;99999;Sub;Survival;1;19132;19133;",
                    ).unwrap();
                    let _ = mock_server.send_to(&pong, client_addr).await;
                }
            }
        });

        // Test probe_once
        let detected = detector.probe_once().await.unwrap();
        assert!(detected.is_some());
        let ident = detected.unwrap();
        assert_eq!(ident.server_name, "Test Detection World");
        assert_eq!(ident.player_count, 2);
    }
}
