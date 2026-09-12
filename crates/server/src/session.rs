use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info};

#[derive(Clone, Debug)]
pub struct Session {
    pub session_id: u32,
    pub socket_addr: SocketAddr,
    pub last_seen: Instant,
}

#[derive(Default)]
pub struct SessionRegistry {
    sessions: RwLock<HashMap<u32, Session>>,
    addr_to_id: RwLock<HashMap<SocketAddr, u32>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers or refreshes a peer session with its current public UDP endpoint.
    pub async fn register_or_refresh(&self, session_id: u32, socket_addr: SocketAddr) -> bool {
        let mut sessions = self.sessions.write().await;
        let mut addr_to_id = self.addr_to_id.write().await;

        if let Some(previous_id) = addr_to_id.get(&socket_addr).copied()
            && previous_id != session_id
        {
            sessions.remove(&previous_id);
            info!(
                "Replaced session {} at reused endpoint {}",
                previous_id, socket_addr
            );
        }

        let is_new = if let Some(existing) = sessions.get_mut(&session_id) {
            if existing.socket_addr != socket_addr {
                addr_to_id.remove(&existing.socket_addr);
                existing.socket_addr = socket_addr;
                addr_to_id.insert(socket_addr, session_id);
                info!(
                    "Session {} migrated endpoint to {}",
                    session_id, socket_addr
                );
            }
            existing.last_seen = Instant::now();
            false
        } else {
            let session = Session {
                session_id,
                socket_addr,
                last_seen: Instant::now(),
            };
            sessions.insert(session_id, session);
            addr_to_id.insert(socket_addr, session_id);
            info!(
                "Registered new peer session {} at {}",
                session_id, socket_addr
            );
            true
        };

        is_new
    }

    pub async fn get_addr(&self, session_id: u32) -> Option<SocketAddr> {
        let sessions = self.sessions.read().await;
        sessions.get(&session_id).map(|s| s.socket_addr)
    }

    pub async fn get_id_by_addr(&self, addr: &SocketAddr) -> Option<u32> {
        let addr_to_id = self.addr_to_id.read().await;
        addr_to_id.get(addr).copied()
    }

    pub async fn remove(&self, session_id: u32) -> Option<SocketAddr> {
        let mut sessions = self.sessions.write().await;
        let mut addr_to_id = self.addr_to_id.write().await;

        if let Some(session) = sessions.remove(&session_id) {
            addr_to_id.remove(&session.socket_addr);
            info!("Removed session {} ({})", session_id, session.socket_addr);
            Some(session.socket_addr)
        } else {
            None
        }
    }

    /// Prunes stale sessions that have not checked in within the timeout duration.
    pub async fn prune_stale(&self, timeout: Duration) -> Vec<u32> {
        let mut sessions = self.sessions.write().await;
        let mut addr_to_id = self.addr_to_id.write().await;

        let now = Instant::now();
        let mut timed_out = Vec::new();

        sessions.retain(|id, session| {
            if now.duration_since(session.last_seen) > timeout {
                timed_out.push(*id);
                addr_to_id.remove(&session.socket_addr);
                false
            } else {
                true
            }
        });

        for id in &timed_out {
            debug!("Pruned timed out session {}", id);
        }

        timed_out
    }

    pub async fn count(&self) -> usize {
        self.sessions.read().await.len()
    }

    pub async fn active_sessions(&self) -> Vec<u32> {
        self.sessions.read().await.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_session_lifecycle() {
        let registry = SessionRegistry::new();
        let addr1: SocketAddr = "127.0.0.1:10001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:10002".parse().unwrap();

        assert!(registry.register_or_refresh(101, addr1).await);
        assert!(!registry.register_or_refresh(101, addr1).await); // already exists
        assert_eq!(registry.get_addr(101).await, Some(addr1));
        assert_eq!(registry.get_id_by_addr(&addr1).await, Some(101));

        // Endpoint migration
        registry.register_or_refresh(101, addr2).await;
        assert_eq!(registry.get_addr(101).await, Some(addr2));
        assert_eq!(registry.get_id_by_addr(&addr1).await, None);
        assert_eq!(registry.get_id_by_addr(&addr2).await, Some(101));

        // Pruning
        let pruned = registry.prune_stale(Duration::from_millis(0)).await;
        assert_eq!(pruned, vec![101]);
        assert_eq!(registry.count().await, 0);
    }
}
