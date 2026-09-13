use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nethernet::Addr;

/// Compact view of a live session tracked by the registry.
#[derive(Debug, Clone)]
pub struct SessionSummary {
    /// NetherNet network ID of the remote peer.
    pub network_id: String,
    /// Connection ID inside the network.
    pub connection_id: u64,
    /// Local transport address for the session.
    pub local_addr: Addr,
    /// Remote transport address for the session.
    pub remote_addr: Addr,
}

/// Tracks active NetherNet sessions by network/connection ID.
#[derive(Default)]
pub struct SessionRegistry {
    sessions: RwLock<HashMap<(String, u64), Arc<nethernet::Session>>>,
}

impl SessionRegistry {
    /// Insert or replace a session for the given key.
    pub fn upsert(&self, network_id: String, connection_id: u64, session: Arc<nethernet::Session>) {
        self.sessions
            .write()
            .expect("session registry poisoned")
            .insert((network_id, connection_id), session);
    }

    /// Remove a session by key.
    pub fn remove(&self, network_id: &str, connection_id: u64) {
        self.sessions
            .write()
            .expect("session registry poisoned")
            .remove(&(network_id.to_string(), connection_id));
    }

    /// Get a session by key.
    pub fn get(&self, network_id: &str, connection_id: u64) -> Option<Arc<nethernet::Session>> {
        self.sessions
            .read()
            .expect("session registry poisoned")
            .get(&(network_id.to_string(), connection_id))
            .cloned()
    }

    /// Snapshot all tracked sessions.
    pub async fn snapshot(&self) -> Vec<SessionSummary> {
        let entries: Vec<((String, u64), Arc<nethernet::Session>)> = self
            .sessions
            .read()
            .expect("session registry poisoned")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let mut out = Vec::with_capacity(entries.len());
        for ((network_id, connection_id), session) in entries {
            out.push(SessionSummary {
                network_id,
                connection_id,
                local_addr: session.local_addr().await,
                remote_addr: session.remote_addr().await,
            });
        }
        out
    }

    /// Number of tracked sessions.
    pub fn len(&self) -> usize {
        self.sessions
            .read()
            .expect("session registry poisoned")
            .len()
    }
}
