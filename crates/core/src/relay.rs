//! Relay registry: which local node hosts which advertised server, and which
//! remote node wants which session.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nethernet_tokio::Addr;
use tokio::sync::mpsc;

use crate::config::RelayConfig;
use crate::discovery::AdvertisedServer;
use crate::error::{CoreError, Result};

/// Shared handle to the relay service.
pub type SharedRelayService = Arc<RelayService>;

/// Commands the relay can consume from signaling/transport layers.
#[derive(Debug)]
pub enum RelayCommand {
    /// A local node publishes an advertised server record.
    RegisterHost(AdvertisedServer),
    /// A remote joiner asks for the currently advertised host list.
    ListHosts,
    /// A joiner selected a server and wants a bridged session.
    BridgeToHost {
        /// Sender/network ID of the advertised server.
        advertised_sender_id: u64,
        /// Address of the joiner's local negotiation endpoint.
        joiner_addr: Addr,
    },
}

/// Relay service owning the host registry and relay bookkeeping.
pub struct RelayService {
    config: RelayConfig,
    hosts: RwLock<HashMap<u64, AdvertisedServer>>,
    command_tx: mpsc::UnboundedSender<RelayCommand>,
    command_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<RelayCommand>>,
}

impl RelayService {
    /// Create a new relay service.
    pub fn new(config: RelayConfig) -> SharedRelayService {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        Arc::new(Self {
            config,
            hosts: RwLock::new(HashMap::new()),
            command_tx,
            command_rx: tokio::sync::Mutex::new(command_rx),
        })
    }

    /// Submit a relay command for processing by the owner task.
    pub fn submit(&self, command: RelayCommand) -> Result<()> {
        self.command_tx
            .send(command)
            .map_err(|_| CoreError::InvalidState("relay command channel closed"))
    }

    /// Process pending commands once. Intended to be called from a single
    /// owner loop; returns the number of commands handled.
    pub async fn pump_commands(&self) -> usize {
        let mut rx = self.command_rx.lock().await;
        let mut handled = 0;
        while let Ok(command) = rx.try_recv() {
            handled += 1;
            match command {
                RelayCommand::RegisterHost(server) => self.register_host(server),
                RelayCommand::ListHosts => {
                    let _ = self.host_list();
                }
                RelayCommand::BridgeToHost {
                    advertised_sender_id,
                    joiner_addr,
                } => {
                    let _ = (advertised_sender_id, joiner_addr);
                }
            }
        }
        handled
    }

    /// Register or replace an advertised host.
    pub fn register_host(&self, server: AdvertisedServer) {
        let mut hosts = self.hosts.write().expect("relay hosts poisoned");
        if hosts.len() >= self.config.max_advertised_hosts && !hosts.contains_key(&server.sender_id)
        {
            if let Some(oldest) = hosts.keys().next().copied() {
                hosts.remove(&oldest);
            }
        }
        hosts.insert(server.sender_id, server);
    }

    /// Snapshot all currently advertised hosts.
    pub fn host_list(&self) -> Vec<AdvertisedServer> {
        self.hosts
            .read()
            .expect("relay hosts poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Look up a host by its advertised sender ID.
    pub fn host_by_sender(&self, sender_id: u64) -> Option<AdvertisedServer> {
        self.hosts
            .read()
            .expect("relay hosts poisoned")
            .get(&sender_id)
            .cloned()
    }
}
