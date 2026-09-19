//! Relay registry: which local node hosts which advertised server, and which
//! remote node wants which session.

use std::sync::Arc;

use dashmap::DashMap;
use nethernet_tokio::Addr;
use tokio::sync::mpsc;

use crate::config::RelayConfig;
use crate::discovery::AdvertisedServer;
use crate::error::{CoreError, Result};

/// Shared handle to the relay service.
pub type SharedRelayService = Arc<RelayService>;

/// Maximum queued commands before submissions start being rejected. Commands
/// are tiny registry operations; a full queue means the owner loop is wedged
/// and backpressure (an error to the caller) is the right answer, not an
/// ever-growing buffer.
const COMMAND_QUEUE_CAPACITY: usize = 256;

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
    hosts: DashMap<u64, AdvertisedServer>,
    command_tx: mpsc::Sender<RelayCommand>,
    command_rx: tokio::sync::Mutex<mpsc::Receiver<RelayCommand>>,
}

impl RelayService {
    /// Create a new relay service.
    pub fn new(config: RelayConfig) -> SharedRelayService {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        Arc::new(Self {
            config,
            hosts: DashMap::new(),
            command_tx,
            command_rx: tokio::sync::Mutex::new(command_rx),
        })
    }

    /// Submit a relay command for processing by the owner task.
    ///
    /// Fails immediately when the queue is full instead of buffering without
    /// bound: a wedged owner loop must surface as an error, not as memory
    /// growth.
    pub fn submit(&self, command: RelayCommand) -> Result<()> {
        self.command_tx
            .try_send(command)
            .map_err(|_| CoreError::InvalidState("relay command queue full or closed"))
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
        if self.hosts.len() >= self.config.max_advertised_hosts
            && !self.hosts.contains_key(&server.sender_id)
            && let Some(oldest) = self.hosts.iter().next().map(|entry| *entry.key())
        {
            self.hosts.remove(&oldest);
        }
        self.hosts.insert(server.sender_id, server);
    }

    /// Snapshot all currently advertised hosts.
    pub fn host_list(&self) -> Vec<AdvertisedServer> {
        self.hosts
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Look up a host by its advertised sender ID.
    pub fn host_by_sender(&self, sender_id: u64) -> Option<AdvertisedServer> {
        self.hosts.get(&sender_id).map(|entry| entry.clone())
    }
}
