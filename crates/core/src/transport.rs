//! Thin transport facade over the vendored NetherNet tokio transport.

use std::sync::Arc;

use nethernet_tokio::{Addr, LanSignaling, NetherClient, NetherServer, Session, SessionReceiver};

use crate::error::Result;

/// Transport facade backed by NetherNet LAN signaling.
///
/// The signaling stack is shared between the listening and dialing paths; the
/// `nethernet-tokio` signaling enums already accept an `Arc<LanSignaling>`,
/// so no wrapper type is needed here.
#[derive(Clone)]
pub struct Transport {
    signaling: Arc<LanSignaling>,
}

impl Transport {
    /// Create a transport from an existing signaling stack.
    pub fn new(signaling: Arc<LanSignaling>) -> Self {
        Self { signaling }
    }

    /// Access the underlying LAN signaling instance.
    pub fn signaling(&self) -> Arc<LanSignaling> {
        self.signaling.clone()
    }

    /// Start listening for inbound NetherNet sessions.
    pub async fn listen(&self) -> Result<Listener> {
        let inner = NetherServer::bind(self.signaling.clone()).await?;
        Ok(Listener { inner })
    }

    /// Connect to a remote network by network ID.
    pub async fn connect(&self, network_id: String) -> Result<NetherClient> {
        Ok(NetherClient::connect(self.signaling.clone(), network_id).await?)
    }

    /// Connect using an explicit address (network + connection ID).
    pub async fn connect_addr(&self, remote: Addr) -> Result<NetherClient> {
        self.connect(remote.network_id).await
    }
}

/// A listener that accepts NetherNet sessions.
pub struct Listener {
    inner: NetherServer,
}

impl Listener {
    /// Accept the next inbound session.
    pub async fn accept(&mut self) -> Result<Accepted> {
        let accepted = self.inner.accept().await?;
        Ok(Accepted {
            session: accepted.session,
            reliable: accepted.reliable,
            unreliable: accepted.unreliable,
        })
    }

    /// Local network address this listener is bound to.
    pub fn local_addr(&self) -> &Addr {
        self.inner.local_addr()
    }

    /// Close the listener and every session that has not been accepted yet.
    pub async fn close(&mut self) -> Result<()> {
        Ok(self.inner.close().await?)
    }
}

/// An accepted inbound session with its two data channel receivers.
pub struct Accepted {
    /// The session handle (cheap to clone).
    pub session: Session,
    /// Receiver of the reliable data channel.
    pub reliable: SessionReceiver,
    /// Receiver of the unreliable data channel.
    pub unreliable: SessionReceiver,
}
