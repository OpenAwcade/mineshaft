//! Thin transport abstraction over the vendored NetherNet tokio transport.

use std::pin::Pin;
use std::sync::Arc;

use nethernet::credentials::Credentials;
use nethernet::{
    Addr, LanSignaling, NethernetListener, NethernetStream, Session, Signal, Signaling,
};

use crate::error::Result;

/// `Signaling` wrapper around a shared `LanSignaling` so the transport can be
/// cloned into both the listener and the dialing paths without consuming the
/// underlying signaling stack.
#[derive(Clone)]
pub struct SharedSignaling {
    inner: Arc<LanSignaling>,
}

impl SharedSignaling {
    /// Wrap an existing shared LAN signaling stack.
    pub fn new(inner: Arc<LanSignaling>) -> Self {
        Self { inner }
    }

    /// Access the wrapped signaling stack.
    pub fn inner(&self) -> Arc<LanSignaling> {
        self.inner.clone()
    }
}

impl Signaling for SharedSignaling {
    async fn signal(&self, signal: Signal) -> nethernet::Result<()> {
        self.inner.signal(signal).await
    }

    fn signals(&self) -> Pin<Box<dyn futures::Stream<Item = Signal> + Send>> {
        self.inner.signals()
    }

    fn network_id(&self) -> String {
        self.inner.network_id()
    }

    fn disable_trickle_ice(&self) -> bool {
        self.inner.disable_trickle_ice()
    }

    async fn credentials(&self) -> nethernet::Result<Option<Credentials>> {
        self.inner.credentials().await
    }

    fn set_pong_data(&self, data: &[u8]) {
        self.inner.set_pong_data(data)
    }
}

/// A listener that accepts NetherNet sessions.
pub struct Listener {
    inner: tokio::sync::Mutex<NethernetListener<SharedSignaling>>,
}

impl Listener {
    /// Accept the next inbound session.
    pub async fn accept(&self) -> Result<Arc<Session>> {
        let mut inner = self.inner.lock().await;
        Ok(inner.accept().await?)
    }

    /// Local network address this listener is bound to.
    pub async fn local_addr(&self) -> Addr {
        let inner = self.inner.lock().await;
        inner.local_addr().clone()
    }
}

/// Transport facade backed by NetherNet LAN signaling.
pub struct Transport {
    signaling: SharedSignaling,
}

impl Transport {
    /// Create a transport from an existing signaling stack.
    pub fn new(signaling: Arc<LanSignaling>) -> Self {
        Self {
            signaling: SharedSignaling::new(signaling),
        }
    }

    /// Access the underlying LAN signaling instance.
    pub fn signaling(&self) -> SharedSignaling {
        self.signaling.clone()
    }

    /// Start listening for inbound NetherNet sessions.
    pub async fn listen(&self) -> Result<Listener> {
        let listener = NethernetListener::bind(self.signaling.clone()).await?;
        Ok(Listener {
            inner: tokio::sync::Mutex::new(listener),
        })
    }

    /// Connect to a remote network by network ID.
    pub async fn connect(&self, network_id: String) -> Result<NethernetStream> {
        Ok(NethernetStream::connect(Arc::new(self.signaling.clone()), network_id).await?)
    }

    /// Connect using an explicit address (network + connection ID).
    pub async fn connect_addr(&self, remote: Addr) -> Result<NethernetStream> {
        self.connect(remote.network_id).await
    }
}
