//! Core NetherNet-based mineshaft runtime.
//!
//! This crate owns shared transport/session/discovery/relay logic. The
//! platform-specific probing and socket creation come from exactly one
//! backend crate selected at compile time via `cfg(target_os = ...)`.

pub mod config;
pub mod discovery;
pub mod error;
pub mod net;
pub mod platform;
pub mod relay;
pub mod session;
pub mod transport;

pub use config::{DiscoveryConfig, RelayConfig, ServerAdvertisement};
pub use discovery::{AdvertisedServer, DiscoveryService, SharedDiscoveryService};
pub use error::{CoreError, Result};
pub use relay::{RelayCommand, RelayService, SharedRelayService};
pub use session::{SessionRegistry, SessionSummary};
pub use transport::{Accepted, Listener, Transport};
