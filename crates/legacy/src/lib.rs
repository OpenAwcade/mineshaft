pub mod bridge;
pub mod detector;
pub mod manager;
pub mod platform;
pub mod pong;
pub mod protocol;
pub mod tunnel;

pub use bridge::MineshaftBridge;

pub mod prelude {
    pub use crate::bridge::MineshaftBridge;
    pub use crate::detector::{LocalServerEvent, ServerDetector, ServerDetectorConfig};
    pub use crate::manager::{RemoteServer, ServerManager};
    pub use crate::platform::{Platform, PlatformNetwork};
    pub use crate::pong::{PongEngine, PongEngineConfig};
    pub use crate::protocol::{
        build_unconnected_ping, build_unconnected_pong, parse_unconnected_ping,
        parse_unconnected_pong, BedrockIdentifier, MineshaftHeader, MINESHAFT_HEADER_LEN,
    };
    pub use crate::tunnel::{AccessControl, TunnelConfig, TunnelEngine, TunnelEvent};
}
