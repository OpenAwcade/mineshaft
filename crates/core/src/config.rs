use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Static server metadata advertised to the local Minecraft client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerAdvertisement {
    /// Name shown as the server owner in the LAN list.
    pub server_name: String,
    /// World/level name shown in the LAN list.
    pub level_name: String,
    /// Default game mode (0=Survival, 1=Creative, 2=Adventure, 3=Spectator).
    pub game_type: u8,
    /// Current player count (must be >= 1 to stay visible).
    pub player_count: i32,
    /// Maximum player count.
    pub max_player_count: i32,
    /// Whether this is an editor-mode world.
    pub editor_world: bool,
    /// Whether hardcore mode is enabled.
    pub hardcore: bool,
    /// Extra compatibility flag observed in ServerData v6.
    pub flag_a: bool,
    /// Extra compatibility flag observed in ServerData v6.
    pub flag_b: bool,
    /// 16-character lowercase hex session identifier used by ServerData v6.
    pub session_id: String,
    /// Transport layer (2 = NetherNet).
    pub transport_layer: u8,
    /// Connection type (4 = LAN).
    pub connection_type: u8,
}

impl Default for ServerAdvertisement {
    fn default() -> Self {
        Self {
            server_name: "mineshaft".to_string(),
            level_name: "mineshaft world".to_string(),
            game_type: 0,
            player_count: 1,
            max_player_count: 8,
            editor_world: false,
            hardcore: false,
            flag_a: true,
            flag_b: true,
            session_id: "0000000000000000".to_string(),
            transport_layer: 2,
            connection_type: 4,
        }
    }
}

/// Discovery/advertising configuration.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// How often each advertised server is re-sent into the local game.
    pub heartbeat_interval: Duration,
    /// Local destinations to notify (`[::1]:7551`, `127.0.0.1:7551`, etc).
    pub local_targets: Vec<std::net::SocketAddr>,
    /// Address used for the local signaling socket (`0.0.0.0:0` by default).
    pub bind_addr: std::net::SocketAddr,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(500),
            local_targets: vec![
                "[::1]:7551".parse().expect("valid v6 target"),
                "127.0.0.1:7551".parse().expect("valid v4 target"),
            ],
            bind_addr: "0.0.0.0:0".parse().expect("valid bind addr"),
        }
    }
}

/// Relay registry configuration.
#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// Optional cap on the number of advertised hosts kept in memory.
    pub max_advertised_hosts: usize,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            max_advertised_hosts: 64,
        }
    }
}
