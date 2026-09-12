use socket2::{Domain, Protocol, Socket, Type};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::UdpSocket;
use tracing::{debug, info};
#[cfg(target_os = "windows")]
use tracing::warn;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Platform {
    Linux,
    Windows,
    MacOS,
    Other,
}

impl Platform {
    pub const fn current() -> Self {
        if cfg!(target_os = "linux") {
            Self::Linux
        } else if cfg!(target_os = "windows") {
            Self::Windows
        } else if cfg!(target_os = "macos") {
            Self::MacOS
        } else {
            Self::Other
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Linux => "Linux",
            Self::Windows => "Windows",
            Self::MacOS => "macOS",
            Self::Other => "Other",
        }
    }
}

/// Creates a UDP socket with platform-optimized reuse options (SO_REUSEADDR / SO_REUSEPORT).
pub fn create_reusable_udp_socket(addr: SocketAddr) -> Result<UdpSocket, std::io::Error> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };

    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

    // Allow address reuse across all platforms
    socket.set_reuse_address(true)?;

    // On Unix platforms (Linux/macOS), SO_REUSEPORT allows multiple sockets to share 19132
    #[cfg(unix)]
    {
        if let Err(e) = socket.set_reuse_port(true) {
            debug!("Could not set SO_REUSEPORT: {}", e);
        }
    }

    // Set non-blocking for Tokio
    socket.set_nonblocking(true)?;
    let socket2_addr = socket2::SockAddr::from(addr);
    socket.bind(&socket2_addr)?;

    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket)
}

/// Network environment configuration for Mineshaft
#[derive(Clone, Debug)]
pub struct PlatformNetwork {
    pub platform: Platform,
    pub loopback_ip: IpAddr,
    pub reflector_ip: Option<IpAddr>,
}

impl Default for PlatformNetwork {
    fn default() -> Self {
        Self {
            platform: Platform::current(),
            loopback_ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            reflector_ip: None,
        }
    }
}

impl PlatformNetwork {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set custom reflector virtual IP (e.g. 10.10.10.10 or virtual adapter IP).
    /// Configures virtual reflector address for WinDivert or TUN adapter.
    pub fn set_reflector(&mut self, ip: IpAddr) {
        self.reflector_ip = Some(ip);
        info!("Set reflector IP to {}", ip);
    }

    /// Effective IP to bind ephemeral sockets and communicate with Minecraft
    pub fn effective_bind_ip(&self) -> IpAddr {
        self.reflector_ip.unwrap_or(self.loopback_ip)
    }

    /// On Windows, Minecraft Bedrock runs inside a UWP AppContainer sandbox.
    /// Standard CheckNetIsolation allows Bedrock to connect to local loopback directly.
    pub async fn ensure_windows_loopback_exempt(&self) -> Result<(), std::io::Error> {
        #[cfg(target_os = "windows")]
        {
            info!("Configuring Windows UWP Loopback Exemption for Minecraft Bedrock...");
            let status = tokio::process::Command::new("CheckNetIsolation.exe")
                .args(["LoopbackExempt", "-a", "-n=Microsoft.MinecraftUWP_8wekyb3d8bbwe"])
                .status()
                .await?;

            if status.success() {
                info!("Successfully enabled Minecraft UWP loopback exemption");
            } else {
                warn!("CheckNetIsolation exited with code: {:?}", status.code());
            }
        }
        #[cfg(not(target_os = "windows"))]
        {
            debug!("Non-Windows platform ({}): UWP loopback exemption not required", self.platform.name());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_detection() {
        let platform = Platform::current();
        assert_eq!(platform, Platform::Linux);
        assert_eq!(platform.name(), "Linux");
    }

    #[tokio::test]
    async fn test_reusable_socket_creation() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let socket = create_reusable_udp_socket(addr).unwrap();
        assert_ne!(socket.local_addr().unwrap().port(), 0);
    }

    #[test]
    fn test_reflector_configuration() {
        let mut net = PlatformNetwork::new();
        assert_eq!(net.effective_bind_ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));

        let reflector: IpAddr = "10.10.10.10".parse().unwrap();
        net.set_reflector(reflector);
        assert_eq!(net.effective_bind_ip(), reflector);
    }
}
