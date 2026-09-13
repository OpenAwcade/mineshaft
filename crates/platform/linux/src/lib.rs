//! Linux platform helpers for mineshaft.
//!
//! No daemon logic lives here; this crate exposes only:
//! - UDP port occupancy probing via `/proc/net/udp{,6}`
//! - Minecraft process detection via `/proc/*/comm` (+ cmdline fallback)
//! - UDP socket creation with the exact options the NetherNet transport needs

use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;

/// Errors produced by Linux platform probing.
#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    /// I/O error while reading kernel tables or creating sockets.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// External command failed.
    #[error("command failed: {0}")]
    Command(String),
}

/// Result type used by this crate.
pub type Result<T> = std::result::Result<T, PlatformError>;

/// Returns true if any UDP/UDP6 socket is bound to the given local port.
///
/// This scans `/proc/net/udp` and `/proc/net/udp6` for entries whose local
/// address port matches `port`. It does not require elevated privileges.
pub fn udp_port_bound(port: u16) -> Result<bool> {
    let needle = format!(":{:04X}", port);
    for table in ["/proc/net/udp", "/proc/net/udp6"] {
        if table_has_port(Path::new(table), &needle)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Best-effort check whether a Bedrock-compatible Minecraft process is running.
///
/// Matches common process names for Linux ports, launchers, and servers.
pub fn minecraft_process_running() -> bool {
    const NAMES: &[&str] = &[
        "Minecraft.Windows.exe",
        "minecraft",
        "mcpelauncher",
        "minecraftlinux",
        "bedrock_server",
        "bedrock-server",
    ];

    if proc_comm_matches(NAMES) || proc_cmdline_matches(NAMES) {
        return true;
    }

    command_succeeds("pgrep", &["-f", "Minecraft.Windows.exe"])
        || command_succeeds("pgrep", &["-f", "bedrock_server"])
}

/// Creates a UDP socket suitable for NetherNet discovery/signaling.
///
/// The socket is configured with `SO_REUSEADDR` (but not `SO_REUSEPORT`) and
/// broadcast enabled. It is returned in blocking mode; callers are expected to
/// convert it to a tokio socket if async I/O is needed.
pub fn create_udp_socket(addr: SocketAddr) -> Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_broadcast(true)?;
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

fn table_has_port(path: &Path, needle: &str) -> Result<bool> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(PlatformError::Io(e)),
    };

    for line in content.lines().skip(1) {
        let local_address = line.split_whitespace().nth(1);
        if local_address
            .map(|local| local.ends_with(needle))
            .unwrap_or(false)
        {
            return Ok(true);
        }
    }

    Ok(false)
}

fn proc_comm_matches(names: &[&str]) -> bool {
    let Ok(entries) = fs::read_dir("/proc") else {
        return false;
    };

    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if !name.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }

        let comm_path = entry.path().join("comm");
        let Ok(comm) = fs::read_to_string(&comm_path) else {
            continue;
        };
        let comm = comm.trim();
        if names.iter().any(|candidate| comm == *candidate) {
            return true;
        }
    }

    false
}

fn proc_cmdline_matches(names: &[&str]) -> bool {
    let Ok(entries) = fs::read_dir("/proc") else {
        return false;
    };

    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if !name.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }

        let cmdline_path = entry.path().join("cmdline");
        let Ok(cmdline) = fs::read(&cmdline_path) else {
            continue;
        };
        let haystack = String::from_utf8_lossy(&cmdline).replace('\0', " ").to_lowercase();
        if names
            .iter()
            .any(|candidate| haystack.contains(&candidate.to_lowercase()))
        {
            return true;
        }
    }

    false
}

fn command_succeeds(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn parses_proc_net_udp_fixture() {
        let path = std::env::temp_dir().join(format!(
            "mineshaft-linux-proc-udp-fixture-{}",
            std::process::id()
        ));
        std::fs::write(
            &path,
            "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n\
             123: 0100007F:0035 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 12345 1 0000000000000000 0\n\
             124: 00000000:1D7F 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 12346 1 0000000000000000 0\n",
        )
        .expect("write fixture");

        // Port 53 (0x0035) is present.
        let needle = format!(":{:04X}", 53);
        assert!(super::table_has_port(&path, &needle).unwrap());

        // Port 9999 (0x270F) is absent.
        let needle = format!(":{:04X}", 9999);
        assert!(!super::table_has_port(&path, &needle).unwrap());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn udp_port_bound_reflects_bind_state() {
        let port = 39_999;
        assert!(!udp_port_bound(port).unwrap());

        let socket = std::net::UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).unwrap();
        assert!(udp_port_bound(port).unwrap());

        drop(socket);
        assert!(!udp_port_bound(port).unwrap());
    }
}
