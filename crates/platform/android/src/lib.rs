//! Android platform helpers for mineshaft.
//!
//! Mirrors the Linux probing strategy because Android exposes the same
//! `/proc` network tables, but keeps Minecraft detection conservative: newer
//! Android builds restrict process visibility, so "unknown" is treated as
//! "running" to avoid squatting on UDP 7551.

use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

/// Errors produced by Android platform probing.
#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    /// I/O error while reading kernel tables or creating sockets.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// Result type used by this crate.
pub type Result<T> = std::result::Result<T, PlatformError>;

type ProtectSocketCallback = Box<dyn Fn(i32) -> bool + Send + Sync>;

static PROTECT_SOCKET: OnceLock<Mutex<Option<ProtectSocketCallback>>> = OnceLock::new();

fn protect_socket_slot() -> &'static Mutex<Option<ProtectSocketCallback>> {
    PROTECT_SOCKET.get_or_init(|| Mutex::new(None))
}

/// Returns true if any UDP/UDP6 socket is bound to the given local port.
///
/// This scans `/proc/net/udp` and `/proc/net/udp6` for entries whose local
/// address port matches `port`. On Android this can be restricted by SELinux
/// on some builds; callers should treat read failures as "unknown".
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
/// Returns `true` when a matching process is found *or* when process
/// enumeration is unavailable/restricted; that failure mode is deliberately
/// biased toward "running" so the transport yields more often instead of
/// squatting on the discovery port.
pub fn minecraft_process_running() -> bool {
    const NAMES: &[&str] = &[
        "Minecraft.Windows.exe",
        "minecraft",
        "mcpelauncher",
        "minecraftlinux",
        "bedrock_server",
        "bedrock-server",
    ];

    match proc_visible_matches(NAMES) {
        Some(found) => found,
        None => true,
    }
}

/// Creates a UDP socket suitable for NetherNet discovery/signaling.
///
/// The socket is configured with `SO_REUSEADDR` and broadcast enabled. Some
/// Android kernels behave differently around reuse/broadcast edge cases, so
/// callers should still verify behavior with runtime probes.
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

/// Registers the callback used to protect a socket from an Android
/// `VpnService` tunnel.
pub fn set_protect_socket_callback<F>(callback: F)
where
    F: Fn(i32) -> bool + Send + Sync + 'static,
{
    *protect_socket_slot()
        .lock()
        .expect("protect socket mutex poisoned") = Some(Box::new(callback));
}

/// Clears the previously registered socket-protection callback.
pub fn clear_protect_socket_callback() {
    *protect_socket_slot()
        .lock()
        .expect("protect socket mutex poisoned") = None;
}

/// Invokes the registered socket-protection callback, if any.
///
/// Returns `Ok(true)` when the socket was protected or when no callback is
/// registered (treated as no-op success).
pub fn protect_socket(fd: i32) -> bool {
    let guard = protect_socket_slot()
        .lock()
        .expect("protect socket mutex poisoned");
    match guard.as_ref() {
        Some(callback) => callback(fd),
        None => true,
    }
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

fn proc_visible_matches(names: &[&str]) -> Option<bool> {
    let entries = fs::read_dir("/proc").ok()?;
    let mut any_visible = false;

    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if !name.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }

        any_visible = true;
        let comm_path = entry.path().join("comm");
        if let Ok(comm) = fs::read_to_string(&comm_path) {
            let comm = comm.trim();
            if names.iter().any(|candidate| comm == *candidate) {
                return Some(true);
            }
        }

        let cmdline_path = entry.path().join("cmdline");
        if let Ok(cmdline) = fs::read(&cmdline_path) {
            let haystack = String::from_utf8_lossy(&cmdline)
                .replace('\0', " ")
                .to_lowercase();
            if names
                .iter()
                .any(|candidate| haystack.contains(&candidate.to_lowercase()))
            {
                return Some(true);
            }
        }
    }

    if any_visible { Some(false) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI32, Ordering};

    #[test]
    fn protect_socket_callback_lifecycle() {
        // Single test: the callback slot is global, so parallel tests would race.
        clear_protect_socket_callback();
        assert!(protect_socket(42));

        static SEEN: AtomicI32 = AtomicI32::new(-1);
        set_protect_socket_callback(|fd| {
            SEEN.store(fd, Ordering::SeqCst);
            fd % 2 == 0
        });

        assert!(protect_socket(8));
        assert_eq!(SEEN.load(Ordering::SeqCst), 8);
        assert!(!protect_socket(7));
        assert_eq!(SEEN.load(Ordering::SeqCst), 7);

        clear_protect_socket_callback();
        assert!(protect_socket(42));
    }
}
