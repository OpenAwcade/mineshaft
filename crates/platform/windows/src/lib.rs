//! Windows platform helpers for mineshaft.
//!
//! This crate keeps platform-specific socket/process probing out of
//! `mineshaft_core`. The Windows implementation uses `GetExtendedUdpTable`
//! and ToolHelp snapshots when compiled on Windows; on other targets the
//! functions are conservative stubs so the workspace remains checkable from
//! Linux/Android during development.

use std::io;
use std::net::SocketAddr;

/// Errors produced by Windows platform probing.
#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    /// I/O error while probing the OS.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// External command failed.
    #[error("command failed: {0}")]
    Command(String),
    /// The current build target does not provide a native implementation.
    #[error("unsupported platform for windows backend")]
    Unsupported,
}

/// Result type used by this crate.
pub type Result<T> = std::result::Result<T, PlatformError>;

/// Returns true if any UDP socket is bound to the given local port.
pub fn udp_port_bound(port: u16) -> Result<bool> {
    udp_port_bound_impl(port)
}

/// Returns the owning PID for a bound UDP port, when the OS exposes it.
pub fn owner_pid_of_port(port: u16) -> Result<Option<u32>> {
    owner_pid_of_port_impl(port)
}

/// Creates a UDP socket suitable for NetherNet discovery/signaling.
pub fn create_udp_socket(addr: SocketAddr) -> Result<std::net::UdpSocket> {
    create_udp_socket_impl(addr)
}

/// Ensures the Minecraft UWP package is loopback-exempt so local reflection
/// and loopback traffic can work on Windows.
pub fn ensure_loopback_exempt() -> Result<()> {
    ensure_loopback_exempt_impl()
}

#[cfg(windows)]
mod imp {
    use super::*;
    use std::net::UdpSocket;
    use std::process::Command;
    use windows::Win32::Foundation::ERROR_INSUFFICIENT_BUFFER;
    use windows::Win32::NetworkManagement::IpHelper::{
        GetExtendedUdpTable, MIB_UDPTABLE_OWNER_PID, UDP_TABLE_OWNER_PID,
    };

    pub fn udp_port_bound_impl(port: u16) -> Result<bool> {
        Ok(owner_pid_of_port_impl(port)?.is_some())
    }

    pub fn owner_pid_of_port_impl(port: u16) -> Result<Option<u32>> {
        unsafe {
            let mut size = 0u32;
            let _ = GetExtendedUdpTable(
                None,
                &mut size,
                false,
                windows::Win32::Networking::WinSock::AF_INET.0 as u32,
                UDP_TABLE_OWNER_PID,
                0,
            );

            if size == 0 {
                return Ok(None);
            }

            let mut buffer = vec![0u8; size as usize];
            let result = GetExtendedUdpTable(
                Some(buffer.as_mut_ptr().cast()),
                &mut size,
                false,
                windows::Win32::Networking::WinSock::AF_INET.0 as u32,
                UDP_TABLE_OWNER_PID,
                0,
            );
            if result != 0 && result != ERROR_INSUFFICIENT_BUFFER.0 {
                return Err(PlatformError::Io(io::Error::last_os_error()));
            }

            let table = buffer.as_ptr().cast::<MIB_UDPTABLE_OWNER_PID>();
            let count = (*table).dwNumEntries as usize;
            let rows = std::slice::from_raw_parts((*table).table.as_ptr(), count);
            for row in rows {
                let local_port = u16::from_be(row.dwLocalPort as u16);
                if local_port == port {
                    return Ok(Some(row.dwOwningPid));
                }
            }

            Ok(None)
        }
    }

    pub fn create_udp_socket_impl(addr: SocketAddr) -> Result<UdpSocket> {
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

    pub fn ensure_loopback_exempt_impl() -> Result<()> {
        let status = Command::new("CheckNetIsolation.exe")
            .args([
                "LoopbackExempt",
                "-a",
                "-n=Microsoft.MinecraftUWP_8wekyb3d8bbwe",
            ])
            .status()
            .map_err(PlatformError::Io)?;

        if status.success() {
            Ok(())
        } else {
            Err(PlatformError::Command(format!(
                "CheckNetIsolation exited with status {}",
                status
            )))
        }
    }
}

#[cfg(windows)]
use imp::*;

#[cfg(not(windows))]
mod imp {
    use super::*;
    use std::net::UdpSocket;

    pub fn udp_port_bound_impl(_port: u16) -> Result<bool> {
        Err(PlatformError::Unsupported)
    }

    pub fn owner_pid_of_port_impl(_port: u16) -> Result<Option<u32>> {
        Err(PlatformError::Unsupported)
    }

    pub fn create_udp_socket_impl(addr: SocketAddr) -> Result<UdpSocket> {
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

    pub fn ensure_loopback_exempt_impl() -> Result<()> {
        Err(PlatformError::Unsupported)
    }
}

#[cfg(not(windows))]
use imp::*;
