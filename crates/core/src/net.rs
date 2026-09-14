//! Rendezvous wire protocol between mineshaft nodes (`client.rs`) and the
//! rendezvous server (`server.rs`).
//!
//! Framing: 4-byte little-endian length prefix + UTF-8 JSON payload.
//! Transport: plain TCP. The rendezvous server is the always-on remote peer;
//! clients stay connected 24/7.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::discovery::AdvertisedServer;
use crate::error::{CoreError, Result};

pub const MAX_FRAME_BYTES: usize = 512 * 1024;

/// Messages a client sends to the rendezvous server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    /// "I am hosting a world" — server stores this in the host list.
    RegisterHost(AdvertisedServer),
    /// Host is going away / stopped hosting.
    UnregisterHost {
        /// Sender/network ID previously registered.
        sender_id: u64,
    },
    /// "Give me the current host list" — sent periodically while browsing.
    ListHosts,
    /// "The player clicked this server" — server relays to the host.
    ConnectRequest {
        /// Sender/network ID of the advertised server to join.
        target_sender_id: u64,
        /// Network ID of the joiner (so the host knows who is coming).
        joiner_network_id: u64,
    },
    /// Forward a raw WebRTC signaling message (`CONNECTREQUEST <conn> <sdp>`,
    /// `CANDIDATEADD <conn> <candidate>`, ...) toward the host.
    Signal {
        /// Advertised sender id the player clicked.
        target_sender_id: u64,
        /// WebRTC connection id inside the message.
        connection_id: u64,
        /// This node's network id (the host presents it as the peer).
        joiner_network_id: u64,
        /// Raw signaling line.
        data: String,
    },
    /// Keepalive ping; the server answers with [`ServerMessage::Pong`].
    ///
    /// Keeps NAT and port-forwarder state alive on otherwise idle connections
    /// (a hosting client sends nothing else) and lets both sides detect a
    /// silently dropped connection.
    Ping,
    /// Update the metadata of a host that this client already registered.
    UpdateHost(AdvertisedServer),
}

/// Messages the rendezvous server sends back to clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    /// Registration acknowledged.
    Registered {
        /// Sender/network ID that was registered.
        sender_id: u64,
    },
    /// Current host list, in response to [`ClientMessage::ListHosts`].
    HostList(Vec<AdvertisedServer>),
    /// A joiner wants in — relayed to the hosting client.
    IncomingJoin {
        /// Network ID of the joining node.
        joiner_network_id: u64,
        /// Relay session id assigned by the server.
        session_id: u64,
    },
    /// Server accepted the join; carries the relay session id.
    JoinAccepted {
        /// Relay session id assigned by the server.
        session_id: u64,
    },
    /// A signaling message forwarded from the other side of the tunnel.
    Signal {
        /// WebRTC connection id inside the message.
        connection_id: u64,
        /// Network id of the joiner (presented as the sender to the game).
        joiner_network_id: u64,
        /// Raw signaling line.
        data: String,
    },
    /// Answer to [`ClientMessage::Ping`].
    Pong,
    /// Generic error surfaced to the client.
    Error(String),
}

/// Read one framed message. Returns `Ok(None)` on clean EOF.
pub async fn read_message<T, R>(stream: &mut R) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(CoreError::Io(e)),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(CoreError::FrameTooLarge(len));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    let msg = serde_json::from_slice(&buf)
        .map_err(|e| CoreError::Other(format!("bad message json: {}", e)))?;
    Ok(Some(msg))
}

/// Write one framed message.
pub async fn write_message<T, W>(stream: &mut W, msg: &T) -> Result<()>
where
    T: Serialize,
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(msg)
        .map_err(|e| CoreError::Other(format!("json encode failed: {}", e)))?;
    // Length prefix + payload in a single buffer so each frame is one write syscall.
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).await?;
    stream.flush().await?;
    Ok(())
}

/// Enable aggressive TCP keepalive on a rendezvous connection.
///
/// A hosting client is otherwise completely silent on the connection, so a
/// silently dropped connection (NAT timeout, port-forwarder idle drop) would
/// go unnoticed on both ends for the OS defaults (typically hours). With
/// these settings a dead peer is detected within about a minute.
pub fn set_tcp_keepalive<S>(stream: &S) -> Result<()>
where
    for<'a> socket2::SockRef<'a>: From<&'a S>,
{
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(30))
        .with_interval(std::time::Duration::from_secs(10));
    #[cfg(not(target_os = "windows"))]
    let keepalive = keepalive.with_retries(3);
    socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive)?;
    Ok(())
}
