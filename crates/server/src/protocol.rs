use std::fmt;

/// Mineshaft 8-byte session routing header:
/// [ 32-bit Destination SessionId, 32-bit Sender SessionId ] (Big Endian)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MineshaftHeader {
    pub target_session_id: u32,
    pub sender_session_id: u32,
}

pub const MINESHAFT_HEADER_LEN: usize = 8;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HeaderError {
    #[error("Packet too short for Mineshaft header (expected at least 8 bytes, got {0})")]
    PacketTooShort(usize),
}

impl MineshaftHeader {
    pub const fn new(target_session_id: u32, sender_session_id: u32) -> Self {
        Self {
            target_session_id,
            sender_session_id,
        }
    }

    /// Creates a handshake/keepalive header where target == sender == session_id.
    pub const fn handshake(session_id: u32) -> Self {
        Self {
            target_session_id: session_id,
            sender_session_id: session_id,
        }
    }

    pub fn is_handshake(&self) -> bool {
        self.target_session_id == self.sender_session_id
    }

    /// Encodes header into an 8-byte array.
    pub fn to_bytes(&self) -> [u8; MINESHAFT_HEADER_LEN] {
        let mut buf = [0u8; MINESHAFT_HEADER_LEN];
        buf[0..4].copy_from_slice(&self.target_session_id.to_be_bytes());
        buf[4..8].copy_from_slice(&self.sender_session_id.to_be_bytes());
        buf
    }

    /// Decodes header from the first 8 bytes of a buffer.
    pub fn decode(buf: &[u8]) -> Result<(Self, &[u8]), HeaderError> {
        if buf.len() < MINESHAFT_HEADER_LEN {
            return Err(HeaderError::PacketTooShort(buf.len()));
        }

        let target = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        let sender = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        let payload = &buf[MINESHAFT_HEADER_LEN..];

        Ok((
            Self {
                target_session_id: target,
                sender_session_id: sender,
            },
            payload,
        ))
    }

    /// Prepends the Mineshaft header to the payload.
    pub fn encapsulate(&self, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(MINESHAFT_HEADER_LEN + payload.len());
        buf.extend_from_slice(&self.to_bytes());
        buf.extend_from_slice(payload);
        buf
    }
}

impl fmt::Display for MineshaftHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MineshaftHeader(target={}, sender={})",
            self.target_session_id, self.sender_session_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let header = MineshaftHeader::new(1001, 2002);
        let _bytes = header.to_bytes();
        let payload = b"minecraft raknet payload";
        let packet = header.encapsulate(payload);

        let (decoded, remaining) = MineshaftHeader::decode(&packet).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(remaining, payload);
    }
}
