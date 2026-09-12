use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Write};

pub const RAKNET_MAGIC: [u8; 16] = [
    0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe, 0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78,
];

pub const ID_UNCONNECTED_PING: u8 = 0x01;
pub const ID_UNCONNECTED_PONG: u8 = 0x1c;

#[derive(Debug, thiserror::Error)]
pub enum PacketError {
    #[error("Packet is too short: {0} bytes")]
    PacketTooShort(usize),
    #[error("Unexpected packet ID: expected {expected:#04x}, got {got:#04x}")]
    UnexpectedPacketId { expected: u8, got: u8 },
    #[error("Magic bytes mismatch")]
    InvalidMagic,
    #[error("Payload is not valid UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Identifier is too long for the RakNet length field: {0} bytes")]
    IdentifierTooLong(usize),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Builds an `ID_UNCONNECTED_PING` (0x01) packet buffer.
pub fn build_unconnected_ping(timestamp: u64, client_guid: u64) -> Result<Vec<u8>, PacketError> {
    let mut buf = Vec::with_capacity(33);
    buf.write_u8(ID_UNCONNECTED_PING)?;
    buf.write_u64::<BigEndian>(timestamp)?;
    buf.write_all(&RAKNET_MAGIC)?;
    buf.write_u64::<BigEndian>(client_guid)?;
    Ok(buf)
}

/// Parses an `ID_UNCONNECTED_PING` (0x01) packet buffer, returning (timestamp, client_guid).
pub fn parse_unconnected_ping(data: &[u8]) -> Result<(u64, u64), PacketError> {
    if data.len() < 33 {
        return Err(PacketError::PacketTooShort(data.len()));
    }

    let mut cursor = Cursor::new(data);
    let id = cursor.read_u8()?;
    if id != ID_UNCONNECTED_PING {
        return Err(PacketError::UnexpectedPacketId {
            expected: ID_UNCONNECTED_PING,
            got: id,
        });
    }

    let timestamp = cursor.read_u64::<BigEndian>()?;
    let mut magic = [0u8; 16];
    cursor.read_exact(&mut magic)?;
    if magic != RAKNET_MAGIC {
        return Err(PacketError::InvalidMagic);
    }

    let client_guid = cursor.read_u64::<BigEndian>()?;
    Ok((timestamp, client_guid))
}

/// Builds an `ID_UNCONNECTED_PONG` (0x1C) packet buffer with the specified Bedrock identifier string.
pub fn build_unconnected_pong(
    timestamp: u64,
    server_guid: u64,
    identifier_str: &str,
) -> Result<Vec<u8>, PacketError> {
    let msg_bytes = identifier_str.as_bytes();
    if msg_bytes.len() > u16::MAX as usize {
        return Err(PacketError::IdentifierTooLong(msg_bytes.len()));
    }
    let mut buf = Vec::with_capacity(35 + msg_bytes.len());
    buf.write_u8(ID_UNCONNECTED_PONG)?;
    buf.write_u64::<BigEndian>(timestamp)?;
    buf.write_u64::<BigEndian>(server_guid)?;
    buf.write_all(&RAKNET_MAGIC)?;
    buf.write_u16::<BigEndian>(msg_bytes.len() as u16)?;
    buf.write_all(msg_bytes)?;
    Ok(buf)
}

/// Parses an `ID_UNCONNECTED_PONG` (0x1C) packet buffer, returning (timestamp, server_guid, identifier_string).
pub fn parse_unconnected_pong(data: &[u8]) -> Result<(u64, u64, String), PacketError> {
    if data.len() < 35 {
        return Err(PacketError::PacketTooShort(data.len()));
    }

    let mut cursor = Cursor::new(data);
    let id = cursor.read_u8()?;
    if id != ID_UNCONNECTED_PONG {
        return Err(PacketError::UnexpectedPacketId {
            expected: ID_UNCONNECTED_PONG,
            got: id,
        });
    }

    let timestamp = cursor.read_u64::<BigEndian>()?;
    let server_guid = cursor.read_u64::<BigEndian>()?;

    let mut magic = [0u8; 16];
    cursor.read_exact(&mut magic)?;
    if magic != RAKNET_MAGIC {
        return Err(PacketError::InvalidMagic);
    }

    let len = cursor.read_u16::<BigEndian>()? as usize;
    let mut msg_buf = vec![0u8; len];
    cursor.read_exact(&mut msg_buf)?;

    let message = String::from_utf8(msg_buf)?;
    Ok((timestamp, server_guid, message))
}

pub fn is_unconnected_ping(data: &[u8]) -> bool {
    data.first().copied() == Some(ID_UNCONNECTED_PING)
}

pub fn is_unconnected_pong(data: &[u8]) -> bool {
    data.first().copied() == Some(ID_UNCONNECTED_PONG)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_pong_roundtrip() {
        let ts = 123456789;
        let guid = 987654321;
        let ident =
            "MCPE;Survival Server;776;1.21.60;1;8;987654321;Mineshaft;Survival;1;19132;19133;";

        let pong = build_unconnected_pong(ts, guid, ident).unwrap();
        assert!(is_unconnected_pong(&pong));

        let (parsed_ts, parsed_guid, parsed_ident) = parse_unconnected_pong(&pong).unwrap();
        assert_eq!(parsed_ts, ts);
        assert_eq!(parsed_guid, guid);
        assert_eq!(parsed_ident, ident);

        let ping = build_unconnected_ping(ts, guid).unwrap();
        assert!(is_unconnected_ping(&ping));
        let (ping_ts, ping_guid) = parse_unconnected_ping(&ping).unwrap();
        assert_eq!(ping_ts, ts);
        assert_eq!(ping_guid, guid);
    }

    #[test]
    fn test_oversized_pong_identifier_is_rejected() {
        let identifier = "x".repeat(usize::from(u16::MAX) + 1);
        assert!(matches!(
            build_unconnected_pong(0, 0, &identifier),
            Err(PacketError::IdentifierTooLong(_))
        ));
    }
}
