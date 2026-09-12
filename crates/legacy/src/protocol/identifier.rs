use std::fmt;

/// Represents a Minecraft Bedrock Server Advertisement String
/// Format: `MCPE;ServerName;Protocol;Version;Players;MaxPlayers;GUID;SubName;GameMode;GameModeNumeric;PortIPv4;PortIPv6;`
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BedrockIdentifier {
    pub edition: String,
    pub server_name: String,
    pub protocol_version: u32,
    pub version_name: String,
    pub player_count: u32,
    pub max_player_count: u32,
    pub server_guid: u64,
    pub sub_name: String,
    pub game_mode: String,
    pub game_mode_numeric: u32,
    pub port_ipv4: u16,
    pub port_ipv6: u16,
    pub extra: Vec<String>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IdentifierError {
    #[error("Identifier string has too few components: {0}")]
    TooFewParts(usize),
    #[error("Failed to parse integer field '{field}': {value}")]
    ParseIntError { field: &'static str, value: String },
}

impl BedrockIdentifier {
    pub fn parse(s: &str) -> Result<Self, IdentifierError> {
        let parts: Vec<&str> = s.split_terminator(';').collect();
        if parts.len() < 7 {
            return Err(IdentifierError::TooFewParts(parts.len()));
        }

        let edition = parts[0].to_string();
        let server_name = parts[1].to_string();
        let protocol_version = parts[2]
            .parse()
            .map_err(|_| IdentifierError::ParseIntError {
                field: "protocol_version",
                value: parts[2].to_string(),
            })?;
        let version_name = parts[3].to_string();
        let player_count = parts[4]
            .parse()
            .map_err(|_| IdentifierError::ParseIntError {
                field: "player_count",
                value: parts[4].to_string(),
            })?;
        let max_player_count = parts[5]
            .parse()
            .map_err(|_| IdentifierError::ParseIntError {
                field: "max_player_count",
                value: parts[5].to_string(),
            })?;
        let server_guid = parts[6]
            .parse()
            .map_err(|_| IdentifierError::ParseIntError {
                field: "server_guid",
                value: parts[6].to_string(),
            })?;

        let sub_name = parts.get(7).copied().unwrap_or("").to_string();
        let game_mode = parts.get(8).copied().unwrap_or("Survival").to_string();
        let game_mode_numeric = parts.get(9).map_or(Ok(1), |value| {
            value.parse().map_err(|_| IdentifierError::ParseIntError {
                field: "game_mode_numeric",
                value: (*value).to_string(),
            })
        })?;
        let port_ipv4 = parts.get(10).map_or(Ok(19132), |value| {
            value.parse().map_err(|_| IdentifierError::ParseIntError {
                field: "port_ipv4",
                value: (*value).to_string(),
            })
        })?;
        let port_ipv6 = parts.get(11).map_or(Ok(19133), |value| {
            value.parse().map_err(|_| IdentifierError::ParseIntError {
                field: "port_ipv6",
                value: (*value).to_string(),
            })
        })?;

        let extra = if parts.len() > 12 {
            parts[12..].iter().map(|s| s.to_string()).collect()
        } else {
            Vec::new()
        };

        Ok(Self {
            edition,
            server_name,
            protocol_version,
            version_name,
            player_count,
            max_player_count,
            server_guid,
            sub_name,
            game_mode,
            game_mode_numeric,
            port_ipv4,
            port_ipv6,
            extra,
        })
    }

    /// Rewrites the advertised IPv4 (and optionally IPv6) port to the local ephemeral port.
    pub fn with_ephemeral_port(mut self, port: u16) -> Self {
        self.port_ipv4 = port;
        self.port_ipv6 = port;
        self
    }

    /// Sets the RakNet server GUID.
    pub fn with_guid(mut self, guid: u64) -> Self {
        self.server_guid = guid;
        self
    }

    /// Serializes the identifier to standard MCPE format string.
    pub fn serialize_to_string(&self) -> String {
        let mut s = format!(
            "{};{};{};{};{};{};{};{};{};{};{};{};",
            self.edition,
            self.server_name,
            self.protocol_version,
            self.version_name,
            self.player_count,
            self.max_player_count,
            self.server_guid,
            self.sub_name,
            self.game_mode,
            self.game_mode_numeric,
            self.port_ipv4,
            self.port_ipv6,
        );
        for extra in &self.extra {
            s.push_str(extra);
            s.push(';');
        }
        s
    }
}

impl fmt::Display for BedrockIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.serialize_to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_and_serialize() {
        let raw = "MCPE;Mehmet's World;776;1.21.60;1;10;123456789;Custom Subtitle;Survival;1;19132;19133;";
        let parsed = BedrockIdentifier::parse(raw).unwrap();
        assert_eq!(parsed.server_name, "Mehmet's World");
        assert_eq!(parsed.protocol_version, 776);
        assert_eq!(parsed.version_name, "1.21.60");
        assert_eq!(parsed.server_guid, 123456789);
        assert_eq!(parsed.port_ipv4, 19132);
        assert_eq!(parsed.serialize_to_string(), raw);

        // Rewrite ephemeral port:
        let rewritten = parsed.with_ephemeral_port(51235);
        assert_eq!(rewritten.port_ipv4, 51235);
        assert_eq!(rewritten.port_ipv6, 51235);
        assert!(rewritten.serialize_to_string().contains(";51235;51235;"));
    }

    #[test]
    fn test_invalid_numeric_fields_are_rejected() {
        let error =
            BedrockIdentifier::parse("MCPE;World;776;1.21;not-a-number;10;123;").unwrap_err();
        assert!(matches!(
            error,
            IdentifierError::ParseIntError {
                field: "player_count",
                ..
            }
        ));
    }
}
