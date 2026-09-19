use thiserror::Error;

/// Unified core error type.
#[derive(Debug, Error)]
pub enum CoreError {
    /// NetherNet transport error.
    #[error("nethernet error: {0}")]
    Nethernet(#[from] nethernet_tokio::NetherError),

    /// Platform probing failed.
    #[error("platform error: {0}")]
    Platform(String),

    /// I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// A received frame exceeded the maximum accepted size; the peer is most
    /// likely not speaking this protocol (port scan, health check, browser
    /// hitting a forwarded port) or the stream is desynchronized.
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),

    /// Invalid local or remote state.
    #[error("invalid state: {0}")]
    InvalidState(&'static str),

    /// Requested peer/server was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Configuration error.
    #[error("config error: {0}")]
    Config(String),

    /// Catch-all for miscellaneous failures.
    #[error("{0}")]
    Other(String),
}

/// Result type used across the core crate.
pub type Result<T> = std::result::Result<T, CoreError>;
