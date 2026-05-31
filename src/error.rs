//! Error type the SDK surfaces. Most plugins never construct these
//! directly — they're produced by the runtime when something goes
//! wrong on the wire.

use thiserror::Error;

/// Anything the SDK can fail with on the wire side. Plugin code
/// returns plain values; runtime + serialisation errors land here.
#[derive(Debug, Error)]
pub enum Error {
    /// I/O failed on stdin/stdout. Usually means the host died.
    #[error("stdio i/o failed: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialisation / deserialisation failed for one envelope.
    /// The runtime keeps reading; the offending line is logged and
    /// dropped.
    #[error("json decode/encode failed: {0}")]
    Json(#[from] serde_json::Error),

    /// The host sent an envelope that doesn't match the JSON-RPC 2.0
    /// shape. The runtime replies with a parse-error response and
    /// keeps reading.
    #[error("malformed json-rpc envelope: {0}")]
    BadEnvelope(String),

    /// The host called a method this plugin doesn't implement —
    /// usually `invoke_stream` or `open_channel` left at default.
    #[error("method not implemented: {0}")]
    NotImplemented(&'static str),
}

/// Convenient alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
