//! Shared JSON-RPC 2.0 envelopes the stdio + HTTP runtimes both
//! marshal against. Keeps the two runtimes symmetric.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Inbound JSON-RPC envelope. `id` is `None` for notifications;
/// `params` is whatever the host sent, opaque to the framing layer.
#[derive(Debug, Deserialize)]
pub(crate) struct Request {
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

/// Outbound JSON-RPC response envelope. Either `result` is set (ok)
/// or `error` is set (failure) — never both, never neither.
#[derive(Debug, Serialize)]
pub(crate) struct Response {
    pub jsonrpc: &'static str,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorObject>,
}

/// Outbound JSON-RPC server-initiated notification. Streaming
/// frames + channel data flow this way (no `id`, no expected reply).
/// `Clone` so the HTTP runtime can fan-out via `tokio::sync::broadcast`
/// (each subscriber receives its own clone of the envelope).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Notification {
    pub jsonrpc: &'static str,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ErrorObject {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl Response {
    pub(crate) fn ok(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub(crate) fn err(id: Value, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(ErrorObject {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }
}
