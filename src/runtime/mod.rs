//! Runtimes that drive a [`crate::BowirePlugin`] over a transport.
//!
//! - [`stdio`] is the default — JSON-RPC 2.0 over NDJSON on
//!   stdin/stdout, no extra deps. The Bowire host spawns the sidecar
//!   binary and pipes both wires.
//! - [`http`] (feature `http`) is the streamable-HTTP variant —
//!   `POST /` lands JSON-RPC requests, `GET /` is a long-lived SSE
//!   stream the runtime pushes server notifications onto. Fits
//!   hosted / multi-tenant deployments where one sidecar serves
//!   many workbenches.
//!
//! Both runtimes route every inbound envelope through the shared
//! [`dispatch`] helper, so the contract semantics live in one place
//! regardless of how the wire is framed.

pub mod stdio;

#[cfg(feature = "http")]
pub mod http;

mod dispatch;
mod jsonrpc;
