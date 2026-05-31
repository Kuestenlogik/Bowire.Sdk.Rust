//! Runtimes that drive a [`crate::BowirePlugin`] over a transport.
//!
//! - [`stdio`] is the default — JSON-RPC 2.0 over NDJSON on
//!   stdin/stdout, no extra deps. The Bowire host spawns the sidecar
//!   binary and pipes both wires.
//! - A streamable-HTTP variant (POST + long-lived SSE GET) lands in a
//!   follow-up release behind a feature flag; the contract surface is
//!   the same.

pub mod stdio;

mod jsonrpc;
