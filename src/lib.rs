//! # bowire-plugin
//!
//! Rust SDK for writing [Bowire](https://bowire.io) protocol plugins as
//! polyglot sidecars. Implement the [`BowirePlugin`] trait, hand the
//! instance to [`run`] (stdio) or — with the `http` feature —
//! [`run_http`], and the SDK speaks the JSON-RPC contract the Bowire
//! host expects.
//!
//! ## Quickstart
//!
//! ```no_run
//! use bowire_plugin::{run, BowirePlugin, InvokeResult, MethodInfo, ServiceInfo};
//!
//! struct Echo;
//!
//! #[async_trait::async_trait]
//! impl BowirePlugin for Echo {
//!     fn id(&self) -> &str { "echo" }
//!     fn name(&self) -> &str { "Echo" }
//!
//!     async fn discover(&self, _server_url: &str, _show_internal: bool)
//!         -> Vec<ServiceInfo>
//!     {
//!         vec![ServiceInfo::new("DemoService").with_methods([
//!             MethodInfo::unary("Echo"),
//!         ])]
//!     }
//!
//!     async fn invoke(
//!         &self, _server_url: &str, _service: &str, _method: &str,
//!         json_messages: Vec<String>, _show_internal: bool,
//!         _metadata: std::collections::HashMap<String, String>,
//!     ) -> InvokeResult
//!     {
//!         let payload = json_messages.first().cloned().unwrap_or_else(|| "{}".into());
//!         InvokeResult::ok(format!("{{\"echoed\":{}}}", payload))
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     std::process::exit(run(Echo).await);
//! }
//! ```
//!
//! Ship it as a sidecar by zipping the binary + a `sidecar.json` and
//! installing with `bowire plugin install --file <zip>`.
//!
//! ## Transports
//!
//! - **stdio** (default) — `run()` reads NDJSON JSON-RPC envelopes off
//!   stdin and writes responses to stdout. Cheap, no extra deps. The
//!   host spawns this as a subprocess.
//! - **HTTP/SSE** (`features = ["http"]`) — `run_http()` boots an axum
//!   server that POSTs receive JSON-RPC requests on a configurable path
//!   and a long-lived SSE GET drains server-initiated notifications.
//!   Fits hosted / multi-tenant deployments where one sidecar serves
//!   many Bowire workbenches.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod error;
mod models;
mod plugin;
mod runtime;

pub use error::{Error, Result};
pub use models::{
    FieldInfo, InvokeResult, MessageInfo, MethodInfo, MethodType, PluginSetting, ServiceInfo,
};
pub use plugin::BowirePlugin;
pub use runtime::stdio::run;
