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
//! use bowire_plugin::{
//!     run, BowirePlugin, DiscoverRequest, InvokeRequest, InvokeResult, MethodInfo, ServiceInfo,
//! };
//!
//! struct Echo;
//!
//! #[async_trait::async_trait]
//! impl BowirePlugin for Echo {
//!     fn id(&self) -> &str { "echo" }
//!     fn name(&self) -> &str { "Echo" }
//!
//!     async fn discover(&self, _request: DiscoverRequest) -> Vec<ServiceInfo> {
//!         vec![ServiceInfo::new("DemoService").with_methods([
//!             MethodInfo::unary("Echo"),
//!         ])]
//!     }
//!
//!     async fn invoke(&self, request: InvokeRequest) -> InvokeResult {
//!         let payload = request.json_messages.first().cloned().unwrap_or_else(|| "{}".into());
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
//!
//! ## Duplex channels
//!
//! Implement [`ChannelHandler`] and return `Some(self)` from
//! [`BowirePlugin::channels`]; the handshake then reports
//! `capabilities.channels: true` and the runtime routes `openChannel` /
//! `channel.send` / `channel.close` to the [`Channel`] it hands you, keyed
//! by the host's `channelId`. The echo example serves one.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod channel;
mod error;
mod manual_entry;
mod models;
mod plugin;
mod runtime;
mod settings;

pub use channel::{Channel, ChannelHandler, ChannelReceiver, ChannelRequest, ChannelSender};
pub use error::{Error, Result};
pub use manual_entry::{manual_entry_problems, ManualCallKind, ManualEntryForm, ManualField};
pub use models::{
    FieldInfo, InvokeResult, MessageInfo, MethodInfo, MethodType, PluginSetting, ServiceInfo,
    SettingOption,
};
pub use plugin::{BowirePlugin, Capabilities, DiscoverRequest, InvokeRequest};
pub use settings::Settings;

/// The sidecar wire-contract version this SDK speaks (#416). The host
/// accepts a sidecar inside its supported range and refuses one outside it
/// at the handshake, rather than failing at the first call.
pub const SIDECAR_PROTOCOL_VERSION: u32 = 1;
pub use runtime::stdio::run;

#[cfg(feature = "http")]
pub use runtime::http::{run_http, serve_http, shutdown_signal};
