//! The plugin trait users implement. Only [`BowirePlugin::id`],
//! [`BowirePlugin::name`], [`BowirePlugin::discover`] and
//! [`BowirePlugin::invoke`] are mandatory — the streaming / channel /
//! settings hooks ship with sensible defaults that return empty
//! topologies or not-implemented errors.

use std::collections::HashMap;

use async_trait::async_trait;
use futures::stream::{self, BoxStream};

use crate::models::{InvokeResult, PluginSetting, ServiceInfo};

/// One Bowire protocol-plugin instance. The runtime ([`crate::run`] /
/// [`crate::run_http`]) owns the wire; you own this trait's bodies.
///
/// Implementations are passed by value into the runtime, so a typical
/// pattern is `run(MyPlugin::new(config))`. The trait is `Send + Sync`
/// because the HTTP runtime may dispatch concurrent requests against
/// the same plugin instance.
#[async_trait]
pub trait BowirePlugin: Send + Sync + 'static {
    /// Short identifier the host uses for routing (e.g. `"mqtt"`,
    /// `"zenoh"`). Must match the `protocol.id` in `sidecar.json`.
    fn id(&self) -> &str;

    /// Human-readable name shown in the workbench's protocol picker.
    fn name(&self) -> &str;

    /// Raw SVG markup for the protocol-picker icon. Optional — the
    /// default returns an empty `<svg/>`, which renders as nothing
    /// (the host falls back to a placeholder).
    fn icon_svg(&self) -> &str {
        "<svg/>"
    }

    /// Walk the topology of services the workbench should render in
    /// the sidebar. Called once per `(server_url, show_internal)`
    /// combination; results are cached by the workbench.
    async fn discover(&self, server_url: &str, show_internal: bool) -> Vec<ServiceInfo>;

    /// Dispatch a unary or client-streaming call. `json_messages`
    /// carries every request body the workbench captured (one entry
    /// for unary, multiple for client-streaming). Return the result
    /// the workbench should render.
    async fn invoke(
        &self,
        server_url: &str,
        service: &str,
        method: &str,
        json_messages: Vec<String>,
        show_internal: bool,
        metadata: HashMap<String, String>,
    ) -> InvokeResult;

    /// Dispatch a server-streaming or duplex call. Default emits a
    /// single not-implemented frame so the workbench fails fast
    /// instead of hanging.
    async fn invoke_stream(
        &self,
        _server_url: &str,
        _service: &str,
        _method: &str,
        _json_messages: Vec<String>,
        _show_internal: bool,
        _metadata: HashMap<String, String>,
    ) -> BoxStream<'static, String> {
        let msg = format!(
            r#"{{"status":"Error","response":"invoke_stream not implemented by plugin '{}'"}}"#,
            self.id()
        );
        Box::pin(stream::iter([msg]))
    }

    /// Open a duplex channel (WebSocket-style). Default returns
    /// `None`, which the host renders as "this plugin doesn't
    /// support duplex methods."
    async fn open_channel(
        &self,
        _server_url: &str,
        _service: &str,
        _method: &str,
        _show_internal: bool,
        _metadata: HashMap<String, String>,
    ) -> Option<()> {
        None
    }

    /// Per-plugin settings the workbench renders in the plugin
    /// settings dialog. Default: no settings.
    fn settings(&self) -> Vec<PluginSetting> {
        Vec::new()
    }
}
