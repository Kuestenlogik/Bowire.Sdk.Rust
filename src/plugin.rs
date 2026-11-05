//! The plugin trait users implement. Only [`BowirePlugin::id`],
//! [`BowirePlugin::name`], [`BowirePlugin::discover`] and
//! [`BowirePlugin::invoke`] are mandatory — the streaming / channel /
//! settings hooks ship with defaults: a not-implemented stream frame, no
//! channels, no settings. Each call arrives as one request value
//! ([`DiscoverRequest`], [`InvokeRequest`], [`crate::ChannelRequest`]) that
//! carries the values behind the declared settings as [`Settings`].

use std::collections::HashMap;

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use serde::{Deserialize, Serialize};

use crate::channel::ChannelHandler;
use crate::manual_entry::ManualEntryForm;
use crate::models::{InvokeResult, PluginSetting, ServiceInfo};
use crate::settings::Settings;

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
    async fn discover(&self, request: DiscoverRequest) -> Vec<ServiceInfo>;

    /// Dispatch a unary or client-streaming call. `json_messages`
    /// carries every request body the workbench captured (one entry
    /// for unary, multiple for client-streaming). Return the result
    /// the workbench should render.
    async fn invoke(&self, request: InvokeRequest) -> InvokeResult;

    /// Dispatch a server-streaming or duplex call. Default emits a
    /// single not-implemented frame so the workbench fails fast
    /// instead of hanging.
    async fn invoke_stream(&self, _request: InvokeRequest) -> BoxStream<'static, String> {
        let msg = format!(
            r#"{{"status":"Error","response":"invoke_stream not implemented by plugin '{}'"}}"#,
            self.id()
        );
        Box::pin(stream::iter([msg]))
    }

    /// The duplex-channel handler, when this plugin serves duplex methods.
    /// Default `None`: no channels, and the handshake says so
    /// (`capabilities.channels: false`), so the host never sends
    /// `openChannel`.
    ///
    /// Implement [`ChannelHandler`] and return `Some(self)` to serve them;
    /// the runtime then routes `openChannel` / `channel.send` /
    /// `channel.close` and reports `channels: true`. See [`crate::Channel`].
    fn channels(&self) -> Option<&dyn ChannelHandler> {
        None
    }

    /// The settings this plugin declares (Kuestenlogik/Bowire#693): sent in
    /// the `initialize` reply and rendered in the workbench's Settings dialog.
    /// The values come back with every call as the `settings` argument, and
    /// the defaults declared here are what its accessors return for a key
    /// nobody set. Asked again per call, so a plugin may compute them.
    /// Default: no settings.
    fn settings(&self) -> Vec<PluginSetting> {
        Vec::new()
    }

    /// How a call to this plugin is entered by hand, without discovery
    /// (Kuestenlogik/Bowire#1003): sent in the `initialize` reply as
    /// `manualEntry`, and rendered by the workbench as a Compose form. A form
    /// the host would refuse fails the handshake, naming why
    /// ([`crate::manual_entry_problems`]). Default: none.
    fn manual_entry(&self) -> Option<ManualEntryForm> {
        None
    }

    /// What this plugin can actually answer, reported in the
    /// `initialize` handshake (#416). The host skips a call whose flag
    /// is `false` — `channels: false` short-circuits `openChannel`
    /// without a round-trip.
    ///
    /// The default says yes to discover and invoke (both are required
    /// trait methods, so an implementation exists by definition) and to
    /// `invoke_stream`, whose default emits one not-implemented frame
    /// the workbench renders as an error. `channels` follows
    /// [`BowirePlugin::channels`]: yes exactly when a handler is there.
    ///
    /// Override it when your plugin knows better — a plugin that does
    /// not stream should say `invoke_stream: false` rather than hand the
    /// operator an error frame. `channels: true` without a handler is
    /// reported as `false`: there would be nothing to route the call to.
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            channels: self.channels().is_some(),
            ..Capabilities::default()
        }
    }
}

/// Capability flags a sidecar advertises in its `initialize` reply
/// (#416). Every flag defaults to `true` on the host when the object is
/// absent, so an omitted capability is read as "can do" — which is why
/// every flag is always reported explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    /// Can answer `discover`.
    pub discover: bool,
    /// Can answer `invoke`.
    pub invoke: bool,
    /// Can answer `invokeStream`.
    pub invoke_stream: bool,
    /// Can answer `openChannel` — see [`BowirePlugin::channels`].
    pub channels: bool,
}

/// Everything but channels: those need a [`ChannelHandler`], which a
/// bare default cannot know about.
impl Default for Capabilities {
    fn default() -> Self {
        Self {
            discover: true,
            invoke: true,
            invoke_stream: true,
            channels: false,
        }
    }
}

/// What the host asked `discover` for. Mirrors the `discover` params.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DiscoverRequest {
    /// The target server the workbench points at.
    pub server_url: String,
    /// Whether the workbench shows internal services.
    pub show_internal: bool,
    /// The values behind the declared settings, as the host sent them with
    /// this call (Kuestenlogik/Bowire#693).
    pub settings: Settings,
}

impl DiscoverRequest {
    /// A request for `server_url`, for driving a plugin from a test.
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            server_url: server_url.into(),
            ..Self::default()
        }
    }

    /// With the given setting values.
    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }
}

/// What the host asked `invoke` or `invokeStream` for. Mirrors their params.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct InvokeRequest {
    /// The target server the workbench points at.
    pub server_url: String,
    /// Service name, as discovered.
    pub service: String,
    /// Method name, as discovered.
    pub method: String,
    /// Every request body the workbench captured: one for a unary call.
    pub json_messages: Vec<String>,
    /// Whether the workbench shows internal services.
    pub show_internal: bool,
    /// Request metadata (headers) the operator set.
    pub metadata: HashMap<String, String>,
    /// The values behind the declared settings, as the host sent them with
    /// this call (Kuestenlogik/Bowire#693).
    pub settings: Settings,
}

impl InvokeRequest {
    /// A call to `service` / `method`, for driving a plugin from a test.
    pub fn new(service: impl Into<String>, method: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            method: method.into(),
            ..Self::default()
        }
    }

    /// With these request bodies.
    pub fn with_messages(mut self, json_messages: impl IntoIterator<Item = String>) -> Self {
        self.json_messages = json_messages.into_iter().collect();
        self
    }

    /// With these metadata entries.
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// With the given setting values.
    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }
}
