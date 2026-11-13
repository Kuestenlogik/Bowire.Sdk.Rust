//! The plugin trait users implement. Only [`BowirePlugin::id`],
//! [`BowirePlugin::name`], [`BowirePlugin::discover`] and
//! [`BowirePlugin::invoke`] are mandatory — the stream / channel /
//! settings / shutdown hooks ship with defaults: no server streams, no
//! channels, no settings, nothing to release. Each call arrives as one request value
//! ([`DiscoverRequest`], [`InvokeRequest`], [`crate::ChannelRequest`]) that
//! carries the values behind the declared settings as [`Settings`].

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::channel::ChannelHandler;
use crate::manual_entry::ManualEntryForm;
use crate::models::{InvokeResult, PluginSetting, ServiceInfo};
use crate::settings::Settings;
use crate::stream::StreamHandler;

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

    /// The server-stream handler, when this plugin serves server-streaming
    /// methods. Default `None`: no streams, and the handshake says so
    /// (`capabilities.invokeStream: false`), so the host does not offer
    /// them.
    ///
    /// Implement [`StreamHandler`] and return `Some(self)` to serve them;
    /// the runtime then routes `invokeStream` and reports
    /// `invokeStream: true`.
    fn streams(&self) -> Option<&dyn StreamHandler> {
        None
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
    /// trait methods, so an implementation exists by definition).
    /// `invoke_stream` follows [`BowirePlugin::streams`] and `channels`
    /// follows [`BowirePlugin::channels`]: yes exactly when a handler is
    /// there.
    ///
    /// Override it when your plugin knows better. `invoke_stream` or
    /// `channels` set to `true` without a handler is reported as `false`:
    /// there would be nothing to route the call to.
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            invoke_stream: self.streams().is_some(),
            channels: self.channels().is_some(),
            ..Capabilities::default()
        }
    }

    /// Called once, when the sidecar itself stops, after every open channel
    /// has closed: release what the plugin holds (connections, files).
    /// Default: nothing to release.
    ///
    /// Over stdio that is the host's `shutdown` request, or stdin closing.
    /// Over http it is the server stopping ([`crate::serve_http`]'s
    /// `shutdown` future completing), never a host's `shutdown` request:
    /// that only ends the host's own session, and the sidecar keeps serving
    /// the others.
    async fn shutdown(&self) {}
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
    /// Can answer `invokeStream` — see [`BowirePlugin::streams`].
    pub invoke_stream: bool,
    /// Can answer `openChannel` — see [`BowirePlugin::channels`].
    pub channels: bool,
}

/// Discover and invoke. Streams and channels need a [`StreamHandler`] or a
/// [`ChannelHandler`], which a bare default cannot know about.
impl Default for Capabilities {
    fn default() -> Self {
        Self {
            discover: true,
            invoke: true,
            invoke_stream: false,
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
