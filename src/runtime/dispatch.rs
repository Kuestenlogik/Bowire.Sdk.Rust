//! Transport-agnostic JSON-RPC method dispatcher. Both the stdio
//! and HTTP runtimes funnel inbound requests through
//! [`dispatch`]; the runtime owns the wire (NDJSON to stdout vs.
//! SSE-queue), the dispatcher owns the contract semantics
//! (which method maps to which trait call + how server-streaming
//! is decomposed into an ack + pump, and which channel a
//! `channel.send` / `channel.close` belongs to).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use futures::stream::BoxStream;
use serde_json::{json, Value};
use tokio::sync::watch;

use crate::channel::{ChannelRefusal, ChannelRegistry, ChannelRequest, Notify};
use crate::plugin::{BowirePlugin, DiscoverRequest, InvokeRequest};
use crate::runtime::jsonrpc::{Request, Response};
use crate::settings::{settings_problems, Settings};

/// JSON-RPC error for a `channel.send` / `channel.close` on a channel that
/// is gone (or never was). The host reads any error here as
/// `SendAsync` → `false`; the code just tells a log reader which case.
pub(crate) const CHANNEL_CLOSED: i32 = -32001;

/// What one host holds on this sidecar: its open channels, its running
/// streams, and the queue its notifications go out on. Over stdio there is
/// one, for the pipe; over http one per `Bowire-Session` id.
pub(crate) struct Session {
    pub(crate) channels: ChannelRegistry,
    pub(crate) notify: Notify,
    /// Counts the times this session ended. A stream pump watches it and
    /// stops at the next change.
    ends: watch::Sender<u64>,
}

impl Session {
    pub(crate) fn new(notify: Notify) -> Self {
        Self {
            channels: ChannelRegistry::default(),
            notify,
            ends: watch::Sender::new(0),
        }
    }

    /// The host is gone: stop its streams and close its channels. What is
    /// opened afterwards is not affected, and a second call finds nothing
    /// left to end.
    pub(crate) fn end(&self) {
        self.ends.send_modify(|ends| *ends += 1);
        self.channels.close_all(&self.notify);
    }

    /// Completes when the session ends next, counted from this call. Take it
    /// before starting the work it should stop, so an end in between is not
    /// missed.
    pub(crate) fn ended(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut ends = self.ends.subscribe();
        async move {
            // An error means the session itself is gone, which ends it too.
            let _ = ends.changed().await;
        }
    }
}

/// Whether `method` starts something its session then owns. Every other
/// method only reads what is already there.
#[cfg(feature = "http")]
pub(crate) fn starts_owned_work(method: &str) -> bool {
    matches!(method, "invokeStream" | "openChannel")
}

/// One method-dispatch outcome the runtime then has to act on.
pub(crate) enum DispatchResult {
    /// Single response envelope to write back over the wire. Covers
    /// `initialize` / `ping` / `discover` / `invoke` / any error.
    Reply(Response),

    /// Server-streaming: write the ack response first, then run the
    /// pump and emit one `$/stream/data` notification per yielded
    /// frame, then a `$/stream/end` notification when the stream
    /// completes.
    Stream {
        ack: Response,
        stream_id: String,
        stream: BoxStream<'static, String>,
    },

    /// The host sent `shutdown`. What that ends is the transport's to
    /// decide: the process over stdio, the caller's session over http. The
    /// reply goes out once that is done.
    Shutdown(Response),
}

/// Dispatch a single JSON-RPC request against `plugin`. No IO, no
/// spawning, no stdout writes: the runtime handles every transport
/// effect. Channel frames leave through `session.notify`, which the
/// runtime backs with its own ordered queue.
pub(crate) async fn dispatch<P: BowirePlugin>(
    plugin: Arc<P>,
    req: Request,
    session: &Session,
) -> DispatchResult {
    let id = req.id.unwrap_or(Value::Null);
    let params = req.params.unwrap_or_else(|| json!({}));

    match req.method.as_str() {
        "shutdown" => DispatchResult::Shutdown(Response::ok(id, json!({}))),
        "initialize" => {
            // Kuestenlogik/Bowire#693: the declarations the Settings dialog
            // renders. A select without options would render with nothing to
            // pick, so it fails the handshake with the reason instead.
            let declared = plugin.settings();
            let problems = settings_problems(&declared);
            if !problems.is_empty() {
                return DispatchResult::Reply(Response::err(
                    id,
                    -32603,
                    format!("settings: {}", problems.join("; ")),
                ));
            }
            // Kuestenlogik/Bowire#1003: a form the host would refuse fails the
            // handshake with the reason, rather than silently never appearing.
            let manual_entry = plugin.manual_entry();
            if let Some(form) = &manual_entry {
                let problems = crate::manual_entry::manual_entry_problems(form);
                if !problems.is_empty() {
                    return DispatchResult::Reply(Response::err(
                        id,
                        -32603,
                        format!("manualEntry: {}", problems.join("; ")),
                    ));
                }
            }
            let mut reply = json!({
                "id": plugin.id(),
                "name": plugin.name(),
                "iconSvg": plugin.icon_svg(),
                "settings": declared,
                // #416: which contract this sidecar speaks, and what it can
                // answer. A reply without them is tolerated as legacy v1 —
                // with a warning in the host log on every boot, which is what
                // this runtime used to earn.
                "protocolVersion": crate::SIDECAR_PROTOCOL_VERSION,
                "capabilities": capabilities(plugin.as_ref()),
            });
            if let Some(form) = manual_entry {
                reply["manualEntry"] = json!(form);
            }
            DispatchResult::Reply(Response::ok(id, reply))
        }
        // The contract's reply is the bare string. `{}` was this runtime's
        // own invention; a host using ping as a liveness probe reads it as a
        // malformed answer.
        "ping" => DispatchResult::Reply(Response::ok(id, json!("pong"))),
        "discover" => {
            let server_url = string_param(&params, "serverUrl").unwrap_or_default();
            let show_internal = bool_param(&params, "showInternalServices").unwrap_or(false);
            let services = plugin
                .discover(DiscoverRequest {
                    server_url,
                    show_internal,
                    settings: call_settings(plugin.as_ref(), &params),
                })
                .await;
            // A bare array. The host reads anything else as "no services,
            // try the next plugin" and moves on without a word, so the
            // `{"services": …}` envelope meant discovery never returned
            // anything from a Rust sidecar.
            DispatchResult::Reply(match serde_json::to_value(&services) {
                Ok(v) => Response::ok(id, v),
                Err(e) => Response::err(id, -32000, format!("discover serialise failed: {e}")),
            })
        }
        "invoke" => {
            let server_url = string_param(&params, "serverUrl").unwrap_or_default();
            let service = string_param(&params, "service").unwrap_or_default();
            let method = string_param(&params, "method").unwrap_or_default();
            let json_messages = vec_string_param(&params, "jsonMessages").unwrap_or_default();
            let show_internal = bool_param(&params, "showInternalServices").unwrap_or(false);
            let metadata = map_param(&params, "metadata");
            let result = plugin
                .invoke(InvokeRequest {
                    server_url,
                    service,
                    method,
                    json_messages,
                    show_internal,
                    metadata,
                    settings: call_settings(plugin.as_ref(), &params),
                })
                .await;
            DispatchResult::Reply(match serde_json::to_value(&result) {
                Ok(v) => Response::ok(id, v),
                Err(e) => Response::err(id, -32000, format!("invoke serialise failed: {e}")),
            })
        }
        "invokeStream" => {
            let Some(handler) = plugin.streams() else {
                return DispatchResult::Reply(Response::err(
                    id,
                    -32601,
                    format!("plugin '{}' does not serve server streams", plugin.id()),
                ));
            };
            // Server-streaming hand-off: build the ack + grab the
            // stream now (so the plugin's `invoke_stream` future
            // resolves before the runtime starts pumping), but
            // don't drain it here — leave that to the transport.
            let stream_id = string_param(&params, "streamId").unwrap_or_default();
            let server_url = string_param(&params, "serverUrl").unwrap_or_default();
            let service = string_param(&params, "service").unwrap_or_default();
            let method = string_param(&params, "method").unwrap_or_default();
            let json_messages = vec_string_param(&params, "jsonMessages").unwrap_or_default();
            let show_internal = bool_param(&params, "showInternalServices").unwrap_or(false);
            let metadata = map_param(&params, "metadata");

            let stream = handler
                .invoke_stream(InvokeRequest {
                    server_url,
                    service,
                    method,
                    json_messages,
                    show_internal,
                    metadata,
                    settings: call_settings(plugin.as_ref(), &params),
                })
                .await;
            DispatchResult::Stream {
                ack: Response::ok(id, json!({ "streamId": stream_id.clone() })),
                stream_id,
                stream,
            }
        }
        "openChannel" => {
            DispatchResult::Reply(open_channel(plugin.as_ref(), id, &params, session).await)
        }
        "channel.send" => {
            let channel_id = string_param(&params, "channelId").unwrap_or_default();
            // The frame is a string on the wire; a host that sends a JSON
            // value instead still gets it through, as its JSON text.
            let message = match params.get("message") {
                Some(Value::String(s)) => s.clone(),
                Some(other) => other.to_string(),
                None => String::new(),
            };
            DispatchResult::Reply(match session.channels.deliver(&channel_id, message) {
                Ok(()) => Response::ok(id, json!(true)),
                Err(refusal) => refused(id, &channel_id, refusal),
            })
        }
        "channel.close" => {
            let channel_id = string_param(&params, "channelId").unwrap_or_default();
            DispatchResult::Reply(match session.channels.close_send_side(&channel_id) {
                Ok(()) => Response::ok(id, json!({})),
                Err(refusal) => refused(id, &channel_id, refusal),
            })
        }
        other => DispatchResult::Reply(Response::err(
            id,
            -32601,
            format!("method '{other}' not handled by bowire-plugin runtime"),
        )),
    }
}

/// The values the host sent with this call, backed by the declared defaults.
/// The declarations are asked for per call rather than captured at
/// `initialize`: a plugin is free to compute them.
fn call_settings<P: BowirePlugin + ?Sized>(plugin: &P, params: &Value) -> Settings {
    Settings::from_wire(params.get("settings"), &plugin.settings())
}

/// What goes into the handshake: the plugin's own report, except that
/// streams and channels cannot be claimed without a handler to route them to.
fn capabilities<P: BowirePlugin>(plugin: &P) -> crate::plugin::Capabilities {
    let mut caps = plugin.capabilities();
    caps.invoke_stream &= plugin.streams().is_some();
    caps.channels &= plugin.channels().is_some();
    caps
}

/// `openChannel`: register the host's id first — so a frame the plugin
/// pushes, or a `channel.send` racing the ack, already finds the channel —
/// then let the plugin accept or refuse it.
async fn open_channel<P: BowirePlugin>(
    plugin: &P,
    id: Value,
    params: &Value,
    session: &Session,
) -> Response {
    let Some(handler) = plugin.channels() else {
        return Response::err(
            id,
            -32601,
            format!("plugin '{}' does not serve duplex channels", plugin.id()),
        );
    };
    // The host mints the id and subscribes to it before asking. An id of
    // our own would publish frames nobody listens to.
    let Some(channel_id) = string_param(params, "channelId").filter(|s| !s.is_empty()) else {
        return Response::err(id, -32602, "openChannel requires a channelId");
    };
    let request = ChannelRequest {
        server_url: string_param(params, "serverUrl").unwrap_or_default(),
        service: string_param(params, "service").unwrap_or_default(),
        method: string_param(params, "method").unwrap_or_default(),
        show_internal: bool_param(params, "showInternalServices").unwrap_or(false),
        metadata: map_param(params, "metadata"),
        settings: call_settings(plugin, params),
    };
    let channel = match session
        .channels
        .begin_open(&channel_id, session.notify.clone())
    {
        Ok(channel) => channel,
        Err(refusal) => return refused(id, &channel_id, refusal),
    };
    let outcome = handler.open_channel(request, channel).await;
    session
        .channels
        .finish_open(&channel_id, outcome.is_ok(), &session.notify);
    match outcome {
        Ok(()) => Response::ok(id, json!({ "channelId": channel_id })),
        Err(message) => Response::err(id, -32000, message),
    }
}

fn refused(id: Value, channel_id: &str, refusal: ChannelRefusal) -> Response {
    match refusal {
        ChannelRefusal::Unknown => Response::err(
            id,
            CHANNEL_CLOSED,
            format!("no open channel '{channel_id}'"),
        ),
        ChannelRefusal::Closed => Response::err(
            id,
            CHANNEL_CLOSED,
            format!("channel '{channel_id}' is closed"),
        ),
        ChannelRefusal::Duplicate => Response::err(
            id,
            -32602,
            format!("channel '{channel_id}' is already open"),
        ),
    }
}

// ---- params helpers ----------------------------------------------

fn string_param(params: &Value, key: &str) -> Option<String> {
    params.get(key).and_then(|v| v.as_str()).map(str::to_owned)
}

fn bool_param(params: &Value, key: &str) -> Option<bool> {
    params.get(key).and_then(|v| v.as_bool())
}

fn vec_string_param(params: &Value, key: &str) -> Option<Vec<String>> {
    let arr = params.get(key)?.as_array()?;
    Some(
        arr.iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect(),
    )
}

fn map_param(params: &Value, key: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(Value::Object(map)) = params.get(key) {
        for (k, v) in map {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_owned());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{InvokeResult, ServiceInfo};
    use async_trait::async_trait;
    use serde_json::Value;

    struct Fake;

    #[async_trait]
    impl BowirePlugin for Fake {
        fn id(&self) -> &str {
            "fake"
        }

        fn name(&self) -> &str {
            "Fake"
        }

        async fn discover(&self, _request: DiscoverRequest) -> Vec<ServiceInfo> {
            vec![ServiceInfo::new("Echo")]
        }

        async fn invoke(&self, _request: InvokeRequest) -> InvokeResult {
            InvokeResult::default()
        }
    }

    fn request(method: &str) -> Request {
        Request {
            id: Some(json!(1)),
            method: method.to_owned(),
            params: Some(json!({})),
        }
    }

    fn session() -> Session {
        Session::new(Arc::new(|_| {}))
    }

    fn result_of(outcome: DispatchResult) -> Value {
        match outcome {
            DispatchResult::Reply(r) => r.result.expect("a reply carries a result"),
            _ => panic!("expected a plain reply"),
        }
    }

    /// #416: without these the host treats the sidecar as legacy contract v1
    /// — a warning on every boot, and no way to refuse an incompatible
    /// sidecar at the handshake instead of at the first call.
    #[tokio::test]
    async fn initialize_advertises_contract_version_and_capabilities() {
        let result = result_of(dispatch(Arc::new(Fake), request("initialize"), &session()).await);

        assert_eq!(
            result["protocolVersion"],
            json!(crate::SIDECAR_PROTOCOL_VERSION)
        );
        assert_eq!(
            result["capabilities"],
            json!({
                "discover": true,
                "invoke": true,
                "invokeStream": false,
                "channels": false,
            })
        );
    }

    /// #13: a plugin without a stream handler used to claim streaming and
    /// answer with one not-implemented frame. It now says it cannot, and
    /// an `invokeStream` that arrives anyway is refused.
    #[tokio::test]
    async fn without_a_stream_handler_invoke_stream_is_refused() {
        let outcome = dispatch(Arc::new(Fake), request("invokeStream"), &session()).await;
        let DispatchResult::Reply(reply) = outcome else {
            panic!("expected a refusal, not a stream");
        };
        let error = reply.error.expect("an error reply");
        assert_eq!(error.code, -32601);
        assert!(
            error.message.contains("does not serve server streams"),
            "{}",
            error.message
        );
    }

    struct Streams;

    #[async_trait]
    impl BowirePlugin for Streams {
        fn id(&self) -> &str {
            "streams"
        }
        fn name(&self) -> &str {
            "Streams"
        }
        async fn discover(&self, _request: DiscoverRequest) -> Vec<ServiceInfo> {
            vec![]
        }
        async fn invoke(&self, _request: InvokeRequest) -> InvokeResult {
            InvokeResult::default()
        }
        fn streams(&self) -> Option<&dyn crate::StreamHandler> {
            Some(self)
        }
    }

    #[async_trait]
    impl crate::StreamHandler for Streams {
        async fn invoke_stream(
            &self,
            _request: InvokeRequest,
        ) -> futures::stream::BoxStream<'static, String> {
            Box::pin(futures::stream::iter(["1".to_string()]))
        }
    }

    #[tokio::test]
    async fn a_stream_handler_is_reported_and_routed() {
        let init = result_of(dispatch(Arc::new(Streams), request("initialize"), &session()).await);
        assert_eq!(init["capabilities"]["invokeStream"], json!(true));

        let outcome = dispatch(Arc::new(Streams), request("invokeStream"), &session()).await;
        assert!(matches!(outcome, DispatchResult::Stream { .. }));
    }

    /// The host reads a non-array discover result as "no services, try the
    /// next plugin" — silently. The `{"services": …}` envelope this runtime
    /// used to send meant a Rust sidecar discovered nothing, ever.
    #[tokio::test]
    async fn discover_replies_with_a_bare_array() {
        let result = result_of(dispatch(Arc::new(Fake), request("discover"), &session()).await);

        let services = result.as_array().expect("discover replies with an array");
        assert_eq!(services.len(), 1);
        assert_eq!(services[0]["name"], json!("Echo"));
    }

    /// The contract's reply is the bare string; `{}` was this runtime's own
    /// invention.
    #[tokio::test]
    async fn ping_replies_with_the_bare_pong_string() {
        let result = result_of(dispatch(Arc::new(Fake), request("ping"), &session()).await);

        assert_eq!(result, json!("pong"));
    }

    /// A plugin that declares settings and answers with what it read.
    struct Configured {
        declared: Vec<crate::models::PluginSetting>,
    }

    impl Configured {
        fn new() -> Self {
            Self {
                declared: vec![
                    crate::models::PluginSetting::new("mode", "Mode", "select")
                        .with_default("fast")
                        .with_options([
                            crate::models::SettingOption::new("fast"),
                            crate::models::SettingOption::labelled("safe", "Safe"),
                        ]),
                    crate::models::PluginSetting::new("ticks", "Ticks", "number").with_default(3),
                ],
            }
        }
    }

    #[async_trait]
    impl BowirePlugin for Configured {
        fn id(&self) -> &str {
            "configured"
        }

        fn name(&self) -> &str {
            "Configured"
        }

        fn settings(&self) -> Vec<crate::models::PluginSetting> {
            self.declared.clone()
        }

        async fn discover(&self, request: DiscoverRequest) -> Vec<ServiceInfo> {
            vec![ServiceInfo::new(
                request.settings.string("mode").unwrap_or_default(),
            )]
        }

        async fn invoke(&self, request: InvokeRequest) -> InvokeResult {
            InvokeResult::ok(format!(
                "{}/{}",
                request.settings.string("mode").unwrap_or_default(),
                request.settings.number("ticks").unwrap_or_default()
            ))
        }
    }

    fn with_params(method: &str, params: Value) -> Request {
        Request {
            id: Some(json!(1)),
            method: method.to_owned(),
            params: Some(params),
        }
    }

    /// Kuestenlogik/Bowire#693, first half: the declarations reach the host,
    /// in its shape — key, label, type, defaultValue, options.
    #[tokio::test]
    async fn initialize_sends_the_declared_settings_in_the_hosts_shape() {
        let result = result_of(
            dispatch(
                Arc::new(Configured::new()),
                request("initialize"),
                &session(),
            )
            .await,
        );

        assert_eq!(
            result["settings"][0],
            json!({
                "key": "mode",
                "label": "Mode",
                "type": "select",
                "defaultValue": "fast",
                "options": [{"value": "fast"}, {"value": "safe", "label": "Safe"}],
                "required": false,
            })
        );
        assert_eq!(result["settings"][1]["defaultValue"], json!(3));
    }

    /// Second half: the values the host sends reach the plugin, per call; a
    /// key nobody set reads as its declared default.
    #[tokio::test]
    async fn the_values_sent_with_a_call_reach_the_plugin() {
        let plugin = Arc::new(Configured::new());
        let set = with_params("invoke", json!({"settings": {"mode": "safe"}}));
        let result = result_of(dispatch(plugin.clone(), set, &session()).await);
        assert_eq!(result["response"], json!("safe/3"));

        let unset = with_params("invoke", json!({}));
        let result = result_of(dispatch(plugin.clone(), unset, &session()).await);
        assert_eq!(result["response"], json!("fast/3"));

        let discover = with_params("discover", json!({"settings": {"mode": "safe"}}));
        let result = result_of(dispatch(plugin, discover, &session()).await);
        assert_eq!(result[0]["name"], json!("safe"));
    }

    /// A select without options would render with nothing to pick: the
    /// handshake fails and says which setting.
    #[tokio::test]
    async fn a_select_without_options_fails_the_handshake() {
        let plugin = Configured {
            declared: vec![crate::models::PluginSetting::new("mode", "Mode", "select")],
        };
        let outcome = dispatch(Arc::new(plugin), request("initialize"), &session()).await;
        let DispatchResult::Reply(reply) = outcome else {
            panic!("expected a plain reply");
        };
        let error = reply.error.expect("an error reply");
        assert!(error
            .message
            .contains("select setting 'mode' declares no options"));
    }

    struct WithForm(Option<crate::manual_entry::ManualEntryForm>);

    #[async_trait]
    impl BowirePlugin for WithForm {
        fn id(&self) -> &str {
            "with-form"
        }

        fn name(&self) -> &str {
            "With form"
        }

        fn manual_entry(&self) -> Option<crate::manual_entry::ManualEntryForm> {
            self.0.clone()
        }

        async fn discover(&self, _request: DiscoverRequest) -> Vec<ServiceInfo> {
            Vec::new()
        }

        async fn invoke(&self, _request: InvokeRequest) -> InvokeResult {
            InvokeResult::default()
        }
    }

    /// Kuestenlogik/Bowire#1003: the declared form travels in the handshake;
    /// none means no field at all; a broken one fails it with the reason.
    #[tokio::test]
    async fn initialize_carries_the_hand_entry_form() {
        use crate::manual_entry::{ManualCallKind, ManualEntryForm, ManualField};

        let form = ManualEntryForm::new([
            ManualCallKind::new("produce", "Unary").with_target("{topic}", "produce")
        ])
        .with_fields([ManualField::template("topic", "Topic").required()]);
        let result = result_of(
            dispatch(
                Arc::new(WithForm(Some(form))),
                request("initialize"),
                &session(),
            )
            .await,
        );
        assert_eq!(
            result["manualEntry"]["callKinds"][0]["service"],
            json!("{topic}")
        );
        assert_eq!(
            result["manualEntry"]["fields"][0]["placement"],
            json!("template")
        );

        let none =
            result_of(dispatch(Arc::new(WithForm(None)), request("initialize"), &session()).await);
        assert!(none.get("manualEntry").is_none());

        let broken = ManualEntryForm::new([
            ManualCallKind::new("produce", "Unary").with_target("{topic}", "produce")
        ]);
        let DispatchResult::Reply(reply) = dispatch(
            Arc::new(WithForm(Some(broken))),
            request("initialize"),
            &session(),
        )
        .await
        else {
            panic!("expected a plain reply");
        };
        assert!(reply
            .error
            .expect("an error")
            .message
            .contains("placeholder 'topic'"));
    }
}
