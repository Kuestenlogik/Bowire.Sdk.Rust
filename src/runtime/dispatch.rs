//! Transport-agnostic JSON-RPC method dispatcher. Both the stdio
//! and HTTP runtimes funnel inbound requests through
//! [`dispatch`]; the runtime owns the wire (NDJSON to stdout vs.
//! SSE-queue), the dispatcher owns the contract semantics
//! (which method maps to which trait call + how server-streaming
//! is decomposed into an ack + pump).

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::BoxStream;
use serde_json::{json, Value};

use crate::plugin::BowirePlugin;
use crate::runtime::jsonrpc::{Request, Response};

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

    /// Host requested shutdown — ack first, then signal the runtime
    /// to terminate its read loop.
    Shutdown(Response),
}

/// Dispatch a single JSON-RPC request against `plugin`. Pure async
/// function: no IO, no spawning, no stdout writes. The runtime
/// handles every transport effect.
pub(crate) async fn dispatch<P: BowirePlugin>(plugin: Arc<P>, req: Request) -> DispatchResult {
    let id = req.id.unwrap_or(Value::Null);
    let params = req.params.unwrap_or_else(|| json!({}));

    match req.method.as_str() {
        "shutdown" => DispatchResult::Shutdown(Response::ok(id, json!({}))),
        "initialize" => DispatchResult::Reply(Response::ok(
            id,
            json!({
                "id": plugin.id(),
                "name": plugin.name(),
                "iconSvg": plugin.icon_svg(),
                // #416: which contract this sidecar speaks, and what it can
                // answer. A reply without them is tolerated as legacy v1 —
                // with a warning in the host log on every boot, which is what
                // this runtime used to earn.
                "protocolVersion": crate::SIDECAR_PROTOCOL_VERSION,
                "capabilities": plugin.capabilities(),
            }),
        )),
        // The contract's reply is the bare string. `{}` was this runtime's
        // own invention; a host using ping as a liveness probe reads it as a
        // malformed answer.
        "ping" => DispatchResult::Reply(Response::ok(id, json!("pong"))),
        "discover" => {
            let server_url = string_param(&params, "serverUrl").unwrap_or_default();
            let show_internal = bool_param(&params, "showInternalServices").unwrap_or(false);
            let services = plugin.discover(&server_url, show_internal).await;
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
                .invoke(
                    &server_url,
                    &service,
                    &method,
                    json_messages,
                    show_internal,
                    metadata,
                )
                .await;
            DispatchResult::Reply(match serde_json::to_value(&result) {
                Ok(v) => Response::ok(id, v),
                Err(e) => Response::err(id, -32000, format!("invoke serialise failed: {e}")),
            })
        }
        "invokeStream" => {
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

            let stream = plugin
                .invoke_stream(
                    &server_url,
                    &service,
                    &method,
                    json_messages,
                    show_internal,
                    metadata,
                )
                .await;
            DispatchResult::Stream {
                ack: Response::ok(id, json!({ "streamId": stream_id.clone() })),
                stream_id,
                stream,
            }
        }
        other => DispatchResult::Reply(Response::err(
            id,
            -32601,
            format!("method '{other}' not handled by bowire-plugin runtime"),
        )),
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

        async fn discover(&self, _server_url: &str, _show_internal: bool) -> Vec<ServiceInfo> {
            vec![ServiceInfo::new("Echo")]
        }

        async fn invoke(
            &self,
            _server_url: &str,
            _service: &str,
            _method: &str,
            _json_messages: Vec<String>,
            _show_internal: bool,
            _metadata: HashMap<String, String>,
        ) -> InvokeResult {
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
        let result = result_of(dispatch(Arc::new(Fake), request("initialize")).await);

        assert_eq!(result["protocolVersion"], json!(crate::SIDECAR_PROTOCOL_VERSION));
        assert_eq!(
            result["capabilities"],
            json!({
                "discover": true,
                "invoke": true,
                "invokeStream": true,
                "channels": false,
            })
        );
    }

    /// The host reads a non-array discover result as "no services, try the
    /// next plugin" — silently. The `{"services": …}` envelope this runtime
    /// used to send meant a Rust sidecar discovered nothing, ever.
    #[tokio::test]
    async fn discover_replies_with_a_bare_array() {
        let result = result_of(dispatch(Arc::new(Fake), request("discover")).await);

        let services = result.as_array().expect("discover replies with an array");
        assert_eq!(services.len(), 1);
        assert_eq!(services[0]["name"], json!("Echo"));
    }

    /// The contract's reply is the bare string; `{}` was this runtime's own
    /// invention.
    #[tokio::test]
    async fn ping_replies_with_the_bare_pong_string() {
        let result = result_of(dispatch(Arc::new(Fake), request("ping")).await);

        assert_eq!(result, json!("pong"));
    }
}
