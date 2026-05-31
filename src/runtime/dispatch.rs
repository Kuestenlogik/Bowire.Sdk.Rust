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
            }),
        )),
        "ping" => DispatchResult::Reply(Response::ok(id, json!({}))),
        "discover" => {
            let server_url = string_param(&params, "serverUrl").unwrap_or_default();
            let show_internal = bool_param(&params, "showInternalServices").unwrap_or(false);
            let services = plugin.discover(&server_url, show_internal).await;
            DispatchResult::Reply(match serde_json::to_value(&services) {
                Ok(v) => Response::ok(id, json!({ "services": v })),
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
