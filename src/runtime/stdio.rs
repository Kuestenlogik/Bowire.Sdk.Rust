//! Stdio runtime — JSON-RPC 2.0 over NDJSON on stdin/stdout, no
//! extra dependencies. The Bowire host spawns the sidecar binary
//! and pipes both wires; this module reads one envelope per
//! `\n`-terminated line, dispatches against the plugin, writes the
//! response back, and pumps streaming notifications as the plugin
//! emits them.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::StreamExt;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

use crate::plugin::BowirePlugin;
use crate::runtime::jsonrpc::{Notification, Request, Response};

/// Drive `plugin` against stdin/stdout. Returns 0 on a clean
/// `shutdown` request, non-zero on an unrecoverable I/O error.
///
/// ```no_run
/// # use bowire_plugin::{run, BowirePlugin, InvokeResult, ServiceInfo};
/// # struct Echo;
/// # #[async_trait::async_trait]
/// # impl BowirePlugin for Echo {
/// #     fn id(&self) -> &str { "echo" }
/// #     fn name(&self) -> &str { "Echo" }
/// #     async fn discover(&self, _: &str, _: bool) -> Vec<ServiceInfo> { vec![] }
/// #     async fn invoke(&self, _: &str, _: &str, _: &str, _: Vec<String>, _: bool,
/// #                     _: std::collections::HashMap<String, String>) -> InvokeResult {
/// #         InvokeResult::ok("{}")
/// #     }
/// # }
/// #[tokio::main]
/// async fn main() {
///     std::process::exit(run(Echo).await);
/// }
/// ```
pub async fn run<P: BowirePlugin>(plugin: P) -> i32 {
    let plugin = Arc::new(plugin);
    // stdout is single-writer (serialised by the Mutex) so concurrent
    // request handlers + stream pumps don't interleave envelopes
    // mid-line.
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin).lines();

    loop {
        let line = match reader.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => return 0, // EOF — host disconnected cleanly
            Err(_) => return 1,   // unrecoverable I/O error
        };
        if line.trim().is_empty() {
            continue;
        }

        let req: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                // Malformed envelope — reply with a parse-error
                // response when we can pluck an id off the raw text,
                // otherwise log to stderr and drop the line.
                eprintln!("bowire-plugin: malformed envelope: {e}");
                continue;
            }
        };

        let plugin = plugin.clone();
        let stdout = stdout.clone();
        tokio::spawn(async move {
            if req.method == "shutdown" {
                // Reply OK, then signal the main loop by closing
                // stdin from the host side (the host already initiated;
                // we just ack). Our loop exits on the next read-EOF.
                if let Some(id) = req.id.clone() {
                    let _ = write_response(&stdout, Response::ok(id, json!({}))).await;
                }
                return;
            }
            dispatch(plugin, req, stdout).await;
        });
    }
}

async fn dispatch<P: BowirePlugin>(
    plugin: Arc<P>,
    req: Request,
    stdout: Arc<Mutex<tokio::io::Stdout>>,
) {
    let id = req.id.unwrap_or(Value::Null);
    let params = req.params.unwrap_or_else(|| json!({}));

    let response = match req.method.as_str() {
        "initialize" => Response::ok(
            id,
            json!({
                "id": plugin.id(),
                "name": plugin.name(),
                "iconSvg": plugin.icon_svg(),
            }),
        ),
        "ping" => Response::ok(id, json!({})),
        "discover" => {
            let server_url = string_param(&params, "serverUrl").unwrap_or_default();
            let show_internal = bool_param(&params, "showInternalServices").unwrap_or(false);
            let services = plugin.discover(&server_url, show_internal).await;
            match serde_json::to_value(&services) {
                Ok(v) => Response::ok(id, json!({ "services": v })),
                Err(e) => Response::err(id, -32000, format!("discover serialise failed: {e}")),
            }
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
            match serde_json::to_value(&result) {
                Ok(v) => Response::ok(id, v),
                Err(e) => Response::err(id, -32000, format!("invoke serialise failed: {e}")),
            }
        }
        "invokeStream" => {
            // Server-streaming: the host hands us a streamId, we
            // ack the request immediately, then push $/stream/data
            // notifications + a $/stream/end notification.
            let stream_id = string_param(&params, "streamId").unwrap_or_default();
            let server_url = string_param(&params, "serverUrl").unwrap_or_default();
            let service = string_param(&params, "service").unwrap_or_default();
            let method = string_param(&params, "method").unwrap_or_default();
            let json_messages = vec_string_param(&params, "jsonMessages").unwrap_or_default();
            let show_internal = bool_param(&params, "showInternalServices").unwrap_or(false);
            let metadata = map_param(&params, "metadata");

            // Ack the request immediately — the host expects the
            // response *before* the first $/stream/data notification
            // so its subscription is live when frames start arriving.
            let _ = write_response(&stdout, Response::ok(id, json!({ "streamId": stream_id }))).await;

            // Pump the stream on this same task (we're inside a
            // tokio::spawn already, so we don't block the read loop).
            // BoxStream is already pinned (it's Pin<Box<dyn Stream>>)
            // and Pin<Box<_>> is Unpin, so .next() works directly.
            let mut stream = plugin
                .invoke_stream(
                    &server_url,
                    &service,
                    &method,
                    json_messages,
                    show_internal,
                    metadata,
                )
                .await;
            while let Some(frame) = stream.next().await {
                let _ = write_notification(
                    &stdout,
                    Notification {
                        jsonrpc: "2.0",
                        method: "$/stream/data".into(),
                        params: Some(json!({ "streamId": stream_id, "message": frame })),
                    },
                )
                .await;
            }
            let _ = write_notification(
                &stdout,
                Notification {
                    jsonrpc: "2.0",
                    method: "$/stream/end".into(),
                    params: Some(json!({ "streamId": stream_id })),
                },
            )
            .await;
            return; // we already wrote our response
        }
        other => Response::err(
            id,
            -32601,
            format!("method '{other}' not handled by bowire-plugin runtime"),
        ),
    };

    let _ = write_response(&stdout, response).await;
}

// ---- IO helpers (NDJSON framing) -------------------------------

async fn write_response(
    stdout: &Arc<Mutex<tokio::io::Stdout>>,
    response: Response,
) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(&response).unwrap_or_default();
    line.push(b'\n');
    let mut guard = stdout.lock().await;
    guard.write_all(&line).await?;
    guard.flush().await
}

async fn write_notification(
    stdout: &Arc<Mutex<tokio::io::Stdout>>,
    notification: Notification,
) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(&notification).unwrap_or_default();
    line.push(b'\n');
    let mut guard = stdout.lock().await;
    guard.write_all(&line).await?;
    guard.flush().await
}

// ---- params helpers --------------------------------------------

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
