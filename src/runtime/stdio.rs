//! Stdio runtime — JSON-RPC 2.0 over NDJSON on stdin/stdout, no
//! extra dependencies. The Bowire host spawns the sidecar binary
//! and pipes both wires; this module reads one envelope per
//! `\n`-terminated line, dispatches through the shared
//! [`super::dispatch`] helper, writes the response back, and pumps
//! streaming notifications as the plugin emits them.

use std::sync::Arc;

use futures::stream::StreamExt;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex};

use crate::plugin::BowirePlugin;
use crate::runtime::dispatch::{dispatch, DispatchResult, Session};
use crate::runtime::jsonrpc::{Notification, Request, Response};

/// Drive `plugin` against stdin/stdout. Returns 0 on a clean
/// `shutdown` request, non-zero on an unrecoverable I/O error.
///
/// ```no_run
/// # use bowire_plugin::{run, BowirePlugin, DiscoverRequest, InvokeRequest, InvokeResult, ServiceInfo};
/// # struct Echo;
/// # #[async_trait::async_trait]
/// # impl BowirePlugin for Echo {
/// #     fn id(&self) -> &str { "echo" }
/// #     fn name(&self) -> &str { "Echo" }
/// #     async fn discover(&self, _: DiscoverRequest) -> Vec<ServiceInfo> { vec![] }
/// #     async fn invoke(&self, _: InvokeRequest) -> InvokeResult {
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

    // Channel frames are pushed synchronously (a dropped channel handle
    // still has to announce `$/channel/closed`), so they go through one
    // queue and a single writer task: in order, never mid-line.
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel::<Notification>();
    {
        let stdout = stdout.clone();
        tokio::spawn(async move {
            while let Some(notification) = notify_rx.recv().await {
                let _ = write_notification(&stdout, notification).await;
            }
        });
    }
    let session = Arc::new(Session::new(Arc::new(move |n| {
        let _ = notify_tx.send(n);
    })));

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
                eprintln!("bowire-plugin: malformed envelope: {e}");
                continue;
            }
        };

        let plugin = plugin.clone();
        let stdout = stdout.clone();
        let session = session.clone();
        tokio::spawn(async move {
            handle(plugin, req, stdout, &session).await;
        });
    }
}

async fn handle<P: BowirePlugin>(
    plugin: Arc<P>,
    req: Request,
    stdout: Arc<Mutex<tokio::io::Stdout>>,
    session: &Session,
) {
    match dispatch(plugin, req, session).await {
        DispatchResult::Reply(resp) | DispatchResult::Shutdown(resp) => {
            // Shutdown is acked just like any other request; the read
            // loop exits on the next EOF the host writes.
            let _ = write_response(&stdout, resp).await;
        }
        DispatchResult::Stream {
            ack,
            stream_id,
            mut stream,
        } => {
            // Ack the request first — the host expects the response
            // *before* the first $/stream/data notification so its
            // subscription is live when frames start arriving.
            let _ = write_response(&stdout, ack).await;
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
        }
    }
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
