//! Stdio runtime — JSON-RPC 2.0 over NDJSON on stdin/stdout, no
//! extra dependencies. The Bowire host spawns the sidecar binary
//! and pipes both wires; this module reads one envelope per
//! `\n`-terminated line, dispatches through the shared
//! [`super::dispatch`] helper, writes the response back, and pumps
//! streaming notifications as the plugin emits them.

use std::sync::Arc;

use futures::stream::StreamExt;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;

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
    run_with(plugin, tokio::io::stdin(), tokio::io::stdout()).await
}

/// What the single writer task puts on the wire, in order: channel frames,
/// and the flush marker the read loop sends at EOF to learn that every
/// frame queued before it has been written.
enum Outbound {
    Frame(Notification),
    Flush(oneshot::Sender<()>),
}

/// The read loop over any reader and writer; [`run`] passes stdin and stdout.
///
/// At EOF it stops reading, lets the requests already read finish — their
/// replies and stream frames included — and drains the frame queue before it
/// returns (#10). It used to return at EOF straight away, so input piped in
/// all at once could produce no output at all.
pub(crate) async fn run_with<P, R, W>(plugin: P, input: R, output: W) -> i32
where
    P: BowirePlugin,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let plugin = Arc::new(plugin);
    // The writer is single-writer (serialised by the Mutex) so concurrent
    // request handlers + stream pumps don't interleave envelopes
    // mid-line.
    let output = Arc::new(Mutex::new(output));

    // Channel frames are pushed synchronously (a dropped channel handle
    // still has to announce `$/channel/closed`), so they go through one
    // queue and a single writer task: in order, never mid-line.
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<Outbound>();
    {
        let output = output.clone();
        tokio::spawn(async move {
            while let Some(item) = outbound_rx.recv().await {
                match item {
                    Outbound::Frame(notification) => {
                        let _ = write_notification(&output, notification).await;
                    }
                    Outbound::Flush(done) => {
                        let _ = done.send(());
                    }
                }
            }
        });
    }
    let frames = outbound_tx.clone();
    let session = Arc::new(Session::new(Arc::new(move |n| {
        let _ = frames.send(Outbound::Frame(n));
    })));

    let mut reader = BufReader::new(input).lines();
    let mut in_flight = JoinSet::new();

    let code = loop {
        // Reap what has finished, so a long session does not hold on to
        // every request it ever handled.
        while in_flight.try_join_next().is_some() {}

        let line = match reader.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break 0, // EOF — the host disconnected cleanly
            Err(_) => break 1,   // unrecoverable I/O error
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
        let output = output.clone();
        let session = session.clone();
        in_flight.spawn(async move {
            handle(plugin, req, output, &session).await;
        });
    };

    // Every request read gets its reply, and every frame queued before the
    // marker is on the wire, before the process goes.
    while in_flight.join_next().await.is_some() {}
    let (done_tx, done_rx) = oneshot::channel();
    if outbound_tx.send(Outbound::Flush(done_tx)).is_ok() {
        let _ = done_rx.await;
    }
    code
}

async fn handle<P: BowirePlugin, W: AsyncWrite + Unpin + Send>(
    plugin: Arc<P>,
    req: Request,
    stdout: Arc<Mutex<W>>,
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

async fn write_response<W: AsyncWrite + Unpin>(
    stdout: &Arc<Mutex<W>>,
    response: Response,
) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(&response).unwrap_or_default();
    line.push(b'\n');
    let mut guard = stdout.lock().await;
    guard.write_all(&line).await?;
    guard.flush().await
}

async fn write_notification<W: AsyncWrite + Unpin>(
    stdout: &Arc<Mutex<W>>,
    notification: Notification,
) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(&notification).unwrap_or_default();
    line.push(b'\n');
    let mut guard = stdout.lock().await;
    guard.write_all(&line).await?;
    guard.flush().await
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::models::{InvokeResult, ServiceInfo};
    use crate::plugin::{DiscoverRequest, InvokeRequest};

    struct Slow;

    #[async_trait::async_trait]
    impl BowirePlugin for Slow {
        fn id(&self) -> &str {
            "slow"
        }

        fn name(&self) -> &str {
            "Slow"
        }

        async fn discover(&self, _request: DiscoverRequest) -> Vec<ServiceInfo> {
            Vec::new()
        }

        // Still busy when the input has long ended: the reply must come anyway.
        async fn invoke(&self, _request: InvokeRequest) -> InvokeResult {
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            InvokeResult::ok("{}")
        }
    }

    /// #10: input piped in all at once, then EOF. Every request read gets
    /// its reply before `run` returns.
    #[tokio::test]
    async fn requests_read_before_eof_all_get_their_reply() {
        let input = concat!(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":2,"method":"invoke","params":{}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":3,"method":"ping","params":{}}"#,
            "\n",
        );
        let (output, mut read_back) = tokio::io::duplex(64 * 1024);

        let code = run_with(Slow, input.as_bytes(), output).await;

        assert_eq!(code, 0);
        let mut written = String::new();
        read_back.read_to_string(&mut written).await.unwrap();
        let mut ids: Vec<i64> = written
            .lines()
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(l).unwrap()["id"]
                    .as_i64()
                    .unwrap()
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3], "every request got its reply: {written}");
    }
}
