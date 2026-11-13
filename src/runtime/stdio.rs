//! Stdio runtime — JSON-RPC 2.0 over NDJSON on stdin/stdout, no
//! extra dependencies. The Bowire host spawns the sidecar binary
//! and pipes both wires; this module reads one envelope per
//! `\n`-terminated line, dispatches through the shared
//! [`super::dispatch`] helper, writes the response back, and pumps
//! streaming notifications as the plugin emits them.

use std::future::Future;
use std::sync::Arc;

use futures::stream::StreamExt;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;

use crate::plugin::BowirePlugin;
use crate::runtime::dispatch::{dispatch, DispatchResult, Session};
use crate::runtime::jsonrpc::{Notification, Request, Response};

/// Drive `plugin` against stdin/stdout. Returns 0 once the host sent
/// `shutdown` or closed stdin, non-zero on an unrecoverable I/O error.
///
/// Over stdio the host owns the sidecar, so either way the process is
/// stopping: every open channel closes and [`BowirePlugin::shutdown`] runs,
/// once, before this returns.
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
///
/// On `shutdown` it stops reading too, without waiting for EOF: the running
/// streams stop, the channels close, the plugin's hook runs, and the reply
/// goes out last, so the host hears "done" only once it is.
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

    // Why the loop stopped: the exit code, and the `shutdown` reply still
    // to be written when the host asked for it.
    let (code, mut ack) = loop {
        tokio::select! {
            line = reader.next_line() => {
                let line = match line {
                    Ok(Some(line)) => line,
                    Ok(None) => break (0, None), // EOF — the host disconnected cleanly
                    Err(_) => break (1, None),   // unrecoverable I/O error
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
                // Taken here rather than in the task, so a `shutdown` read
                // right behind this request cannot end the session before
                // the task has started watching for it.
                let ended = session.ended();
                in_flight.spawn(
                    async move { handle(plugin, req, output, &session, ended).await },
                );
            }
            // Reaps what has finished, so a long session does not hold on to
            // every request it ever handled; and hears the `shutdown`.
            Some(finished) = in_flight.join_next() => {
                if let Ok(Some(ack)) = finished {
                    // The streams still running end here rather than keep
                    // the process alive.
                    session.end();
                    break (0, Some(ack));
                }
            }
        }
    };

    // Every request read gets its reply before the process goes.
    while let Some(finished) = in_flight.join_next().await {
        if let Ok(Some(late)) = finished {
            ack.get_or_insert(late);
        }
    }

    // The process is stopping, asked or not: close what is open, then tell
    // the plugin, once.
    session.end();
    plugin.shutdown().await;

    // Every frame queued so far is on the wire before the reply, and so
    // before the process goes.
    let (done_tx, done_rx) = oneshot::channel();
    if outbound_tx.send(Outbound::Flush(done_tx)).is_ok() {
        let _ = done_rx.await;
    }
    if let Some(ack) = ack {
        let _ = write_response(&output, ack).await;
    }
    code
}

/// Answers one request. Returns the reply to a `shutdown` instead of
/// writing it: it goes out once the runtime has stopped.
async fn handle<P: BowirePlugin, W: AsyncWrite + Unpin + Send>(
    plugin: Arc<P>,
    req: Request,
    stdout: Arc<Mutex<W>>,
    session: &Session,
    ended: impl Future<Output = ()>,
) -> Option<Response> {
    match dispatch(plugin, req, session).await {
        DispatchResult::Reply(resp) => {
            let _ = write_response(&stdout, resp).await;
        }
        DispatchResult::Shutdown(ack) => return Some(ack),
        DispatchResult::Stream {
            ack,
            stream_id,
            stream,
        } => {
            // Ack the request first — the host expects the response
            // *before* the first $/stream/data notification so its
            // subscription is live when frames start arriving.
            let _ = write_response(&stdout, ack).await;
            let mut stream = std::pin::pin!(stream.take_until(ended));
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
    None
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

    /// Echoes on a duplex channel, serves a stream that never ends, and
    /// reports to the test what the runtime tells it.
    struct Stoppable {
        told: mpsc::UnboundedSender<&'static str>,
    }

    #[async_trait::async_trait]
    impl BowirePlugin for Stoppable {
        fn id(&self) -> &str {
            "stoppable"
        }

        fn name(&self) -> &str {
            "Stoppable"
        }

        async fn discover(&self, _request: DiscoverRequest) -> Vec<ServiceInfo> {
            Vec::new()
        }

        async fn invoke(&self, _request: InvokeRequest) -> InvokeResult {
            InvokeResult::ok("{}")
        }

        fn streams(&self) -> Option<&dyn crate::StreamHandler> {
            Some(self)
        }

        fn channels(&self) -> Option<&dyn crate::ChannelHandler> {
            Some(self)
        }

        async fn shutdown(&self) {
            let _ = self.told.send("shutdown hook");
        }
    }

    #[async_trait::async_trait]
    impl crate::StreamHandler for Stoppable {
        async fn invoke_stream(
            &self,
            _request: InvokeRequest,
        ) -> futures::stream::BoxStream<'static, String> {
            Box::pin(futures::stream::pending())
        }
    }

    #[async_trait::async_trait]
    impl crate::ChannelHandler for Stoppable {
        async fn open_channel(
            &self,
            _request: crate::ChannelRequest,
            mut channel: crate::Channel,
        ) -> Result<(), String> {
            let told = self.told.clone();
            tokio::spawn(async move {
                while channel.recv().await.is_some() {}
                let _ = told.send("channel closed");
            });
            Ok(())
        }
    }

    fn methods_and_ids(written: &str) -> Vec<String> {
        written
            .lines()
            .map(|l| {
                let v = serde_json::from_str::<serde_json::Value>(l).unwrap();
                match v["method"].as_str() {
                    Some(method) => method.to_owned(),
                    None => format!("reply {}", v["id"]),
                }
            })
            .collect()
    }

    async fn told_all(mut told: mpsc::UnboundedReceiver<&'static str>) -> Vec<&'static str> {
        let mut all = Vec::new();
        while let Some(t) = told.recv().await {
            all.push(t);
        }
        all.sort_unstable();
        all
    }

    /// Over stdio `shutdown` means the process stops: the channel closes,
    /// the stream that would never end does, the hook runs once, the reply
    /// comes last, and `run` returns although stdin is still open.
    #[tokio::test]
    async fn shutdown_closes_what_is_open_runs_the_hook_and_returns() {
        let (told_tx, told) = mpsc::unbounded_channel();
        let (mut host, input) = tokio::io::duplex(64 * 1024);
        let (output, mut read_back) = tokio::io::duplex(64 * 1024);
        let running = tokio::spawn(run_with(Stoppable { told: told_tx }, input, output));

        // One at a time, each acknowledged before the next: both are open
        // by the time the host says `shutdown`.
        let mut replies = BufReader::new(&mut read_back).lines();
        for request in [
            r#"{"jsonrpc":"2.0","id":1,"method":"openChannel","params":{"channelId":"c1"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"invokeStream","params":{"streamId":"s1"}}"#,
        ] {
            host.write_all(format!("{request}\n").as_bytes())
                .await
                .unwrap();
            replies.next_line().await.unwrap().expect("an ack");
        }
        host.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"shutdown\"}\n")
            .await
            .unwrap();

        assert_eq!(running.await.unwrap(), 0);
        let mut written = String::new();
        read_back.read_to_string(&mut written).await.unwrap();
        let mut frames = methods_and_ids(&written);
        assert_eq!(frames.pop().as_deref(), Some("reply 3"), "{written}");
        frames.sort_unstable();
        assert_eq!(frames, ["$/channel/closed", "$/stream/end"], "{written}");
        assert_eq!(told_all(told).await, ["channel closed", "shutdown hook"]);
        drop(host);
    }

    /// stdin closing without a `shutdown` stops the process just the same,
    /// so the plugin is told then too, once.
    #[tokio::test]
    async fn eof_without_a_shutdown_still_closes_channels_and_runs_the_hook() {
        let (told_tx, told) = mpsc::unbounded_channel();
        let input = concat!(
            r#"{"jsonrpc":"2.0","id":1,"method":"openChannel","params":{"channelId":"c1"}}"#,
            "\n",
        );
        let (output, mut read_back) = tokio::io::duplex(64 * 1024);

        let code = run_with(Stoppable { told: told_tx }, input.as_bytes(), output).await;

        assert_eq!(code, 0);
        let mut written = String::new();
        read_back.read_to_string(&mut written).await.unwrap();
        assert_eq!(
            methods_and_ids(&written),
            ["reply 1", "$/channel/closed"],
            "{written}"
        );
        assert_eq!(told_all(told).await, ["channel closed", "shutdown hook"]);
    }
}
