//! HTTP/SSE runtime (`features = ["http"]`) — streamable-HTTP wire,
//! same JSON-RPC contract as stdio. Routes:
//!
//! - `POST /` — JSON-RPC request, response goes in the POST body.
//! - `GET /` — long-lived Server-Sent-Events stream the runtime
//!   pushes server-initiated notifications onto
//!   (`$/stream/data` / `$/stream/end` / `$/channel/*`). Connect once,
//!   stay subscribed for the session.
//!
//! One sidecar serves several hosts here. Each names itself with the
//! `Bowire-Session` request header, and what a request opens belongs to the
//! session it named: its frames go to that session's SSE streams only, and
//! the session ending (its host sent `shutdown`, or its last SSE stream
//! closed) stops its streams and closes its channels and nobody else's (see
//! `sessions.rs`). The plugin's own `shutdown` hook runs when this
//! server stops, not when a host leaves.
//!
//! Use this when the sidecar should live outside the Bowire host's
//! process tree — multi-tenant deployments, Kubernetes pods, &c.
//! For the default subprocess-spawn case stick with [`super::stdio`].

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::Response as AxumResponse;
use axum::routing::post;
use axum::{Json, Router};
use futures::stream::StreamExt;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::BroadcastStream;

use crate::plugin::BowirePlugin;
use crate::runtime::dispatch::{dispatch, starts_owned_work, DispatchResult, Session};
use crate::runtime::jsonrpc::{Notification, Request};
use crate::runtime::sessions::{SessionKey, Sessions};

/// Drive `plugin` over HTTP/SSE on `host:port`. Returns once the
/// process is asked to stop (Ctrl-C, or SIGTERM on Unix). Errors during
/// bind / serve surface as the returned `std::io::Error`.
///
/// To choose the port yourself, e.g. port 0 and then report the one the
/// OS assigned, bind a [`TcpListener`] and use [`serve_http`].
///
/// ```no_run
/// # use bowire_plugin::{run_http, BowirePlugin, DiscoverRequest, InvokeRequest, InvokeResult, ServiceInfo};
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
/// async fn main() -> std::io::Result<()> {
///     run_http(Echo, "127.0.0.1", 8770).await
/// }
/// ```
pub async fn run_http<P: BowirePlugin>(plugin: P, host: &str, port: u16) -> std::io::Result<()> {
    let addr: SocketAddr =
        format!("{host}:{port}")
            .parse()
            .map_err(|e: std::net::AddrParseError| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
            })?;
    let listener = TcpListener::bind(addr).await?;
    serve_http(plugin, listener, shutdown_signal()).await
}

/// Drive `plugin` over HTTP/SSE on a listener the caller bound, until
/// `shutdown` completes. [`run_http`] is this with a listener on
/// `host:port` and the process's stop signals.
///
/// A host keeps its SSE `GET` open for as long as it uses the sidecar, so
/// on `shutdown` the runtime ends those streams itself. Waiting for the
/// host to hang up would keep a sidecar that was told to stop running for
/// as long as any workbench stays connected.
///
/// Stopping is also when the plugin is told: every session ends, which
/// closes the channels still open, and then [`BowirePlugin::shutdown`] runs,
/// once. A host's `shutdown` request does neither; it ends that host's
/// session and the server keeps serving the others.
///
/// ```no_run
/// # use bowire_plugin::{serve_http, BowirePlugin, DiscoverRequest, InvokeRequest, InvokeResult, ServiceInfo};
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
/// async fn main() -> std::io::Result<()> {
///     let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
///     println!("listening on http://{}/", listener.local_addr()?);
///     serve_http(Echo, listener, bowire_plugin::shutdown_signal()).await
/// }
/// ```
pub async fn serve_http<P, F>(plugin: P, listener: TcpListener, shutdown: F) -> std::io::Result<()>
where
    P: BowirePlugin,
    F: Future<Output = ()> + Send + 'static,
{
    let plugin = Arc::new(plugin);
    let sessions = Arc::new(Sessions::default());
    let (stopping_tx, stopping) = watch::channel(false);
    let state = HttpState {
        plugin: plugin.clone(),
        sessions: sessions.clone(),
        stopping,
    };

    let app = Router::new()
        .route("/", post(post_rpc).get(sse_subscribe))
        .with_state(state);

    let served = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.await;
            // Ends every open SSE stream, so graceful shutdown has no
            // connection left to wait for.
            let _ = stopping_tx.send(true);
        })
        .await;

    // The sidecar itself is stopping, which is the one time the plugin is
    // told. A session whose SSE stream just closed has ended already; this
    // ends the ones that never had a stream.
    sessions.end_all();
    plugin.shutdown().await;
    served
}

/// Completes on Ctrl-C, and on Unix also on SIGTERM: what a container
/// manager or a test harness sends a hosted sidecar it wants gone. The
/// signals [`run_http`] stops on; hand it to [`serve_http`] for the same.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

struct HttpState<P: BowirePlugin> {
    plugin: Arc<P>,
    sessions: Arc<Sessions>,
    /// Turns `true` once the runtime shuts down; every SSE stream ends on it.
    stopping: watch::Receiver<bool>,
}

// Manual `Clone` impl — `#[derive(Clone)]` would add an unwanted
// `P: Clone` bound. `Arc<P>` is always `Clone` regardless of `P`, so this
// struct clones cheaply without forcing plugin types to be `Clone`.
impl<P: BowirePlugin> Clone for HttpState<P> {
    fn clone(&self) -> Self {
        Self {
            plugin: self.plugin.clone(),
            sessions: self.sessions.clone(),
            stopping: self.stopping.clone(),
        }
    }
}

/// `POST /` — JSON-RPC request handler. Decodes the envelope, runs
/// the shared dispatcher against the caller's session, returns the
/// response as the POST body. Streaming requests get their ack here and
/// the pump runs in a background task pushing `$/stream/data` /
/// `$/stream/end` notifications onto the session's queue.
async fn post_rpc<P: BowirePlugin>(
    State(state): State<HttpState<P>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let req: Request = match serde_json::from_value(payload) {
        Ok(r) => r,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    let key = SessionKey::of(&headers);
    // Only a request that starts a stream or a channel needs a session to
    // own it. Any other finds the caller's session, or one that holds
    // nothing: a `channel.send` then reaches no channel, which is the answer
    // for a channel of somebody else's session too.
    let lease = if starts_owned_work(&req.method) {
        Some(state.sessions.open(&key))
    } else {
        state.sessions.existing(&key)
    };
    let nothing_held;
    let session = match &lease {
        Some(lease) => lease.session(),
        None => {
            nothing_held = Session::new(Arc::new(|_| {}));
            &nothing_held
        }
    };
    // Taken before the dispatch, so a session that ends while the plugin
    // is still starting the stream stops it all the same.
    let ended = session.ended();

    match dispatch(state.plugin.clone(), req, session).await {
        DispatchResult::Reply(resp) => Ok(Json(
            serde_json::to_value(resp).unwrap_or_else(|_| json!({})),
        )),
        DispatchResult::Shutdown(resp) => {
            // The host is leaving; the sidecar stays. Without a session id
            // there is no telling whose streams this would end, so it ends
            // nothing.
            if key.is_named() {
                state.sessions.end(&key);
            }
            Ok(Json(
                serde_json::to_value(resp).unwrap_or_else(|_| json!({})),
            ))
        }
        DispatchResult::Stream {
            ack,
            stream_id,
            stream,
        } => {
            // Spawn the pump after the response is constructed but
            // before we return — so the workbench's subscription
            // (already live by the time it issued the POST) catches
            // the first frame.
            let notify = session.notify.clone();
            tokio::spawn(async move {
                // Keeps the session registered while its stream runs.
                let _lease = lease;
                let mut stream = std::pin::pin!(stream.take_until(ended));
                while let Some(frame) = stream.next().await {
                    notify(Notification {
                        jsonrpc: "2.0",
                        method: "$/stream/data".into(),
                        params: Some(json!({ "streamId": stream_id, "message": frame })),
                    });
                }
                notify(Notification {
                    jsonrpc: "2.0",
                    method: "$/stream/end".into(),
                    params: Some(json!({ "streamId": stream_id })),
                });
            });
            Ok(Json(
                serde_json::to_value(ack).unwrap_or_else(|_| json!({})),
            ))
        }
    }
}

/// `GET /` — Server-Sent-Events subscription. The workbench keeps
/// one of these open per session; every notification of that session
/// gets serialised as `data: <json>\n\n`. The stream closing, whoever
/// closes it, tells the session: when it was the last one, the session ends.
async fn sse_subscribe<P: BowirePlugin>(
    State(state): State<HttpState<P>>,
    headers: HeaderMap,
) -> AxumResponse {
    let (lease, rx) = state.sessions.subscribe(&SessionKey::of(&headers));
    let mut stopping = state.stopping.clone();
    let body_stream = BroadcastStream::new(rx)
        .filter_map(|item| async move {
            let n = item.ok()?;
            let payload = serde_json::to_string(&n).ok()?;
            Some(Ok::<_, Infallible>(format!("data: {payload}\n\n")))
        })
        .take_until(async move {
            // Owned by the stream: dropped with it when the client hangs
            // up, and here when the runtime stops.
            let _lease = lease;
            let _ = stopping.wait_for(|stopping| *stopping).await;
        });
    let body = Body::from_stream(body_stream);

    let mut resp = AxumResponse::new(body);
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    resp.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    resp.headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    resp
}

#[cfg(test)]
mod session_tests;

#[cfg(test)]
mod tests {
    //! The runtime over a real socket, with a raw HTTP/1.1 client: what a
    //! host sees on the wire, without a host.

    use super::*;
    use crate::{DiscoverRequest, InvokeRequest, InvokeResult, ServiceInfo};
    use futures::stream::{self, BoxStream};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;

    struct Ticker;

    #[async_trait::async_trait]
    impl BowirePlugin for Ticker {
        fn id(&self) -> &str {
            "ticker"
        }
        fn name(&self) -> &str {
            "Ticker"
        }
        async fn discover(&self, _: DiscoverRequest) -> Vec<ServiceInfo> {
            vec![]
        }
        async fn invoke(&self, _: InvokeRequest) -> InvokeResult {
            InvokeResult::ok("{}")
        }
        fn streams(&self) -> Option<&dyn crate::StreamHandler> {
            Some(self)
        }
    }

    #[async_trait::async_trait]
    impl crate::StreamHandler for Ticker {
        async fn invoke_stream(&self, _: InvokeRequest) -> BoxStream<'static, String> {
            Box::pin(stream::iter(["1", "2"].map(String::from)))
        }
    }

    pub(super) struct Server {
        pub(super) addr: SocketAddr,
        pub(super) stop: oneshot::Sender<()>,
        pub(super) served: JoinHandle<std::io::Result<()>>,
    }

    async fn start() -> Server {
        start_with(Ticker).await
    }

    /// Serves `plugin` on a port the OS picks, until `stop` is sent.
    pub(super) async fn start_with<P: BowirePlugin>(plugin: P) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let served = tokio::spawn(serve_http(plugin, listener, async {
            let _ = stopped.await;
        }));
        Server { addr, stop, served }
    }

    /// The header line a host names its session with; none for a host
    /// older than the rule.
    fn session_header(session: Option<&str>) -> String {
        session
            .map(|id| format!("bowire-session: {id}\r\n"))
            .unwrap_or_default()
    }

    async fn subscribe(addr: SocketAddr) -> BufReader<TcpStream> {
        subscribe_as(addr, None).await
    }

    /// Opens the SSE `GET` of `session` and returns once its response
    /// headers are in, which is when the runtime has subscribed this client.
    pub(super) async fn subscribe_as(
        addr: SocketAddr,
        session: Option<&str>,
    ) -> BufReader<TcpStream> {
        let mut sse = TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "GET / HTTP/1.1\r\nhost: x\r\naccept: text/event-stream\r\n{}\r\n",
            session_header(session)
        );
        sse.write_all(req.as_bytes()).await.unwrap();
        let mut sse = BufReader::new(sse);
        let mut line = String::new();
        loop {
            line.clear();
            sse.read_line(&mut line).await.unwrap();
            if line == "\r\n" {
                return sse;
            }
        }
    }

    async fn post(addr: SocketAddr, body: &str) -> String {
        post_as(addr, None, body).await
    }

    /// POSTs `body` as `session` and returns the whole HTTP reply.
    pub(super) async fn post_as(addr: SocketAddr, session: Option<&str>, body: &str) -> String {
        let mut conn = TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST / HTTP/1.1
host: x
content-type: application/json
content-length: {}
{}connection: close

{body}",
            body.len(),
            session_header(session)
        );
        conn.write_all(req.as_bytes()).await.unwrap();
        let mut reply = String::new();
        conn.read_to_string(&mut reply).await.unwrap();
        reply
    }

    /// Reads SSE `data:` lines (chunk framing and all) until one contains
    /// `needle`.
    pub(super) async fn read_until(sse: &mut BufReader<TcpStream>, needle: &str) -> Vec<String> {
        let mut seen = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            assert!(
                sse.read_line(&mut line).await.unwrap() > 0,
                "the stream ended before {needle}; saw {seen:?}"
            );
            if line.starts_with("data: ") {
                seen.push(line.trim_end().to_string());
                if line.contains(needle) {
                    return seen;
                }
            }
        }
    }

    #[tokio::test]
    async fn a_server_stream_arrives_over_sse_and_ends() {
        let server = start().await;
        let mut sse = subscribe(server.addr).await;

        let ack = post(
            server.addr,
            r#"{"jsonrpc":"2.0","id":7,"method":"invokeStream","params":{"streamId":"s1"}}"#,
        )
        .await;
        assert!(ack.contains(r#""streamId":"s1""#), "{ack}");

        let seen = read_until(&mut sse, "$/stream/end").await;
        assert_eq!(seen.len(), 3, "{seen:?}");
        assert!(seen[0].contains(r#""message":"1""#), "{seen:?}");
        assert!(seen[1].contains(r#""message":"2""#), "{seen:?}");

        let _ = server.stop.send(());
        server.served.await.unwrap().unwrap();
    }

    /// A host holds its SSE `GET` open for as long as it uses the sidecar.
    /// Stopping used to wait for that connection to close, so a sidecar
    /// told to stop kept running while any workbench was connected.
    #[tokio::test]
    async fn stopping_ends_the_open_sse_streams_and_returns() {
        let server = start().await;
        let mut sse = subscribe(server.addr).await;

        let _ = server.stop.send(());
        server.served.await.unwrap().unwrap();

        let mut rest = Vec::new();
        sse.read_to_end(&mut rest).await.unwrap();
    }
}
