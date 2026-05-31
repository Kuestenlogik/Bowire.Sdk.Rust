//! HTTP/SSE runtime (`features = ["http"]`) — streamable-HTTP wire,
//! same JSON-RPC contract as stdio. Routes:
//!
//! - `POST /` — JSON-RPC request, response goes in the POST body.
//! - `GET /` — long-lived Server-Sent-Events stream the runtime
//!   pushes server-initiated notifications onto
//!   (`$/stream/data` / `$/stream/end` / `$/channel/*`). Connect once,
//!   stay subscribed for the session.
//!
//! Use this when the sidecar should live outside the Bowire host's
//! process tree — multi-tenant deployments, Kubernetes pods, &c.
//! For the default subprocess-spawn case stick with [`super::stdio`].

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::Response as AxumResponse;
use axum::routing::post;
use axum::{Json, Router};
use futures::stream::StreamExt;
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use crate::plugin::BowirePlugin;
use crate::runtime::dispatch::{dispatch, DispatchResult};
use crate::runtime::jsonrpc::{Notification, Request};

/// Channel capacity for server-initiated notifications. Bounded so a
/// slow / disconnected SSE client can't grow memory without limit;
/// when full, the oldest entry is dropped and the SSE wrapper signals
/// `lagged` to its consumer.
const NOTIFICATION_CHANNEL_CAPACITY: usize = 1024;

/// Drive `plugin` over HTTP/SSE on `host:port`. Returns once the
/// server shuts down (e.g. Ctrl-C signal). Errors during bind /
/// serve surface as the returned `std::io::Error`.
///
/// ```no_run
/// # use bowire_plugin::{run_http, BowirePlugin, InvokeResult, ServiceInfo};
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
/// async fn main() -> std::io::Result<()> {
///     run_http(Echo, "127.0.0.1", 8770).await
/// }
/// ```
pub async fn run_http<P: BowirePlugin>(plugin: P, host: &str, port: u16) -> std::io::Result<()> {
    let plugin = Arc::new(plugin);
    let (notification_tx, _) = broadcast::channel::<Notification>(NOTIFICATION_CHANNEL_CAPACITY);

    let state = HttpState {
        plugin,
        notifications: notification_tx,
    };

    let app = Router::new()
        .route("/", post(post_rpc).get(sse_subscribe))
        .with_state(state);

    let addr: SocketAddr =
        format!("{host}:{port}")
            .parse()
            .map_err(|e: std::net::AddrParseError| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
            })?;

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
}

/// Listen for SIGTERM / Ctrl-C so a hosted sidecar shuts down
/// gracefully when the container manager sends it the signal.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

struct HttpState<P: BowirePlugin> {
    plugin: Arc<P>,
    notifications: broadcast::Sender<Notification>,
}

// Manual `Clone` impl — `#[derive(Clone)]` would add an unwanted
// `P: Clone` bound. `Arc<P>` is always `Clone` regardless of `P`,
// and `broadcast::Sender` is `Clone`, so this struct clones cheaply
// without forcing plugin types to be `Clone`.
impl<P: BowirePlugin> Clone for HttpState<P> {
    fn clone(&self) -> Self {
        Self {
            plugin: self.plugin.clone(),
            notifications: self.notifications.clone(),
        }
    }
}

/// `POST /` — JSON-RPC request handler. Decodes the envelope, runs
/// the shared dispatcher, returns the response as the POST body.
/// Streaming requests get their ack here and the pump runs in a
/// background task pushing `$/stream/data` / `$/stream/end`
/// notifications onto the broadcast channel.
async fn post_rpc<P: BowirePlugin>(
    State(state): State<HttpState<P>>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let req: Request = match serde_json::from_value(payload) {
        Ok(r) => r,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    let plugin = state.plugin.clone();
    let outcome = dispatch(plugin, req).await;

    match outcome {
        DispatchResult::Reply(resp) | DispatchResult::Shutdown(resp) => Ok(Json(
            serde_json::to_value(resp).unwrap_or_else(|_| json!({})),
        )),
        DispatchResult::Stream {
            ack,
            stream_id,
            mut stream,
        } => {
            // Spawn the pump after the response is constructed but
            // before we return — so the workbench's subscription
            // (already live by the time it issued the POST) catches
            // the first frame.
            let notifications = state.notifications.clone();
            tokio::spawn(async move {
                while let Some(frame) = stream.next().await {
                    let _ = notifications.send(Notification {
                        jsonrpc: "2.0",
                        method: "$/stream/data".into(),
                        params: Some(json!({ "streamId": stream_id, "message": frame })),
                    });
                }
                let _ = notifications.send(Notification {
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
/// one of these open per session; every server-initiated
/// notification gets serialised as `data: <json>\n\n`.
async fn sse_subscribe<P: BowirePlugin>(State(state): State<HttpState<P>>) -> AxumResponse {
    let rx = state.notifications.subscribe();
    let body_stream = BroadcastStream::new(rx).filter_map(|item| async move {
        let n = item.ok()?;
        let payload = serde_json::to_string(&n).ok()?;
        Some(Ok::<_, Infallible>(format!("data: {payload}\n\n")))
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
