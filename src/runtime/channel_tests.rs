//! Duplex channels through the dispatcher, the way the host drives them:
//! `openChannel` with a host-minted id, `channel.send` / `channel.close`
//! requests, `$/channel/data` / `$/channel/closed` notifications back.
//!
//! No test waits on time. Each one reads the notification queue, and the
//! final "nothing else arrived" check drains it until every sender is gone —
//! the session's and the ones inside the plugin's channel handles — so it
//! returns exactly when the plugin's tasks have finished.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::channel::{Channel, ChannelHandler, ChannelRequest};
use crate::models::{InvokeResult, ServiceInfo};
use crate::plugin::{BowirePlugin, Capabilities, DiscoverRequest, InvokeRequest};
use crate::runtime::dispatch::{dispatch, DispatchResult, Session, CHANNEL_CLOSED};
use crate::runtime::jsonrpc::{Notification, Request, Response};

/// Speaks first, echoes every frame, closes on `bye`, and pushes one last
/// `flushed` frame once its inbound side has ended — which only reaches the
/// host when the host, not the plugin, did the closing.
struct Chat {
    observed: mpsc::UnboundedSender<String>,
}

#[async_trait]
impl BowirePlugin for Chat {
    fn id(&self) -> &str {
        "chat"
    }

    fn name(&self) -> &str {
        "Chat"
    }

    async fn discover(&self, _request: DiscoverRequest) -> Vec<ServiceInfo> {
        Vec::new()
    }

    async fn invoke(&self, _request: InvokeRequest) -> InvokeResult {
        InvokeResult::default()
    }

    fn channels(&self) -> Option<&dyn ChannelHandler> {
        Some(self)
    }
}

#[async_trait]
impl ChannelHandler for Chat {
    async fn open_channel(
        &self,
        request: ChannelRequest,
        mut channel: Channel,
    ) -> Result<(), String> {
        match request.method.as_str() {
            "Refuse" => return Err("refused by the test plugin".into()),
            // Accepts, then lets the handle go before the ack is out.
            "Drop" => return Ok(()),
            _ => {}
        }
        channel.send(format!("hello {}", request.method));
        let observed = self.observed.clone();
        tokio::spawn(async move {
            while let Some(frame) = channel.recv().await {
                if frame == "bye" {
                    channel.close();
                } else {
                    channel.send(format!("echo {frame}"));
                }
            }
            let _ = observed.send(format!("inbound ended {}", channel.id()));
            channel.send("flushed");
        });
        Ok(())
    }
}

struct Harness {
    plugin: Arc<Chat>,
    session: Session,
    notifications: mpsc::UnboundedReceiver<Notification>,
    observed: mpsc::UnboundedReceiver<String>,
    next_id: i64,
}

impl Harness {
    fn new() -> Self {
        let (notify_tx, notifications) = mpsc::unbounded_channel();
        let (observed_tx, observed) = mpsc::unbounded_channel();
        Self {
            plugin: Arc::new(Chat {
                observed: observed_tx,
            }),
            session: Session::new(Arc::new(move |n| {
                let _ = notify_tx.send(n);
            })),
            notifications,
            observed,
            next_id: 0,
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Response {
        self.next_id += 1;
        let req = Request {
            id: Some(json!(self.next_id)),
            method: method.to_owned(),
            params: Some(params),
        };
        match dispatch(self.plugin.clone(), req, &self.session).await {
            DispatchResult::Reply(r) => r,
            _ => panic!("channel calls answer with a plain reply"),
        }
    }

    async fn open(&mut self, channel_id: &str, method: &str) -> Response {
        self.call(
            "openChannel",
            json!({
                "channelId": channel_id,
                "serverUrl": "chat://test",
                "service": "Room",
                "method": method,
                "showInternalServices": false,
                "metadata": {},
            }),
        )
        .await
    }

    async fn send(&mut self, channel_id: &str, message: &str) -> Response {
        self.call(
            "channel.send",
            json!({ "channelId": channel_id, "message": message }),
        )
        .await
    }

    async fn close(&mut self, channel_id: &str) -> Response {
        self.call("channel.close", json!({ "channelId": channel_id }))
            .await
    }

    /// The next notification, as `(method, channelId, message)`.
    async fn next(&mut self) -> (String, String, Option<String>) {
        let n = self
            .notifications
            .recv()
            .await
            .expect("a notification is due");
        frame(n)
    }

    /// Everything still to come, once the plugin's tasks are done: drops the
    /// session and the plugin, then drains both queues to their end.
    async fn finish(self) -> (Vec<(String, String, Option<String>)>, Vec<String>) {
        let Self {
            plugin,
            session,
            mut notifications,
            mut observed,
            ..
        } = self;
        drop(session);
        drop(plugin);
        let mut rest = Vec::new();
        while let Some(n) = notifications.recv().await {
            rest.push(frame(n));
        }
        let mut seen = Vec::new();
        while let Some(o) = observed.recv().await {
            seen.push(o);
        }
        (rest, seen)
    }
}

fn frame(n: Notification) -> (String, String, Option<String>) {
    let params = n.params.unwrap_or(Value::Null);
    (
        n.method,
        params["channelId"].as_str().unwrap_or_default().to_owned(),
        params["message"].as_str().map(str::to_owned),
    )
}

fn data(id: &str, message: &str) -> (String, String, Option<String>) {
    ("$/channel/data".into(), id.into(), Some(message.into()))
}

fn closed(id: &str) -> (String, String, Option<String>) {
    ("$/channel/closed".into(), id.into(), None)
}

fn error_code(r: &Response) -> Option<i32> {
    r.error.as_ref().map(|e| e.code)
}

#[tokio::test]
async fn open_send_both_ways_then_host_close() {
    let mut h = Harness::new();

    let ack = h.open("ab12", "Chat").await;
    assert_eq!(ack.result, Some(json!({ "channelId": "ab12" })));
    // Plugin → host, unprompted, under the host's id.
    assert_eq!(h.next().await, data("ab12", "hello Chat"));

    // Host → plugin → host.
    let sent = h.send("ab12", "ping").await;
    assert_eq!(sent.result, Some(json!(true)));
    assert_eq!(h.next().await, data("ab12", "echo ping"));

    // The host closes its send side; a send after that is refused with the
    // error the host maps onto SendAsync → false.
    assert!(h.close("ab12").await.result.is_some());
    assert_eq!(
        error_code(&h.send("ab12", "late").await),
        Some(CHANNEL_CLOSED)
    );

    // The plugin's inbound stream ended once, it flushed, and its handle
    // going away closed the host's read stream once.
    let (rest, seen) = h.finish().await;
    assert_eq!(rest, vec![data("ab12", "flushed"), closed("ab12")]);
    assert_eq!(seen, vec!["inbound ended ab12".to_owned()]);
}

#[tokio::test]
async fn plugin_close_ends_the_host_stream_once() {
    let mut h = Harness::new();

    assert!(h.open("cd34", "Chat").await.result.is_some());
    assert_eq!(h.next().await, data("cd34", "hello Chat"));

    assert_eq!(h.send("cd34", "bye").await.result, Some(json!(true)));
    assert_eq!(h.next().await, closed("cd34"));

    // Gone from the host's point of view: sends fail, and the close the
    // host sends while disposing its side gets an error it swallows.
    assert_eq!(
        error_code(&h.send("cd34", "after").await),
        Some(CHANNEL_CLOSED)
    );
    assert_eq!(error_code(&h.close("cd34").await), Some(CHANNEL_CLOSED));
    assert_eq!(h.session.channels.len(), 0);

    // No second close, and the frame pushed after the close went nowhere.
    let (rest, seen) = h.finish().await;
    assert_eq!(rest, Vec::new());
    assert_eq!(seen, vec!["inbound ended cd34".to_owned()]);
}

#[tokio::test]
async fn two_channels_at_once_do_not_cross_frames() {
    let mut h = Harness::new();

    assert!(h.open("left", "Chat").await.result.is_some());
    assert!(h.open("right", "Chat").await.result.is_some());
    for i in 0..5 {
        assert!(h.send("left", &format!("L{i}")).await.result.is_some());
        assert!(h.send("right", &format!("R{i}")).await.result.is_some());
    }
    assert!(h.close("left").await.result.is_some());
    assert_eq!(h.send("right", "bye").await.result, Some(json!(true)));

    let (all, seen) = h.finish().await;
    let of = |id: &str| -> Vec<_> { all.iter().filter(|f| f.1 == id).cloned().collect() };

    let mut left = vec![data("left", "hello Chat")];
    left.extend((0..5).map(|i| data("left", &format!("echo L{i}"))));
    left.extend([data("left", "flushed"), closed("left")]);
    assert_eq!(of("left"), left);

    let mut right = vec![data("right", "hello Chat")];
    right.extend((0..5).map(|i| data("right", &format!("echo R{i}"))));
    right.push(closed("right"));
    assert_eq!(of("right"), right);

    assert_eq!(all.len(), left.len() + right.len());
    let mut seen = seen;
    seen.sort();
    assert_eq!(seen, vec!["inbound ended left", "inbound ended right"]);
}

#[tokio::test]
async fn a_refused_open_leaves_nothing_behind() {
    let mut h = Harness::new();

    assert_eq!(error_code(&h.open("nope", "Refuse").await), Some(-32000));
    assert_eq!(h.session.channels.len(), 0);
    assert_eq!(error_code(&h.send("nope", "x").await), Some(CHANNEL_CLOSED));

    // The host unsubscribed on the error; nothing is sent for the id.
    let (rest, _) = h.finish().await;
    assert_eq!(rest, Vec::new());
}

#[tokio::test]
async fn a_handle_dropped_during_open_closes_after_the_ack() {
    let mut h = Harness::new();

    assert!(h.open("gone", "Drop").await.result.is_some());
    assert_eq!(h.next().await, closed("gone"));
    assert_eq!(h.session.channels.len(), 0);

    let (rest, _) = h.finish().await;
    assert_eq!(rest, Vec::new());
}

#[tokio::test]
async fn an_id_already_open_is_refused_and_the_first_channel_lives_on() {
    let mut h = Harness::new();

    assert!(h.open("twice", "Chat").await.result.is_some());
    assert_eq!(error_code(&h.open("twice", "Chat").await), Some(-32602));
    assert_eq!(h.next().await, data("twice", "hello Chat"));
    assert!(h.send("twice", "still").await.result.is_some());
    assert_eq!(h.next().await, data("twice", "echo still"));
}

#[tokio::test]
async fn open_without_a_channel_id_is_refused() {
    let mut h = Harness::new();

    let reply = h
        .call(
            "openChannel",
            json!({ "service": "Room", "method": "Chat" }),
        )
        .await;
    assert_eq!(error_code(&reply), Some(-32602));
    assert_eq!(h.session.channels.len(), 0);
}

#[tokio::test]
async fn initialize_reports_channels_when_a_handler_is_there() {
    let mut h = Harness::new();

    let caps = h.call("initialize", json!({})).await.result.unwrap()["capabilities"].clone();
    assert_eq!(caps["channels"], json!(true));
}

/// Claims channels without a handler to route them to.
struct Claims;

#[async_trait]
impl BowirePlugin for Claims {
    fn id(&self) -> &str {
        "claims"
    }

    fn name(&self) -> &str {
        "Claims"
    }

    async fn discover(&self, _request: DiscoverRequest) -> Vec<ServiceInfo> {
        Vec::new()
    }

    async fn invoke(&self, _request: InvokeRequest) -> InvokeResult {
        InvokeResult::default()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            channels: true,
            ..Capabilities::default()
        }
    }
}

#[tokio::test]
async fn channels_cannot_be_claimed_without_a_handler() {
    let session = Session::new(Arc::new(|_| {}));
    let req = |method: &str, params: Value| Request {
        id: Some(json!(1)),
        method: method.to_owned(),
        params: Some(params),
    };

    let DispatchResult::Reply(init) =
        dispatch(Arc::new(Claims), req("initialize", json!({})), &session).await
    else {
        panic!("initialize answers with a plain reply");
    };
    assert_eq!(
        init.result.unwrap()["capabilities"]["channels"],
        json!(false)
    );

    let DispatchResult::Reply(open) = dispatch(
        Arc::new(Claims),
        req("openChannel", json!({ "channelId": "x" })),
        &session,
    )
    .await
    else {
        panic!("openChannel answers with a plain reply");
    };
    assert_eq!(error_code(&open), Some(-32601));
}
