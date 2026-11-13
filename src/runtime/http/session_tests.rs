//! Several hosts on one http sidecar (Kuestenlogik/Bowire#1014): each names
//! itself with the `Bowire-Session` header, owns what it opens, reads only
//! its own frames, and takes only its own streams and channels with it when
//! it leaves.
//!
//! Over a real socket with a raw HTTP/1.1 client, like the tests next door.
//! No test waits on time: each reads an SSE stream up to the frame it
//! expects, or the plugin's own report (`seen`) up to the entry it expects,
//! and `stop` drains that report until the plugin and all its tasks are gone.

use std::net::SocketAddr;

use futures::stream::{self, BoxStream, StreamExt};
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use super::tests::{post_as, read_until, start_with, subscribe_as, Server};
use crate::{
    BowirePlugin, Channel, ChannelHandler, ChannelRequest, DiscoverRequest, InvokeRequest,
    InvokeResult, ServiceInfo, StreamHandler,
};

const A: Option<&str> = Some("host-a");
const B: Option<&str> = Some("host-b");
/// A host older than the rule: it sends no header.
const UNNAMED: Option<&str> = None;

/// Greets and echoes on every channel, serves streams that tick once and
/// then never end, and reports what happens to it: `got <channel> <frame>`,
/// `closed <channel>`, `stopped <stream method>` and `shutdown hook`.
struct Rooms {
    seen: mpsc::UnboundedSender<String>,
}

fn rooms() -> (Rooms, mpsc::UnboundedReceiver<String>) {
    let (seen_tx, seen) = mpsc::unbounded_channel();
    (Rooms { seen: seen_tx }, seen)
}

#[async_trait::async_trait]
impl BowirePlugin for Rooms {
    fn id(&self) -> &str {
        "rooms"
    }

    fn name(&self) -> &str {
        "Rooms"
    }

    async fn discover(&self, _: DiscoverRequest) -> Vec<ServiceInfo> {
        vec![]
    }

    async fn invoke(&self, _: InvokeRequest) -> InvokeResult {
        InvokeResult::ok("{}")
    }

    fn streams(&self) -> Option<&dyn StreamHandler> {
        Some(self)
    }

    fn channels(&self) -> Option<&dyn ChannelHandler> {
        Some(self)
    }

    async fn shutdown(&self) {
        let _ = self.seen.send("shutdown hook".into());
    }
}

#[async_trait::async_trait]
impl ChannelHandler for Rooms {
    async fn open_channel(&self, _: ChannelRequest, mut channel: Channel) -> Result<(), String> {
        channel.send(format!("hello {}", channel.id()));
        let seen = self.seen.clone();
        tokio::spawn(async move {
            while let Some(frame) = channel.recv().await {
                let _ = seen.send(format!("got {} {frame}", channel.id()));
                channel.send(format!("echo {frame}"));
            }
            let _ = seen.send(format!("closed {}", channel.id()));
        });
        Ok(())
    }
}

/// Reports when the runtime lets go of the stream it sits in.
struct Stopped {
    seen: mpsc::UnboundedSender<String>,
    method: String,
}

impl Drop for Stopped {
    fn drop(&mut self) {
        let _ = self.seen.send(format!("stopped {}", self.method));
    }
}

#[async_trait::async_trait]
impl StreamHandler for Rooms {
    async fn invoke_stream(&self, request: InvokeRequest) -> BoxStream<'static, String> {
        let stopped = Stopped {
            seen: self.seen.clone(),
            method: request.method,
        };
        Box::pin(
            stream::once(async { "tick".to_owned() })
                .chain(stream::pending())
                .inspect(move |_| {
                    // Lives as long as the stream does.
                    let _ = &stopped;
                }),
        )
    }
}

async fn open(addr: SocketAddr, session: Option<&str>, channel: &str) {
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"openChannel","params":{{"channelId":"{channel}"}}}}"#
    );
    let reply = post_as(addr, session, &body).await;
    assert!(
        reply.contains(&format!(r#""channelId":"{channel}""#)),
        "{reply}"
    );
}

async fn send(addr: SocketAddr, session: Option<&str>, channel: &str, message: &str) -> String {
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"channel.send","params":{{"channelId":"{channel}","message":"{message}"}}}}"#
    );
    post_as(addr, session, &body).await
}

async fn close(addr: SocketAddr, session: Option<&str>, channel: &str) -> String {
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"channel.close","params":{{"channelId":"{channel}"}}}}"#
    );
    post_as(addr, session, &body).await
}

/// Starts a stream that ticks once and then stays open; `method` is how
/// the plugin names it in its report.
async fn start_stream(addr: SocketAddr, session: Option<&str>, stream: &str, method: &str) {
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"invokeStream","params":{{"streamId":"{stream}","method":"{method}"}}}}"#
    );
    let reply = post_as(addr, session, &body).await;
    assert!(
        reply.contains(&format!(r#""streamId":"{stream}""#)),
        "{reply}"
    );
}

async fn shutdown(addr: SocketAddr, session: Option<&str>) {
    let reply = post_as(
        addr,
        session,
        r#"{"jsonrpc":"2.0","id":5,"method":"shutdown"}"#,
    )
    .await;
    assert!(reply.contains(r#""result":{}"#), "{reply}");
}

fn accepted(reply: &str) -> bool {
    reply.contains(r#""result":true"#)
}

/// The "channel is closed" error, the same for a channel that is gone, one
/// that never was, and one of another session.
fn refused(reply: &str) -> bool {
    reply.contains(r#""code":-32001"#)
}

/// Sends `message` on `channel` and reads the session's SSE stream up to
/// its echo. Returns every frame read on the way.
async fn echo(
    addr: SocketAddr,
    session: Option<&str>,
    sse: &mut BufReader<TcpStream>,
    channel: &str,
    message: &str,
) -> Vec<String> {
    let reply = send(addr, session, channel, message).await;
    assert!(accepted(&reply), "{reply}");
    read_until(sse, &format!("echo {message}")).await
}

/// Reads the plugin's report until every entry of `wanted` has come up, in
/// whatever order. Returns everything read.
async fn seen_all(seen: &mut mpsc::UnboundedReceiver<String>, wanted: &[&str]) -> Vec<String> {
    let mut read = Vec::new();
    while !wanted.iter().all(|w| read.iter().any(|r| r == w)) {
        let entry = seen.recv().await;
        read.push(entry.unwrap_or_else(|| panic!("the report ended before {wanted:?}: {read:?}")));
    }
    read
}

/// Stops the server and returns the rest of the plugin's report, complete:
/// the queue ends when the plugin and every task it started are gone.
async fn stop(server: Server, mut seen: mpsc::UnboundedReceiver<String>) -> Vec<String> {
    let _ = server.stop.send(());
    server.served.await.unwrap().unwrap();
    let mut rest = Vec::new();
    while let Some(entry) = seen.recv().await {
        rest.push(entry);
    }
    rest
}

fn position(report: &[String], entry: &str) -> usize {
    report
        .iter()
        .position(|r| r == entry)
        .unwrap_or_else(|| panic!("no '{entry}' in {report:?}"))
}

fn count(report: &[String], entry: &str) -> usize {
    report.iter().filter(|r| *r == entry).count()
}

/// The case the rule exists for: two hosts, one says `shutdown`. Its channel
/// closes, the other host's keeps working both ways, and the plugin is not
/// told to shut down until the server itself stops.
#[tokio::test]
async fn a_host_that_leaves_takes_its_own_channels_and_nobody_elses() {
    let (plugin, mut seen) = rooms();
    let server = start_with(plugin).await;
    let addr = server.addr;
    let mut a = subscribe_as(addr, A).await;
    let mut b = subscribe_as(addr, B).await;
    open(addr, A, "a1").await;
    read_until(&mut a, "hello a1").await;
    open(addr, B, "b1").await;
    read_until(&mut b, "hello b1").await;

    shutdown(addr, A).await;

    // The leaving host's channel is closed on both sides: its read stream
    // ends, the plugin's inbound side ended, a send is refused.
    read_until(&mut a, r#""method":"$/channel/closed""#).await;
    let before = seen_all(&mut seen, &["closed a1"]).await;
    assert!(refused(&send(addr, A, "a1", "late").await));
    // Ending it again finds nothing left to end.
    shutdown(addr, A).await;

    // The other host's channel still delivers, in and out.
    echo(addr, B, &mut b, "b1", "ping").await;

    // The plugin's hook ran once, and only when the server stopped: after
    // the frame host B sent once host A had long left.
    let after = stop(server, seen).await;
    assert_eq!(count(&before, "shutdown hook"), 0, "{before:?}");
    assert_eq!(count(&after, "shutdown hook"), 1, "{after:?}");
    assert!(
        position(&after, "got b1 ping") < position(&after, "shutdown hook"),
        "{after:?}"
    );
    assert!(
        position(&after, "got b1 ping") < position(&after, "closed b1"),
        "{after:?}"
    );
}

/// A frame goes to the SSE streams of the session that owns its channel or
/// stream. Each host here is read to its end only after the others were
/// busy, so a frame delivered to the wrong one would be among its lines.
#[tokio::test]
async fn a_host_reads_only_its_own_frames() {
    let (plugin, seen) = rooms();
    let server = start_with(plugin).await;
    let addr = server.addr;
    let mut a = subscribe_as(addr, A).await;
    let mut b = subscribe_as(addr, B).await;
    let mut unnamed = subscribe_as(addr, UNNAMED).await;

    open(addr, A, "a1").await;
    let mut a_lines = echo(addr, A, &mut a, "a1", "first").await;

    open(addr, B, "b1").await;
    start_stream(addr, B, "b-stream", "b-feed").await;
    let mut b_lines = read_until(&mut b, r#""streamId":"b-stream""#).await;
    b_lines.extend(echo(addr, B, &mut b, "b1", "last").await);

    open(addr, UNNAMED, "u1").await;
    let unnamed_lines = echo(addr, UNNAMED, &mut unnamed, "u1", "last").await;

    a_lines.extend(echo(addr, A, &mut a, "a1", "last").await);

    assert_eq!(a_lines.len(), 3, "hello and two echoes: {a_lines:?}");
    assert!(
        a_lines.iter().all(|l| l.contains(r#""channelId":"a1""#)),
        "{a_lines:?}"
    );
    assert_eq!(b_lines.len(), 3, "hello, a tick and the echo: {b_lines:?}");
    assert!(
        b_lines
            .iter()
            .all(|l| l.contains(r#""channelId":"b1""#) || l.contains(r#""streamId":"b-stream""#)),
        "{b_lines:?}"
    );
    assert_eq!(unnamed_lines.len(), 2, "{unnamed_lines:?}");
    assert!(
        unnamed_lines
            .iter()
            .all(|l| l.contains(r#""channelId":"u1""#)),
        "{unnamed_lines:?}"
    );

    stop(server, seen).await;
}

/// A host that crashed or lost the network says nothing; its SSE stream
/// closing is what ends its session.
#[tokio::test]
async fn a_closed_sse_stream_ends_the_hosts_channels_and_streams() {
    let (plugin, mut seen) = rooms();
    let server = start_with(plugin).await;
    let addr = server.addr;
    let mut a = subscribe_as(addr, A).await;
    let mut b = subscribe_as(addr, B).await;
    open(addr, A, "a1").await;
    read_until(&mut a, "hello a1").await;
    start_stream(addr, A, "a-stream", "a-feed").await;
    read_until(&mut a, r#""streamId":"a-stream""#).await;
    open(addr, B, "b1").await;
    read_until(&mut b, "hello b1").await;

    drop(a);

    seen_all(&mut seen, &["closed a1", "stopped a-feed"]).await;
    assert!(refused(&send(addr, A, "a1", "late").await));
    echo(addr, B, &mut b, "b1", "ping").await;

    let after = stop(server, seen).await;
    assert!(
        position(&after, "got b1 ping") < position(&after, "closed b1"),
        "{after:?}"
    );
}

/// `shutdown` ends the caller's streams and no others.
#[tokio::test]
async fn a_host_that_leaves_stops_its_own_streams_and_nobody_elses() {
    let (plugin, mut seen) = rooms();
    let server = start_with(plugin).await;
    let addr = server.addr;
    let mut a = subscribe_as(addr, A).await;
    let mut b = subscribe_as(addr, B).await;
    start_stream(addr, A, "a-stream", "a-feed").await;
    read_until(&mut a, r#""streamId":"a-stream""#).await;
    start_stream(addr, B, "b-stream", "b-feed").await;
    read_until(&mut b, r#""streamId":"b-stream""#).await;

    shutdown(addr, A).await;

    // The host's read stream gets its end, and the plugin's stream is let go.
    let ended = read_until(&mut a, r#""method":"$/stream/end""#).await;
    assert!(
        ended.last().unwrap().contains(r#""streamId":"a-stream""#),
        "{ended:?}"
    );
    let before = seen_all(&mut seen, &["stopped a-feed"]).await;
    assert_eq!(count(&before, "stopped b-feed"), 0, "{before:?}");

    // Host B's stream is still running when B is busy afterwards.
    open(addr, B, "b1").await;
    echo(addr, B, &mut b, "b1", "ping").await;
    let after = stop(server, seen).await;
    assert!(
        position(&after, "got b1 ping") < position(&after, "stopped b-feed"),
        "{after:?}"
    );
}

/// A host older than the rule cannot say which streams are its own, so its
/// `shutdown` is acknowledged and ends nothing: not the unnamed session's
/// channels, and not a named one's.
#[tokio::test]
async fn shutdown_without_the_header_ends_nothing() {
    let (plugin, seen) = rooms();
    let server = start_with(plugin).await;
    let addr = server.addr;
    let mut a = subscribe_as(addr, A).await;
    let mut unnamed = subscribe_as(addr, UNNAMED).await;
    open(addr, A, "a1").await;
    open(addr, UNNAMED, "u1").await;

    shutdown(addr, UNNAMED).await;

    echo(addr, UNNAMED, &mut unnamed, "u1", "ping").await;
    echo(addr, A, &mut a, "a1", "ping").await;

    let all = stop(server, seen).await;
    assert!(
        position(&all, "got u1 ping") < position(&all, "closed u1"),
        "{all:?}"
    );
    assert!(
        position(&all, "got a1 ping") < position(&all, "closed a1"),
        "{all:?}"
    );
    assert_eq!(count(&all, "shutdown hook"), 1, "{all:?}");
}

/// A channel id is only good inside the session that opened it. Anybody
/// else gets the error for a channel that never existed, and may use the
/// same id for a channel of its own.
#[tokio::test]
async fn a_host_cannot_reach_another_hosts_channel() {
    let (plugin, seen) = rooms();
    let server = start_with(plugin).await;
    let addr = server.addr;
    let mut a = subscribe_as(addr, A).await;
    let mut b = subscribe_as(addr, B).await;
    open(addr, A, "a1").await;

    assert!(refused(&send(addr, B, "a1", "intruder").await));
    assert!(refused(&close(addr, B, "a1").await));
    assert!(refused(&send(addr, UNNAMED, "a1", "intruder").await));
    assert!(refused(&close(addr, UNNAMED, "a1").await));

    // Host A's channel is untouched, and host B's own "a1" is another one.
    open(addr, B, "a1").await;
    let b_lines = echo(addr, B, &mut b, "a1", "from-b").await;
    let a_lines = echo(addr, A, &mut a, "a1", "from-a").await;
    assert_eq!(b_lines.len(), 2, "hello and its own echo: {b_lines:?}");
    assert_eq!(a_lines.len(), 2, "hello and its own echo: {a_lines:?}");

    let all = stop(server, seen).await;
    let got: Vec<_> = all.iter().filter(|e| e.starts_with("got ")).collect();
    assert_eq!(got, ["got a1 from-b", "got a1 from-a"], "{all:?}");
}
