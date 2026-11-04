//! Duplex channels (`openChannel` / `channel.send` / `channel.close`).
//!
//! A plugin that serves duplex methods implements [`ChannelHandler`] and
//! returns itself from [`crate::BowirePlugin::channels`]. The runtime then
//! reports `capabilities.channels: true` and routes the host's channel
//! calls, keyed by the `channelId` the **host** minted:
//!
//! - `openChannel` calls [`ChannelHandler::open_channel`] with a fresh
//!   [`Channel`] carrying that id.
//! - `channel.send` delivers a frame to [`Channel::recv`].
//! - `channel.close` closes the host's send side: [`Channel::recv`]
//!   returns `None`, once. The plugin's push side stays open so it can
//!   flush what is left, then closes.
//! - [`Channel::send`] / [`ChannelSender::send`] push a `$/channel/data`
//!   frame to the host; [`Channel::close`] (or dropping the last handle)
//!   sends `$/channel/closed`, exactly once, which ends the host's read
//!   stream.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::mpsc;

use crate::runtime::jsonrpc::Notification;

/// What the host asked to open. Mirrors the `openChannel` params.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ChannelRequest {
    /// The target server the workbench points at.
    pub server_url: String,
    /// Service name, as discovered.
    pub service: String,
    /// Method name, as discovered.
    pub method: String,
    /// Whether the workbench shows internal services.
    pub show_internal: bool,
    /// Request metadata (headers) the operator set.
    pub metadata: HashMap<String, String>,
}

/// Opens duplex channels. Implement it next to [`crate::BowirePlugin`] and
/// return `Some(self)` from [`crate::BowirePlugin::channels`].
#[async_trait]
pub trait ChannelHandler: Send + Sync {
    /// Accept or refuse a channel. On `Ok`, the host gets its ack and the
    /// channel is live: keep `channel` (typically by moving it into a
    /// spawned task) and drive it. Frames pushed before this returns are
    /// delivered too — the host subscribed before it asked.
    ///
    /// On `Err`, the host gets a JSON-RPC error and reports that the
    /// channel could not be opened; nothing else reaches the wire for it.
    async fn open_channel(&self, request: ChannelRequest, channel: Channel) -> Result<(), String>;
}

/// One open duplex channel, as the plugin sees it: an inbound side the
/// host writes to with `channel.send`, and an outbound side the plugin
/// pushes frames on. Dropping it (and every [`ChannelSender`] taken from
/// it) closes the channel.
#[derive(Debug)]
pub struct Channel {
    inbound: mpsc::UnboundedReceiver<String>,
    sender: ChannelSender,
}

impl Channel {
    /// The host-minted channel id.
    pub fn id(&self) -> &str {
        &self.sender.inner.entry.id
    }

    /// Next frame the host sent. `None` once the host closed its send side
    /// or the channel closed from this side.
    pub async fn recv(&mut self) -> Option<String> {
        self.inbound.recv().await
    }

    /// Push a frame to the host as `$/channel/data`. `false` when the
    /// channel is already closed.
    pub fn send(&self, message: impl Into<String>) -> bool {
        self.sender.send(message)
    }

    /// Close the channel: the host's read stream ends. Idempotent.
    pub fn close(&self) {
        self.sender.close();
    }

    /// A cloneable push handle, for pushing from another task.
    pub fn sender(&self) -> ChannelSender {
        self.sender.clone()
    }

    /// Split into the inbound side and the push handle.
    pub fn into_parts(self) -> (ChannelReceiver, ChannelSender) {
        (
            ChannelReceiver {
                inbound: self.inbound,
            },
            self.sender,
        )
    }
}

/// The inbound half of a [`Channel`] after [`Channel::into_parts`].
#[derive(Debug)]
pub struct ChannelReceiver {
    inbound: mpsc::UnboundedReceiver<String>,
}

impl ChannelReceiver {
    /// See [`Channel::recv`].
    pub async fn recv(&mut self) -> Option<String> {
        self.inbound.recv().await
    }
}

/// Cloneable push handle for one channel. The channel closes when
/// [`ChannelSender::close`] is called or the last handle is dropped.
#[derive(Debug, Clone)]
pub struct ChannelSender {
    inner: Arc<SenderInner>,
}

impl ChannelSender {
    /// The host-minted channel id.
    pub fn id(&self) -> &str {
        &self.inner.entry.id
    }

    /// See [`Channel::send`].
    pub fn send(&self, message: impl Into<String>) -> bool {
        self.inner.entry.push(&self.inner.notify, message.into())
    }

    /// See [`Channel::close`].
    pub fn close(&self) {
        self.inner.entry.close_from_plugin(&self.inner.notify);
    }
}

struct SenderInner {
    entry: Arc<Entry>,
    notify: Notify,
}

impl std::fmt::Debug for SenderInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SenderInner")
            .field("id", &self.entry.id)
            .finish_non_exhaustive()
    }
}

impl Drop for SenderInner {
    fn drop(&mut self) {
        self.entry.close_from_plugin(&self.notify);
    }
}

// ---- runtime side -------------------------------------------------

/// Sink for server-initiated notifications. Synchronous so a dropped
/// handle can still announce `$/channel/closed`; each runtime backs it with
/// an ordered queue onto its wire.
pub(crate) type Notify = Arc<dyn Fn(Notification) + Send + Sync>;

/// Where a channel is in its life. Only the transitions below put anything
/// on the wire, which is what makes `$/channel/closed` exactly-once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// `open_channel` is running; the ack has not gone out.
    Opening,
    /// The plugin closed before its `open_channel` returned — announce
    /// the close once the outcome is known.
    OpeningClosed,
    /// Acked and live.
    Open,
    /// Finished: closed, or refused by the plugin.
    Done,
}

struct State {
    phase: Phase,
    /// Feeds [`Channel::recv`]. `None` once the host closed its send side
    /// or the channel finished.
    inbound: Option<mpsc::UnboundedSender<String>>,
}

struct Entry {
    id: String,
    state: Mutex<State>,
    registry: ChannelRegistry,
}

impl Entry {
    fn lock(&self) -> MutexGuard<'_, State> {
        // A poisoned lock means a panic while holding it; the state itself
        // is plain data and still consistent enough to wind down.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn push(&self, notify: &Notify, message: String) -> bool {
        let state = self.lock();
        match state.phase {
            Phase::Opening | Phase::Open => {
                // Under the lock, so a concurrent close cannot overtake it.
                notify(data_notification(&self.id, message));
                true
            }
            Phase::OpeningClosed | Phase::Done => false,
        }
    }

    fn close_from_plugin(&self, notify: &Notify) {
        let mut state = self.lock();
        state.inbound = None;
        match state.phase {
            Phase::Opening => state.phase = Phase::OpeningClosed,
            Phase::Open => {
                state.phase = Phase::Done;
                notify(closed_notification(&self.id));
                drop(state);
                self.registry.remove(&self.id);
            }
            Phase::OpeningClosed | Phase::Done => {}
        }
    }
}

/// Every channel this runtime has open, by host-minted id.
#[derive(Clone, Default)]
pub(crate) struct ChannelRegistry {
    channels: Arc<Mutex<HashMap<String, Arc<Entry>>>>,
}

/// Why a host call on a channel was refused.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ChannelRefusal {
    /// No channel with that id is open.
    Unknown,
    /// The channel exists but no longer accepts frames from the host.
    Closed,
    /// `openChannel` named an id that is already in use.
    Duplicate,
}

impl ChannelRegistry {
    fn map(&self) -> MutexGuard<'_, HashMap<String, Arc<Entry>>> {
        self.channels.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn remove(&self, id: &str) {
        self.map().remove(id);
    }

    /// Register `id` and build the plugin's handle for it.
    pub(crate) fn begin_open(&self, id: &str, notify: Notify) -> Result<Channel, ChannelRefusal> {
        let (tx, rx) = mpsc::unbounded_channel();
        let entry = Arc::new(Entry {
            id: id.to_owned(),
            state: Mutex::new(State {
                phase: Phase::Opening,
                inbound: Some(tx),
            }),
            registry: self.clone(),
        });
        {
            let mut map = self.map();
            if map.contains_key(id) {
                return Err(ChannelRefusal::Duplicate);
            }
            map.insert(id.to_owned(), entry.clone());
        }
        Ok(Channel {
            inbound: rx,
            sender: ChannelSender {
                inner: Arc::new(SenderInner { entry, notify }),
            },
        })
    }

    /// The plugin's `open_channel` returned. `accepted` decides whether the
    /// channel goes live; a close the plugin made in the meantime is
    /// announced now.
    pub(crate) fn finish_open(&self, id: &str, accepted: bool, notify: &Notify) {
        let Some(entry) = self.map().get(id).cloned() else {
            return;
        };
        let mut state = entry.lock();
        match (state.phase, accepted) {
            (Phase::Opening, true) => state.phase = Phase::Open,
            (Phase::OpeningClosed, true) => {
                state.phase = Phase::Done;
                notify(closed_notification(id));
                drop(state);
                self.remove(id);
            }
            (_, false) => {
                state.phase = Phase::Done;
                state.inbound = None;
                drop(state);
                self.remove(id);
            }
            (Phase::Open | Phase::Done, true) => {}
        }
    }

    /// `channel.send`: hand a frame to the plugin.
    pub(crate) fn deliver(&self, id: &str, message: String) -> Result<(), ChannelRefusal> {
        let entry = self.map().get(id).cloned().ok_or(ChannelRefusal::Unknown)?;
        let state = entry.lock();
        match (&state.phase, &state.inbound) {
            (Phase::Opening | Phase::Open, Some(tx)) => {
                tx.send(message).map_err(|_| ChannelRefusal::Closed)
            }
            _ => Err(ChannelRefusal::Closed),
        }
    }

    /// `channel.close`: end the host's send side. The plugin sees
    /// [`Channel::recv`] return `None` and closes when it is done.
    pub(crate) fn close_send_side(&self, id: &str) -> Result<(), ChannelRefusal> {
        let entry = self.map().get(id).cloned().ok_or(ChannelRefusal::Unknown)?;
        entry.lock().inbound = None;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map().len()
    }
}

fn data_notification(id: &str, message: String) -> Notification {
    Notification {
        jsonrpc: "2.0",
        method: "$/channel/data".into(),
        params: Some(json!({ "channelId": id, "message": message })),
    }
}

fn closed_notification(id: &str) -> Notification {
    Notification {
        jsonrpc: "2.0",
        method: "$/channel/closed".into(),
        params: Some(json!({ "channelId": id })),
    }
}
