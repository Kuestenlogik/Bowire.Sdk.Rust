//! The sessions of one http runtime (Kuestenlogik/Bowire#1014).
//!
//! An http sidecar serves several hosts at once, so "the host left" must end
//! that host's streams and channels and nobody else's. A host names itself
//! with the `Bowire-Session` request header, on every POST and on its SSE
//! `GET`. What a request opens belongs to the session it named, and that
//! session's notifications go to its own SSE streams only.
//!
//! Requests without the header come from a host older than the rule, or from
//! a hand-written client. They share one unnamed session.
//!
//! A session is kept for as long as something refers to it: a request in
//! flight, a running stream, an SSE stream (each holds a [`Lease`]) or an
//! open channel. It ends, which stops its streams and closes its channels,
//! when its last SSE stream closes or its host sends `shutdown`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use axum::http::HeaderMap;
use tokio::sync::broadcast;

use crate::runtime::dispatch::Session;
use crate::runtime::jsonrpc::Notification;

/// The request header a host names itself with.
pub(crate) const SESSION_HEADER: &str = "bowire-session";

/// Queue length for one session's notifications. Bounded so a slow or
/// stalled SSE client cannot grow memory without limit: when full, the
/// oldest entry is dropped for the reader that fell behind. One queue per
/// session, so a host that falls behind loses its own frames only.
const NOTIFICATION_CHANNEL_CAPACITY: usize = 1024;

/// Which session a request belongs to: the id its host sent, or none.
/// The id is opaque: compared byte for byte, never parsed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SessionKey(Option<Vec<u8>>);

impl SessionKey {
    /// The session the request with these headers belongs to.
    pub(crate) fn of(headers: &HeaderMap) -> Self {
        Self(headers.get(SESSION_HEADER).map(|id| id.as_bytes().to_vec()))
    }

    /// Whether the host sent an id. The unnamed session cannot be ended by a
    /// `shutdown`: there is no telling whose streams that would end.
    pub(crate) fn is_named(&self) -> bool {
        self.0.is_some()
    }
}

/// One session and the queue its SSE streams read from.
struct Held {
    session: Session,
    frames: broadcast::Sender<Notification>,
}

struct Slot {
    held: Arc<Held>,
    /// Requests in flight, running streams and SSE streams.
    leases: usize,
    /// The SSE streams among them.
    subscribers: usize,
}

/// Every session of one http runtime, by the id its host sent.
#[derive(Default)]
pub(crate) struct Sessions {
    slots: Mutex<HashMap<SessionKey, Slot>>,
}

impl Sessions {
    fn slots(&self) -> MutexGuard<'_, HashMap<SessionKey, Slot>> {
        // A poisoned lock means a panic while holding it; the map is plain
        // data and still good enough to wind down with.
        self.slots.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The session `key` names, created on first sight: for a request that
    /// starts something the session then owns.
    pub(crate) fn open(self: &Arc<Self>, key: &SessionKey) -> Lease {
        self.lease(key, false).0
    }

    /// The session `key` names, when it holds anything: for a request that
    /// only reaches what is already there.
    pub(crate) fn existing(self: &Arc<Self>, key: &SessionKey) -> Option<Lease> {
        let mut slots = self.slots();
        let slot = slots.get_mut(key)?;
        slot.leases += 1;
        Some(Lease {
            sessions: self.clone(),
            key: key.clone(),
            held: slot.held.clone(),
            subscriber: false,
        })
    }

    /// Attach an SSE stream to the session `key` names. The lease belongs to
    /// the stream: dropping it is how the session learns the stream closed.
    pub(crate) fn subscribe(
        self: &Arc<Self>,
        key: &SessionKey,
    ) -> (Lease, broadcast::Receiver<Notification>) {
        self.lease(key, true)
    }

    fn lease(
        self: &Arc<Self>,
        key: &SessionKey,
        subscriber: bool,
    ) -> (Lease, broadcast::Receiver<Notification>) {
        let mut slots = self.slots();
        let slot = slots.entry(key.clone()).or_insert_with(|| {
            let (frames, _) = broadcast::channel(NOTIFICATION_CHANNEL_CAPACITY);
            let session = {
                let frames = frames.clone();
                Session::new(Arc::new(move |n| {
                    // No SSE stream attached: nobody to tell.
                    let _ = frames.send(n);
                }))
            };
            Slot {
                held: Arc::new(Held { session, frames }),
                leases: 0,
                subscribers: 0,
            }
        });
        slot.leases += 1;
        if subscriber {
            slot.subscribers += 1;
        }
        // Under the lock, so the stream misses nothing sent after it counts.
        let frames = slot.held.frames.subscribe();
        let lease = Lease {
            sessions: self.clone(),
            key: key.clone(),
            held: slot.held.clone(),
            subscriber,
        };
        (lease, frames)
    }

    /// The host of `key` said it is leaving: stop its streams and close its
    /// channels. Its SSE streams stay attached, so the closing frames still
    /// reach it. Harmless for a session that is unknown or already ended.
    pub(crate) fn end(&self, key: &SessionKey) {
        let held = self.slots().get(key).map(|slot| slot.held.clone());
        if let Some(held) = held {
            held.session.end();
        }
        self.forget_if_idle(key);
    }

    /// The runtime is stopping: end every session.
    pub(crate) fn end_all(&self) {
        let all: Vec<Arc<Held>> = self.slots().values().map(|s| s.held.clone()).collect();
        for held in all {
            held.session.end();
        }
    }

    /// Drop the session once nothing refers to it and it holds no channel.
    fn forget_if_idle(&self, key: &SessionKey) {
        let mut slots = self.slots();
        let idle = slots
            .get(key)
            .is_some_and(|slot| slot.leases == 0 && slot.held.session.channels.len() == 0);
        if idle {
            slots.remove(key);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.slots().len()
    }
}

/// A reference to one session, held by a request in flight, a running
/// stream or an SSE stream. While any lease is out the session stays
/// registered; when the last SSE stream's lease goes, the session ends.
pub(crate) struct Lease {
    sessions: Arc<Sessions>,
    key: SessionKey,
    held: Arc<Held>,
    subscriber: bool,
}

impl Lease {
    /// The session, to dispatch against.
    pub(crate) fn session(&self) -> &Session {
        &self.held.session
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let last_subscriber = {
            let mut slots = self.sessions.slots();
            // A slot is only removed with no lease out, so it is still there.
            let Some(slot) = slots.get_mut(&self.key) else {
                return;
            };
            slot.leases -= 1;
            if self.subscriber {
                slot.subscribers -= 1;
            }
            self.subscriber && slot.subscribers == 0
        };
        if last_subscriber {
            // The host is gone, whether or not it said so.
            self.held.session.end();
        }
        self.sessions.forget_if_idle(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn key(id: &str) -> SessionKey {
        let mut headers = HeaderMap::new();
        headers.insert(SESSION_HEADER, HeaderValue::from_str(id).unwrap());
        SessionKey::of(&headers)
    }

    fn unnamed() -> SessionKey {
        SessionKey::of(&HeaderMap::new())
    }

    /// Opens a channel in the leased session and returns the plugin's handle.
    fn open_channel(lease: &Lease, id: &str) -> crate::channel::Channel {
        let session = lease.session();
        let channel = session
            .channels
            .begin_open(id, session.notify.clone())
            .expect("the id is free");
        session.channels.finish_open(id, true, &session.notify);
        channel
    }

    #[test]
    fn the_header_names_the_session_and_its_absence_the_unnamed_one() {
        assert!(key("a").is_named());
        assert!(!unnamed().is_named());
        assert_eq!(key("a"), key("a"));
        assert_ne!(key("a"), key("b"));
        assert_ne!(key("a"), unnamed());
    }

    #[test]
    fn a_request_that_opens_nothing_leaves_no_session_behind() {
        let sessions = Arc::new(Sessions::default());

        assert!(sessions.existing(&key("a")).is_none());
        drop(sessions.open(&key("a")));

        assert_eq!(sessions.len(), 0);
    }

    #[tokio::test]
    async fn a_session_lives_until_its_last_sse_stream_closes() {
        let sessions = Arc::new(Sessions::default());
        let (first, _frames) = sessions.subscribe(&key("a"));
        let (second, mut frames) = sessions.subscribe(&key("a"));
        let mut channel = open_channel(&sessions.open(&key("a")), "c1");

        drop(first);
        assert!(channel.send("still open"), "one SSE stream is left");

        drop(second);
        assert_eq!(channel.recv().await, None, "the plugin's side ended");
        assert!(!channel.send("too late"));
        assert_eq!(frames.recv().await.unwrap().method, "$/channel/data");
        assert_eq!(frames.recv().await.unwrap().method, "$/channel/closed");
        assert_eq!(sessions.len(), 0);
    }

    #[tokio::test]
    async fn ending_a_session_leaves_the_others_alone_and_twice_is_harmless() {
        let sessions = Arc::new(Sessions::default());
        let (_a_sse, _a_frames) = sessions.subscribe(&key("a"));
        let (_b_sse, _b_frames) = sessions.subscribe(&key("b"));
        let mut a = open_channel(&sessions.open(&key("a")), "c1");
        let b = open_channel(&sessions.open(&key("b")), "c1");
        let a_stream = sessions.open(&key("a")).session().ended();
        let b_stream = sessions.open(&key("b")).session().ended();

        sessions.end(&key("a"));
        sessions.end(&key("a"));
        sessions.end(&key("nobody"));

        assert_eq!(a.recv().await, None);
        a_stream.await;
        assert!(b.send("still open"));
        assert!(
            futures::poll!(std::pin::pin!(b_stream)).is_pending(),
            "the other session's streams keep running"
        );
        // The SSE stream is still attached, so the session is still known and
        // what it opens next is not affected by the end before.
        let again = open_channel(&sessions.open(&key("a")), "c2");
        assert!(again.send("a new channel"));
    }

    #[tokio::test]
    async fn a_session_with_an_open_channel_is_kept_until_it_ends() {
        let sessions = Arc::new(Sessions::default());
        let mut channel = open_channel(&sessions.open(&unnamed()), "c1");
        assert_eq!(sessions.len(), 1);

        sessions.end(&unnamed());

        assert_eq!(channel.recv().await, None);
        assert_eq!(sessions.len(), 0);
    }
}
