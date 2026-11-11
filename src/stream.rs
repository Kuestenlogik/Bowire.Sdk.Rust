//! Server streams (`invokeStream`).
//!
//! A plugin that serves server-streaming methods implements
//! [`StreamHandler`] and returns itself from
//! [`crate::BowirePlugin::streams`]. The runtime then reports
//! `capabilities.invokeStream: true` and routes the host's `invokeStream`
//! calls to it: each item the returned stream yields goes to the host as a
//! `$/stream/data` frame, and the end of the stream as `$/stream/end`.
//!
//! Without a handler the handshake says `invokeStream: false`, so the host
//! does not offer server-streaming calls the plugin cannot answer, and an
//! `invokeStream` that arrives anyway is refused as method-not-found.

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::plugin::InvokeRequest;

/// Serves server streams. Implement it next to [`crate::BowirePlugin`] and
/// return `Some(self)` from [`crate::BowirePlugin::streams`].
#[async_trait]
pub trait StreamHandler: Send + Sync {
    /// Start a server-streaming call. Every item is one message (a JSON
    /// string) for the host; the stream ending ends the call.
    async fn invoke_stream(&self, request: InvokeRequest) -> BoxStream<'static, String>;
}
