# bowire-plugin (Rust)

Rust SDK for writing [Bowire](https://bowire.io) protocol plugins as polyglot sidecars. Sibling of [`Bowire.Sdk.Python`](https://github.com/Kuestenlogik/Bowire.Sdk.Python) — same JSON-RPC contract, same lifecycle, idiomatic Rust surface.

## Install

```bash
cargo add bowire-plugin
# or for the HTTP/SSE-mode runtime alongside stdio:
cargo add bowire-plugin --features http
```

## Write a plugin

```rust
use bowire_plugin::{
    run, BowirePlugin, DiscoverRequest, InvokeRequest, InvokeResult, MethodInfo, ServiceInfo,
};

struct Echo;

#[async_trait::async_trait]
impl BowirePlugin for Echo {
    fn id(&self) -> &str { "echo" }
    fn name(&self) -> &str { "Echo" }

    async fn discover(&self, _request: DiscoverRequest) -> Vec<ServiceInfo> {
        vec![ServiceInfo::new("DemoService")
            .with_methods([MethodInfo::unary("Echo")])]
    }

    async fn invoke(&self, request: InvokeRequest) -> InvokeResult {
        let payload = request.json_messages.first().cloned().unwrap_or_default();
        InvokeResult::ok(format!(r#"{{"echoed":{payload}}}"#))
    }
}

#[tokio::main]
async fn main() {
    std::process::exit(run(Echo).await);
}
```

That's it. The SDK drives the JSON-RPC contract (`initialize` / `ping` / `discover` / `invoke` / `invokeStream` / `openChannel` / `channel.send` / `channel.close` / `shutdown`) over NDJSON on stdin/stdout. The Bowire host spawns your binary as a subprocess; your plugin code never touches the wire.

## Settings

A plugin declares its settings once. Bowire renders them in its Settings
dialog, in the plugin's own section, and sends the values back with every
call, so a changed setting takes effect without a restart.

**Declare** them in `settings()`. `type` is `"bool"`, `"string"`, `"number"`
or `"select"`, and a `select` needs `options`. One without them fails the
`initialize` handshake, naming the setting.

```rust
use bowire_plugin::{PluginSetting, SettingOption};

fn settings(&self) -> Vec<PluginSetting> {
    vec![
        PluginSetting::new("probeWindow", "Probe window", "number").with_default(5),
        PluginSetting::new("mode", "Mode", "select")
            .with_default("fast")
            .with_options([SettingOption::new("fast"), SettingOption::labelled("safe", "Safe")]),
    ]
}
```

**Read** them from the request's `settings`: `DiscoverRequest`,
`InvokeRequest` and `ChannelRequest` all carry one.

```rust
async fn invoke(&self, request: InvokeRequest) -> InvokeResult {
    let window = request.settings.number("probeWindow"); // Some(5.0) until somebody sets it
    let mode = request.settings.string("mode");          // Some("fast") by default
    // …
}
```

- Only keys somebody set travel. An absent key, or a value that does not
  parse as its type, reads as the declared default.
- `bool` accepts `true` / `false` in any case, and `number` a finite decimal.
- `has(key)` and `raw(key)` say whether the host sent a value and what text
  it was.

## Hand-entry form

A user can call your plugin without discovery, from the workbench's freeform
form or Compose, and only your plugin knows what such a call needs. Return it
from `manual_entry()`. The runtime puts it in the `initialize` reply, and the
workbench renders a Compose form from it and offers exactly these method types
in the freeform form (Bowire 2.11 or later).

```rust
use bowire_plugin::{ManualCallKind, ManualEntryForm, ManualField};

fn manual_entry(&self) -> Option<ManualEntryForm> {
    Some(
        ManualEntryForm::new([
            ManualCallKind::new("produce", "Unary").with_label("Produce").with_target("{topic}", "produce"),
            ManualCallKind::new("consume", "ServerStreaming").with_label("Consume").with_target("{topic}", "consume"),
        ])
        .with_fields([
            ManualField::template("topic", "Topic").required(),
            ManualField::metadata("key", "Key").for_call_kinds(["produce"]),
        ])
        .with_url_placeholder("mybroker://localhost:9000"),
    )
}
```

- A **call kind** names how it runs: `Unary` is `invoke`, `ServerStreaming`
  is `invoke_stream`, and `ClientStreaming` or `Duplex` opens a channel. Its
  service and method are templates, in which `{key}` is the value of the field
  with that key.
- A **template** field only feeds the templates. A **metadata** field goes
  into the call's metadata under its key, which is where your `invoke` reads it.
- A form the host would refuse fails `initialize`, naming the problem.
  Examples are an unknown method type, a placeholder no field declares, and a
  `select` without options. `manual_entry_problems(&form)` runs the same check.

## Server streams

A plugin that serves server-streaming methods implements `StreamHandler` and returns itself from `streams()`. Every item of the stream goes to the host as one message, and the stream ending ends the call. The handshake then reports `capabilities.invokeStream: true`. Without a handler it reports `false`, so the host does not offer server-streaming calls, and an `invokeStream` that arrives anyway is refused.

```rust
use bowire_plugin::{InvokeRequest, StreamHandler};
use futures::stream::{self, BoxStream};

#[async_trait::async_trait]
impl StreamHandler for MyPlugin {
    async fn invoke_stream(&self, request: InvokeRequest) -> BoxStream<'static, String> {
        Box::pin(stream::iter((1..=3).map(|n| format!(r#"{{"n":{n}}}"#))))
    }
}

// in impl BowirePlugin for MyPlugin:
fn streams(&self) -> Option<&dyn StreamHandler> { Some(self) }
```

## Duplex channels

A plugin that serves duplex methods implements `ChannelHandler` and returns itself from `channels()`. The handshake then reports `capabilities.channels: true`; without a handler it reports `false` and the host never sends `openChannel`.

```rust
use bowire_plugin::{Channel, ChannelHandler, ChannelRequest};

#[async_trait::async_trait]
impl ChannelHandler for Echo {
    async fn open_channel(&self, request: ChannelRequest, mut channel: Channel) -> Result<(), String> {
        if request.method != "Chat" {
            return Err(format!("no duplex method '{}'", request.method));
        }
        tokio::spawn(async move {
            while let Some(frame) = channel.recv().await {
                channel.send(format!(r#"{{"echoed":{frame}}}"#));
            }
        });
        Ok(())
    }
}

// in `impl BowirePlugin for Echo`:
fn channels(&self) -> Option<&dyn ChannelHandler> { Some(self) }
```

The runtime keys every channel by the `channelId` the host minted, so several can be open at once:

| Wire | Plugin side |
|---|---|
| `openChannel` | `open_channel(request, channel)`; `Ok` acks, `Err` refuses |
| `channel.send` | the frame arrives at `channel.recv()`; a send to a closed channel gets a JSON-RPC error, which the host reads as `SendAsync` → `false` |
| `channel.close` | `channel.recv()` returns `None`, once; the plugin can still flush frames |
| `$/channel/data` | `channel.send(frame)` (or a `ChannelSender` from `channel.sender()`) |
| `$/channel/closed` | `channel.close()`, or dropping the channel and every sender; sent exactly once |

## Run the example

```bash
cargo run --example echo
```

…and pipe an envelope at it manually if you want to see the round-trip:

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"discover","params":{"serverUrl":""}}' \
  | cargo run --example echo
```

Over HTTP/SSE it runs on its own and prints the address it listens on:

```bash
cargo run --features http --example echo -- --http 127.0.0.1:0
```

`scripts/e2e.sh stdio|http <echo-binary>` installs the example into a real Bowire host either way and drives discovery, settings, a server stream and a duplex channel through it. CI runs both.

## Ship to Bowire

Build a release binary, drop it next to `sidecar.json`, zip both, install:

```bash
cargo build --release --example echo
mkdir -p out
cp target/release/examples/echo out/
cp examples/echo/sidecar.json out/
( cd out && zip -r ../echo-rs.zip . )

bowire plugin install --file echo-rs.zip
# or, from an OCI registry:
# bowire plugin install --file oci://ghcr.io/your-org/echo-rs:0.1.0
```

The host extracts into `~/.bowire/plugins/<packageId>/`, reads `sidecar.json`, and spawns the binary you bundled.

## Transports

| Transport | Feature flag | Entry point |
|---|---|---|
| stdio (default) | always on | `run(plugin)` |
| HTTP / SSE      | `http`     | `run_http(plugin, host, port)`, or `serve_http(plugin, listener, shutdown)` |

stdio is what the host spawns as a subprocess (cheap, no extra deps). HTTP/SSE fits hosted / multi-tenant deployments where one sidecar serves many workbenches — `POST /` lands JSON-RPC requests, `GET /` is a long-lived SSE stream the runtime pushes server notifications onto. Both runtimes share the same JSON-RPC dispatcher, so plugin code never knows which wire it's running over.

```rust
// HTTP/SSE mode — cargo add bowire-plugin --features http
use bowire_plugin::run_http;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    run_http(MyPlugin::default(), "127.0.0.1", 8770).await
}
```

Then point `sidecar.json` at `"transport": "http"` + `"url": "http://127.0.0.1:8770"` and the Bowire host bridges it the same way it bridges stdio sidecars. The host neither starts nor stops an http sidecar; it connects to one that is running.

`run_http` stops on Ctrl-C, and on Unix also on SIGTERM. It ends the SSE streams that hosts hold open, so a sidecar told to stop does not wait for every workbench to hang up. To bind the listener yourself, for example on port 0 so the OS picks a free port, use `serve_http` with the stop signal of your choice. `shutdown_signal()` is the one `run_http` uses:

```rust
let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
println!("listening on http://{}/", listener.local_addr()?);
bowire_plugin::serve_http(MyPlugin::default(), listener, bowire_plugin::shutdown_signal()).await
```

### Sessions over HTTP

One http sidecar serves several hosts at once. Each host names itself with a
session id in the `Bowire-Session` request header, on every POST and on its
SSE `GET`. The runtime keeps the hosts apart by it. Your plugin code does not
see sessions.

- A stream or channel belongs to the session of the request that started it.
  Its frames go to that session's SSE streams only.
- A session ends when its host sends `shutdown`, or when its last SSE stream
  closes. The second covers a host that crashed. The runtime then closes the
  session's channels, so `channel.recv()` returns `None`, and drops its
  streams. Every other session keeps running.
- `channel.send` and `channel.close` reach only a channel of the caller's own
  session. Any other id gets the "channel is closed" error.
- Requests without the header share one unnamed session. Their frames go to
  every SSE stream that sent no header either. A `shutdown` without the header
  is acknowledged and ends nothing.

## Shutdown

Override `shutdown()` to release what the plugin holds. It runs once, when the
sidecar itself stops, after every open channel has closed:

```rust
async fn shutdown(&self) {
    self.pool.close().await;
}
```

- **stdio:** the host owns the process. On its `shutdown` request the runtime
  stops the running streams, closes the channels, runs the hook, replies, and
  `run` returns. Stdin closing without a `shutdown` runs the hook too.
- **HTTP:** the hook runs when the server stops (Ctrl-C, SIGTERM, or the future
  you gave `serve_http`). A host's `shutdown` request never runs it: that host
  is leaving, and the sidecar stays for the others.

## Trait surface

```text
BowirePlugin
├── id() -> &str                                          // required
├── name() -> &str                                        // required
├── icon_svg() -> &str                                    // default: "<svg/>"
├── async discover(DiscoverRequest)                       // required
├── async invoke(InvokeRequest) -> InvokeResult           // required
├── streams() -> Option<&dyn StreamHandler>               // default: None (no server streams)
├── channels() -> Option<&dyn ChannelHandler>             // default: None (no duplex)
├── settings() -> Vec<PluginSetting>                      // default: []
├── manual_entry() -> Option<ManualEntryForm>             // default: None
├── capabilities() -> Capabilities                        // default: derived
└── async shutdown()                                      // default: nothing to release

StreamHandler
└── async invoke_stream(InvokeRequest) -> BoxStream<String>

ChannelHandler
└── async open_channel(request, channel) -> Result<(), String>
```

## Breaking changes

Pre-1.0, changes land in one cut.

Unreleased:

- HTTP: hosts are kept apart by the `Bowire-Session` request header
  (Kuestenlogik/Bowire#1014). A stream or channel belongs to the session that
  started it, and its frames go to that session's SSE streams only. They used
  to go to every connected host. See [Sessions over HTTP](#sessions-over-http).
- HTTP: a host's `shutdown`, or its last SSE stream closing, ends that
  session's streams and channels. `shutdown` used to be acknowledged and
  nothing else, so streams and channels outlived every host.
- `BowirePlugin::shutdown()` is new: a hook that runs once when the sidecar
  itself stops (not breaking; the default does nothing).
- stdio: `run` returns on the host's `shutdown` request, after it closed the
  channels, ran the hook and replied. It used to reply and then wait for stdin
  to close. Stdin closing now closes the open channels as well.

In 0.3.0:

- `invoke_stream` moved from `BowirePlugin` to a `StreamHandler` trait,
  which a plugin returns from `streams()`, the way it returns a
  `ChannelHandler` from `channels()` (#13). The old default emitted one
  error frame, and `capabilities.invokeStream` said `true` for every plugin,
  so the host offered streaming calls that could only fail. A plugin that
  streams implements `StreamHandler` and returns `Some(self)` from
  `streams()`. `Capabilities::default()` now says `invoke_stream: false`.
- `serve_http(plugin, listener, shutdown)` and `shutdown_signal()` are new,
  and `run_http` stops on SIGTERM as well as Ctrl-C (not breaking).

In 0.2.0:

- `discover`, `invoke` and `invoke_stream` take one request value,
  `DiscoverRequest` or `InvokeRequest`, instead of positional arguments.
  The request carries the call's setting values, and a field added later
  does not break an implementation.
- `PluginSetting` has the host's shape: `key`, `label`, `type_name`,
  `default_value` (any JSON value), `description`, `options` (`SettingOption`
  with `value` and `label`). The old `id` / `name` / `default` fields never
  reached the host.
- `ChannelRequest` gains `settings`.

## Releasing

Pushing a tag `vX.Y.Z` that matches `version` in `Cargo.toml` publishes the
crate to crates.io (`.github/workflows/release.yml`). The workflow tests that
commit, then publishes through crates.io Trusted Publishing, so no registry
key is stored. A version already on crates.io is skipped. To publish an
existing tag again, run the workflow by hand with that tag.

## Contract reference

The JSON-RPC contract this SDK marshals against is documented in the host repo: [Sidecar plugins (docs/architecture)](https://bowire.io/docs/architecture/sidecar-plugins.html).

## License

Apache-2.0 — same as the rest of the Bowire family.
