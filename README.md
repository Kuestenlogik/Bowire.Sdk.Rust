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
| HTTP / SSE      | `http`     | `run_http(plugin, host, port)` |

stdio is what the host spawns as a subprocess (cheap, no extra deps). HTTP/SSE fits hosted / multi-tenant deployments where one sidecar serves many workbenches — `POST /` lands JSON-RPC requests, `GET /` is a long-lived SSE stream the runtime pushes server notifications onto. Both runtimes share the same JSON-RPC dispatcher, so plugin code never knows which wire it's running over.

```rust
// HTTP/SSE mode — cargo add bowire-plugin --features http
use bowire_plugin::run_http;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    run_http(MyPlugin::default(), "127.0.0.1", 8770).await
}
```

Then point `sidecar.json` at `"transport": "http"` + `"url": "http://127.0.0.1:8770"` and the Bowire host bridges it the same way it bridges stdio sidecars.

## Trait surface

```text
BowirePlugin
├── id() -> &str                                          // required
├── name() -> &str                                        // required
├── icon_svg() -> &str                                    // default: "<svg/>"
├── async discover(DiscoverRequest)                       // required
├── async invoke(InvokeRequest) -> InvokeResult           // required
├── async invoke_stream(InvokeRequest) -> BoxStream       // default: NotImplemented
├── channels() -> Option<&dyn ChannelHandler>             // default: None (no duplex)
├── settings() -> Vec<PluginSetting>                      // default: []
└── capabilities() -> Capabilities                        // default: derived

ChannelHandler
└── async open_channel(request, channel) -> Result<(), String>
```

## Breaking changes

Pre-1.0, changes land in one cut. Since 0.2.0:

- `discover`, `invoke` and `invoke_stream` take one request value,
  `DiscoverRequest` or `InvokeRequest`, instead of positional arguments.
  The request carries the call's setting values, and a field added later
  does not break an implementation.
- `PluginSetting` has the host's shape: `key`, `label`, `type_name`,
  `default_value` (any JSON value), `description`, `options` (`SettingOption`
  with `value` and `label`). The old `id` / `name` / `default` fields never
  reached the host.
- `ChannelRequest` gains `settings`.

## Contract reference

The JSON-RPC contract this SDK marshals against is documented in the host repo: [Sidecar plugins (docs/architecture)](https://bowire.io/docs/architecture/sidecar-plugins.html).

## License

Apache-2.0 — same as the rest of the Bowire family.
