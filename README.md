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
use std::collections::HashMap;
use bowire_plugin::{
    run, BowirePlugin, InvokeResult, MethodInfo, ServiceInfo,
};

struct Echo;

#[async_trait::async_trait]
impl BowirePlugin for Echo {
    fn id(&self) -> &str { "echo" }
    fn name(&self) -> &str { "Echo" }

    async fn discover(&self, _server_url: &str, _show_internal: bool) -> Vec<ServiceInfo> {
        vec![ServiceInfo::new("DemoService")
            .with_methods([MethodInfo::unary("Echo")])]
    }

    async fn invoke(
        &self, _server_url: &str, _service: &str, _method: &str,
        json_messages: Vec<String>, _show_internal: bool,
        _metadata: HashMap<String, String>,
    ) -> InvokeResult {
        let payload = json_messages.first().cloned().unwrap_or_default();
        InvokeResult::ok(format!(r#"{{"echoed":{payload}}}"#))
    }
}

#[tokio::main]
async fn main() {
    std::process::exit(run(Echo).await);
}
```

That's it. The SDK drives the JSON-RPC contract (`initialize` / `ping` / `discover` / `invoke` / `invokeStream` / `shutdown`) over NDJSON on stdin/stdout. The Bowire host spawns your binary as a subprocess; your plugin code never touches the wire.

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
├── async discover(server_url, show_internal)             // required
├── async invoke(server_url, service, method,             // required
│                json_messages, show_internal, metadata)
├── async invoke_stream(...) -> BoxStream<String>         // default: NotImplemented
├── async open_channel(...) -> Option<()>                 // default: None (no duplex)
└── settings() -> Vec<PluginSetting>                      // default: []
```

## Contract reference

The JSON-RPC contract this SDK marshals against is documented in the host repo: [Sidecar plugins (docs/architecture)](https://bowire.io/docs/architecture/sidecar-plugins.html).

## License

Apache-2.0 — same as the rest of the Bowire family.
