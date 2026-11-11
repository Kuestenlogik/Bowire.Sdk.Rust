//! Echo — minimal Bowire sidecar plugin. Parrots whatever the
//! workbench POSTs against `DemoService.Echo` back inside an
//! `{"echoed": <input>}` envelope, streams it back three times from the
//! server-streaming `DemoService.Count`, and echoes every frame over the
//! duplex `DemoService.Chat` channel. Run with
//!
//! ```text
//! cargo run --example echo
//! ```
//!
//! …then ship next to `sidecar.json` and install via
//! `bowire plugin install --file <zip>`.
//!
//! Over HTTP/SSE instead of stdio, the sidecar runs on its own and the
//! manifest points the host at it (`"transport": "http"`, `"url"`):
//!
//! ```text
//! cargo run --features http --example echo -- --http 127.0.0.1:0
//! ```
//!
//! It prints `listening on http://<addr>/` once it accepts connections,
//! with the port the OS picked for `:0`.

use futures::stream::{self, BoxStream};

use bowire_plugin::{
    run, BowirePlugin, Channel, ChannelHandler, ChannelRequest, DiscoverRequest, FieldInfo,
    InvokeRequest, InvokeResult, ManualCallKind, ManualEntryForm, MessageInfo, MethodInfo,
    PluginSetting, ServiceInfo, SettingOption, Settings, StreamHandler,
};

/// What Echo and Chat do to the payload before sending it back: a select
/// setting (Kuestenlogik/Bowire#693), read fresh on every call.
fn transform(payload: &str, settings: &Settings) -> String {
    match settings.string("transform").as_deref() {
        Some("upper") => payload.to_uppercase(),
        Some("lower") => payload.to_lowercase(),
        _ => payload.to_string(),
    }
}

struct Echo;

#[async_trait::async_trait]
impl BowirePlugin for Echo {
    fn id(&self) -> &str {
        "echo-rs"
    }

    fn name(&self) -> &str {
        "Echo (Rust)"
    }

    fn icon_svg(&self) -> &str {
        // Tiny circle so the workbench shows *something* in the
        // protocol picker. Override with your protocol's real icon.
        r#"<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/></svg>"#
    }

    fn settings(&self) -> Vec<PluginSetting> {
        vec![PluginSetting::new("transform", "Transform", "select")
            .with_description("What Echo and Chat do to the payload before sending it back")
            .with_default("none")
            .with_options([
                SettingOption::labelled("none", "As sent"),
                SettingOption::labelled("upper", "Upper case"),
                SettingOption::labelled("lower", "Lower case"),
            ])]
    }

    // How the two methods are called by hand, without discovery
    // (Kuestenlogik/Bowire#1003): the workbench renders a form from this.
    fn manual_entry(&self) -> Option<ManualEntryForm> {
        Some(ManualEntryForm::new([
            ManualCallKind::new("echo", "Unary")
                .with_label("Echo")
                .with_target("DemoService", "Echo")
                .with_body_example(r#"{"message":"hello"}"#),
            ManualCallKind::new("count", "ServerStreaming")
                .with_label("Count")
                .with_target("DemoService", "Count")
                .with_body_example(r#"{"message":"hello"}"#),
            ManualCallKind::new("chat", "Duplex")
                .with_label("Chat")
                .with_target("DemoService", "Chat")
                .with_body_example(r#"{"message":"hello"}"#),
        ]))
    }

    async fn discover(&self, _request: DiscoverRequest) -> Vec<ServiceInfo> {
        let message_field = FieldInfo::string("message")
            .with_description("Anything you want echoed back.")
            .required();
        let request =
            MessageInfo::new("EchoRequest", "echo.EchoRequest").with_fields([message_field]);
        let reply = MessageInfo::new("EchoReply", "echo.EchoReply")
            .with_fields([FieldInfo::string("echoed")]);

        let echo = MethodInfo::unary("Echo")
            .with_input(request.clone())
            .with_output(reply.clone())
            .with_summary("Echo the request payload back.");

        let count = MethodInfo::server_streaming("Count")
            .with_input(request.clone())
            .with_output(reply.clone())
            .with_summary("Stream the request payload back three times.");

        let chat = MethodInfo::duplex("Chat")
            .with_input(request)
            .with_output(reply)
            .with_summary("Echo every frame back over one open channel.");

        vec![ServiceInfo::new("DemoService").with_methods([echo, count, chat])]
    }

    async fn invoke(&self, request: InvokeRequest) -> InvokeResult {
        let payload = request
            .json_messages
            .first()
            .cloned()
            .unwrap_or_else(|| "{}".into());
        // Wrap the request body so the workbench's response viewer renders
        // whatever was sent, transformed as the setting says. A real plugin
        // would route this through the underlying wire (HTTP, Zenoh, &c).
        let echoed = transform(&payload, &request.settings);
        InvokeResult::ok(format!(r#"{{"echoed":{echoed}}}"#))
    }

    fn streams(&self) -> Option<&dyn StreamHandler> {
        Some(self)
    }

    fn channels(&self) -> Option<&dyn ChannelHandler> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl StreamHandler for Echo {
    async fn invoke_stream(&self, request: InvokeRequest) -> BoxStream<'static, String> {
        let payload = request
            .json_messages
            .first()
            .cloned()
            .unwrap_or_else(|| "{}".into());
        let echoed = transform(&payload, &request.settings);
        Box::pin(stream::iter(
            (1..=3).map(move |n| format!(r#"{{"n":{n},"echoed":{echoed}}}"#)),
        ))
    }
}

#[async_trait::async_trait]
impl ChannelHandler for Echo {
    async fn open_channel(
        &self,
        request: ChannelRequest,
        mut channel: Channel,
    ) -> Result<(), String> {
        if request.method != "Chat" {
            return Err(format!("no duplex method '{}'", request.method));
        }
        // Drive the channel on its own task: open_channel returning Ok is
        // what acks the host. When the host closes its send side, recv()
        // ends, the task ends, and dropping the channel tells the host the
        // channel is closed.
        // The values sent with openChannel apply to the whole channel.
        let settings = request.settings.clone();
        tokio::spawn(async move {
            while let Some(frame) = channel.recv().await {
                let echoed = transform(&frame, &settings);
                channel.send(format!(r#"{{"echoed":{echoed}}}"#));
            }
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => std::process::exit(run(Echo).await),
        [flag, addr] if flag == "--http" => std::process::exit(serve(addr).await),
        _ => {
            eprintln!("usage: echo [--http <host:port>]");
            std::process::exit(2);
        }
    }
}

/// Serve over HTTP/SSE on `addr`. The line on stdout is the signal that
/// the sidecar accepts connections, and says which port `:0` became.
#[cfg(feature = "http")]
async fn serve(addr: &str) -> i32 {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("echo: cannot listen on {addr}: {e}");
            return 1;
        }
    };
    match listener.local_addr() {
        Ok(bound) => println!("listening on http://{bound}/"),
        Err(e) => {
            eprintln!("echo: {e}");
            return 1;
        }
    }
    match bowire_plugin::serve_http(Echo, listener, bowire_plugin::shutdown_signal()).await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("echo: {e}");
            1
        }
    }
}

#[cfg(not(feature = "http"))]
async fn serve(_addr: &str) -> i32 {
    eprintln!("echo: built without the http feature; use cargo build --features http");
    2
}
