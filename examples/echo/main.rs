//! Echo — minimal Bowire sidecar plugin. Parrots whatever the
//! workbench POSTs against `DemoService.Echo` back inside an
//! `{"echoed": <input>}` envelope, and does the same per frame over the
//! duplex `DemoService.Chat` channel. Run with
//!
//! ```text
//! cargo run --example echo
//! ```
//!
//! …then ship next to `sidecar.json` and install via
//! `bowire plugin install --file <zip>`.

use std::collections::HashMap;

use bowire_plugin::{
    run, BowirePlugin, Channel, ChannelHandler, ChannelRequest, FieldInfo, InvokeResult,
    MessageInfo, MethodInfo, ServiceInfo,
};

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

    async fn discover(&self, _server_url: &str, _show_internal: bool) -> Vec<ServiceInfo> {
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

        let chat = MethodInfo::duplex("Chat")
            .with_input(request)
            .with_output(reply)
            .with_summary("Echo every frame back over one open channel.");

        vec![ServiceInfo::new("DemoService").with_methods([echo, chat])]
    }

    async fn invoke(
        &self,
        _server_url: &str,
        _service: &str,
        _method: &str,
        json_messages: Vec<String>,
        _show_internal: bool,
        _metadata: HashMap<String, String>,
    ) -> InvokeResult {
        let payload = json_messages
            .first()
            .cloned()
            .unwrap_or_else(|| "{}".into());
        // Wrap the request body verbatim so the workbench's response
        // viewer renders whatever was sent. A real plugin would route
        // this through the underlying wire (HTTP, Zenoh, &c).
        InvokeResult::ok(format!(r#"{{"echoed":{payload}}}"#))
    }

    fn channels(&self) -> Option<&dyn ChannelHandler> {
        Some(self)
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
        tokio::spawn(async move {
            while let Some(frame) = channel.recv().await {
                channel.send(format!(r#"{{"echoed":{frame}}}"#));
            }
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    std::process::exit(run(Echo).await);
}
