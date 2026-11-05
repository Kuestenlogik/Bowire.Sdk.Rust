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

use bowire_plugin::{
    run, BowirePlugin, Channel, ChannelHandler, ChannelRequest, DiscoverRequest, FieldInfo,
    InvokeRequest, InvokeResult, ManualCallKind, ManualEntryForm, MessageInfo, MethodInfo,
    PluginSetting, ServiceInfo, SettingOption, Settings,
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

        let chat = MethodInfo::duplex("Chat")
            .with_input(request)
            .with_output(reply)
            .with_summary("Echo every frame back over one open channel.");

        vec![ServiceInfo::new("DemoService").with_methods([echo, chat])]
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
    std::process::exit(run(Echo).await);
}
