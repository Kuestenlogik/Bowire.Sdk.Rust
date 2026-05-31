//! Echo — minimal Bowire sidecar plugin. Parrots whatever the
//! workbench POSTs against `DemoService.Echo` back inside an
//! `{"echoed": <input>}` envelope. Run with
//!
//! ```text
//! cargo run --example echo
//! ```
//!
//! …then ship next to `sidecar.json` and install via
//! `bowire plugin install --file <zip>`.

use std::collections::HashMap;

use bowire_plugin::{
    run, BowirePlugin, FieldInfo, InvokeResult, MessageInfo, MethodInfo, ServiceInfo,
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
            .with_input(request)
            .with_output(reply)
            .with_summary("Echo the request payload back.");

        vec![ServiceInfo::new("DemoService").with_methods([echo])]
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
}

#[tokio::main]
async fn main() {
    std::process::exit(run(Echo).await);
}
