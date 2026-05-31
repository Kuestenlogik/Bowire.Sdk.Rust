//! Wire-side data models the Bowire host expects. The shapes mirror
//! the Python SDK's dataclasses 1:1 — same field names (camelCase on
//! the wire), same semantics. Builders (`with_*`) let plugin code
//! write concise topology declarations without spelling out every
//! optional field.

use serde::{Deserialize, Serialize};

/// One service node the workbench shows in the sidebar.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceInfo {
    /// Display name of the service node.
    pub name: String,
    /// Methods exposed by this service.
    pub methods: Vec<MethodInfo>,
    /// Optional one-line description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Source-schema version reported by the host's discovery output
    /// (e.g. an OpenAPI `info.version` value).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

impl ServiceInfo {
    /// Build a service with the given name and no methods yet.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            methods: Vec::new(),
            description: None,
            version: None,
        }
    }

    /// Chainable: attach the given method list to this service.
    pub fn with_methods<I: IntoIterator<Item = MethodInfo>>(mut self, methods: I) -> Self {
        self.methods.extend(methods);
        self
    }

    /// Chainable: set the description shown in the sidebar tooltip.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// One callable method on a [`ServiceInfo`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MethodInfo {
    /// Bowire's short name (rendered in the sidebar).
    pub name: String,
    /// Fully qualified name (e.g. `package.Service.Method`).
    pub full_name: String,
    /// Whether the workbench writes more than one request frame.
    pub client_streaming: bool,
    /// Whether the workbench reads more than one response frame.
    pub server_streaming: bool,
    /// Cardinality marker the workbench uses to pick the right UI.
    pub method_type: MethodType,
    /// Input schema (shown as the request form).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_type: Option<MessageInfo>,
    /// Output schema (shown as the response view).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_type: Option<MessageInfo>,
    /// HTTP verb when this method came from an OpenAPI doc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_method: Option<String>,
    /// HTTP path template when this method came from an OpenAPI doc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_path: Option<String>,
    /// Short summary shown next to the method node in the sidebar.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl MethodInfo {
    /// Build a unary method (one request, one response).
    pub fn unary(name: impl Into<String>) -> Self {
        let n: String = name.into();
        Self {
            full_name: n.clone(),
            name: n,
            client_streaming: false,
            server_streaming: false,
            method_type: MethodType::Unary,
            input_type: None,
            output_type: None,
            http_method: None,
            http_path: None,
            summary: None,
        }
    }

    /// Build a server-streaming method (one request, many responses).
    pub fn server_streaming(name: impl Into<String>) -> Self {
        Self {
            method_type: MethodType::ServerStreaming,
            server_streaming: true,
            ..Self::unary(name)
        }
    }

    /// Chainable: set the request schema.
    pub fn with_input(mut self, input: MessageInfo) -> Self {
        self.input_type = Some(input);
        self
    }

    /// Chainable: set the response schema.
    pub fn with_output(mut self, output: MessageInfo) -> Self {
        self.output_type = Some(output);
        self
    }

    /// Chainable: attach a one-line summary.
    pub fn with_summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }
}

/// Method cardinality marker. Mirrors gRPC's four shapes — these are
/// the strings the workbench branches on to pick a UI.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MethodType {
    /// One request, one response.
    Unary,
    /// One request, many responses.
    ServerStreaming,
    /// Many requests, one response.
    ClientStreaming,
    /// Many requests, many responses (full duplex).
    Duplex,
}

/// One message schema attached to a method. Shape mirrors protobuf's
/// message descriptor so the workbench's form-renderer can drive any
/// schema through one code path.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MessageInfo {
    /// Display name (rendered in the form header).
    pub name: String,
    /// Fully qualified type name.
    pub full_name: String,
    /// Fields the form-renderer turns into inputs.
    pub fields: Vec<FieldInfo>,
}

impl MessageInfo {
    /// Build a message with the given (short, full) names.
    pub fn new(name: impl Into<String>, full_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            full_name: full_name.into(),
            fields: Vec::new(),
        }
    }

    /// Chainable: attach fields.
    pub fn with_fields<I: IntoIterator<Item = FieldInfo>>(mut self, fields: I) -> Self {
        self.fields.extend(fields);
        self
    }
}

/// One field on a [`MessageInfo`]. Optional flags + description ride
/// along so the workbench can render hints + required markers.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FieldInfo {
    /// Field name.
    pub name: String,
    /// Type name (`string` / `int32` / `bool` / `bytes` / …).
    #[serde(rename = "type")]
    pub type_name: String,
    /// Marks the field as required for form validation.
    pub required: bool,
    /// Description shown as a tooltip.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// REST: where the field travels (`path` / `query` / `header` /
    /// `body`). `None` for non-REST.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

impl FieldInfo {
    /// Build a string field.
    pub fn string(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: "string".into(),
            ..Default::default()
        }
    }

    /// Build an int32 field.
    pub fn int32(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: "int32".into(),
            ..Default::default()
        }
    }

    /// Build a bool field.
    pub fn bool(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: "bool".into(),
            ..Default::default()
        }
    }

    /// Chainable: mark this field required.
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Chainable: set a tooltip description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// The result a plugin returns from [`crate::BowirePlugin::invoke`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InvokeResult {
    /// JSON body the workbench renders as the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,
    /// Status string (`OK`, `Error`, `connect:invalid_argument`, …).
    pub status: String,
    /// Wall-clock duration in milliseconds — let the SDK fill in 0;
    /// the host derives its own latency from the wire arrival time.
    pub duration_ms: i64,
    /// Free-form metadata (headers, gRPC trailers, &c).
    pub metadata: std::collections::HashMap<String, String>,
}

impl InvokeResult {
    /// Success result with a JSON response body.
    pub fn ok(response: impl Into<String>) -> Self {
        Self {
            response: Some(response.into()),
            status: "OK".into(),
            duration_ms: 0,
            metadata: Default::default(),
        }
    }

    /// Error result with a status string + optional response body.
    pub fn err(status: impl Into<String>, message: impl Into<Option<String>>) -> Self {
        Self {
            response: message.into(),
            status: status.into(),
            duration_ms: 0,
            metadata: Default::default(),
        }
    }
}

/// One configurable setting the plugin declares; the workbench shows
/// them in the per-plugin Settings dialog. Most plugins return an
/// empty vec from [`crate::BowirePlugin::settings`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PluginSetting {
    /// Setting id (must be stable; the host stores values keyed on it).
    pub id: String,
    /// Display name shown next to the input.
    pub name: String,
    /// Description rendered as helper text.
    pub description: String,
    /// Type marker — `string` / `int` / `bool` / `select` (with `options`).
    #[serde(rename = "type")]
    pub type_name: String,
    /// Default value as a string (the host parses by `type_name`).
    pub default: String,
    /// For `type=select`: the choice list.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}
