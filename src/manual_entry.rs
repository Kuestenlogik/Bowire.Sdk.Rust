//! How a call to the plugin is entered by hand, without discovery
//! (Kuestenlogik/Bowire#1003).
//!
//! A plugin returns a [`ManualEntryForm`] from
//! [`crate::BowirePlugin::manual_entry`]; the runtime puts it in the
//! `initialize` reply as `manualEntry`, and the workbench renders a Compose
//! form from it and offers exactly these method types in its freeform form.
//! The wire shape is the Bowire repo's docs/architecture/sidecar-plugins.md,
//! "A hand-entry form".

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::models::SettingOption;

const METHOD_TYPES: [&str; 4] = ["Unary", "ServerStreaming", "ClientStreaming", "Duplex"];

/// How a call to this plugin is entered by hand.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualEntryForm {
    /// The kinds of call the plugin offers, in the order the form lists them.
    pub call_kinds: Vec<ManualCallKind>,
    /// The fields a call needs, in the order the form shows them.
    #[serde(default)]
    pub fields: Vec<ManualField>,
    /// An example server URL for the URL field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url_placeholder: Option<String>,
}

impl ManualEntryForm {
    /// A form offering `call_kinds`, without fields yet.
    pub fn new(call_kinds: impl IntoIterator<Item = ManualCallKind>) -> Self {
        Self {
            call_kinds: call_kinds.into_iter().collect(),
            fields: Vec::new(),
            url_placeholder: None,
        }
    }

    /// With these fields.
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = ManualField>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// With an example server URL.
    pub fn with_url_placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.url_placeholder = Some(placeholder.into());
        self
    }
}

/// One kind of call the plugin offers, e.g. produce or consume.
///
/// `method_type` is `"Unary"` (invoke), `"ServerStreaming"` (invokeStream),
/// `"ClientStreaming"` or `"Duplex"` (a channel). `service` and `method` are
/// templates: `{key}` is the value of the field with that key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualCallKind {
    /// Stable within the plugin, e.g. `"produce"`.
    pub id: String,
    /// What the form shows.
    pub label: String,
    /// How the call runs.
    pub method_type: String,
    /// The call's service, as a template.
    pub service: String,
    /// The call's method, as a template.
    pub method: String,
    /// One sentence on what the call does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// An example payload, in the protocol's own format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_example: Option<String>,
}

impl ManualCallKind {
    /// A call kind `id` of `method_type`, labelled with its id.
    pub fn new(id: impl Into<String>, method_type: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            label: id.clone(),
            id,
            method_type: method_type.into(),
            service: String::new(),
            method: String::new(),
            description: None,
            body_example: None,
        }
    }

    /// What the form shows.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }

    /// The service and method templates.
    pub fn with_target(mut self, service: impl Into<String>, method: impl Into<String>) -> Self {
        self.service = service.into();
        self.method = method.into();
        self
    }

    /// One sentence on what the call does.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// An example payload.
    pub fn with_body_example(mut self, body: impl Into<String>) -> Self {
        self.body_example = Some(body.into());
        self
    }
}

/// One field of the form.
///
/// `placement` `"template"` feeds only the call kinds' templates;
/// `"metadata"` (the default) puts the value into the call's metadata under
/// `key`. `call_kinds` limits the field to those kinds; empty means all.
/// `type_name` is `"string"` (default), `"number"`, `"bool"` or `"select"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualField {
    /// The template placeholder or metadata key.
    pub key: String,
    /// What the form shows.
    pub label: String,
    /// `"metadata"` or `"template"`.
    pub placement: String,
    /// Help text below the field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// An example value shown in the empty field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
    /// Whether the call cannot be made without a value.
    pub required: bool,
    /// `"string"`, `"number"`, `"bool"` or `"select"`.
    #[serde(rename = "type")]
    pub type_name: String,
    /// The choices of a `select`.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub options: Vec<SettingOption>,
    /// The value the field starts with.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    /// The call kinds the field belongs to; empty means all.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub call_kinds: Vec<String>,
}

impl ManualField {
    /// A string field `key` that goes into the call's metadata.
    pub fn metadata(key: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
            placement: "metadata".into(),
            description: None,
            placeholder: None,
            required: false,
            type_name: "string".into(),
            options: Vec::new(),
            default_value: None,
            call_kinds: Vec::new(),
        }
    }

    /// A string field `key` that only feeds the templates.
    pub fn template(key: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            placement: "template".into(),
            ..Self::metadata(key, label)
        }
    }

    /// Mark the field required.
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// `"string"`, `"number"`, `"bool"` or `"select"`.
    pub fn with_type(mut self, type_name: impl Into<String>) -> Self {
        self.type_name = type_name.into();
        self
    }

    /// The choices of a `select`.
    pub fn with_options(mut self, options: impl IntoIterator<Item = SettingOption>) -> Self {
        self.options.extend(options);
        self
    }

    /// Help text below the field.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// An example value shown in the empty field.
    pub fn with_placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.placeholder = Some(placeholder.into());
        self
    }

    /// Limit the field to these call kinds.
    pub fn for_call_kinds<S: Into<String>>(mut self, ids: impl IntoIterator<Item = S>) -> Self {
        self.call_kinds = ids.into_iter().map(Into::into).collect();
        self
    }
}

/// What is wrong with a form, or nothing — the check the host runs before it
/// serves one. The runtime refuses a form with a problem at `initialize`, so
/// the plugin's author sees it rather than a form that silently never appears.
pub fn manual_entry_problems(form: &ManualEntryForm) -> Vec<String> {
    let mut problems = Vec::new();
    if form.call_kinds.is_empty() {
        problems.push("it offers no call kind".to_string());
    }

    let mut kind_ids = HashSet::new();
    for kind in &form.call_kinds {
        if kind.id.is_empty() {
            problems.push("a call kind has no id".to_string());
        } else if !kind_ids.insert(kind.id.as_str()) {
            problems.push(format!("call kind '{}' is declared twice", kind.id));
        }
        if !METHOD_TYPES.contains(&kind.method_type.as_str()) {
            problems.push(format!(
                "call kind '{}' has method type '{}'",
                kind.id, kind.method_type
            ));
        }
    }

    let mut field_keys = HashSet::new();
    for field in &form.fields {
        if field.key.is_empty() {
            problems.push("a field has no key".to_string());
        } else if !field_keys.insert(field.key.as_str()) {
            problems.push(format!("field '{}' is declared twice", field.key));
        }
        if field.type_name == "select" && field.options.is_empty() {
            problems.push(format!("select field '{}' has no options", field.key));
        }
        for id in &field.call_kinds {
            if !kind_ids.contains(id.as_str()) {
                problems.push(format!(
                    "field '{}' names unknown call kind '{}'",
                    field.key, id
                ));
            }
        }
    }

    for kind in &form.call_kinds {
        for template in [&kind.service, &kind.method] {
            for key in placeholders(template) {
                if !field_keys.contains(key) {
                    problems.push(format!(
                        "call kind '{}' uses the placeholder '{}', which no field declares",
                        kind.id, key
                    ));
                }
            }
        }
    }
    problems
}

/// The `{key}` placeholders of a template.
fn placeholders(template: &str) -> Vec<&str> {
    let mut keys = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else { break };
        let key = &after[..close];
        if !key.is_empty() && !key.contains('{') {
            keys.push(key);
        }
        rest = &after[close + 1..];
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> ManualEntryForm {
        ManualEntryForm::new([
            ManualCallKind::new("produce", "Unary")
                .with_label("Produce")
                .with_target("{topic}", "produce"),
            ManualCallKind::new("consume", "ServerStreaming").with_target("{topic}", "consume"),
        ])
        .with_fields([
            ManualField::template("topic", "Topic").required(),
            ManualField::metadata("priority", "Priority")
                .with_type("select")
                .with_options([SettingOption::new("high"), SettingOption::new("low")])
                .for_call_kinds(["produce"]),
        ])
        .with_url_placeholder("mybroker://localhost:9000")
    }

    #[test]
    fn a_sound_form_has_no_problems_and_the_hosts_shape() {
        let form = broker();
        assert!(manual_entry_problems(&form).is_empty());
        let wire = serde_json::to_value(&form).unwrap();
        assert_eq!(wire["urlPlaceholder"], "mybroker://localhost:9000");
        assert_eq!(wire["callKinds"][0]["methodType"], "Unary");
        assert_eq!(wire["callKinds"][1]["label"], "consume");
        assert_eq!(wire["fields"][0]["placement"], "template");
        assert_eq!(wire["fields"][1]["type"], "select");
        assert_eq!(wire["fields"][1]["callKinds"][0], "produce");
    }

    #[test]
    fn each_problem_is_named() {
        let cases = [
            (ManualEntryForm::new([]), "it offers no call kind"),
            (
                ManualEntryForm::new([ManualCallKind::new("a", "Sometimes")]),
                "call kind 'a' has method type 'Sometimes'",
            ),
            (
                ManualEntryForm::new([
                    ManualCallKind::new("a", "Unary"),
                    ManualCallKind::new("a", "Unary"),
                ]),
                "call kind 'a' is declared twice",
            ),
            (
                ManualEntryForm::new([ManualCallKind::new("a", "Unary")])
                    .with_fields([ManualField::metadata("v", "V").with_type("select")]),
                "select field 'v' has no options",
            ),
            (
                ManualEntryForm::new([ManualCallKind::new("a", "Unary")])
                    .with_fields([ManualField::metadata("w", "W").for_call_kinds(["b"])]),
                "field 'w' names unknown call kind 'b'",
            ),
            (
                ManualEntryForm::new([
                    ManualCallKind::new("a", "Unary").with_target("{topic}", "x")
                ]),
                "call kind 'a' uses the placeholder 'topic', which no field declares",
            ),
        ];
        for (form, expected) in cases {
            let problems = manual_entry_problems(&form);
            assert!(
                problems.iter().any(|p| p == expected),
                "want {expected:?} among {problems:?}"
            );
        }
    }

    #[test]
    fn placeholders_are_read_out_of_a_template() {
        assert_eq!(
            placeholders("nats/jetstream/{stream}/publish/{subject}"),
            vec!["stream", "subject"]
        );
        assert!(placeholders("produce").is_empty());
    }
}
