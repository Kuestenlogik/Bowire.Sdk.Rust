//! Unit coverage for the wire-side data models. Verifies that the
//! camelCase serialisation matches what the Bowire host expects on
//! the wire (cross-checked against the Python SDK's emitted shape).

use bowire_plugin::{FieldInfo, InvokeResult, MessageInfo, MethodInfo, MethodType, ServiceInfo};

#[test]
fn service_info_serialises_camel_case_with_method_array() {
    let svc = ServiceInfo::new("DemoService")
        .with_methods([MethodInfo::unary("Echo")])
        .with_description("A demo service");

    let json = serde_json::to_value(&svc).unwrap();
    assert_eq!(json["name"], "DemoService");
    assert_eq!(json["description"], "A demo service");
    assert_eq!(json["methods"][0]["name"], "Echo");
    assert_eq!(json["methods"][0]["methodType"], "Unary");
}

#[test]
fn method_info_unary_is_not_streaming() {
    let m = MethodInfo::unary("Get");
    assert!(!m.client_streaming);
    assert!(!m.server_streaming);
    assert_eq!(m.method_type, MethodType::Unary);
}

#[test]
fn method_info_server_streaming_sets_flag_and_marker() {
    let m = MethodInfo::server_streaming("Watch");
    assert!(!m.client_streaming);
    assert!(m.server_streaming);
    assert_eq!(m.method_type, MethodType::ServerStreaming);
}

#[test]
fn method_info_chains_input_output_summary() {
    let m = MethodInfo::unary("Echo")
        .with_input(MessageInfo::new("Req", "echo.Req"))
        .with_output(MessageInfo::new("Resp", "echo.Resp"))
        .with_summary("Echo back");
    assert!(m.input_type.is_some());
    assert!(m.output_type.is_some());
    assert_eq!(m.summary.as_deref(), Some("Echo back"));
}

#[test]
fn field_info_builders_pick_the_expected_type_names() {
    assert_eq!(FieldInfo::string("a").type_name, "string");
    assert_eq!(FieldInfo::int32("b").type_name, "int32");
    assert_eq!(FieldInfo::bool("c").type_name, "bool");
    assert!(FieldInfo::string("a").required().required);
}

#[test]
fn message_info_with_fields_extends_field_list() {
    let m = MessageInfo::new("M", "ns.M").with_fields([
        FieldInfo::string("name").required(),
        FieldInfo::int32("count"),
    ]);
    assert_eq!(m.fields.len(), 2);
    assert!(m.fields[0].required);
    assert!(!m.fields[1].required);
}

#[test]
fn invoke_result_ok_sets_status_and_response() {
    let r = InvokeResult::ok(r#"{"echoed":true}"#);
    assert_eq!(r.status, "OK");
    assert_eq!(r.response.as_deref(), Some(r#"{"echoed":true}"#));
}

#[test]
fn invoke_result_err_supports_optional_response_body() {
    let with_msg = InvokeResult::err("Error", Some("nope".to_string()));
    assert_eq!(with_msg.status, "Error");
    assert_eq!(with_msg.response.as_deref(), Some("nope"));

    let without: InvokeResult = InvokeResult::err("Error", None);
    assert!(without.response.is_none());
}

#[test]
fn invoke_result_serialises_camel_case() {
    let r = InvokeResult::ok("{}");
    let json = serde_json::to_value(&r).unwrap();
    assert!(json.get("durationMs").is_some());
    assert!(json.get("metadata").is_some());
}

#[test]
fn method_info_omits_none_fields_from_serialisation() {
    // skip_serializing_if = "Option::is_none" — none fields should
    // not show up as null keys, matching the wire convention the
    // host's deserialiser tolerates.
    let m = MethodInfo::unary("X");
    let json = serde_json::to_value(&m).unwrap();
    assert!(json.get("inputType").is_none());
    assert!(json.get("outputType").is_none());
    assert!(json.get("httpMethod").is_none());
}
