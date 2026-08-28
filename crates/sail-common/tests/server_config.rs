#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use sail_common::config::ServerConfig;

#[test]
fn test_server_config_session_id_deserializes() {
    let config: ServerConfig = serde_json::from_str(
        r#"{
            "http2_keepalive_timeout_secs": 120,
            "session_id": "fixed-canonical"
        }"#,
    )
    .expect("server config should deserialize");

    assert_eq!(config.http2_keepalive_timeout_secs, 120);
    assert_eq!(config.session_id.as_deref(), Some("fixed-canonical"));
}

#[test]
fn test_server_config_empty_session_id_is_none() {
    let config: ServerConfig = serde_json::from_str(
        r#"{
            "http2_keepalive_timeout_secs": 120,
            "session_id": ""
        }"#,
    )
    .expect("server config should deserialize");

    assert_eq!(config.session_id, None);
}

#[test]
fn test_server_config_missing_session_id_is_none() {
    let config: ServerConfig = serde_json::from_str(
        r#"{
            "http2_keepalive_timeout_secs": 120
        }"#,
    )
    .expect("server config should deserialize");

    assert_eq!(config.session_id, None);
}
