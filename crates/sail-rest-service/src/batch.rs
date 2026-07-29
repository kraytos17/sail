use std::io::Write;
use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use sail_session::session_manager::SessionManager;
use serde::Deserialize;

use crate::error::RestError;
use crate::query::{execute_and_write_result, write_json_string};
use crate::session::{default_session_id, get_session_context};

#[derive(Deserialize)]
pub struct BatchRequest {
    statements: Vec<String>,
    #[serde(rename = "sessionId", default = "default_session_id")]
    session_id: String,
    #[serde(rename = "continueOnError", default)]
    continue_on_error: bool,
    #[serde(rename = "timeoutSecs", default)]
    timeout_secs: Option<u64>,
}

pub async fn handle_batch(
    State(session_manager): State<Arc<SessionManager>>,
    Json(req): Json<BatchRequest>,
) -> Response {
    let ctx = match get_session_context(&session_manager, &req.session_id).await {
        Ok(ctx) => ctx,
        Err(e) => return RestError::Session(format!("session error: {e}")).into_response(),
    };

    let mut buf = Vec::new();
    let _ = write!(
        buf,
        r#"{{"status":"ok","sessionId":"{}","results":["#,
        req.session_id
    );

    let total = req.statements.len();
    for (idx, sql) in req.statements.iter().enumerate() {
        if idx > 0 {
            buf.push(b',');
        }

        buf.push(b'{');
        match execute_and_write_result(&ctx, sql, &mut buf, req.timeout_secs).await {
            Ok(()) => {}
            Err(e) => {
                buf.extend_from_slice(b"\"columns\":[],\"rows\":[],\"rowCount\":0,\"status\":");
                write_json_string(&mut buf, &format!("error: {}", e));
                if !req.continue_on_error {
                    for _ in (idx + 1)..total {
                        buf.extend_from_slice(
                            b",{\"columns\":[],\"rows\":[],\"rowCount\":0,\"status\":\"skipped\"}",
                        );
                    }
                    buf.push(b'}');
                    break;
                }
            }
        }
        buf.push(b'}');
    }

    buf.push(b']');
    buf.push(b'}');

    Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(buf))
        .unwrap()
}
