use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use sail_session::session_manager::SessionManager;
use serde::Deserialize;

use crate::error::{RestError, with_timeout};
use crate::query::{execute_and_write_result, execute_sql_to_batches};
use crate::session::{default_session_id, get_session_context};

#[derive(Deserialize)]
pub struct ReadRequest {
    #[serde(rename = "filePath")]
    file_path: String,
    #[serde(rename = "fileFormat", default = "default_csv_format")]
    file_format: String,
    #[serde(default)]
    options: Vec<(String, String)>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(rename = "sessionId", default = "default_session_id")]
    session_id: String,
    #[serde(rename = "timeoutSecs", default)]
    timeout_secs: Option<u64>,
}

fn default_csv_format() -> String {
    "csv".to_string()
}

fn build_read_view_sql(req: &ReadRequest) -> String {
    let format = req.file_format.to_lowercase();
    let mut options = req.options.clone();

    if !options.iter().any(|(k, _)| k.eq_ignore_ascii_case("path")) {
        options.insert(0, ("path".to_string(), req.file_path.clone()));
    }

    match format.as_str() {
        "csv" => {
            if !options
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("header"))
            {
                options.push(("header".to_string(), "true".to_string()));
            }
            if !options
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("infer_schema"))
            {
                options.push(("infer_schema".to_string(), "true".to_string()));
            }
        }
        _ => {}
    }

    let opts_str: Vec<String> = options
        .iter()
        .map(|(k, v)| format!("{} '{}'", k, v))
        .collect();

    format!(
        "CREATE OR REPLACE TEMPORARY VIEW _sail_read_ USING {} OPTIONS ({})",
        format,
        opts_str.join(", ")
    )
}

pub async fn handle_read(
    State(session_manager): State<Arc<SessionManager>>,
    Json(req): Json<ReadRequest>,
) -> Response {
    let ctx = match get_session_context(&session_manager, &req.session_id).await {
        Ok(ctx) => ctx,
        Err(e) => return RestError::Session(format!("session error: {e}")).into_response(),
    };

    let create_sql = build_read_view_sql(&req);
    if let Err(e) = with_timeout(execute_sql_to_batches(&ctx, &create_sql), req.timeout_secs).await
    {
        return RestError::Session(format!("create view failed: {e}")).into_response();
    }

    let select_sql = match req.limit {
        Some(n) => format!("SELECT * FROM _sail_read_ LIMIT {n}"),
        None => "SELECT * FROM _sail_read_".to_string(),
    };

    let mut buf = Vec::new();
    buf.extend_from_slice(b"{\"status\":\"ok\",");

    if let Err(e) = execute_and_write_result(&ctx, &select_sql, &mut buf, req.timeout_secs).await {
        let _ = execute_sql_to_batches(&ctx, "DROP VIEW IF EXISTS _sail_read_").await;
        return e.into_response();
    }

    buf.push(b'}');

    let _ = execute_sql_to_batches(&ctx, "DROP VIEW IF EXISTS _sail_read_").await;

    Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(buf))
        .unwrap()
}
