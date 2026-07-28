use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use datafusion::prelude::SessionContext;
use sail_session::session_manager::SessionManager;
use serde::Deserialize;
use uuid::Uuid;

use crate::error::{RestError, with_timeout};
use crate::query::execute_sql_to_batches;
use crate::session::get_session_context;

fn unique_view_name() -> String {
    format!(
        "_sail_load_{}",
        Uuid::new_v4().to_string().replace('-', "_")
    )
}

#[derive(Deserialize)]
pub struct LoadRequest {
    #[serde(rename = "filePath")]
    file_path: String,
    #[serde(rename = "schemaName")]
    schema_name: String,
    #[serde(rename = "tableName")]
    table_name: String,
    #[serde(rename = "fileFormat", default = "default_format")]
    file_format: String,
    #[serde(default)]
    options: Vec<(String, String)>,
    #[serde(rename = "sessionId", default = "default_session_id")]
    session_id: String,
    #[serde(rename = "timeoutSecs", default)]
    timeout_secs: Option<u64>,
    #[serde(rename = "mode", default = "default_load_mode")]
    mode: String,
}

fn default_format() -> String {
    "csv".to_string()
}

fn default_load_mode() -> String {
    "append".to_string()
}

fn default_session_id() -> String {
    "rest-api".to_string()
}

fn build_create_view_sql(req: &LoadRequest, view_name: &str) -> String {
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
            if !options.iter().any(|(k, _)| k.eq_ignore_ascii_case("mode")) {
                options.push(("mode".to_string(), "FAILFAST".to_string()));
            }
        }
        _ => {}
    }

    let opts_str: Vec<String> = options
        .iter()
        .map(|(k, v)| format!("{} '{}'", k, v))
        .collect();

    format!(
        "CREATE OR REPLACE TEMPORARY VIEW {} USING {} OPTIONS ({})",
        view_name,
        format,
        opts_str.join(", ")
    )
}

#[derive(serde::Serialize)]
struct LoadResponse {
    status: String,
    schema: String,
    table: String,
    #[serde(rename = "filePath")]
    file_path: String,
    #[serde(rename = "fileFormat")]
    file_format: String,
    #[serde(rename = "rowsLoaded")]
    rows_loaded: i64,
    message: String,
}

async fn drop_temp_view(ctx: &SessionContext, view_name: &str) {
    let _ = execute_sql_to_batches(ctx, &format!("DROP VIEW IF EXISTS {}", view_name)).await;
}

pub async fn handle_load(
    State(session_manager): State<Arc<SessionManager>>,
    Json(req): Json<LoadRequest>,
) -> Response {
    let ctx = match get_session_context(&session_manager, &req.session_id).await {
        Ok(ctx) => ctx,
        Err(e) => return RestError::Session(format!("session error: {e}")).into_response(),
    };

    let view_name = unique_view_name();
    let create_view_sql = build_create_view_sql(&req, &view_name);

    if let Err(e) = with_timeout(
        execute_sql_to_batches(&ctx, &create_view_sql),
        req.timeout_secs,
    )
    .await
    {
        return e.into_response();
    }

    let insert_sql = match req.mode.as_str() {
        "overwrite" => format!(
            "INSERT OVERWRITE {}.{} SELECT * FROM {}",
            req.schema_name, req.table_name, view_name
        ),
        _ => format!(
            "INSERT INTO {}.{} SELECT * FROM {}",
            req.schema_name, req.table_name, view_name
        ),
    };

    let result =
        match with_timeout(execute_sql_to_batches(&ctx, &insert_sql), req.timeout_secs).await {
            Ok(batches) => batches,
            Err(e) => {
                drop_temp_view(&ctx, &view_name).await;
                return e.into_response();
            }
        };

    let rows_loaded: i64 = result.iter().map(|b| b.num_rows() as i64).sum();

    drop_temp_view(&ctx, &view_name).await;

    Json(LoadResponse {
        status: "ok".to_string(),
        schema: req.schema_name,
        table: req.table_name,
        file_path: req.file_path,
        file_format: req.file_format,
        rows_loaded,
        message: String::new(),
    })
    .into_response()
}
