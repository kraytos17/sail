use std::io::Write;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    TimestampMillisecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::DataType;
use axum::Json;
use axum::body::Body;
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use datafusion::prelude::SessionContext;
use futures::{StreamExt, stream};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_plan::config::PlanConfig;
use sail_plan::resolve_and_execute_plan;
use sail_session::session_manager::SessionManager;
use sail_sql_analyzer::parser::parse_one_statement;
use sail_sql_analyzer::statement::from_ast_statement;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::error::{RestError, with_timeout};
use crate::session::get_session_context;

#[derive(Deserialize)]
pub struct QueryRequest {
    sql: String,
    #[serde(rename = "sessionId", default = "default_session_id")]
    session_id: String,
    #[serde(rename = "timeoutSecs", default)]
    timeout_secs: Option<u64>,
}

fn default_session_id() -> String {
    "rest-api".to_string()
}

fn data_type_to_string(dt: &DataType) -> &'static str {
    match dt {
        DataType::Null => "null",
        DataType::Boolean => "boolean",
        DataType::Int8 | DataType::UInt8 => "tinyint",
        DataType::Int16 | DataType::UInt16 => "smallint",
        DataType::Int32 | DataType::UInt32 => "int",
        DataType::Int64 | DataType::UInt64 => "bigint",
        DataType::Float16 | DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Utf8 | DataType::LargeUtf8 => "string",
        DataType::Binary | DataType::LargeBinary => "binary",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Timestamp(_, _) => "timestamp",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "decimal",
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => "array",
        DataType::Struct(_) => "struct",
        _ => "unknown",
    }
}

pub(crate) fn write_json_string(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'"');
    for &byte in s.as_bytes() {
        match byte {
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            0..=31 => {
                let _ = write!(buf, "\\u{:04x}", byte);
            }
            _ => buf.push(byte),
        }
    }
    buf.push(b'"');
}

pub(crate) fn write_json_value(buf: &mut Vec<u8>, array: &Arc<dyn Array>, row_idx: usize) {
    if array.is_null(row_idx) {
        buf.extend_from_slice(b"null");
        return;
    }
    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            buf.extend_from_slice(if arr.value(row_idx) {
                b"true"
            } else {
                b"false"
            });
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            write_json_string(buf, arr.value(row_idx));
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            write_json_string(buf, arr.value(row_idx));
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days: i64 = arr.value(row_idx).into();
            if let Ok(d) = i32::try_from(days) {
                if let Some(naive) = chrono::NaiveDate::from_num_days_from_ce_opt(d + 719163) {
                    write_json_string(buf, &naive.to_string());
                    return;
                }
            }
            buf.extend_from_slice(b"null");
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let millis = arr.value(row_idx);
            if let Some(naive) = chrono::DateTime::from_timestamp_millis(millis) {
                write_json_string(buf, &naive.date_naive().to_string());
            } else {
                buf.extend_from_slice(b"null");
            }
        }
        DataType::Timestamp(_, tz) => {
            let has_tz = tz.is_some();
            if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                let value = arr.value(row_idx);
                if let Some(dt) = chrono::DateTime::from_timestamp(value, 0) {
                    let s = if has_tz {
                        dt.to_rfc3339()
                    } else {
                        dt.naive_utc().to_string()
                    };
                    write_json_string(buf, &s);
                    return;
                }
            }
            if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                let value = arr.value(row_idx);
                if let Some(dt) = chrono::DateTime::from_timestamp_millis(value) {
                    let s = if has_tz {
                        dt.to_rfc3339()
                    } else {
                        dt.naive_utc().to_string()
                    };
                    write_json_string(buf, &s);
                    return;
                }
            }
            buf.extend_from_slice(b"null");
        }
        DataType::Decimal128(_p, s) => {
            if let Some(arr) = array.as_any().downcast_ref::<Decimal128Array>() {
                let val = arr.value(row_idx);
                let scale = *s as u8;
                let sign = if val < 0 { "-" } else { "" };
                let val_abs = val.unsigned_abs();
                let divisor = 10u128.pow(scale as u32);
                let int_part = val_abs / divisor;
                let frac_part = val_abs % divisor;
                if scale > 0 {
                    let _ = write!(
                        buf,
                        "{}{}.{:0width$}",
                        sign,
                        int_part,
                        frac_part,
                        width = scale as usize
                    );
                } else {
                    let _ = write!(buf, "{}{}", sign, int_part);
                }
            } else {
                buf.extend_from_slice(b"null");
            }
        }
        DataType::Decimal256(_, _) => buf.extend_from_slice(b"null"),
        _ => buf.extend_from_slice(b"null"),
    }
}

pub(crate) fn write_row(buf: &mut Vec<u8>, batch: &RecordBatch, row_idx: usize) {
    buf.push(b'[');
    let columns = batch.columns();
    for (col_idx, array) in columns.iter().enumerate() {
        if col_idx > 0 {
            buf.push(b',');
        }
        write_json_value(buf, array, row_idx);
    }
    buf.push(b']');
}

pub(crate) fn write_columns(buf: &mut Vec<u8>, schema: &arrow_schema::Schema) {
    buf.push(b'[');
    for (i, field) in schema.fields().iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        buf.extend_from_slice(b"{\"name\":");
        write_json_string(buf, field.name());
        buf.extend_from_slice(b",\"type\":");
        write_json_string(buf, data_type_to_string(field.data_type()));
        buf.push(b'}');
    }
    buf.push(b']');
}

pub async fn execute_sql_to_batches(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<arrow::record_batch::RecordBatch>, RestError> {
    let statement =
        parse_one_statement(sql).map_err(|e| RestError::Session(format!("parse error: {e}")))?;
    let plan = from_ast_statement(statement)
        .map_err(|e| RestError::Session(format!("plan error: {e}")))?;
    let config = Arc::new(PlanConfig::default());
    let (resolved_plan, _) = resolve_and_execute_plan(ctx, config, plan)
        .await
        .map_err(|e| RestError::Session(format!("resolve error: {e}")))?;
    let service = ctx
        .extension::<JobService>()
        .map_err(|e| RestError::Session(format!("job service error: {e}")))?;
    let mut stream = service
        .runner()
        .execute(ctx, resolved_plan)
        .await
        .map_err(|e| RestError::Session(format!("execution error: {e}")))?;

    let mut batches = Vec::new();
    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| RestError::Session(format!("stream error: {e}")))?;
        batches.push(batch);
    }
    Ok(batches)
}

pub(crate) async fn execute_and_write_result(
    ctx: &SessionContext,
    sql: &str,
    buf: &mut Vec<u8>,
    timeout_secs: Option<u64>,
) -> Result<(), RestError> {
    let statement =
        parse_one_statement(sql).map_err(|e| RestError::Session(format!("parse error: {e}")))?;
    let plan = from_ast_statement(statement)
        .map_err(|e| RestError::Session(format!("plan error: {e}")))?;
    let (resolved_plan, _) = resolve_and_execute_plan(ctx, Arc::new(PlanConfig::default()), plan)
        .await
        .map_err(|e| RestError::Session(format!("resolve error: {e}")))?;
    let service = ctx
        .extension::<JobService>()
        .map_err(|e| RestError::Session(format!("job service error: {e}")))?;
    let mut stream = service
        .runner()
        .execute(ctx, resolved_plan)
        .await
        .map_err(|e| RestError::Session(format!("execution error: {e}")))?;

    let mut first_batch = true;
    let mut has_rows = false;
    let mut row_count: i64 = 0;

    let stream_fut = async {
        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| RestError::Session(format!("stream error: {e}")))?;
            if first_batch {
                buf.extend_from_slice(b"\"status\":\"ok\",\"columns\":");
                write_columns(buf, batch.schema().as_ref());
                buf.extend_from_slice(b",\"rows\":[");
                first_batch = false;
            }
            for row_idx in 0..batch.num_rows() {
                if has_rows {
                    buf.push(b',');
                }
                write_row(buf, &batch, row_idx);
                row_count += 1;
                has_rows = true;
            }
        }
        Ok::<_, RestError>(())
    };
    with_timeout(stream_fut, timeout_secs).await?;

    if first_batch {
        buf.extend_from_slice(b"\"status\":\"ok\",\"columns\":[],\"rows\":[],\"rowCount\":0");
    } else {
        let _ = write!(buf, "],\"rowCount\":{}", row_count);
    }
    Ok(())
}

pub async fn handle_query(
    State(session_manager): State<Arc<SessionManager>>,
    Json(req): Json<QueryRequest>,
) -> Response {
    let ctx = match get_session_context(&session_manager, &req.session_id).await {
        Ok(ctx) => ctx,
        Err(e) => return RestError::Session(format!("session error: {e}")).into_response(),
    };

    let (tx, rx) = mpsc::channel::<Result<Bytes, std::convert::Infallible>>(32);
    let timeout = req.timeout_secs;
    let sql = req.sql.clone();

    tokio::spawn(async move {
        let result = with_timeout(
            async {
                let statement = parse_one_statement(&sql)
                    .map_err(|e| RestError::Session(format!("parse error: {e}")))?;
                let plan = from_ast_statement(statement)
                    .map_err(|e| RestError::Session(format!("plan error: {e}")))?;
                let (resolved_plan, _) =
                    resolve_and_execute_plan(&ctx, Arc::new(PlanConfig::default()), plan)
                        .await
                        .map_err(|e| RestError::Session(format!("resolve error: {e}")))?;
                let service = ctx
                    .extension::<JobService>()
                    .map_err(|e| RestError::Session(format!("job service error: {e}")))?;
                let mut stream = service
                    .runner()
                    .execute(&ctx, resolved_plan)
                    .await
                    .map_err(|e| RestError::Session(format!("execution error: {e}")))?;

                let mut first_batch = true;
                let mut has_rows = false;
                let mut row_count: i64 = 0;

                while let Some(result) = stream.next().await {
                    let batch =
                        result.map_err(|e| RestError::Session(format!("stream error: {e}")))?;

                    let mut chunk = Vec::new();
                    if first_batch {
                        chunk.extend_from_slice(b"{\"status\":\"ok\",\"columns\":");
                        write_columns(&mut chunk, batch.schema().as_ref());
                        chunk.extend_from_slice(b",\"rows\":[");
                        first_batch = false;
                    }

                    for row_idx in 0..batch.num_rows() {
                        if has_rows {
                            chunk.push(b',');
                        }
                        write_row(&mut chunk, &batch, row_idx);
                        row_count += 1;
                        has_rows = true;
                    }

                    if tx.send(Ok(Bytes::from(chunk))).await.is_err() {
                        return Ok(());
                    }
                }

                let tail = if first_batch {
                    Bytes::from_static(
                        b"{\"status\":\"ok\",\"columns\":[],\"rows\":[],\"rowCount\":0}",
                    )
                } else {
                    Bytes::from(format!("],\"rowCount\":{}}}", row_count))
                };
                let _ = tx.send(Ok(tail)).await;
                Ok(())
            },
            timeout,
        )
        .await;

        if let Err(e) = result {
            let err_json = match e {
                RestError::Timeout => Bytes::from_static(
                    b"{\"status\":\"error: query timed out\",\"columns\":[],\"rows\":[],\"rowCount\":0}",
                ),
                _ => {
                    let escaped = serde_json::to_string(&e.to_string())
                        .unwrap_or_else(|_| "\"unknown\"".to_string());
                    let json = format!(
                        "{{\"status\":{},\"columns\":[],\"rows\":[],\"rowCount\":0}}",
                        escaped
                    );
                    Bytes::from(json.into_bytes())
                },
            };
            let _ = tx.send(Ok(err_json)).await;
        }
    });

    let rx_stream = stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(rx_stream))
        .unwrap()
}
