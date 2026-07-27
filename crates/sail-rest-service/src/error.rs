use std::fmt;
use std::future::Future;
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use datafusion::common::DataFusionError;

#[derive(Debug)]
pub enum RestError {
    DataFusion(DataFusionError),
    Session(String),
    Timeout,
}

impl fmt::Display for RestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RestError::DataFusion(e) => write!(f, "{e}"),
            RestError::Session(e) => write!(f, "{e}"),
            RestError::Timeout => write!(f, "query timed out"),
        }
    }
}

impl std::error::Error for RestError {}

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        let (status, body) = match &self {
            RestError::Timeout => (
                StatusCode::REQUEST_TIMEOUT,
                r#"{"status":"error: query timed out","columns":[],"rows":[],"rowCount":0}"#
                    .to_string(),
            ),
            _ => {
                let body = format!(
                    r#"{{"status":"error: {}","columns":[],"rows":[],"rowCount":0}}"#,
                    self
                );
                (StatusCode::INTERNAL_SERVER_ERROR, body)
            }
        };
        (status, body).into_response()
    }
}

impl From<DataFusionError> for RestError {
    fn from(e: DataFusionError) -> Self {
        RestError::DataFusion(e)
    }
}

impl From<Box<dyn std::error::Error>> for RestError {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        RestError::Session(e.to_string())
    }
}

pub async fn with_timeout<T, F: Future<Output = Result<T, RestError>>>(
    fut: F,
    timeout_secs: Option<u64>,
) -> Result<T, RestError> {
    match timeout_secs {
        Some(secs) => match tokio::time::timeout(Duration::from_secs(secs), fut).await {
            Ok(result) => result,
            Err(_) => Err(RestError::Timeout),
        },
        None => fut.await,
    }
}
