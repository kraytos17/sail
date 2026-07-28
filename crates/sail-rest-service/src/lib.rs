pub mod batch;
pub mod error;
pub mod load;
pub mod query;
pub mod read;
pub mod session;

use std::sync::Arc;

use axum::Router;
use axum::middleware::{self, Next};
use axum::response::Response;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_session::session_manager::SessionManager;

pub struct RestService {
    session_manager: Arc<SessionManager>,
}

async fn request_logger(req: axum::http::Request<axum::body::Body>, next: Next) -> Response {
    let start = std::time::Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let response = next.run(req).await;
    let duration = start.elapsed();
    let status = response.status();
    if status.is_server_error() {
        log::error!("{} {} {} - {:?}", method, path, status.as_u16(), duration);
    } else {
        log::info!("{} {} {} - {:?}", method, path, status.as_u16(), duration);
    }
    response
}

impl RestService {
    pub fn try_new(
        config: Arc<AppConfig>,
        runtime: RuntimeHandle,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let session_manager = Arc::new(crate::session::create_rest_session_manager(
            config, runtime,
        )?);
        Ok(Self { session_manager })
    }

    pub fn from_session_manager(session_manager: Arc<SessionManager>) -> Self {
        Self { session_manager }
    }

    pub fn session_manager(&self) -> &Arc<SessionManager> {
        &self.session_manager
    }

    pub fn router(self) -> Router {
        let state: Arc<SessionManager> = self.session_manager;
        Router::new()
            .route(
                "/engine/dbt/query",
                axum::routing::post(crate::query::handle_query),
            )
            .route(
                "/engine/dbt/load",
                axum::routing::post(crate::load::handle_load),
            )
            .route(
                "/engine/dbt/session",
                axum::routing::get(crate::session::handle_create_session),
            )
            .route(
                "/engine/dbt/session",
                axum::routing::delete(crate::session::handle_delete_session),
            )
            .route(
                "/engine/dbt/batch",
                axum::routing::post(crate::batch::handle_batch),
            )
            .route(
                "/engine/dbt/read",
                axum::routing::post(crate::read::handle_read),
            )
            .route(
                "/engine/dbt/health",
                axum::routing::get(crate::session::handle_health),
            )
            .layer(middleware::from_fn(request_logger))
            .with_state(state)
    }
}
