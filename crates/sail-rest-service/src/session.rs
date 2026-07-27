use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use datafusion::common::{Result, internal_datafusion_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::plan::PlanService;
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_server::actor::ActorSystem;
use sail_session::session_factory::{
    ServerSessionFactory, ServerSessionInfo, ServerSessionMutator, SessionFactory,
};
use sail_session::session_manager::{SessionManager, SessionManagerOptions};
use uuid::Uuid;

use crate::error::RestError;

const REST_SESSION_TIMEOUT_SECS: i64 = 3600;

pub struct RestSessionMutator {
    #[expect(dead_code)]
    config: Arc<AppConfig>,
}

impl ServerSessionMutator for RestSessionMutator {
    fn mutate_config(
        &self,
        config: SessionConfig,
        _info: &ServerSessionInfo,
    ) -> Result<SessionConfig> {
        let plan_service = PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        );
        Ok(config.with_extension(Arc::new(plan_service)))
    }

    fn mutate_state(
        &self,
        builder: SessionStateBuilder,
        _info: &ServerSessionInfo,
    ) -> Result<SessionStateBuilder> {
        Ok(builder)
    }

    fn mutate_runtime_env(
        &self,
        builder: RuntimeEnvBuilder,
        _info: &ServerSessionInfo,
    ) -> Result<RuntimeEnvBuilder> {
        Ok(builder)
    }
}

fn create_rest_session_factory(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    system: Arc<Mutex<ActorSystem>>,
) -> Box<dyn SessionFactory<ServerSessionInfo>> {
    let mutator = Box::new(RestSessionMutator {
        config: config.clone(),
    });
    Box::new(ServerSessionFactory::new(config, runtime, system, mutator))
}

pub fn create_rest_session_manager(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<SessionManager, RestError> {
    let system = Arc::new(Mutex::new(ActorSystem::new()));
    let factory = {
        let config = config.clone();
        let runtime = runtime.clone();
        let system = system.clone();
        Box::new(move || {
            create_rest_session_factory(config.clone(), runtime.clone(), system.clone())
        })
    };
    let session_timeout = if REST_SESSION_TIMEOUT_SECS < 0 {
        log::info!("REST session timeout: infinite (session_timeout_secs = -1)");
        Duration::MAX
    } else {
        let secs = REST_SESSION_TIMEOUT_SECS as u64;
        log::info!("REST session timeout: {secs} seconds");
        Duration::from_secs(secs)
    };
    let options = SessionManagerOptions::new(runtime.clone(), system, factory)
        .with_session_timeout(session_timeout);
    SessionManager::try_new(options).map_err(|e| {
        RestError::DataFusion(internal_datafusion_err!(
            "Failed to create session manager: {}",
            e
        ))
    })
}

pub async fn get_session_context(
    session_manager: &SessionManager,
    session_id: &str,
) -> Result<SessionContext, RestError> {
    session_manager
        .get_or_create_session_context(session_id.to_string(), "rest-user".to_string())
        .await
        .map_err(|e| RestError::Session(format!("session error: {e}")))
}

#[derive(serde::Serialize)]
pub struct SessionResponse {
    status: String,
    #[serde(rename = "sessionId")]
    session_id: String,
}

pub fn default_session_id() -> String {
    "rest-api".to_string()
}

pub async fn handle_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"ok"}))
}

pub async fn handle_create_session(
    State(session_manager): State<Arc<SessionManager>>,
) -> Result<Json<SessionResponse>, RestError> {
    let session_id = Uuid::new_v4().to_string();
    get_session_context(&session_manager, &session_id).await?;
    Ok(Json(SessionResponse {
        status: "ok".to_string(),
        session_id,
    }))
}

#[derive(serde::Deserialize)]
pub struct DeleteSessionRequest {
    #[serde(rename = "sessionId", default = "default_session_id")]
    session_id: String,
}

pub async fn handle_delete_session(
    State(session_manager): State<Arc<SessionManager>>,
    Json(req): Json<DeleteSessionRequest>,
) -> Result<Json<serde_json::Value>, RestError> {
    session_manager
        .delete_session(req.session_id.clone())
        .await
        .map_err(|e| RestError::Session(format!("delete session failed: {e}")))?;
    Ok(Json(serde_json::json!({"status":"ok"})))
}
