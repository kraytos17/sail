//! Self-healing sessions: `get_or_create` recreates `Deleted`/`Failed` entries
//! instead of permanently returning "session {id} is not running", and delete
//! on terminal states is idempotent. See `port/14-self-healing-sessions.md`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use opentelemetry::InstrumentationScope;
use opentelemetry::logs::LoggerProvider;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use sail_common::actor::ActorSystem;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_session::session_factory::{
    ServerSessionInfo, ServerSessionJobRunnerFactory, SessionFactory,
};
use sail_session::session_manager::{
    SessionManager, SessionManagerComponents, SessionManagerOptions,
};
use sail_telemetry::events::SystemEventReporter;

/// A session factory whose failures are toggled from the test body.
/// The handler calls `create` with `&mut self` while owning the factory, so
/// the flag is shared rather than borrowed.
#[derive(Clone)]
struct FlakySessionFactory {
    fail: Arc<AtomicBool>,
}

impl SessionFactory<ServerSessionInfo> for FlakySessionFactory {
    fn create(&mut self, _info: ServerSessionInfo) -> Result<SessionContext, DataFusionError> {
        if self.fail.load(Ordering::SeqCst) {
            Err(DataFusionError::Internal(
                "injected session creation failure".to_string(),
            ))
        } else {
            Ok(SessionContext::new())
        }
    }
}

fn test_event_reporter() -> SystemEventReporter {
    // No processors attached: `report` serializes and drops, touching no I/O.
    let provider = SdkLoggerProvider::builder().build();
    let scope = InstrumentationScope::builder("sail-session-test").build();
    SystemEventReporter::new(provider.logger_with_scope(scope))
}

fn test_session_manager(
    fail: Arc<AtomicBool>,
) -> Result<(SessionManager, ActorSystem), Box<dyn std::error::Error>> {
    // The checked-in defaults run in `local` mode: session creation uses a
    // `LocalJobRunner` with no driver, so no gateway or cluster is needed.
    let config = Arc::new(AppConfig::load()?);
    // Reuse the test runtime's handle instead of a `RuntimeManager`: owned
    // runtimes cannot be dropped inside an async context.
    let handle = tokio::runtime::Handle::current();
    let runtime = RuntimeHandle::new(handle.clone(), handle);
    let mut system = ActorSystem::new();
    let options = SessionManagerOptions::new(runtime.clone());
    let components = SessionManagerComponents {
        session_factory: Box::new(FlakySessionFactory { fail }),
        job_runner_factory: Box::new(ServerSessionJobRunnerFactory::new(config, runtime)),
        driver_gateway: None,
        event_reporter: test_event_reporter(),
    };
    let manager = SessionManager::try_new(options, components, &mut system)?;
    Ok((manager, system))
}

async fn get_or_create(
    manager: &SessionManager,
    id: &str,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    Ok(manager
        .get_or_create_session_context(id.to_string(), "u".to_string())
        .await?)
}

async fn delete(manager: &SessionManager, id: &str) -> Result<(), Box<dyn std::error::Error>> {
    manager.delete_session(id.to_string()).await?;
    Ok(())
}

async fn shutdown(
    manager: SessionManager,
    mut system: ActorSystem,
) -> Result<(), Box<dyn std::error::Error>> {
    manager.shutdown().await?;
    system.join().await;
    Ok(())
}

#[tokio::test]
async fn recreates_session_after_delete() -> Result<(), Box<dyn std::error::Error>> {
    let (manager, system) = test_session_manager(Arc::new(AtomicBool::new(false)))?;
    let first = get_or_create(&manager, "s").await?;
    delete(&manager, "s").await?;
    // Deleting a `Deleted` session succeeds instead of erroring.
    delete(&manager, "s").await?;
    let second = get_or_create(&manager, "s").await?;
    // A fresh context proves recreation rather than a stale return.
    assert_ne!(first.session_id(), second.session_id());
    shutdown(manager, system).await
}

#[tokio::test]
async fn recreates_session_after_failure() -> Result<(), Box<dyn std::error::Error>> {
    let fail = Arc::new(AtomicBool::new(true));
    let (manager, system) = test_session_manager(fail.clone())?;
    let err = match get_or_create(&manager, "s").await {
        Err(err) => err.to_string(),
        Ok(_) => return Err("creation must fail".into()),
    };
    assert!(
        err.contains("injected session creation failure"),
        "unexpected error: {err}"
    );
    // Deleting a `Failed` session succeeds and frees the ID.
    fail.store(false, Ordering::SeqCst);
    delete(&manager, "s").await?;
    get_or_create(&manager, "s").await?;
    shutdown(manager, system).await
}

#[tokio::test]
async fn concurrent_requests_share_single_creation() -> Result<(), Box<dyn std::error::Error>> {
    // The `Creating` waiter path is unchanged: concurrent requests for an ID
    // that is still being created all receive the same context.
    let (manager, system) = test_session_manager(Arc::new(AtomicBool::new(false)))?;
    let (first, second) = tokio::join!(get_or_create(&manager, "s"), get_or_create(&manager, "s"),);
    let (first, second) = (first?, second?);
    assert_eq!(first.session_id(), second.session_id());
    shutdown(manager, system).await
}
