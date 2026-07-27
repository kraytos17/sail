use std::net::IpAddr;
use std::sync::Arc;

use log::info;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeManager;
use sail_rest_service::RestService;
use tokio::net::TcpListener;

async fn shutdown(session_manager: Arc<sail_session::session_manager::SessionManager>) {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {
            info!("Received SIGTERM");
        }
    }
    info!("Shutting down REST server, cleaning up sessions...");
    let _ = session_manager.delete_session("rest-api".to_string()).await;
    info!("Session cleaned up.");
}

pub fn run_rest_server(ip: IpAddr, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);
    let runtime = RuntimeManager::try_new(&config.runtime)?;

    let _telemetry = runtime
        .handle()
        .primary()
        .block_on(async { crate::spark::server::telemetry::TelemetryGuard::try_new(&config) })?;

    runtime.handle().primary().block_on(async {
        let address = std::net::SocketAddr::new(ip, port);
        let listener = TcpListener::bind(address).await?;
        info!("Starting the REST server on {address}...");

        let service = RestService::try_new(config, runtime.handle().clone())?;
        let session_manager = service.session_manager().clone();
        let router = service.router();

        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown(session_manager))
            .await?;
        Ok(())
    })
}
