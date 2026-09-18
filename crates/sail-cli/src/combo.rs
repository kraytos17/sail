// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A single long-running process that serves both the Spark Connect and Arrow
//! Flight SQL protocols off ONE shared session.
//!
//! Sail's worker fleet is scoped per session id within a [`SessionManager`].
//! By hosting both protocols off one shared session manager *and* one shared
//! canonical session id, a Spark Connect client and a Flight SQL client reuse
//! the *same* driver + worker set instead of each protocol (or each client)
//! spawning its own fleet.
//!
//! Note on the shared session manager: each protocol normally builds a session
//! manager with its own `ServerSessionFactory` (a `SessionMutator` that tweaks
//! per-session Spark/Flight `SessionConfig`) and its own session timeout. Worker
//! pool allocation is driven by the cluster config (not the mutator), so the pool
//! itself is identical regardless of which factory is used. This combined server
//! therefore builds ONE session manager with the **Spark** factory and wires both
//! services to it. The Spark factory is mandatory, not a preference: every
//! cancel/reattach/release path resolves `ctx.extension::<SparkSession>()`, and
//! the base factory attaches `ActivityTracker` + `JobService` regardless of the
//! mutator, so Flight's `JobService` dependency is satisfied on a Spark-mutated
//! session.
//!
//! There is deliberately NO plain (non-multiplexed) Spark Connect listener here,
//! only the multiplexed one. The multiplexer echoes the canonical session id in
//! `server_side_session_id`, and a plain listener sharing the same manager would
//! honor `release_session(canonical)` for real — stopping the runner, wiping
//! checkpoints, and dropping the driver out from under every other client.
//!
//! [`SessionManager`]: sail_session::session_manager::SessionManager

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use log::info;
use sail_common::actor::ActorSystem;
use sail_common::config::{AppConfig, GRPC_MAX_MESSAGE_LENGTH_DEFAULT};
use sail_common::runtime::RuntimeManager;
use sail_common::server::ServerBuilder;
use sail_flight::service::SailFlightSqlService;
use sail_spark_connect::create_spark_session_manager;
use sail_spark_connect::multiplexer::{
    MultiplexedSparkConnectServer, resolve_canonical_session_id,
};
use sail_spark_connect::spark::connect::spark_connect_service_server::SparkConnectServiceServer;
use sail_telemetry::telemetry::{ResourceOptions, init_telemetry, shutdown_telemetry};
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down the combined Sail server...");
}

struct TelemetryGuard {
    _marker: (),
}

impl TelemetryGuard {
    fn try_new(config: &AppConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Initialized exactly once for this process: the standalone Spark and
        // Flight CLIs each initialize telemetry their own way, and a second
        // init here would conflict with either.
        let resource = ResourceOptions { kind: "server" };
        init_telemetry(&config.telemetry, resource)?;
        Ok(Self { _marker: () })
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        shutdown_telemetry();
    }
}

/// Starts a single process hosting the multiplexed Spark Connect server and the
/// Flight SQL server off one shared session manager and one shared canonical
/// session id, keeping a single warm worker fleet.
///
/// Every Spark Connect client keeps sending its own `session_id` (responses echo
/// it back), but all requests are stamped with the canonical id on the backend;
/// the Flight SQL service uses that same canonical id as its default session.
/// The canonical session is never created explicitly: it materializes lazily
/// through the ordinary `get_or_create` path on first use and self-heals the
/// same way after a restart or idle eviction.
pub fn run_combo_server(
    ip: IpAddr,
    spark_port: u16,
    flight_port: u16,
    canonical_session_id: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);

    let runtime_manager = RuntimeManager::try_new(&config.runtime)?;

    let _telemetry = runtime_manager
        .handle()
        .primary()
        .block_on(async { TelemetryGuard::try_new(&config) })?;

    runtime_manager.handle().primary().block_on(async {
        // One ActorSystem for the process, following the `entrypoint::serve`
        // idiom (single owner calls `shutdown()` then `join()` at the end).
        let mut system = ActorSystem::new();
        let session_manager = create_spark_session_manager(
            config.clone(),
            runtime_manager.handle().clone(),
            &mut system,
        )
        .await?;

        // Resolve the canonical session id ONCE so the multiplexer and the
        // Flight SQL service share it. A blank input falls back to a fresh
        // UUID for the lifetime of this process.
        let canonical_session_id = resolve_canonical_session_id(canonical_session_id);

        // Bind both listeners up front so a bind error surfaces before either
        // server starts accepting connections.
        let spark_listener = TcpListener::bind(SocketAddr::new(ip, spark_port)).await?;
        let flight_listener = TcpListener::bind(SocketAddr::new(ip, flight_port)).await?;

        let mux_server = MultiplexedSparkConnectServer::new(
            session_manager.clone(),
            Some(canonical_session_id.clone()),
        );
        info!(
            "Sail session multiplexer listening on port {spark_port} (canonical session: {})",
            mux_server.canonical_session_id()
        );
        let mux_service = SparkConnectServiceServer::new(mux_server)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        let flight_service = FlightServiceServer::new(SailFlightSqlService::with_default_session(
            session_manager.clone(),
            Some(canonical_session_id),
        ));

        // Keepalive/timeout behavior comes from `ServerBuilderOptions::default()`
        // (60 s HTTP/2 keepalive timeout), matching the standalone servers.
        let spark_task = ServerBuilder::new("sail_spark_connect_mux", Default::default())
            .add_service(
                mux_service,
                Some(sail_spark_connect::spark::connect::FILE_DESCRIPTOR_SET),
            )
            .await
            .serve(spark_listener, shutdown());
        let flight_task = ServerBuilder::new("flight_sql", Default::default())
            .add_service(flight_service, None)
            .await
            .serve(flight_listener, shutdown());

        // Fail fast on the first serve error: `join!` would wait for the peer
        // even after one side has failed, hanging the process in a degraded
        // half-serving state until SIGINT. Dropping the peer future cancels it.
        // On graceful shutdown both tasks complete around the same time; the
        // winner is returned first and the peer is then awaited so its
        // graceful teardown still runs to completion.
        tokio::pin!(spark_task);
        tokio::pin!(flight_task);
        let (first_is_spark, first_result) = tokio::select! {
            result = &mut spark_task => (true, result),
            result = &mut flight_task => (false, result),
        };
        let (spark_result, flight_result) = match (first_is_spark, first_result) {
            (_, Err(e)) => return Err(e),
            (true, Ok(())) => (Ok(()), flight_task.await),
            (false, Ok(())) => (spark_task.await, Ok(())),
        };

        session_manager
            .shutdown()
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        system.join().await;

        match (spark_result, flight_result) {
            (Err(e), _) | (_, Err(e)) => Err(e),
            _ => Ok(()),
        }
    })
}
