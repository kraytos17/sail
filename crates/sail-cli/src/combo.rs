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
//! Flight SQL protocols.
//!
//! Sail's worker fleet is scoped per session id within a [`SessionManager`]. By
//! hosting both protocols off ONE shared session manager, a Spark Connect client
//! and a Flight SQL client that pin the same `session_id` reuse the *same* driver
//! + worker set instead of each protocol spawning its own fleet.
//!
//! Note on the shared session manager: each protocol normally builds a session
//! manager with its own `ServerSessionFactory` (a `SessionMutator` that tweaks
//! per-session Spark/Flight `SessionConfig`) and its own session timeout. Worker
//! pool allocation is driven by the cluster config (not the mutator), so the pool
//! itself is identical regardless of which factory is used. This combined server
//! therefore builds ONE session manager and wires both services to it, so that a
//! shared `session_id` maps to a single driver + worker fleet.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use log::info;
use sail_common::config::{AppConfig, GRPC_MAX_MESSAGE_LENGTH_DEFAULT};
use sail_common::runtime::RuntimeManager;
use sail_flight::service::SailFlightSqlService;
use sail_server::{ServerBuilder, ServerBuilderOptions};
use sail_spark_connect::create_spark_session_manager;
use sail_spark_connect::multiplexer::{
    MultiplexedSparkConnectServer, resolve_canonical_session_id,
};
use sail_spark_connect::server::SparkConnectServer;
use sail_spark_connect::spark::connect::spark_connect_service_server::SparkConnectServiceServer;
use sail_telemetry::telemetry::{ResourceOptions, init_telemetry, shutdown_telemetry};
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down the combined Sail server...");
}

/// Starts a single process hosting both the Spark Connect and Flight SQL servers
/// off one shared session manager, keeping a single warm worker fleet.
///
/// When `mux_port` is non-zero, an additional Spark Connect listener is hosted
/// on it that multiplexes every client onto ONE canonical backend session
/// (clients keep their own ids; responses echo them back), so heterogeneous
/// clients share a single fleet without coordinating on a session id. The
/// Flight SQL service then uses that same canonical session as its default,
/// so Spark Connect and Flight SQL clients share ONE driver + worker fleet.
pub fn run_combo_server(
    ip: IpAddr,
    spark_port: u16,
    flight_port: u16,
    mux_port: u16,
    canonical_session_id: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);

    let runtime_manager = RuntimeManager::try_new(&config.runtime)?;

    runtime_manager.handle().primary().block_on(async {
        init_telemetry(&config.telemetry, ResourceOptions { kind: "server" })
    })?;

    let result = runtime_manager.handle().primary().block_on(async {
        let http2_keepalive_timeout =
            std::time::Duration::from_secs(config.server.http2_keepalive_timeout_secs);

        // Build ONE shared session manager backing both protocols so a pinned
        // `session_id` resolves to a single driver + worker fleet.
        let session_manager =
            create_spark_session_manager(config, runtime_manager.handle().clone()).await?;

        let spark_listener = TcpListener::bind(SocketAddr::new(ip, spark_port)).await?;
        let flight_listener = TcpListener::bind(SocketAddr::new(ip, flight_port)).await?;

        let spark_server = SparkConnectServer::new(session_manager.clone());
        let spark_service = SparkConnectServiceServer::new(spark_server)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        // Resolve the canonical session id ONCE so the multiplexer and the
        // Flight SQL service share it: when the mux is enabled, Spark Connect
        // and Flight SQL clients then land on ONE driver + worker fleet.
        let canonical_session_id = resolve_canonical_session_id(canonical_session_id);

        let flight_service = FlightServiceServer::new(SailFlightSqlService::with_default_session(
            session_manager.clone(),
            (mux_port > 0).then(|| canonical_session_id.clone()),
        ));

        let mux_task = if mux_port > 0 {
            let mux_listener = TcpListener::bind(SocketAddr::new(ip, mux_port)).await?;
            let mux_server = MultiplexedSparkConnectServer::new(
                session_manager.clone(),
                Some(canonical_session_id),
            );
            info!(
                "Sail session multiplexer listening on port {mux_port} (canonical session: {})",
                mux_server.canonical_session_id()
            );
            let mux_service = SparkConnectServiceServer::new(mux_server)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd)
                .send_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Zstd);
            Some(
                ServerBuilder::new(
                    "sail_spark_connect_mux",
                    ServerBuilderOptions {
                        http2_keepalive_timeout: Some(http2_keepalive_timeout),
                        ..Default::default()
                    },
                )
                .add_service(
                    mux_service,
                    Some(sail_spark_connect::spark::connect::FILE_DESCRIPTOR_SET),
                )
                .await
                .serve(mux_listener, shutdown()),
            )
        } else {
            None
        };

        let spark_task = ServerBuilder::new(
            "sail_spark_connect",
            ServerBuilderOptions {
                http2_keepalive_timeout: Some(http2_keepalive_timeout),
                ..Default::default()
            },
        )
        .add_service(
            spark_service,
            Some(sail_spark_connect::spark::connect::FILE_DESCRIPTOR_SET),
        )
        .await
        .serve(spark_listener, shutdown());
        let flight_task = ServerBuilder::new(
            "flight_sql",
            ServerBuilderOptions {
                http2_keepalive_timeout: Some(http2_keepalive_timeout),
                ..Default::default()
            },
        )
        .add_service(flight_service, None)
        .await
        .serve(flight_listener, shutdown());

        let mux_result = async {
            match mux_task {
                Some(task) => task.await,
                None => Ok(()),
            }
        };
        let (spark_result, flight_result, mux_result) =
            tokio::join!(spark_task, flight_task, mux_result);

        session_manager
            .shutdown()
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        match (spark_result, flight_result, mux_result) {
            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => Err(e),
            _ => Ok(()),
        }
    });

    shutdown_telemetry();

    result
}
