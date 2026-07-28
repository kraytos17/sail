use std::future::Future;
use std::sync::Arc;

use sail_common::config::{AppConfig, GRPC_MAX_MESSAGE_LENGTH_DEFAULT};
use sail_common::runtime::RuntimeHandle;
use sail_server::ServerBuilder;
pub use sail_session::session_manager::{SessionManager, SessionManagerOptions};
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;

use crate::server::SparkConnectServer;
use crate::session_manager::create_spark_session_manager;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

fn build_service(server: SparkConnectServer) -> SparkConnectServiceServer<SparkConnectServer> {
    SparkConnectServiceServer::new(server)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_LENGTH_DEFAULT)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Zstd)
}

/// The meat of the gRPC server.
pub async fn serve<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let session_manager = Arc::new(create_spark_session_manager(config, runtime)?);
    let server = SparkConnectServer::new(session_manager);
    let service = build_service(server);
    ServerBuilder::new("sail_spark_connect", Default::default())
        .add_service(service, Some(crate::spark::connect::FILE_DESCRIPTOR_SET))
        .await
        .serve(listener, signal)
        .await
}

/// Same as [serve], but accepts a pre-created session manager (shared with REST server).
pub async fn serve_with_session_manager<F>(
    listener: TcpListener,
    signal: F,
    session_manager: Arc<SessionManager>,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let server = SparkConnectServer::new(session_manager);
    let service = build_service(server);
    ServerBuilder::new("sail_spark_connect", Default::default())
        .add_service(service, Some(crate::spark::connect::FILE_DESCRIPTOR_SET))
        .await
        .serve(listener, signal)
        .await
}
