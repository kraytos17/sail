//! A dedicated module for worker pool options to ensure readonly access.
use std::time::Duration;

use sail_server::RetryStrategy;

use crate::driver::DriverOptions;
use crate::id::DriverId;
use crate::shuffle::ShuffleBackendKind;

#[readonly::make]
pub struct WorkerPoolOptions {
    pub enable_tls: bool,
    pub session_id: String,
    pub driver_id: DriverId,
    pub driver_server_port: u16,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub worker_max_idle_time: Duration,
    pub worker_heartbeat_interval: Duration,
    pub worker_heartbeat_timeout: Duration,
    pub worker_launch_timeout: Duration,
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
    pub rpc_retry_strategy: RetryStrategy,
    pub shuffle_backend: ShuffleBackendKind,
}

#[cfg(test)]
impl WorkerPoolOptions {
    pub fn new_for_test(
        enable_tls: bool,
        session_id: String,
        driver_id: DriverId,
        driver_server_port: u16,
        driver_external_host: String,
        driver_external_port: u16,
        worker_max_idle_time: Duration,
        worker_heartbeat_interval: Duration,
        worker_heartbeat_timeout: Duration,
        worker_launch_timeout: Duration,
        task_stream_buffer: usize,
        task_stream_creation_timeout: Duration,
        rpc_retry_strategy: RetryStrategy,
        shuffle_backend: ShuffleBackendKind,
    ) -> Self {
        Self {
            enable_tls,
            session_id,
            driver_id,
            driver_server_port,
            driver_external_host,
            driver_external_port,
            worker_max_idle_time,
            worker_heartbeat_interval,
            worker_heartbeat_timeout,
            worker_launch_timeout,
            task_stream_buffer,
            task_stream_creation_timeout,
            rpc_retry_strategy,
            shuffle_backend,
        }
    }
}

impl From<&DriverOptions> for WorkerPoolOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            enable_tls: options.enable_tls,
            session_id: options.session_id.clone(),
            driver_id: options.driver_id,
            driver_server_port: options.driver_server_port,
            driver_external_host: options.driver_external_host.clone(),
            driver_external_port: options.driver_external_port,
            worker_max_idle_time: options.worker_max_idle_time,
            worker_heartbeat_interval: options.worker_heartbeat_interval,
            worker_heartbeat_timeout: options.worker_heartbeat_timeout,
            worker_launch_timeout: options.worker_launch_timeout,
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
            rpc_retry_strategy: options.rpc_retry_strategy.clone(),
            shuffle_backend: options.shuffle_backend.clone(),
        }
    }
}
