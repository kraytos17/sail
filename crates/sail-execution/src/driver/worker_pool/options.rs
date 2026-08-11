//! A dedicated module for worker pool options to ensure readonly access.
use std::time::Duration;

use sail_server::RetryStrategy;

use crate::driver::DriverOptions;

#[readonly::make]
pub struct WorkerPoolOptions {
    pub enable_tls: bool,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub worker_max_idle_time: Duration,
    pub worker_heartbeat_interval: Duration,
    pub worker_heartbeat_timeout: Duration,
    pub worker_launch_timeout: Duration,
    pub spawn_retry_strategy: RetryStrategy,
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
    pub rpc_retry_strategy: RetryStrategy,
}

impl WorkerPoolOptions {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self {
            enable_tls: false,
            driver_external_host: "localhost".to_string(),
            driver_external_port: 0,
            worker_max_idle_time: Duration::from_secs(10),
            worker_heartbeat_interval: Duration::from_secs(1),
            worker_heartbeat_timeout: Duration::from_secs(3),
            worker_launch_timeout: Duration::from_secs(5),
            spawn_retry_strategy: RetryStrategy::Fixed {
                max_count: 1,
                delay: Duration::from_secs(1),
            },
            task_stream_buffer: 16,
            task_stream_creation_timeout: Duration::from_secs(5),
            rpc_retry_strategy: RetryStrategy::Fixed {
                max_count: 1,
                delay: Duration::from_millis(10),
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test_with_spawn_retry(spawn_retry_strategy: RetryStrategy) -> Self {
        Self {
            spawn_retry_strategy,
            ..Self::for_test()
        }
    }
}

impl From<&DriverOptions> for WorkerPoolOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            enable_tls: options.enable_tls,
            driver_external_host: options.driver_external_host.clone(),
            driver_external_port: options.driver_external_port,
            worker_max_idle_time: options.worker_max_idle_time,
            worker_heartbeat_interval: options.worker_heartbeat_interval,
            worker_heartbeat_timeout: options.worker_heartbeat_timeout,
            worker_launch_timeout: options.worker_launch_timeout,
            spawn_retry_strategy: options.worker_spawn_retry_strategy.clone(),
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
            rpc_retry_strategy: options.rpc_retry_strategy.clone(),
        }
    }
}
