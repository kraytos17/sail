use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::repartition::RepartitionBufferConfig;
use sail_delta_lake::session_extension::DeltaTableCache;

use crate::runtime::RuntimeEnvFactory;
use crate::session_config::SessionConfigFactory;
use crate::session_factory::SessionFactory;

pub struct WorkerSessionFactory {
    runtime_env: RuntimeEnvFactory,
    session_config: SessionConfigFactory,
    repartition_buffer_size: usize,
}

impl WorkerSessionFactory {
    pub fn new(config: Arc<AppConfig>, runtime: RuntimeHandle) -> Self {
        let repartition_buffer_size = config.cluster.task_stream_buffer;
        let runtime_env = RuntimeEnvFactory::new(config.clone(), runtime.clone());
        let session_config = SessionConfigFactory::new(config);
        Self {
            runtime_env,
            session_config,
            repartition_buffer_size,
        }
    }
}

impl SessionFactory<()> for WorkerSessionFactory {
    fn create(&mut self, _info: ()) -> Result<SessionContext> {
        let runtime = self.runtime_env.create(Ok)?;
        // We still add default features for the worker session
        // since we need built-in functions to be available for the codec
        // when decoding the execution plan.
        let mut config = SessionConfig::default()
            .with_extension(Arc::new(DeltaTableCache::default()))
            .with_extension(Arc::new(RepartitionBufferConfig::new(
                self.repartition_buffer_size,
            )));
        // Sail decodes and executes serialized physical plans on the worker using this
        // session config, so it must carry the same execution, parquet, and optimizer
        // settings as the driver session (see `SessionConfigFactory`). This includes
        // disabling file-stream work stealing: each partition task decodes its own plan
        // instance, and DataFusion's sibling-stream work stealing would otherwise make every
        // partition drain the whole shared file queue for byte-range split scans (Nx rows).
        self.session_config.apply_execution_config(&mut config);
        self.session_config
            .apply_execution_parquet_config(&mut config);
        self.session_config.apply_optimizer_config(&mut config);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();
        let session = SessionContext::new_with_state(state);
        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use sail_common::config::AppConfig;
    use tokio::runtime::Runtime;

    use super::*;

    #[test]
    fn worker_session_mirrors_server_execution_config() {
        let app_config = Arc::new(AppConfig::load().expect("load application config"));
        let runtime = Runtime::new().expect("create tokio runtime");
        let runtime_handle = RuntimeHandle::new(runtime.handle().clone(), runtime.handle().clone());
        let mut factory = WorkerSessionFactory::new(app_config.clone(), runtime_handle);

        let session = factory.create(()).expect("create worker session");

        let session_config = session.copied_config();
        let options = session_config.options();
        assert!(!options.execution.enable_file_stream_work_stealing);
        let default_target_partitions = SessionConfig::default()
            .options()
            .execution
            .target_partitions;
        let expected_target_partitions = if app_config.execution.default_parallelism > 0 {
            app_config.execution.default_parallelism
        } else {
            default_target_partitions
        };
        assert_eq!(
            options.execution.target_partitions,
            expected_target_partitions
        );
        assert_eq!(
            options.execution.batch_size,
            app_config.execution.batch_size
        );
        assert_eq!(
            options.execution.collect_statistics,
            app_config.execution.collect_statistics
        );
        assert_eq!(
            options.execution.parquet.binary_as_string,
            app_config.parquet.binary_as_string
        );
    }
}
