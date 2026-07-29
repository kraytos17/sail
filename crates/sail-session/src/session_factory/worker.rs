use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::ScalarUDF;
use sail_catalog::provider::CatalogCacheManager;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::repartition::RepartitionBufferConfig;
use sail_delta_lake::session_extension::DeltaTableCache;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_date_part::SparkDatePart;
use sail_function::scalar::datetime::spark_date_trunc::SparkDateTrunc;
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;
use sail_function::scalar::datetime::spark_make_timestamp_ntz::SparkMakeTimestampNtz;
use sail_function::scalar::datetime::spark_next_day::SparkNextDay;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_year::SparkYear;

use crate::catalog::create_catalog_manager;
use crate::runtime::RuntimeEnvFactory;
use crate::session_factory::SessionFactory;

pub struct WorkerSessionFactory {
    runtime_env: RuntimeEnvFactory,
    repartition_buffer_size: usize,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    catalog_cache_manager: Arc<CatalogCacheManager>,
}

impl WorkerSessionFactory {
    pub fn new(
        config: Arc<AppConfig>,
        runtime: RuntimeHandle,
        catalog_cache_manager: Arc<CatalogCacheManager>,
    ) -> Self {
        let repartition_buffer_size = config.cluster.task_stream_buffer;
        let runtime_env = RuntimeEnvFactory::new(config.clone(), runtime.clone());
        Self {
            runtime_env,
            repartition_buffer_size,
            config,
            runtime,
            catalog_cache_manager,
        }
    }
}

impl SessionFactory<()> for WorkerSessionFactory {
    fn create(&mut self, _info: ()) -> Result<SessionContext> {
        let runtime = self.runtime_env.create(Ok)?;
        // We still add default features for the worker session
        // since we need built-in functions to be available for the codec
        // when decoding the execution plan.
        let config = SessionConfig::default()
            .with_extension(Arc::new(DeltaTableCache::default()))
            .with_extension(Arc::new(create_catalog_manager(
                &self.config,
                self.runtime.clone(),
                self.catalog_cache_manager.clone(),
            )?))
            .with_extension(Arc::new(RepartitionBufferConfig::new(
                self.repartition_buffer_size,
            )));
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_scalar_functions(vec![
                Arc::new(ScalarUDF::from(SparkDate::new(false))),
                Arc::new(ScalarUDF::from(SparkDateTrunc::new())),
                Arc::new(ScalarUDF::from(SparkDatePart::new())),
                Arc::new(ScalarUDF::from(SparkLastDay::new())),
                Arc::new(ScalarUDF::from(SparkTime::new(false))),
                Arc::new(ScalarUDF::from(SparkYear::new())),
                Arc::new(ScalarUDF::from(SparkNextDay::new(false))),
                Arc::new(ScalarUDF::from(SparkMakeTimestampNtz::new(false))),
            ])
            .build();
        let session = SessionContext::new_with_state(state);
        Ok(session)
    }
}
