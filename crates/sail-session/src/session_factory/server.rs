use std::sync::Arc;

use datafusion::common::{Result, internal_err};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::functions_aggregate::first_last::first_value_udaf;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::registry::FunctionRegistry;
use sail_cache::remote_checkpoint::RemoteCheckpointRegistry;
use sail_catalog::provider::CatalogCacheManager;
use sail_catalog_system::service::SystemTableService;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::activity::ActivityTracker;
use sail_common_datafusion::session::job::{JobRunner, JobService};
use sail_common_datafusion::session::repartition::RepartitionBufferConfig;
use sail_delta_lake::session_extension::DeltaTableCache;
use sail_physical_optimizer::{PhysicalOptimizerOptions, get_physical_optimizers};
use sail_server::actor::ActorHandle;

use crate::catalog::create_catalog_manager;
use crate::formats::create_table_format_registry;
use crate::observable::SessionManagerHandle;
use crate::optimizer::{default_analyzer_rules, default_optimizer_rules};
use crate::planner::new_query_planner;
use crate::runtime::RuntimeEnvFactory;
use crate::session_config::SessionConfigFactory;
use crate::session_factory::SessionFactory;
use crate::session_manager::SessionManagerActor;

pub struct ServerSessionInfo {
    pub session_id: String,
    pub user_id: String,
    pub session_manager: ActorHandle<SessionManagerActor>,
    pub job_runner: Option<Box<dyn JobRunner>>,
}

pub trait ServerSessionMutator: Send {
    fn mutate_config(
        &self,
        config: SessionConfig,
        info: &ServerSessionInfo,
    ) -> Result<SessionConfig>;
    fn mutate_state(
        &self,
        builder: SessionStateBuilder,
        info: &ServerSessionInfo,
    ) -> Result<SessionStateBuilder>;
    fn mutate_runtime_env(
        &self,
        builder: RuntimeEnvBuilder,
        info: &ServerSessionInfo,
    ) -> Result<RuntimeEnvBuilder>;
}

pub struct ServerSessionFactory {
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    mutator: Box<dyn ServerSessionMutator>,
    runtime_env: RuntimeEnvFactory,
    session_config: SessionConfigFactory,
    catalog_cache_manager: Arc<CatalogCacheManager>,
}

impl ServerSessionFactory {
    pub fn new(
        config: Arc<AppConfig>,
        runtime: RuntimeHandle,
        mutator: Box<dyn ServerSessionMutator>,
    ) -> Self {
        let runtime_env = RuntimeEnvFactory::new(config.clone(), runtime.clone());
        let session_config = SessionConfigFactory::new(config.clone());
        Self {
            config,
            runtime,
            mutator,
            runtime_env,
            session_config,
            catalog_cache_manager: Arc::new(CatalogCacheManager::new()),
        }
    }
}

impl SessionFactory<ServerSessionInfo> for ServerSessionFactory {
    fn create(&mut self, mut info: ServerSessionInfo) -> Result<SessionContext> {
        let state = self.create_session_state(&mut info)?;
        let context = SessionContext::new_with_state(state);

        // Register the `first_value` UDAF since the `replace_distinct_aggregate` optimizer rule
        // assumes that this UDAF is available in the function registry.
        // This is a hidden assumption made by the optimizer rule.
        // We have to do so because we do not add default features (including built-in functions)
        // to the session state.
        //
        // See also: https://github.com/apache/datafusion/issues/10703
        context
            .state_ref()
            .write()
            .register_udaf(first_value_udaf())?;

        Ok(context)
    }
}

impl ServerSessionFactory {
    fn create_session_config(&mut self, info: &mut ServerSessionInfo) -> Result<SessionConfig> {
        let Some(job_runner) = info.job_runner.take() else {
            return internal_err!("job runner is missing from server session information");
        };
        let mut config = SessionConfig::new()
            // We do not use the DataFusion catalog and schema since we manage catalogs ourselves.
            .with_create_default_catalog_and_schema(false)
            .with_information_schema(false)
            .with_extension(create_table_format_registry()?)
            .with_extension(Arc::new(create_catalog_manager(
                &self.config,
                self.runtime.clone(),
                self.catalog_cache_manager.clone(),
            )?))
            .with_extension(Arc::new(ActivityTracker::new()))
            .with_extension(Arc::new(JobService::new(job_runner)))
            .with_extension(Arc::new(RemoteCheckpointRegistry::new(
                self.config.execution.checkpoint.path.clone(),
                info.session_id.clone(),
            )))
            .with_extension(Arc::new(RepartitionBufferConfig::new(
                self.config.cluster.task_stream_buffer,
            )))
            .with_extension(Arc::new(self.create_system_table_service(info)?))
            .with_extension(Arc::new(DeltaTableCache::default()));
        self.session_config.apply_execution_config(&mut config);
        self.session_config
            .apply_execution_parquet_config(&mut config);
        self.session_config.apply_optimizer_config(&mut config);
        let config = self.mutator.mutate_config(config, info)?;
        Ok(config)
    }

    fn create_session_state(&mut self, info: &mut ServerSessionInfo) -> Result<SessionState> {
        let config = self.create_session_config(info)?;
        let runtime = self
            .runtime_env
            .create(|builder| self.mutator.mutate_runtime_env(builder, info))?;
        // We do not add default features to the session state,
        // since we manage table formats and functions ourselves.
        let builder = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_analyzer_rules(default_analyzer_rules())
            .with_optimizer_rules(default_optimizer_rules())
            .with_physical_optimizer_rules(get_physical_optimizers(PhysicalOptimizerOptions {
                enable_join_reorder: self.config.optimizer.enable_join_reorder,
                ..Default::default()
            }))
            .with_query_planner(new_query_planner());
        let builder = self.mutator.mutate_state(builder, info)?;
        Ok(builder.build())
    }

    fn create_system_table_service(&self, info: &ServerSessionInfo) -> Result<SystemTableService> {
        Ok(SystemTableService::new(Box::new(
            SessionManagerHandle::new(info.session_manager.clone()),
        )))
    }
}
