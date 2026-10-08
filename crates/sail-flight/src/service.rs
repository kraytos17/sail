use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::{Stream, StreamExt, stream};
use log::{debug, info};
use prost::Message;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_plan::config::PlanConfig;
use sail_plan::resolve_and_execute_plan;
use sail_session::session_manager::SessionManager;
use sail_sql_analyzer::parser::parse_one_statement;
use sail_sql_analyzer::statement::from_ast_statement;
use sail_telemetry::metrics::MetricRegistry;
use sail_telemetry::telemetry::global_metrics;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, Streaming};

use crate::metrics::{MetricsRecordingContext, MetricsRecordingStream, StatementType};
use crate::state::{QueryHandle, SailFlightSqlState};

pub struct SailFlightSqlService {
    session_manager: SessionManager,
    config: Arc<PlanConfig>,
    metrics: Option<Arc<MetricRegistry>>,
    state: Arc<Mutex<SailFlightSqlState>>,
    default_session_id: String,
}

impl SailFlightSqlService {
    pub fn new(session_manager: SessionManager) -> Self {
        Self::with_default_session(session_manager, None)
    }

    /// Creates a service whose clients share the given backend session id.
    ///
    /// All Flight SQL clients of one service instance always share a single
    /// session; this chooses WHICH one. When `None` (standalone mode), they
    /// share [`Self::DEFAULT_SESSION_ID`]. The combined server injects the
    /// multiplexer's canonical session so Spark Connect and Flight SQL
    /// clients share ONE driver + worker fleet.
    pub fn with_default_session(
        session_manager: SessionManager,
        default_session_id: Option<String>,
    ) -> Self {
        let config = Arc::new(PlanConfig::default());
        let metrics = global_metrics().map(|m| m.registry);
        if metrics.is_some() {
            info!("OpenTelemetry metrics enabled for Flight SQL service");
        }
        SailFlightSqlService {
            session_manager,
            config,
            metrics,
            state: Arc::new(Mutex::new(SailFlightSqlState::new())),
            default_session_id: default_session_id
                .filter(|id| !id.trim().is_empty())
                .unwrap_or_else(|| Self::DEFAULT_SESSION_ID.to_string()),
        }
    }

    const DEFAULT_SESSION_ID: &'static str = "flight-default";
    const DEFAULT_USER_ID: &'static str = "flight-user";

    async fn get_session_context(&self) -> Result<SessionContext, Status> {
        self.session_manager
            .get_or_create_session_context(
                self.default_session_id.clone(),
                Self::DEFAULT_USER_ID.to_string(),
            )
            .await
            .map_err(|e| Status::internal(format!("session error: {e}")))
    }
}

#[tonic::async_trait]
impl FlightSqlService for SailFlightSqlService {
    type FlightService = SailFlightSqlService;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        // Note: not all clients perform handshake with the server.
        debug!("handshake received from client");
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        let output = stream::iter(vec![Ok(response)]);
        Ok(Response::new(Box::pin(output)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_statement: {}", query.query);

        let statement = parse_one_statement(&query.query)
            .map_err(|e| Status::invalid_argument(format!("parse error: {e}")))?;
        let plan = from_ast_statement(statement)
            .map_err(|e| Status::invalid_argument(format!("plan conversion error: {e}")))?;
        let statement_type = match &plan {
            spec::Plan::Query(_) => StatementType::Query,
            spec::Plan::Command(_) => StatementType::Command,
        };

        let ctx = self.get_session_context().await?;
        let (plan, _) = resolve_and_execute_plan(&ctx, self.config.clone(), plan)
            .await
            .map_err(|e| Status::internal(format!("plan error: {e}")))?;
        let schema = plan.schema();
        let service = ctx
            .extension::<JobService>()
            .map_err(|e| Status::internal(format!("job service not found: {e}")))?;
        let stream = service
            .runner()
            .execute(&ctx, plan)
            .await
            .map_err(|e| Status::internal(format!("execution error: {e}")))?;

        let stream: SendableRecordBatchStream = if let Some(ref m) = self.metrics {
            Box::pin(MetricsRecordingStream::new(
                stream,
                m.clone(),
                MetricsRecordingContext {
                    session_id: self.default_session_id.clone(),
                    statement_type,
                },
            ))
        } else {
            stream
        };

        let stream: SendableRecordBatchStream = match statement_type {
            StatementType::Query => stream,
            StatementType::Command => {
                // execute command eagerly and store the result stream in memory
                let mut stream = stream;
                let mut batches = Vec::new();
                while let Some(result) = stream.next().await {
                    batches.push(
                        result.map_err(|e| Status::internal(format!("execution error: {e}")))?,
                    );
                }
                Box::pin(RecordBatchStreamAdapter::new(
                    schema.clone(),
                    stream::iter(batches.into_iter().map(Ok)),
                ))
            }
        };

        let handle = QueryHandle::new();
        self.state.lock().await.add_stream(handle.clone(), stream);
        debug!("query execution started with handle {handle}");

        let ticket = TicketStatementQuery {
            statement_handle: handle.as_bytes().to_vec().into(),
        };
        let ticket = ticket.as_any().encode_to_vec();

        let endpoint = FlightEndpoint {
            ticket: Some(Ticket {
                ticket: ticket.into(),
            }),
            location: vec![],
            expiration_time: None,
            app_metadata: Default::default(),
        };

        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner())
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema error: {e}")))?;

        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle = QueryHandle::try_from(ticket.statement_handle.as_ref())?;
        debug!("do_get_statement: {handle}");

        let stream = self
            .state
            .lock()
            .await
            .remove_stream(&handle)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "query handle not found or already consumed: {handle}"
                ))
            })?;
        let schema = stream.schema();

        let output = stream.map(|result| {
            result.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)))
        });

        let output = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(output)
            .map(|result| result.map_err(|e| Status::internal(format!("encoding error: {e}"))));

        Ok(Response::new(Box::pin(output)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

    use super::SailFlightSqlService;

    struct StubSessionFactory;

    impl SessionFactory<ServerSessionInfo> for StubSessionFactory {
        fn create(&mut self, _info: ServerSessionInfo) -> Result<SessionContext, DataFusionError> {
            Ok(SessionContext::new())
        }
    }

    fn test_session_manager() -> Result<(SessionManager, ActorSystem), Box<dyn std::error::Error>> {
        // The checked-in defaults run in `local` mode, so no gateway or
        // cluster is needed.
        let config = Arc::new(AppConfig::load()?);
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let mut system = ActorSystem::new();
        let options = SessionManagerOptions::new(runtime.clone());
        // No processors attached: `report` serializes and drops, touching no I/O.
        let provider = SdkLoggerProvider::builder().build();
        let scope = InstrumentationScope::builder("sail-flight-test").build();
        let components = SessionManagerComponents {
            session_factory: Box::new(StubSessionFactory),
            job_runner_factory: Box::new(ServerSessionJobRunnerFactory::new(config, runtime)),
            driver_gateway: None,
            event_reporter: SystemEventReporter::new(provider.logger_with_scope(scope)),
        };
        let manager = SessionManager::try_new(options, components, &mut system)?;
        Ok((manager, system))
    }

    #[tokio::test]
    async fn with_default_session_stores_injected_id() -> Result<(), Box<dyn std::error::Error>> {
        let (manager, mut system) = test_session_manager()?;
        let service = SailFlightSqlService::with_default_session(
            manager.clone(),
            Some("canonical".to_string()),
        );
        assert_eq!(service.default_session_id, "canonical");
        manager.shutdown().await?;
        system.join().await;
        Ok(())
    }

    #[tokio::test]
    async fn with_default_session_falls_back_on_none_or_blank()
    -> Result<(), Box<dyn std::error::Error>> {
        for id in [None, Some(String::new()), Some("   ".to_string())] {
            let (manager, mut system) = test_session_manager()?;
            let service = SailFlightSqlService::with_default_session(manager.clone(), id);
            assert_eq!(service.default_session_id, "flight-default");
            manager.shutdown().await?;
            system.join().await;
        }
        Ok(())
    }
}
