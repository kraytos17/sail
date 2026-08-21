mod core;
mod handler;

use indexmap::IndexMap;
use sail_execution::driver::{DriverGateway, DriverRegistry};
use sail_execution::{DriverId, IdGenerator};

use crate::session_factory::{ServerSessionInfo, SessionFactory, SessionJobRunnerFactory};
use crate::session_manager::session::ServerSession;

pub struct SessionManagerActor {
    options: super::options::SessionManagerOptions,
    session_factory: Box<dyn SessionFactory<ServerSessionInfo>>,
    job_runner_factory: Box<dyn SessionJobRunnerFactory>,
    /// Server-owned session identifier, minted once per process (a fresh UUID per server
    /// restart). This is the fleet key Sail uses for every session: Spark Connect and
    /// Flight clients are normalized to it so they all share ONE driver + worker fleet
    /// regardless of any client-supplied session id.
    server_session_id: String,
    sessions: IndexMap<String, ServerSession>,
    drivers: DriverRegistry,
    driver_gateway: Option<DriverGateway>,
    driver_id_generator: IdGenerator<DriverId>,
    shutdown_notifier: Option<tokio::sync::oneshot::Sender<()>>,
}
