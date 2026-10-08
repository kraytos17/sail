mod core;
mod options;
mod state;

use indexmap::IndexMap;
pub use options::WorkerScalerOptions;
pub(crate) use state::{WorkerDemandReason, WorkerLaunchRequest, WorkerRetryRequest};

use crate::driver::worker_scaler::state::WorkerDemand;
use crate::id::{IdGenerator, WorkerDemandId, WorkerId};

/// A compact snapshot of worker demand state used for scheduling diagnostics.
#[derive(Debug, Default, Clone, Copy)]
pub struct WorkerScalerSummary {
    /// Total number of tracked demands.
    pub total: usize,
    /// Demands that have not been launched yet.
    pub created: usize,
    /// Demands whose worker launch is in flight.
    pub launching: usize,
    /// Demands waiting for a launch retry.
    pub waiting_for_retry: usize,
    /// Demands whose launch retries are exhausted.
    pub exhausted: usize,
    /// Demands created for task capacity (as opposed to initial workers).
    pub task: usize,
}

impl WorkerScalerSummary {
    /// Whether any demand can still produce a usable worker.
    pub fn has_pending(&self) -> bool {
        self.created + self.launching + self.waiting_for_retry > 0
    }
}

impl std::fmt::Display for WorkerScalerSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} demand(s) (created {}, launching {}, waiting-for-retry {}, exhausted {}, task {})",
            self.total,
            self.created,
            self.launching,
            self.waiting_for_retry,
            self.exhausted,
            self.task,
        )
    }
}

pub struct WorkerScaler {
    options: WorkerScalerOptions,
    demands: IndexMap<WorkerDemandId, WorkerDemand>,
    workers: IndexMap<WorkerId, WorkerDemandId>,
    worker_demand_id_generator: IdGenerator<WorkerDemandId>,
}

impl WorkerScaler {
    pub fn new(options: WorkerScalerOptions) -> Self {
        Self {
            options,
            demands: IndexMap::new(),
            workers: IndexMap::new(),
            worker_demand_id_generator: IdGenerator::new(),
        }
    }
}
