mod kubernetes;
mod local;
mod options;

pub(crate) use options::WorkerLaunchOptions;

use crate::error::ExecutionResult;
use crate::id::WorkerId;

#[tonic::async_trait]
pub trait WorkerManager: Send + Sync + 'static {
    /// Launch a worker.
    async fn launch_worker(
        &self,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> ExecutionResult<()>;

    /// Delete a single worker that failed to start or has been reaped.
    /// This is used to clean up a pod that never registered (e.g. stuck in
    /// `Pending` because it could not be scheduled), which would otherwise linger
    /// in the cluster until [`WorkerManager::stop`].
    async fn delete_worker(&self, id: WorkerId) -> ExecutionResult<()>;

    /// Stop all workers on a best-effort basis.
    /// The driver have attempted to sent shutdown events to all workers
    /// at this point, but it is unknown whether the events have been received.
    /// The worker manager is supposed to wait for the termination of all workers
    /// before returning from this method.
    /// Note that the driver process may terminate abnormally before this method finishes.
    /// In such cases, the external systems should be responsible for cleaning up the workers
    /// (e.g. via the cascading deletion mechanism in Kubernetes).
    async fn stop(&self) -> ExecutionResult<()>;
}

pub use kubernetes::{KubernetesWorkerManager, KubernetesWorkerManagerOptions};
pub use local::LocalWorkerManager;
