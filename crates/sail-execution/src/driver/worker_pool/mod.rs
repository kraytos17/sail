mod core;
mod observer;
mod options;
mod state;

use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
pub use options::WorkerPoolOptions;

use crate::driver::worker_pool::state::WorkerDescriptor;
use crate::id::{IdGenerator, WorkerId};
use crate::worker_manager::WorkerManager;

pub struct WorkerPool {
    options: WorkerPoolOptions,
    driver_server_port: Option<u16>,
    worker_manager: Arc<dyn WorkerManager>,
    workers: IndexMap<WorkerId, WorkerDescriptor>,
    worker_id_generator: IdGenerator<WorkerId>,
    /// The remaining delays to wait between re-spawn attempts after a worker pod
    /// failed to start. Rebuilt from `spawn_retry_strategy` on the first failure
    /// and reset once a worker registers successfully.
    spawn_retry_delays: Option<Box<dyn Iterator<Item = Duration> + Send>>,
    /// Whether a re-spawn retry is currently armed (i.e. a delayed retry event has
    /// been scheduled and not yet fired). While this is set, pending tasks must not
    /// fail on the scheduling timeout because a replacement worker may still come up.
    spawn_retry_armed: bool,
}

impl WorkerPool {
    pub fn new(worker_manager: Arc<dyn WorkerManager>, options: WorkerPoolOptions) -> Self {
        Self {
            options,
            driver_server_port: None,
            worker_manager,
            workers: IndexMap::new(),
            worker_id_generator: IdGenerator::new(),
            spawn_retry_delays: None,
            spawn_retry_armed: false,
        }
    }

    /// Reserves `n` fresh worker ids ahead of spawning. The caller marks each id
    /// `Pending` in the task assigner before calling [`WorkerPool::start_worker_with_id`].
    ///
    /// Returns the ids that were actually reserved; on id-exhaustion this propagates
    /// the [`crate::error::ExecutionResult`] error rather than silently returning fewer.
    pub fn reserve_worker_ids(&mut self, n: usize) -> crate::error::ExecutionResult<Vec<WorkerId>> {
        let mut ids = Vec::with_capacity(n);
        for _ in 0..n {
            ids.push(self.worker_id_generator.next()?);
        }
        Ok(ids)
    }

    /// Returns the delay to wait before re-spawning a replacement worker, or
    /// `None` when the spawn retry strategy has been exhausted.
    ///
    /// The delay iterator is created lazily on the first failed spawn and advances
    /// by one delay on each subsequent failure, so a worker pod that cannot be
    /// scheduled (e.g. `Insufficient cpu`) stops being re-spawned after the
    /// configured number of retries instead of churning forever. When a delay is
    /// returned, the retry is marked as armed (see [`WorkerPool::has_pending_spawn_retry`]).
    ///
    /// The armed flag is only ever set here, never cleared: when several workers
    /// fail close together, the first failure arms a retry (and schedules a
    /// delayed event) while the later ones may exhaust the iterator and return
    /// `None`. Clearing the flag on `None` would make pending tasks stop waiting
    /// even though a retry event is still in flight. The flag is cleared by
    /// [`WorkerPool::fire_spawn_retry`] when the retry event is processed, or by
    /// [`WorkerPool::reset_spawn_retry`] when a worker registers.
    pub fn next_spawn_retry_delay(&mut self) -> Option<Duration> {
        let delays = self
            .spawn_retry_delays
            .get_or_insert_with(|| self.options.spawn_retry_strategy.delay());
        let delay = delays.next();
        if delay.is_some() {
            self.spawn_retry_armed = true;
        }
        delay
    }

    /// Marks the armed re-spawn retry as fired. Called when the delayed retry event
    /// is processed, right before scaling up the worker pool.
    pub fn fire_spawn_retry(&mut self) {
        self.spawn_retry_armed = false;
    }

    /// Returns true while a re-spawn retry is armed and has not fired yet. Pending
    /// tasks should keep waiting (rather than failing on the scheduling timeout)
    /// because a replacement worker may still register.
    pub fn has_pending_spawn_retry(&self) -> bool {
        self.spawn_retry_armed
    }

    /// Resets the spawn retry state. Called when a worker registers successfully,
    /// since the cluster can schedule workers again and the next failure should
    /// start a fresh retry cycle.
    pub fn reset_spawn_retry(&mut self) {
        self.spawn_retry_delays = None;
        self.spawn_retry_armed = false;
    }
}

#[cfg(test)]
mod tests {
    use sail_server::RetryStrategy;

    use super::*;
    use crate::error::ExecutionResult;

    // reserve_worker_ids only touches the id generator, so a placeholder manager
    // is fine here (it is never launched).
    struct NoopManager;
    #[tonic::async_trait]
    impl crate::worker_manager::WorkerManager for NoopManager {
        async fn launch_worker(
            &self,
            _id: WorkerId,
            _options: crate::worker_manager::WorkerLaunchOptions,
        ) -> ExecutionResult<()> {
            Ok(())
        }
        async fn delete_worker(&self, _id: WorkerId) -> ExecutionResult<()> {
            Ok(())
        }
        async fn stop(&self) -> ExecutionResult<()> {
            Ok(())
        }
    }

    fn pool() -> WorkerPool {
        WorkerPool::new(Arc::new(NoopManager), WorkerPoolOptions::for_test())
    }

    #[test]
    fn reserve_worker_ids_returns_unique_consecutive_ids() {
        let mut p = pool();
        let ids = p.reserve_worker_ids(3).expect("reserve ids");
        assert_eq!(ids.len(), 3);
        // Ids start at 1 and advance consecutively.
        assert_eq!(ids[0], WorkerId::from(1));
        assert_eq!(ids[1], WorkerId::from(2));
        assert_eq!(ids[2], WorkerId::from(3));

        // Subsequent reservations continue from the last id.
        let more = p.reserve_worker_ids(2).expect("reserve more");
        assert_eq!(more, vec![WorkerId::from(4), WorkerId::from(5)]);
    }

    #[test]
    fn reserve_worker_ids_zero_returns_empty() {
        let mut p = pool();
        let ids = p.reserve_worker_ids(0).expect("reserve zero");
        assert!(ids.is_empty());
    }

    fn retry_pool(max_count: usize, delay_secs: u64) -> WorkerPool {
        let options = WorkerPoolOptions::for_test_with_spawn_retry(RetryStrategy::Fixed {
            max_count,
            delay: Duration::from_secs(delay_secs),
        });
        WorkerPool::new(Arc::new(NoopManager), options)
    }

    #[test]
    fn spawn_retry_delay_advances_then_exhausts() {
        let mut p = retry_pool(2, 5);
        assert!(!p.has_pending_spawn_retry());
        assert_eq!(p.next_spawn_retry_delay(), Some(Duration::from_secs(5)));
        assert!(p.has_pending_spawn_retry());
        assert_eq!(p.next_spawn_retry_delay(), Some(Duration::from_secs(5)));
        // Exhausted: no more re-spawn delays. The retry armed by the first call is
        // still in flight, so the armed flag must remain set until it fires.
        assert_eq!(p.next_spawn_retry_delay(), None);
        assert!(p.has_pending_spawn_retry());
    }

    #[test]
    fn concurrent_failures_do_not_clear_an_armed_retry() {
        // Regression: three workers fail close together (e.g. all stuck `Pending`).
        // The first arms a retry; the later ones exhaust the shared iterator and get
        // `None`. They must NOT clear the armed flag, otherwise pending tasks would
        // fail on the scheduling timeout while a retry is still in flight.
        let mut p = retry_pool(1, 5);
        assert_eq!(p.next_spawn_retry_delay(), Some(Duration::from_secs(5)));
        assert!(p.has_pending_spawn_retry());
        assert_eq!(p.next_spawn_retry_delay(), None);
        assert!(p.has_pending_spawn_retry());
        assert_eq!(p.next_spawn_retry_delay(), None);
        assert!(p.has_pending_spawn_retry());

        // The retry eventually fires and scales up; only then is it disarmed.
        p.fire_spawn_retry();
        assert!(!p.has_pending_spawn_retry());
    }

    #[test]
    fn spawn_retry_resets_after_fire() {
        let mut p = retry_pool(2, 5);
        assert_eq!(p.next_spawn_retry_delay(), Some(Duration::from_secs(5)));
        assert!(p.has_pending_spawn_retry());
        // The delayed retry event fired and scaled up; the retry is no longer armed.
        p.fire_spawn_retry();
        assert!(!p.has_pending_spawn_retry());
    }

    #[test]
    fn spawn_retry_resets_after_registration() {
        let mut p = retry_pool(2, 5);
        assert_eq!(p.next_spawn_retry_delay(), Some(Duration::from_secs(5)));
        // A worker registered successfully: the retry cycle must start fresh.
        p.reset_spawn_retry();
        assert!(!p.has_pending_spawn_retry());
        assert_eq!(p.next_spawn_retry_delay(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn spawn_retry_with_no_attempts_returns_none() {
        let mut p = retry_pool(0, 5);
        assert_eq!(p.next_spawn_retry_delay(), None);
        assert!(!p.has_pending_spawn_retry());
    }
}
