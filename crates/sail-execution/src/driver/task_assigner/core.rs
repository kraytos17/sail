use std::collections::HashSet;

use indexmap::IndexSet;
use log::{error, warn};

use crate::driver::task_assigner::state::{TaskSlot, WorkerResource};
use crate::driver::task_assigner::{TaskAssigner, TaskRegion};
use crate::id::{JobId, TaskKey, WorkerId};
use crate::job_graph::TaskPlacement;
use crate::task::scheduling::{
    TaskAssignment, TaskAssignmentGetter, TaskSetAssignment, TaskStreamAssignment,
};

impl TaskAssigner {
    /// Number of workers whose spawn was requested but which have not yet
    /// registered (`Pending`). Derived from state, never a separate counter.
    fn pending_worker_count(&self) -> usize {
        self.workers
            .values()
            .filter(|w| matches!(w, WorkerResource::Pending))
            .count()
    }

    /// Number of registered (`Active`) workers. Derived from state.
    fn active_worker_count(&self) -> usize {
        self.workers
            .values()
            .filter(|w| matches!(w, WorkerResource::Active { .. }))
            .count()
    }

    /// Number of live workers (pending + active) charged against `worker_max_count`.
    fn total_live_worker_count(&self) -> usize {
        self.pending_worker_count() + self.active_worker_count()
    }

    pub fn request_workers(&mut self) -> usize {
        let enqueued_slots = self
            .task_queue
            .iter()
            .map(|region| {
                region
                    .tasks
                    .iter()
                    .filter(|(placement, _)| matches!(placement, TaskPlacement::Worker))
                    .count()
            })
            .sum::<usize>();
        let vacant_slots = self
            .workers
            .values()
            .map(|worker| match worker {
                WorkerResource::Active { task_slots, .. } => {
                    task_slots.iter().filter(|x| x.is_vacant()).count()
                }
                WorkerResource::Pending => 0,
            })
            .sum::<usize>();
        let required_slots = enqueued_slots.saturating_sub(vacant_slots);
        let allowed_workers = if self.options.worker_max_count == 0 {
            usize::MAX
        } else {
            self.options
                .worker_max_count
                .saturating_sub(self.total_live_worker_count())
        };
        required_slots
            .div_ceil(self.options.worker_task_slots)
            .min(allowed_workers)
    }

    /// Number of workers to pre-spawn at session start so `worker_initial_count`
    /// live workers are ready, capped by `worker_max_count`. Unlike
    /// `request_workers`, this ignores `enqueued_slots`.
    pub fn request_initial_workers(&mut self) -> usize {
        if self.options.worker_max_count == 0 {
            self.options.worker_initial_count
        } else {
            self.options
                .worker_initial_count
                .saturating_sub(self.total_live_worker_count())
                .min(
                    self.options
                        .worker_max_count
                        .saturating_sub(self.total_live_worker_count()),
                )
        }
    }

    /// Records that a spawn for `worker_id` was requested. The worker consumes the
    /// `worker_max_count` budget until it registers (`activate_worker`) or its pod
    /// fails to start (`track_worker_failed_to_start`).
    pub fn add_pending_worker(&mut self, worker_id: WorkerId) {
        if self.workers.contains_key(&worker_id) {
            warn!("worker {worker_id} is already tracked");
            return;
        }
        self.workers.insert(worker_id, WorkerResource::Pending);
    }

    /// Removes a requested-but-never-registered worker from the pool (pod failed to
    /// start before calling `RegisterWorker`). This releases its share of the budget.
    pub fn track_worker_failed_to_start(&mut self, worker_id: WorkerId) {
        match self.workers.shift_remove(&worker_id) {
            Some(WorkerResource::Pending) => {}
            Some(_) => warn!("worker {worker_id} failed to start in a non-pending state"),
            None => warn!("worker {worker_id} not found"),
        }
    }

    pub fn activate_worker(&mut self, worker_id: WorkerId) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        match worker {
            WorkerResource::Pending => {
                *worker = WorkerResource::Active {
                    task_slots: vec![TaskSlot::default(); self.options.worker_task_slots],
                    local_streams: IndexSet::new(),
                };
            }
            WorkerResource::Active { .. } => warn!("worker {worker_id} is already active"),
        }
    }

    pub fn deactivate_worker(&mut self, worker_id: WorkerId) {
        // The worker is reaped (idle/lost) and must no longer consume the budget:
        // remove it entirely rather than leaving it as `Inactive` in the map.
        if self.workers.shift_remove(&worker_id).is_none() {
            warn!("worker {worker_id} not found");
        }
    }

    pub fn enqueue_tasks(&mut self, region: TaskRegion) {
        self.task_queue.push_back(region);
    }

    pub fn is_task_queue_empty(&self) -> bool {
        self.task_queue.is_empty()
    }

    pub fn exclude_task(&mut self, key: &TaskKey) {
        self.task_queue.retain(|x| !x.contains(key));
    }

    pub fn assign_tasks(&mut self) -> Vec<TaskSetAssignment> {
        let mut assignments = vec![];
        let mut assigner = self.build_worker_task_slot_assigner();

        while let Some(region) = self.task_queue.pop_front() {
            match assigner.try_assign_task_region(region) {
                Ok(x) => assignments.extend(x),
                Err(region) => {
                    // The region cannot be successfully assigned as a whole
                    // due to insufficient worker task slots.
                    // Put the region back to the queue and try again later.
                    // We must put the region back to the front of the queue to
                    // avoid starvation.
                    // This does result in head-of-line blocking, but we would
                    // like the regions to be assigned in the same order as they
                    // are enqueued.
                    self.task_queue.push_front(region);
                    break;
                }
            }
        }

        // Update the driver and worker based on the assignments.
        for assignment in assignments.iter() {
            match assignment.assignment {
                TaskAssignment::Driver => {
                    self.driver.add_task_set(assignment.set.clone());
                    for key in assignment.set.tasks() {
                        self.task_assignments
                            .insert(key.clone(), TaskAssignment::Driver);
                    }
                }
                TaskAssignment::Worker { worker_id, slot } => {
                    if let Some(worker) = self.workers.get_mut(&worker_id) {
                        worker.add_task_set(slot, assignment.set.clone());
                        for key in assignment.set.tasks() {
                            self.task_assignments
                                .insert(key.clone(), TaskAssignment::Worker { worker_id, slot });
                        }
                    } else {
                        error!("worker {worker_id} not found");
                    }
                }
            }
        }

        assignments
    }

    pub fn unassign_task(&mut self, key: &TaskKey) -> Option<TaskAssignment> {
        let assignment = self.task_assignments.get(key)?;
        match assignment {
            TaskAssignment::Driver => {
                self.driver.remove_task(key);
            }
            TaskAssignment::Worker { worker_id, slot } => {
                let Some(worker) = self.workers.get_mut(worker_id) else {
                    warn!("worker {worker_id} not found");
                    return None;
                };
                worker.remove_task(key, *slot);
            }
        }
        Some(assignment.clone())
    }

    /// Records local and remote stream ownership for each resource based on the given task assignments.
    pub fn track_streams(&mut self, assignments: &[TaskSetAssignment]) {
        for assignment in assignments {
            self.driver.track_remote_streams(&assignment.set);
            match &assignment.assignment {
                TaskAssignment::Driver => {
                    self.driver.track_local_streams(&assignment.set);
                }
                TaskAssignment::Worker { worker_id, .. } => {
                    if let Some(worker) = self.workers.get_mut(worker_id) {
                        worker.track_local_streams(&assignment.set);
                    } else {
                        error!("worker {worker_id} not found");
                    }
                }
            }
        }
    }

    pub fn untrack_local_streams(
        &mut self,
        job_id: JobId,
        stage: Option<usize>,
    ) -> HashSet<TaskStreamAssignment> {
        let mut assignments = HashSet::new();
        if self.driver.untrack_local_streams(job_id, stage) {
            assignments.insert(TaskStreamAssignment::Driver);
        }
        for (worker_id, worker) in self.workers.iter_mut() {
            if matches!(worker, WorkerResource::Active { .. })
                && worker.untrack_local_streams(job_id, stage)
            {
                assignments.insert(TaskStreamAssignment::Worker {
                    worker_id: *worker_id,
                });
            }
        }
        assignments
    }

    pub fn untrack_remote_streams(&mut self, job_id: JobId, stage: Option<usize>) -> bool {
        self.driver.untrack_remote_streams(job_id, stage)
    }

    pub fn is_worker_idle(&self, worker_id: WorkerId) -> bool {
        let Some(worker) = self.workers.get(&worker_id) else {
            warn!("worker {worker_id} not found");
            return false;
        };
        match worker {
            WorkerResource::Active {
                task_slots: slots,
                local_streams: streams,
            } => slots.iter().all(|s| s.is_vacant()) && streams.is_empty(),
            WorkerResource::Pending => false,
        }
    }

    pub fn find_worker_tasks(&self, worker_id: WorkerId) -> Vec<TaskKey> {
        let Some(worker) = self.workers.get(&worker_id) else {
            warn!("worker {worker_id} not found");
            return vec![];
        };
        match worker {
            WorkerResource::Active {
                task_slots: slots, ..
            } => slots
                .iter()
                .flat_map(|x| x.list_tasks().cloned().collect::<Vec<_>>())
                .collect(),
            WorkerResource::Pending => vec![],
        }
    }

    /// Builds a snapshot of available task slots across the driver and active workers for assignment.
    fn build_worker_task_slot_assigner(&self) -> TaskSlotAssigner {
        let slots = self
            .workers
            .iter()
            .filter_map(|(id, worker)| {
                let slots = match worker {
                    WorkerResource::Pending => vec![],
                    WorkerResource::Active {
                        task_slots: slots, ..
                    } => slots
                        .iter()
                        .enumerate()
                        .filter_map(|(i, s)| s.is_vacant().then_some(i))
                        .collect(),
                };
                if !slots.is_empty() {
                    Some((*id, slots))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        TaskSlotAssigner::new(slots)
    }
}

impl TaskAssignmentGetter for TaskAssigner {
    fn get(&self, key: &TaskKey) -> Option<&TaskAssignment> {
        self.task_assignments.get(key)
    }
}

/// Assigns task regions to driver or worker slots, consuming available slots as tasks are placed.
struct TaskSlotAssigner {
    /// The available task slots on workers.
    slots: Vec<(WorkerId, Vec<usize>)>,
}

impl TaskSlotAssigner {
    fn new(slots: Vec<(WorkerId, Vec<usize>)>) -> Self {
        Self { slots }
    }

    fn next(&mut self) -> Option<(WorkerId, usize)> {
        self.slots
            .iter_mut()
            .find_map(|(worker_id, slots)| slots.pop().map(|slot| (*worker_id, slot)))
    }

    fn try_assign_task_region(
        &mut self,
        region: TaskRegion,
    ) -> Result<Vec<TaskSetAssignment>, TaskRegion> {
        let mut assignments = vec![];

        for (placement, set) in &region.tasks {
            match placement {
                TaskPlacement::Driver => {
                    assignments.push(TaskSetAssignment {
                        set: set.clone(),
                        assignment: TaskAssignment::Driver,
                    });
                }
                TaskPlacement::Worker => {
                    if let Some((worker_id, slot)) = self.next() {
                        assignments.push(TaskSetAssignment {
                            set: set.clone(),
                            assignment: TaskAssignment::Worker { worker_id, slot },
                        });
                    } else {
                        // The worker task slots are not enough for assigning all the
                        // worker tasks in this region. So we return the region back
                        // to indicate the error.
                        return Err(region);
                    }
                }
            }
        }
        Ok(assignments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::task_assigner::TaskAssignerOptions;
    use crate::id::JobId;
    use crate::task::scheduling::{TaskOutputKind, TaskRegion, TaskSet, TaskSetEntry};

    fn options(initial_count: usize, task_slots: usize, max_count: usize) -> TaskAssignerOptions {
        TaskAssignerOptions::for_test(initial_count, task_slots, max_count)
    }

    fn key(job_id: u64, partition: usize) -> TaskKey {
        TaskKey {
            job_id: JobId::from(job_id),
            stage: 0,
            partition,
            attempt: 0,
        }
    }

    /// Builds a region with `n` worker task sets, one task each.
    fn worker_region(n: usize) -> TaskRegion {
        TaskRegion {
            tasks: (0..n)
                .map(|i| {
                    (
                        TaskPlacement::Worker,
                        TaskSet {
                            entries: vec![TaskSetEntry {
                                key: key(1, i),
                                output: TaskOutputKind::Local,
                            }],
                        },
                    )
                })
                .collect(),
        }
    }

    fn enqueue_worker_tasks(assigner: &mut TaskAssigner, n: usize) {
        assigner.enqueue_tasks(worker_region(n));
    }

    #[test]
    fn request_workers_respects_max_count_against_live_workers() {
        let mut a = TaskAssigner::new(options(0, 2, 4));
        enqueue_worker_tasks(&mut a, 10);
        // 10 tasks / 2 slots = 5 required, capped at max_count 4.
        assert_eq!(a.request_workers(), 4);
        assert_eq!(a.total_live_worker_count(), 0);

        for id in 1..=4 {
            a.add_pending_worker(WorkerId::from(id));
        }
        assert_eq!(a.total_live_worker_count(), 4);
        // Budget exhausted: no more workers may be requested.
        assert_eq!(a.request_workers(), 0);
    }

    #[test]
    fn activate_then_deactivate_releases_budget() {
        let mut a = TaskAssigner::new(options(0, 2, 4));
        enqueue_worker_tasks(&mut a, 10);
        assert_eq!(a.request_workers(), 4);
        for id in 1..=4 {
            a.add_pending_worker(WorkerId::from(id));
        }

        for id in 1..=4 {
            a.activate_worker(WorkerId::from(id));
        }
        assert_eq!(a.total_live_worker_count(), 4);
        assert_eq!(a.request_workers(), 0);

        // Reap all four: the budget must be fully released so the next scale-up can
        // request them again. This is the regression test for the accounting leak.
        for id in 1..=4 {
            a.deactivate_worker(WorkerId::from(id));
        }
        assert_eq!(a.total_live_worker_count(), 0);
        assert_eq!(a.request_workers(), 4);
    }

    #[test]
    fn initial_workers_are_charged_against_max_count() {
        let mut a = TaskAssigner::new(options(2, 2, 4));
        let n = a.request_initial_workers();
        assert_eq!(n, 2);
        for id in 1..=n {
            a.add_pending_worker(WorkerId::from(id as u64));
        }
        assert_eq!(a.total_live_worker_count(), 2);

        enqueue_worker_tasks(&mut a, 10);
        // max_count 4 minus the 2 live = 2 more allowed, not the full 4.
        assert_eq!(a.request_workers(), 2);
    }

    #[test]
    fn failed_to_start_releases_budget() {
        let mut a = TaskAssigner::new(options(0, 2, 4));
        enqueue_worker_tasks(&mut a, 8);
        assert_eq!(a.request_workers(), 4);
        a.add_pending_worker(WorkerId::from(1));
        a.add_pending_worker(WorkerId::from(2));
        assert_eq!(a.total_live_worker_count(), 2);

        a.track_worker_failed_to_start(WorkerId::from(1));
        assert_eq!(a.total_live_worker_count(), 1);
        // Budget freed: 4 workers needed, 1 live → 3 more allowed.
        assert_eq!(a.request_workers(), 3);
    }

    #[test]
    fn pending_worker_is_not_idle() {
        let mut a = TaskAssigner::new(options(0, 2, 4));
        a.add_pending_worker(WorkerId::from(1));
        assert!(!a.is_worker_idle(WorkerId::from(1)));
        assert!(!a.is_worker_idle(WorkerId::from(999)));
    }

    #[test]
    fn is_task_queue_empty_reflects_enqueued_regions() {
        let mut a = TaskAssigner::new(options(0, 2, 4));
        assert!(a.is_task_queue_empty());
        enqueue_worker_tasks(&mut a, 3);
        assert!(!a.is_task_queue_empty());
    }

    #[test]
    fn activate_is_idempotent_and_does_not_double_count() {
        let mut a = TaskAssigner::new(options(0, 2, 4));
        a.add_pending_worker(WorkerId::from(1));
        a.activate_worker(WorkerId::from(1));
        // Second activation is a no-op (warn), not a budget change.
        a.activate_worker(WorkerId::from(1));
        assert_eq!(a.total_live_worker_count(), 1);
    }
}
