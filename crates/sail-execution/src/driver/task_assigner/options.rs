use crate::driver::DriverOptions;

#[readonly::make]
pub struct TaskAssignerOptions {
    pub worker_initial_count: usize,
    pub worker_task_slots: usize,
    pub worker_max_count: usize,
}

impl TaskAssignerOptions {
    #[cfg(test)]
    pub(crate) fn for_test(
        worker_initial_count: usize,
        worker_task_slots: usize,
        worker_max_count: usize,
    ) -> Self {
        Self {
            worker_initial_count,
            worker_task_slots,
            worker_max_count,
        }
    }
}

impl From<&DriverOptions> for TaskAssignerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            worker_initial_count: options.worker_initial_count,
            worker_task_slots: options.worker_task_slots,
            worker_max_count: options.worker_max_count,
        }
    }
}
