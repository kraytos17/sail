use sail_common::runtime::RuntimeHandle;

use crate::id::WorkerId;
use crate::worker::WorkerOptions;

#[readonly::make]
pub struct PeerTrackerOptions {
    pub worker_id: WorkerId,
    pub enable_tls: bool,
    pub runtime: RuntimeHandle,
}

impl From<&WorkerOptions> for PeerTrackerOptions {
    fn from(options: &WorkerOptions) -> Self {
        Self {
            worker_id: options.worker_id,
            enable_tls: options.enable_tls,
            runtime: options.runtime.clone(),
        }
    }
}
