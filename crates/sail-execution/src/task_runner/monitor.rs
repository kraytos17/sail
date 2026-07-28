use std::time::Instant;

use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{Actor, ActorHandle};
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::id::{TaskKey, TaskKeyDisplay, WorkerId};
use crate::task_runner::TaskRunnerMessage;

pub struct TaskMonitor<T: Actor> {
    handle: ActorHandle<T>,
    key: TaskKey,
    stream: SendableRecordBatchStream,
    signal: oneshot::Receiver<()>,
    worker_id: WorkerId,
}

impl<T: Actor> TaskMonitor<T> {
    pub fn new(
        handle: ActorHandle<T>,
        key: TaskKey,
        stream: SendableRecordBatchStream,
        signal: oneshot::Receiver<()>,
        worker_id: WorkerId,
    ) -> Self {
        Self {
            handle,
            key,
            stream,
            signal,
            worker_id,
        }
    }
}

impl<T: Actor> TaskMonitor<T>
where
    T::Message: TaskRunnerMessage,
{
    /// Runs the task monitor, reporting running and terminal status updates.
    pub async fn run(self) {
        let Self {
            handle,
            key,
            stream,
            signal,
            worker_id,
        } = self;
        let start = Instant::now();
        let event = Self::running(key.clone());
        let _ = handle.send(event).await;
        let (event, status, row_count) = tokio::select! {
            x = Self::execute(key.clone(), stream) => x,
            x = Self::cancel(key.clone(), signal) => x,
        };
        let _ = handle.send(event).await;
        let duration = start.elapsed();
        let wid = u64::from(worker_id);
        let jid = u64::from(key.job_id);
        match status {
            TaskStatus::Succeeded => {
                log::info!(
                    "worker={} task job={} stage={} partition={} attempt={} status=succeeded duration={:.1}s rows={}",
                    wid,
                    jid,
                    key.stage,
                    key.partition,
                    key.attempt,
                    duration.as_secs_f64(),
                    row_count,
                );
            }
            TaskStatus::Failed => {
                log::error!(
                    "worker={} task job={} stage={} partition={} attempt={} status=failed duration={:.1}s",
                    wid,
                    jid,
                    key.stage,
                    key.partition,
                    key.attempt,
                    duration.as_secs_f64(),
                );
            }
            TaskStatus::Canceled => {
                log::warn!(
                    "worker={} task job={} stage={} partition={} attempt={} status=canceled duration={:.1}s",
                    wid,
                    jid,
                    key.stage,
                    key.partition,
                    key.attempt,
                    duration.as_secs_f64(),
                );
            }
            _ => {}
        }
    }

    /// Builds a "task is running" status message.
    fn running(key: TaskKey) -> T::Message {
        T::Message::report_task_status(key, TaskStatus::Running, None, None)
    }

    /// Waits for a cancellation signal and builds a canceled status message.
    async fn cancel(key: TaskKey, signal: oneshot::Receiver<()>) -> (T::Message, TaskStatus, u64) {
        let _ = signal.await;
        (
            T::Message::report_task_status(
                key.clone(),
                TaskStatus::Canceled,
                Some(format!("{} canceled", TaskKeyDisplay(&key))),
                None,
            ),
            TaskStatus::Canceled,
            0,
        )
    }

    /// Drains the output stream and builds a succeeded or failed status message.
    async fn execute(
        key: TaskKey,
        mut stream: SendableRecordBatchStream,
    ) -> (T::Message, TaskStatus, u64) {
        let mut row_count: u64 = 0;
        loop {
            let Some(batch) = stream.next().await else {
                break (
                    T::Message::report_task_status(key.clone(), TaskStatus::Succeeded, None, None),
                    TaskStatus::Succeeded,
                    row_count,
                );
            };
            let error = match &batch {
                Ok(b) => {
                    row_count += b.num_rows() as u64;
                    None
                }
                Err(e) => Some((
                    format!("task error: {e}"),
                    CommonErrorCause::new::<PyErrExtractor>(e),
                )),
            };
            if let Some((message, cause)) = error {
                break (
                    T::Message::report_task_status(
                        key.clone(),
                        TaskStatus::Failed,
                        Some(message),
                        Some(cause),
                    ),
                    TaskStatus::Failed,
                    row_count,
                );
            }
        }
    }
}
