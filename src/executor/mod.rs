pub mod tokio_executor;
#[cfg(feature = "remote")]
pub mod remote;

pub use tokio_executor::TokioExecutor;
#[cfg(feature = "remote")]
pub use remote::{
    CancelRequest, RemoteExecutor, ResultReceiver, TaskDispatchRequest, TaskDispatcher,
    TaskOutcome, TaskResultReport,
};

use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use crate::error::SchedulerError;
use crate::schedule::ScheduleId;

pub type BoxedTaskFn = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<(), SchedulerError>> + Send>> + Send>;

#[async_trait]
pub trait Executor: Send + Sync {
    async fn submit(&self, schedule_id: &ScheduleId, task: BoxedTaskFn) -> Result<(), SchedulerError>;
    fn running_count(&self, schedule_id: &ScheduleId) -> u32;
    async fn shutdown(&self, timeout: Duration) -> Result<(), SchedulerError>;

    /// Returns `Some` if this executor supports remote dispatch.
    #[cfg(feature = "remote")]
    fn as_dispatchable(&self) -> Option<&dyn DispatchableExecutor> {
        None
    }
}

/// Extension of [`Executor`] for executors that dispatch tasks to remote workers.
#[cfg(feature = "remote")]
#[async_trait]
pub trait DispatchableExecutor: Executor {
    async fn dispatch(&self, request: remote::TaskDispatchRequest) -> Result<(), SchedulerError>;
    async fn cancel_remote(&self, request: remote::CancelRequest) -> Result<(), SchedulerError>;
    fn decrement_running_count(&self, schedule_id: &ScheduleId);
}
