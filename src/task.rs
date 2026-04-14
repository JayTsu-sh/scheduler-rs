use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;
use crate::schedule::ScheduleId;
use crate::execution::ExecutionId;
use crate::error::SchedulerError;

pub type TaskResult = Result<(), SchedulerError>;

#[derive(Clone)]
pub struct TaskContext {
    pub schedule_id: ScheduleId,
    pub execution_id: ExecutionId,
    pub cancellation_token: CancellationToken,
}

#[async_trait]
pub trait Task: Send + Sync + Serialize + DeserializeOwned + Clone + 'static {
    const TYPE_NAME: &'static str;
    async fn run(&self, ctx: &TaskContext) -> TaskResult;
}
