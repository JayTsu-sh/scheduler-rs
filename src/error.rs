use thiserror::Error;
use crate::schedule::ScheduleId;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("Schedule not found: {0}")]
    ScheduleNotFound(ScheduleId),
    #[error("Schedule already exists: {0}")]
    ScheduleAlreadyExists(ScheduleId),
    #[error("Invalid cron expression: {0}")]
    InvalidCronExpression(String),
    #[error("Invalid trigger configuration: {0}")]
    InvalidTrigger(String),
    #[error("Task execution failed: {0}")]
    ExecutionError(String),
    #[error("Store error: {0}")]
    StoreError(String),
    #[error("Task type not registered: {0}")]
    TaskTypeNotRegistered(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Scheduler is not running")]
    NotRunning,
    #[error("Scheduler is already running")]
    AlreadyRunning,
    #[cfg(feature = "remote")]
    #[error("Dispatch error: {0}")]
    DispatchError(String),
}
