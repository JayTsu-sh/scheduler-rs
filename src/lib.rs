pub mod error;
pub mod task;
pub mod schedule;
pub mod execution;
pub mod trigger;
pub mod store;
pub mod executor;
pub mod event;
pub mod middleware;
pub mod metrics;
pub mod scheduler;
#[cfg(feature = "api")]
pub mod api;
#[cfg(feature = "remote")]
pub mod worker;
#[cfg(feature = "remote")]
pub mod transport;

pub mod prelude {
    pub use crate::error::SchedulerError;
    pub use crate::task::{Task, TaskContext, TaskResult};
    pub use crate::schedule::{
        ScheduleId, ScheduleRecord, ScheduleState, ScheduleConfig,
        MisfirePolicy, RecoveryPolicy,
    };
    pub use crate::execution::{ExecutionId, Execution, ExecutionState};
    pub use crate::trigger::Trigger;
    pub use crate::store::DataStore;
    pub use crate::executor::Executor;
    #[cfg(feature = "remote")]
    pub use crate::executor::{
        DispatchableExecutor, RemoteExecutor, TaskDispatcher, ResultReceiver,
        TaskDispatchRequest, TaskResultReport, TaskOutcome, CancelRequest,
    };
    #[cfg(feature = "remote")]
    pub use crate::worker::Worker;
    pub use crate::event::SchedulerEvent;
    pub use crate::scheduler::{Scheduler, SchedulerBuilder, SchedulerConfig, SchedulerHandle};
    pub use crate::middleware::{TaskMiddleware, TimeoutMiddleware, RetryMiddleware};
    pub use crate::metrics::{SchedulerMetrics, HealthStatus};
    pub use async_trait::async_trait;
    pub use serde::{Serialize, Deserialize};
    pub use chrono::{DateTime, Utc};
    pub use tokio_util::sync::CancellationToken;

    #[cfg(feature = "timezone")]
    pub use crate::trigger::{TzTrigger, TriggerExt};
}
