pub mod memory;
#[cfg(feature = "sqlx-sqlite")]
pub mod sqlite;
#[cfg(feature = "sqlx-mysql")]
pub mod mysql;
#[cfg(feature = "redis-store")]
pub mod redis_store;

pub use memory::MemoryStore;
#[cfg(feature = "sqlx-sqlite")]
pub use sqlite::SqliteStore;
#[cfg(feature = "sqlx-mysql")]
pub use mysql::MysqlStore;
#[cfg(feature = "redis-store")]
pub use redis_store::RedisStore;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::error::SchedulerError;
use crate::schedule::{ScheduleId, ScheduleRecord};
use crate::execution::{Execution, ExecutionState};

#[async_trait]
pub trait DataStore: Send + Sync {
    async fn add_schedule(&self, schedule: ScheduleRecord) -> Result<(), SchedulerError>;
    async fn get_schedule(&self, id: &ScheduleId) -> Result<Option<ScheduleRecord>, SchedulerError>;
    async fn get_due_schedules(&self, now: &DateTime<Utc>) -> Result<Vec<ScheduleRecord>, SchedulerError>;
    async fn get_next_fire_time(&self) -> Result<Option<DateTime<Utc>>, SchedulerError>;
    async fn update_schedule(&self, schedule: &ScheduleRecord) -> Result<(), SchedulerError>;
    async fn remove_schedule(&self, id: &ScheduleId) -> Result<bool, SchedulerError>;
    async fn list_schedules(&self) -> Result<Vec<ScheduleRecord>, SchedulerError>;

    /// List schedules whose name starts with the given prefix.
    /// Default implementation filters `list_schedules()` client-side;
    /// backends should override with a server-side query when possible.
    async fn list_schedules_by_name_prefix(&self, prefix: &str) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        let all = self.list_schedules().await?;
        Ok(all.into_iter().filter(|s| {
            s.name.as_deref().is_some_and(|n| n.starts_with(prefix))
        }).collect())
    }

    async fn add_execution(&self, execution: Execution) -> Result<(), SchedulerError>;
    async fn update_execution(&self, execution: &Execution) -> Result<(), SchedulerError>;
    async fn get_executions(&self, schedule_id: &ScheduleId) -> Result<Vec<Execution>, SchedulerError>;
    async fn get_executions_by_state(&self, state: ExecutionState) -> Result<Vec<Execution>, SchedulerError>;
    async fn cleanup_executions_before(&self, before: DateTime<Utc>) -> Result<u64, SchedulerError>;
}
