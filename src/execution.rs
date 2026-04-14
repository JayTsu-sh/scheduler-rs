use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::fmt;
use crate::schedule::ScheduleId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionId(pub Uuid);

impl ExecutionId {
    pub fn new() -> Self { Self(Uuid::new_v4()) }
}

impl Default for ExecutionId {
    fn default() -> Self { Self::new() }
}

impl fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionState {
    Scheduled,
    Running,
    Succeeded,
    Failed,
    Missed,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub id: ExecutionId,
    pub schedule_id: ScheduleId,
    pub state: ExecutionState,
    pub scheduled_fire_time: DateTime<Utc>,
    pub actual_fire_time: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}
