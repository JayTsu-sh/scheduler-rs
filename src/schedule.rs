use chrono::{DateTime, Utc, Duration};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScheduleId(pub Uuid);

impl ScheduleId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ScheduleId {
    fn default() -> Self { Self::new() }
}

impl fmt::Display for ScheduleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScheduleState {
    Active,
    Paused,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MisfirePolicy {
    DoNothing,
    FireNow,
    Coalesce,
}

impl Default for MisfirePolicy {
    fn default() -> Self { Self::Coalesce }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RecoveryPolicy {
    Resubmit,
    #[default]
    Skip,
    MarkFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleConfig {
    pub misfire_policy: MisfirePolicy,
    pub misfire_grace_time: Option<Duration>,
    pub max_instances: u32,
    pub recovery_policy: RecoveryPolicy,
    /// Higher values = higher priority. Default is 0.
    pub priority: i32,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            misfire_policy: MisfirePolicy::default(),
            misfire_grace_time: Some(Duration::seconds(1)),
            max_instances: 1,
            recovery_policy: RecoveryPolicy::default(),
            priority: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRecord {
    pub id: ScheduleId,
    pub name: Option<String>,
    pub type_name: String,
    pub task_data: serde_json::Value,
    pub state: ScheduleState,
    pub next_fire_time: Option<DateTime<Utc>>,
    pub window_end_time: Option<DateTime<Utc>>,
    pub config: ScheduleConfig,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
