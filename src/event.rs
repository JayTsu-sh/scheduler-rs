use chrono::{DateTime, Utc};
use crate::schedule::ScheduleId;
use crate::execution::ExecutionId;

#[derive(Debug, Clone)]
pub enum SchedulerEvent {
    ScheduleAdded { id: ScheduleId },
    ScheduleRemoved { id: ScheduleId },
    SchedulePaused { id: ScheduleId },
    ScheduleResumed { id: ScheduleId },
    ExecutionStarted { schedule_id: ScheduleId, execution_id: ExecutionId },
    ExecutionSucceeded { schedule_id: ScheduleId, execution_id: ExecutionId },
    ExecutionFailed { schedule_id: ScheduleId, execution_id: ExecutionId, error: String },
    ExecutionMissed { schedule_id: ScheduleId, scheduled_time: DateTime<Utc> },
    ExecutionInterrupted { schedule_id: ScheduleId, execution_id: ExecutionId },
    ExecutionRecovered { schedule_id: ScheduleId, execution_id: ExecutionId },
    ScheduleExpired { id: ScheduleId },
}
