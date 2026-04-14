use serde::Serialize;

/// Aggregated scheduler metrics.
#[derive(Debug, Clone, Serialize)]
pub struct SchedulerMetrics {
    pub total_schedules: u64,
    pub active_schedules: u64,
    pub paused_schedules: u64,
    pub completed_schedules: u64,
    pub total_executions: u64,
    pub running_executions: u64,
    pub succeeded_executions: u64,
    pub failed_executions: u64,
    pub missed_executions: u64,
    pub interrupted_executions: u64,
}

/// Scheduler health status.
#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub healthy: bool,
    pub store_accessible: bool,
    pub active_schedules: u64,
    pub running_executions: u64,
}
