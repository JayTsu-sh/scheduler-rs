//! HTTP API for scheduler monitoring and management.
//!
//! Requires the `api` feature flag.
//!
//! # Example
//!
//! ```ignore
//! let scheduler = Scheduler::builder().build()?;
//! let router = scheduler.api_router();
//!
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
//! axum::serve(listener, router).await?;
//! ```

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::execution::Execution;
use crate::metrics::{HealthStatus, SchedulerMetrics};
use crate::schedule::{MisfirePolicy, RecoveryPolicy, ScheduleId, ScheduleRecord};
use crate::scheduler::Scheduler;

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// Trigger specification for the HTTP API.
///
/// Allows creating triggers from JSON without compile-time types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerSpec {
    /// Cron expression trigger. Example: `{"type": "cron", "expression": "0 0 * * * *"}`
    Cron { expression: String },
    /// Fixed interval trigger. `interval_secs` is the interval in seconds.
    Interval { interval_secs: u64 },
    /// Fire once at a specific time.
    Once { at: chrono::DateTime<chrono::Utc> },
    /// Fire immediately, one time only.
    Immediate,
}

impl TriggerSpec {
    fn into_trigger(self) -> Result<Box<dyn crate::trigger::Trigger>, (StatusCode, String)> {
        match self {
            TriggerSpec::Cron { expression } => {
                let trigger = crate::trigger::CronTrigger::new(&expression)
                    .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid cron: {e}")))?;
                Ok(Box::new(trigger))
            }
            TriggerSpec::Interval { interval_secs } => {
                let trigger = crate::trigger::IntervalTrigger::every(
                    std::time::Duration::from_secs(interval_secs),
                );
                Ok(Box::new(trigger))
            }
            TriggerSpec::Once { at } => {
                Ok(Box::new(crate::trigger::OnceTrigger::at(at)))
            }
            TriggerSpec::Immediate => {
                Ok(Box::new(crate::trigger::ImmediateTrigger::new()))
            }
        }
    }
}

/// Request body for `POST /api/schedules`.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateScheduleRequest {
    /// Task type name (must be pre-registered via `scheduler.register::<T>()`).
    pub type_name: String,
    /// Serialized task parameters.
    pub task_data: serde_json::Value,
    /// Trigger specification.
    pub trigger: TriggerSpec,
    /// Optional human-readable name.
    #[serde(default)]
    pub name: Option<String>,
    /// Misfire policy. Defaults to Coalesce.
    #[serde(default)]
    pub misfire_policy: Option<MisfirePolicy>,
    /// Maximum concurrent instances. Defaults to 1.
    #[serde(default)]
    pub max_instances: Option<u32>,
    /// Recovery policy. Defaults to Skip.
    #[serde(default)]
    pub recovery_policy: Option<RecoveryPolicy>,
    /// Priority (higher = more priority). Defaults to 0.
    #[serde(default)]
    pub priority: Option<i32>,
}

/// Response for `POST /api/schedules`.
#[derive(Debug, Serialize)]
pub struct CreateScheduleResponse {
    pub id: ScheduleId,
}

impl Scheduler {
    /// Create an axum `Router` with monitoring and management endpoints.
    ///
    /// Endpoints:
    /// - `GET  /api/health` — health check
    /// - `GET  /api/metrics` — aggregated metrics
    /// - `GET  /api/schedules` — list all schedules
    /// - `POST /api/schedules` — create a new schedule
    /// - `GET  /api/schedules/:id` — get a schedule
    /// - `GET  /api/schedules/:id/executions` — execution history
    /// - `POST /api/schedules/:id/pause` — pause a schedule
    /// - `POST /api/schedules/:id/resume` — resume a schedule
    /// - `POST /api/schedules/:id/remove` — remove a schedule
    pub fn api_router(&self) -> Router {
        Router::new()
            .route("/api/health", get(health_handler))
            .route("/api/metrics", get(metrics_handler))
            .route(
                "/api/schedules",
                get(list_schedules_handler).post(create_schedule_handler),
            )
            .route("/api/schedules/:id", get(get_schedule_handler))
            .route("/api/schedules/:id/remove", post(remove_schedule_handler))
            .route("/api/schedules/:id/executions", get(get_executions_handler))
            .route("/api/schedules/:id/pause", post(pause_handler))
            .route("/api/schedules/:id/resume", post(resume_handler))
            .with_state(self.clone())
    }
}

fn parse_schedule_id(id_str: &str) -> Result<ScheduleId, (StatusCode, String)> {
    Uuid::parse_str(id_str)
        .map(ScheduleId)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid schedule ID: {e}")))
}

async fn health_handler(State(scheduler): State<Scheduler>) -> Json<HealthStatus> {
    Json(scheduler.health().await)
}

async fn metrics_handler(
    State(scheduler): State<Scheduler>,
) -> Result<Json<SchedulerMetrics>, (StatusCode, String)> {
    scheduler
        .metrics()
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn list_schedules_handler(
    State(scheduler): State<Scheduler>,
) -> Result<Json<Vec<ScheduleRecord>>, (StatusCode, String)> {
    scheduler
        .list_schedules()
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn get_schedule_handler(
    State(scheduler): State<Scheduler>,
    Path(id_str): Path<String>,
) -> Result<Json<ScheduleRecord>, (StatusCode, String)> {
    let id = parse_schedule_id(&id_str)?;
    match scheduler.list_schedules().await {
        Ok(schedules) => schedules
            .into_iter()
            .find(|s| s.id == id)
            .map(Json)
            .ok_or((StatusCode::NOT_FOUND, "Schedule not found".to_string())),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn get_executions_handler(
    State(scheduler): State<Scheduler>,
    Path(id_str): Path<String>,
) -> Result<Json<Vec<Execution>>, (StatusCode, String)> {
    let id = parse_schedule_id(&id_str)?;
    scheduler
        .get_executions(&id)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn pause_handler(
    State(scheduler): State<Scheduler>,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = parse_schedule_id(&id_str)?;
    scheduler
        .pause(&id)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn resume_handler(
    State(scheduler): State<Scheduler>,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = parse_schedule_id(&id_str)?;
    scheduler
        .resume(&id)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn create_schedule_handler(
    State(scheduler): State<Scheduler>,
    Json(req): Json<CreateScheduleRequest>,
) -> Result<(StatusCode, Json<CreateScheduleResponse>), (StatusCode, String)> {
    let trigger = req.trigger.into_trigger()?;

    let mut config = crate::schedule::ScheduleConfig::default();
    if let Some(p) = req.misfire_policy {
        config.misfire_policy = p;
    }
    if let Some(m) = req.max_instances {
        config.max_instances = m;
    }
    if let Some(r) = req.recovery_policy {
        config.recovery_policy = r;
    }
    if let Some(p) = req.priority {
        config.priority = p;
    }

    let id = scheduler
        .add_raw(req.type_name, req.task_data, trigger, config, req.name)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    Ok((StatusCode::CREATED, Json(CreateScheduleResponse { id })))
}

async fn remove_schedule_handler(
    State(scheduler): State<Scheduler>,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = parse_schedule_id(&id_str)?;
    scheduler
        .remove(&id)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
