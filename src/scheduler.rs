use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json;
use tokio::sync::{watch, Notify, RwLock};
use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::event::SchedulerEvent;
use crate::execution::{Execution, ExecutionId, ExecutionState};
use crate::executor::{BoxedTaskFn, Executor};
use crate::executor::TokioExecutor;
use crate::metrics::{HealthStatus, SchedulerMetrics};
use crate::middleware::{TaskMiddleware, TaskRunner};
use crate::schedule::{
    MisfirePolicy, RecoveryPolicy, ScheduleConfig, ScheduleId, ScheduleRecord, ScheduleState,
};
use crate::store::{DataStore, MemoryStore};
use crate::task::{Task, TaskContext};
use crate::trigger::Trigger;

// ---------------------------------------------------------------------------
// TaskRegistry
// ---------------------------------------------------------------------------

struct TaskRegistry {
    runners: RwLock<HashMap<String, TaskRunner>>,
}

impl TaskRegistry {
    fn new() -> Self {
        Self {
            runners: RwLock::new(HashMap::new()),
        }
    }

    async fn register<T: Task>(&self) {
        let runner: TaskRunner = Arc::new(|value, ctx| {
            Box::pin(async move {
                let task: T = serde_json::from_value(value)
                    .map_err(|e| SchedulerError::SerializationError(e.to_string()))?;
                task.run(&ctx).await
            })
        });
        self.runners
            .write()
            .await
            .insert(T::TYPE_NAME.to_string(), runner);
    }

    async fn get(&self, type_name: &str) -> Option<TaskRunner> {
        self.runners.read().await.get(type_name).cloned()
    }
}

// ---------------------------------------------------------------------------
// SchedulerConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub poll_interval: std::time::Duration,
    pub default_misfire_policy: MisfirePolicy,
    pub default_misfire_grace_time: Option<chrono::Duration>,
    pub max_concurrent_jobs: u32,
    pub execution_retention: Option<chrono::Duration>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval: std::time::Duration::from_secs(1),
            default_misfire_policy: MisfirePolicy::default(),
            default_misfire_grace_time: Some(chrono::Duration::seconds(1)),
            max_concurrent_jobs: 100,
            execution_retention: None,
        }
    }
}

// ---------------------------------------------------------------------------
// SchedulerHandle
// ---------------------------------------------------------------------------

pub struct SchedulerHandle {
    shutdown_tx: watch::Sender<bool>,
    join_handle: tokio::task::JoinHandle<Result<(), SchedulerError>>,
}

impl SchedulerHandle {
    pub async fn shutdown(self) -> Result<(), SchedulerError> {
        let _ = self.shutdown_tx.send(true);
        self.join_handle
            .await
            .map_err(|e| SchedulerError::ExecutionError(e.to_string()))?
    }
}

// ---------------------------------------------------------------------------
// SchedulerInner
// ---------------------------------------------------------------------------

type EventListener = Box<dyn Fn(&SchedulerEvent) + Send + Sync>;

struct SchedulerInner {
    config: SchedulerConfig,
    store: Arc<dyn DataStore>,
    executor: Arc<dyn Executor>,
    triggers: RwLock<HashMap<ScheduleId, Box<dyn Trigger>>>,
    cancellation_tokens: Arc<RwLock<HashMap<ScheduleId, Vec<CancellationToken>>>>,
    registry: TaskRegistry,
    event_listeners: Arc<RwLock<Vec<EventListener>>>,
    global_middleware: Vec<Box<dyn TaskMiddleware>>,
    schedule_middleware: RwLock<HashMap<ScheduleId, Vec<Box<dyn TaskMiddleware>>>>,
    notify: Notify,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    #[cfg(feature = "remote")]
    result_receiver: Option<Arc<dyn crate::executor::remote::ResultReceiver>>,
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

impl Scheduler {
    pub fn builder() -> SchedulerBuilder {
        SchedulerBuilder {
            store: None,
            executor: None,
            config: SchedulerConfig::default(),
            global_middleware: Vec::new(),
            #[cfg(feature = "remote")]
            result_receiver: None,
        }
    }

    /// Register a task type so the scheduler can deserialize and run it.
    pub async fn register<T: Task>(&self) {
        self.inner.registry.register::<T>().await;
    }

    /// Simple API: add a task with a trigger using default config.
    pub async fn add<T: Task>(
        &self,
        task: T,
        trigger: impl Trigger + 'static,
    ) -> Result<ScheduleId, SchedulerError> {
        let config = ScheduleConfig {
            misfire_policy: self.inner.config.default_misfire_policy,
            misfire_grace_time: self.inner.config.default_misfire_grace_time,
            ..ScheduleConfig::default()
        };
        self.add_schedule_internal(task, Box::new(trigger), config, None, Vec::new())
            .await
    }

    /// Create a schedule from raw (pre-serialized) data.
    ///
    /// The `type_name` must have been previously registered via
    /// [`Scheduler::register`]. This is used by the HTTP API to create
    /// schedules without compile-time task types.
    pub async fn add_raw(
        &self,
        type_name: String,
        task_data: serde_json::Value,
        trigger: Box<dyn Trigger>,
        config: ScheduleConfig,
        name: Option<String>,
    ) -> Result<ScheduleId, SchedulerError> {
        // Verify task type is registered
        if self.inner.registry.get(&type_name).await.is_none() {
            return Err(SchedulerError::TaskTypeNotRegistered(type_name));
        }

        let now = Utc::now();
        let next_fire_time = trigger.next_fire_time(&now);
        let id = ScheduleId::new();

        let record = ScheduleRecord {
            id: id.clone(),
            name,
            type_name,
            task_data,
            state: if next_fire_time.is_some() {
                ScheduleState::Active
            } else {
                ScheduleState::Completed
            },
            next_fire_time,
            window_end_time: None,
            config,
            created_at: now,
            updated_at: now,
        };

        self.inner.store.add_schedule(record).await?;
        self.inner
            .triggers
            .write()
            .await
            .insert(id.clone(), trigger);

        self.emit(SchedulerEvent::ScheduleAdded { id: id.clone() });
        self.inner.notify.notify_one();
        Ok(id)
    }

    /// Builder API: configure a schedule before submitting.
    pub fn schedule<T: Task>(&self, task: T) -> ScheduleBuilder<'_, T> {
        ScheduleBuilder {
            scheduler: self,
            task,
            trigger: None,
            config: ScheduleConfig {
                misfire_policy: self.inner.config.default_misfire_policy,
                misfire_grace_time: self.inner.config.default_misfire_grace_time,
                ..ScheduleConfig::default()
            },
            name: None,
            middleware: Vec::new(),
        }
    }

    pub async fn pause(&self, id: &ScheduleId) -> Result<(), SchedulerError> {
        let mut schedule = self
            .inner
            .store
            .get_schedule(id)
            .await?
            .ok_or_else(|| SchedulerError::ScheduleNotFound(id.clone()))?;
        schedule.state = ScheduleState::Paused;
        schedule.updated_at = Utc::now();
        self.inner.store.update_schedule(&schedule).await?;
        self.emit(SchedulerEvent::SchedulePaused { id: id.clone() });
        Ok(())
    }

    pub async fn resume(&self, id: &ScheduleId) -> Result<(), SchedulerError> {
        let mut schedule = self
            .inner
            .store
            .get_schedule(id)
            .await?
            .ok_or_else(|| SchedulerError::ScheduleNotFound(id.clone()))?;
        schedule.state = ScheduleState::Active;
        schedule.updated_at = Utc::now();

        // Recompute next fire time if needed
        if schedule.next_fire_time.is_none() || schedule.next_fire_time.unwrap() < Utc::now() {
            let triggers = self.inner.triggers.read().await;
            if let Some(trigger) = triggers.get(id) {
                let now = Utc::now();
                schedule.next_fire_time = trigger.next_fire_time(&now);
            }
        }

        self.inner.store.update_schedule(&schedule).await?;
        self.emit(SchedulerEvent::ScheduleResumed { id: id.clone() });
        self.inner.notify.notify_one();
        Ok(())
    }

    pub async fn remove(&self, id: &ScheduleId) -> Result<(), SchedulerError> {
        // Fetch executions before removing the schedule (needed for remote cancellation)
        #[cfg(feature = "remote")]
        let running_executions = self.inner.store.get_executions(id).await.ok();

        self.inner.store.remove_schedule(id).await?;
        self.inner.triggers.write().await.remove(id);
        self.inner.schedule_middleware.write().await.remove(id);

        // Cancel any running executions
        {
            let mut tokens = self.inner.cancellation_tokens.write().await;
            if let Some(token_list) = tokens.remove(id) {
                for token in token_list {
                    token.cancel();
                }
            }
        }

        // Cancel remote executions if applicable
        #[cfg(feature = "remote")]
        if let Some(remote) = self.inner.executor.as_dispatchable()
            && let Some(execs) = running_executions
        {
            for exec in execs.iter().filter(|e| e.state == ExecutionState::Running) {
                let _ = remote
                    .cancel_remote(crate::executor::remote::CancelRequest {
                        execution_id: exec.id.clone(),
                        schedule_id: id.clone(),
                    })
                    .await;
            }
        }

        self.emit(SchedulerEvent::ScheduleRemoved { id: id.clone() });
        Ok(())
    }

    pub async fn reschedule(
        &self,
        id: &ScheduleId,
        trigger: impl Trigger + 'static,
    ) -> Result<(), SchedulerError> {
        let mut schedule = self
            .inner
            .store
            .get_schedule(id)
            .await?
            .ok_or_else(|| SchedulerError::ScheduleNotFound(id.clone()))?;

        let now = Utc::now();
        schedule.next_fire_time = trigger.next_fire_time(&now);
        schedule.updated_at = now;

        if schedule.next_fire_time.is_none() {
            schedule.state = ScheduleState::Completed;
        } else {
            schedule.state = ScheduleState::Active;
        }

        self.inner.store.update_schedule(&schedule).await?;
        self.inner
            .triggers
            .write()
            .await
            .insert(id.clone(), Box::new(trigger));
        self.inner.notify.notify_one();
        Ok(())
    }

    pub async fn list_schedules(&self) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        self.inner.store.list_schedules().await
    }

    /// List schedules whose name starts with the given prefix.
    /// Uses server-side filtering when the backend supports it.
    pub async fn list_schedules_by_name_prefix(&self, prefix: &str) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        self.inner.store.list_schedules_by_name_prefix(prefix).await
    }

    pub async fn get_executions(
        &self,
        id: &ScheduleId,
    ) -> Result<Vec<Execution>, SchedulerError> {
        self.inner.store.get_executions(id).await
    }

    pub fn on_event(&self, listener: impl Fn(&SchedulerEvent) + Send + Sync + 'static) {
        let listeners = self.inner.event_listeners.clone();
        let boxed: Box<dyn Fn(&SchedulerEvent) + Send + Sync> = Box::new(listener);
        tokio::spawn(async move {
            listeners.write().await.push(boxed);
        });
    }

    // -----------------------------------------------------------------------
    // Metrics & Health
    // -----------------------------------------------------------------------

    /// Collect aggregated scheduler metrics.
    pub async fn metrics(&self) -> Result<SchedulerMetrics, SchedulerError> {
        let schedules = self.inner.store.list_schedules().await?;
        let mut active = 0u64;
        let mut paused = 0u64;
        let mut completed = 0u64;

        for s in &schedules {
            match s.state {
                ScheduleState::Active => active += 1,
                ScheduleState::Paused => paused += 1,
                ScheduleState::Completed => completed += 1,
            }
        }

        // Count executions by state across all schedules
        let mut total_execs = 0u64;
        let mut running = 0u64;
        let mut succeeded = 0u64;
        let mut failed = 0u64;
        let mut missed = 0u64;
        let mut interrupted = 0u64;

        for s in &schedules {
            let execs = self.inner.store.get_executions(&s.id).await?;
            for e in &execs {
                total_execs += 1;
                match e.state {
                    ExecutionState::Running => running += 1,
                    ExecutionState::Succeeded => succeeded += 1,
                    ExecutionState::Failed => failed += 1,
                    ExecutionState::Missed => missed += 1,
                    ExecutionState::Interrupted => interrupted += 1,
                    ExecutionState::Scheduled => {}
                }
            }
        }

        Ok(SchedulerMetrics {
            total_schedules: schedules.len() as u64,
            active_schedules: active,
            paused_schedules: paused,
            completed_schedules: completed,
            total_executions: total_execs,
            running_executions: running,
            succeeded_executions: succeeded,
            failed_executions: failed,
            missed_executions: missed,
            interrupted_executions: interrupted,
        })
    }

    /// Quick health check.
    pub async fn health(&self) -> HealthStatus {
        let store_ok = self.inner.store.list_schedules().await.is_ok();

        let (active, running) = if store_ok {
            let schedules = self.inner.store.list_schedules().await.unwrap_or_default();
            let active = schedules.iter().filter(|s| s.state == ScheduleState::Active).count() as u64;

            let mut running = 0u64;
            for s in &schedules {
                running += self.inner.executor.running_count(&s.id) as u64;
            }
            (active, running)
        } else {
            (0, 0)
        };

        HealthStatus {
            healthy: store_ok,
            store_accessible: store_ok,
            active_schedules: active,
            running_executions: running,
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Start the scheduler loop, blocking until shutdown.
    pub async fn start(&self) -> Result<(), SchedulerError> {
        #[cfg(feature = "remote")]
        self.spawn_result_processor();
        self.recover().await?;
        self.run_loop().await
    }

    /// Spawn the scheduler loop in a background task.
    pub fn spawn(&self) -> SchedulerHandle {
        #[cfg(feature = "remote")]
        self.spawn_result_processor();

        let (shutdown_tx, _) = watch::channel(false);
        let scheduler = self.clone();

        let mut rx = shutdown_tx.subscribe();

        let join_handle = tokio::spawn(async move {
            tracing::info!("scheduler loop starting — running recovery");
            if let Err(e) = scheduler.recover().await {
                tracing::error!(error = %e, "scheduler recovery failed");
                return Err(e);
            }
            tracing::info!("scheduler recovery complete");

            let inner = &scheduler.inner;
            loop {
                let sleep_duration = scheduler.compute_sleep_duration().await;
                tracing::debug!(sleep_ms = sleep_duration.as_millis() as u64, "scheduler sleeping");

                tokio::select! {
                    _ = tokio::time::sleep(sleep_duration) => {
                        tracing::debug!("scheduler woke: timer");
                    },
                    _ = inner.notify.notified() => {
                        tracing::debug!("scheduler woke: notify");
                    },
                    _ = rx.changed() => {
                        tracing::info!("scheduler loop: shutdown signal received");
                        break;
                    },
                    _ = scheduler.wait_for_inner_shutdown() => {
                        tracing::info!("scheduler loop: inner shutdown");
                        break;
                    },
                }

                if let Err(e) = scheduler.check_window_ends().await {
                    tracing::error!(error = %e, "check_window_ends failed");
                    return Err(e);
                }
                if let Err(e) = scheduler.process_due_schedules().await {
                    tracing::error!(error = %e, "process_due_schedules failed");
                    return Err(e);
                }
            }

            inner
                .executor
                .shutdown(std::time::Duration::from_secs(30))
                .await?;
            Ok(())
        });

        SchedulerHandle {
            shutdown_tx,
            join_handle,
        }
    }

    /// Signal shutdown (used by start() callers).
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown_tx.send(true);
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    async fn add_schedule_internal<T: Task>(
        &self,
        task: T,
        trigger: Box<dyn Trigger>,
        config: ScheduleConfig,
        name: Option<String>,
        middleware: Vec<Box<dyn TaskMiddleware>>,
    ) -> Result<ScheduleId, SchedulerError> {
        // Auto-register the task type
        self.inner.registry.register::<T>().await;

        let task_data = serde_json::to_value(&task)
            .map_err(|e| SchedulerError::SerializationError(e.to_string()))?;

        let now = Utc::now();
        let next_fire_time = trigger.next_fire_time(&now);
        let id = ScheduleId::new();

        let record = ScheduleRecord {
            id: id.clone(),
            name,
            type_name: T::TYPE_NAME.to_string(),
            task_data,
            state: if next_fire_time.is_some() {
                ScheduleState::Active
            } else {
                ScheduleState::Completed
            },
            next_fire_time,
            window_end_time: None,
            config,
            created_at: now,
            updated_at: now,
        };

        self.inner.store.add_schedule(record).await?;
        self.inner
            .triggers
            .write()
            .await
            .insert(id.clone(), trigger);

        // Store per-schedule middleware
        if !middleware.is_empty() {
            self.inner
                .schedule_middleware
                .write()
                .await
                .insert(id.clone(), middleware);
        }

        self.emit(SchedulerEvent::ScheduleAdded { id: id.clone() });
        self.inner.notify.notify_one();
        Ok(id)
    }

    async fn compute_sleep_duration(&self) -> std::time::Duration {
        let poll_interval = self.inner.config.poll_interval;

        let next_fire = match self.inner.store.get_next_fire_time().await {
            Ok(Some(next)) => {
                let now = Utc::now();
                if next <= now {
                    return std::time::Duration::ZERO;
                }
                (next - now)
                    .to_std()
                    .unwrap_or(poll_interval)
            }
            _ => poll_interval,
        };

        // Also consider nearest window_end_time
        let window_end = match self.inner.store.list_schedules().await {
            Ok(schedules) => {
                let now = Utc::now();
                schedules
                    .iter()
                    .filter_map(|s| s.window_end_time)
                    .filter(|t| *t > now)
                    .min()
                    .map(|t| (t - now).to_std().unwrap_or(poll_interval))
                    .unwrap_or(poll_interval)
            }
            Err(_) => poll_interval,
        };

        next_fire.min(window_end).min(poll_interval)
    }

    async fn wait_for_inner_shutdown(&self) {
        let mut rx = self.inner.shutdown_rx.clone();
        let _ = rx.changed().await;
    }

    async fn recover(&self) -> Result<(), SchedulerError> {
        let now = Utc::now();

        // 1. Handle orphaned Running executions
        let orphaned = self
            .inner
            .store
            .get_executions_by_state(ExecutionState::Running)
            .await?;
        for mut exec in orphaned {
            exec.state = ExecutionState::Interrupted;
            exec.finished_at = Some(now);
            exec.error = Some("Scheduler restarted while task was running".to_string());
            self.inner.store.update_execution(&exec).await?;

            self.emit(SchedulerEvent::ExecutionInterrupted {
                schedule_id: exec.schedule_id.clone(),
                execution_id: exec.id.clone(),
            });

            if let Some(schedule) = self.inner.store.get_schedule(&exec.schedule_id).await? {
                match schedule.config.recovery_policy {
                    RecoveryPolicy::Resubmit => {
                        // For window-based triggers, only resubmit if still
                        // within the active window. If the window has already
                        // closed, skip to the next fire time instead.
                        let still_in_window = match schedule.window_end_time {
                            Some(end) => now < end,
                            None => true, // non-window trigger, always resubmit
                        };

                        if still_in_window {
                            self.submit_execution(&schedule).await?;
                            self.emit(SchedulerEvent::ExecutionRecovered {
                                schedule_id: exec.schedule_id.clone(),
                                execution_id: exec.id.clone(),
                            });
                        } else {
                            // Window expired during downtime — advance instead
                            let mut s = schedule.clone();
                            s.window_end_time = None;
                            self.advance_schedule(&mut s, &now).await?;
                        }
                    }
                    RecoveryPolicy::Skip => {
                        let mut s = schedule.clone();
                        self.advance_schedule(&mut s, &now).await?;
                    }
                    RecoveryPolicy::MarkFailed => {
                        exec.state = ExecutionState::Failed;
                        self.inner.store.update_execution(&exec).await?;
                    }
                }
            }
        }

        // 2. Clean up expired one-shot schedules
        let all = self.inner.store.list_schedules().await?;
        tracing::info!(schedule_count = all.len(), "recovery: checking schedules");
        for schedule in all {
            if schedule.state != ScheduleState::Active {
                continue;
            }
            match schedule.next_fire_time {
                Some(nft) if nft < now => {
                    let triggers = self.inner.triggers.read().await;
                    let has_trigger = triggers.contains_key(&schedule.id);
                    tracing::info!(
                        schedule_id = %schedule.id,
                        name = ?schedule.name,
                        nft = %nft,
                        now = %now,
                        has_trigger_in_memory = has_trigger,
                        "recovery: schedule past due"
                    );
                    if let Some(trigger) = triggers.get(&schedule.id)
                        && trigger.next_fire_time(&now).is_none()
                    {
                        tracing::warn!(
                            schedule_id = %schedule.id,
                            name = ?schedule.name,
                            "recovery: marking one-shot schedule as completed"
                        );
                        drop(triggers);
                        let mut s = schedule.clone();
                        s.state = ScheduleState::Completed;
                        s.updated_at = now;
                        self.inner.store.update_schedule(&s).await?;
                        self.emit(SchedulerEvent::ScheduleExpired {
                            id: schedule.id.clone(),
                        });
                    }
                }
                None if schedule.state == ScheduleState::Active => {
                    let mut s = schedule.clone();
                    s.state = ScheduleState::Completed;
                    s.updated_at = now;
                    self.inner.store.update_schedule(&s).await?;
                }
                _ => {}
            }
        }

        // 3. Clean up old execution history
        if let Some(retention) = self.inner.config.execution_retention {
            let cutoff = now - retention;
            self.inner.store.cleanup_executions_before(cutoff).await?;
        }

        Ok(())
    }

    async fn run_loop(&self) -> Result<(), SchedulerError> {
        let mut shutdown_rx = self.inner.shutdown_rx.clone();

        loop {
            let sleep_duration = self.compute_sleep_duration().await;

            tokio::select! {
                _ = tokio::time::sleep(sleep_duration) => {},
                _ = self.inner.notify.notified() => {},
                _ = shutdown_rx.changed() => break,
            }

            self.check_window_ends().await?;
            self.process_due_schedules().await?;
        }

        self.inner
            .executor
            .shutdown(std::time::Duration::from_secs(30))
            .await?;
        Ok(())
    }

    async fn process_due_schedules(&self) -> Result<(), SchedulerError> {
        let now = Utc::now();
        tracing::debug!(now = %now, "checking for due schedules");
        let mut due = self.inner.store.get_due_schedules(&now).await?;
        tracing::info!(due_count = due.len(), now = %now, "due schedules found");

        // Sort by priority (highest first)
        due.sort_by(|a, b| b.config.priority.cmp(&a.config.priority));

        for mut schedule in due {
            if schedule.state != ScheduleState::Active {
                continue;
            }

            let next_fire_time = match schedule.next_fire_time {
                Some(t) => t,
                None => continue,
            };
            let delay = now - next_fire_time;

            // Misfire check
            let is_misfire = match schedule.config.misfire_grace_time {
                Some(grace) => delay > grace,
                None => false,
            };

            if is_misfire {
                match schedule.config.misfire_policy {
                    MisfirePolicy::DoNothing => {
                        self.emit(SchedulerEvent::ExecutionMissed {
                            schedule_id: schedule.id.clone(),
                            scheduled_time: next_fire_time,
                        });
                    }
                    MisfirePolicy::FireNow | MisfirePolicy::Coalesce => {
                        self.submit_execution(&schedule).await?;
                    }
                }
            } else {
                // On time - check max_instances
                let running = self.inner.executor.running_count(&schedule.id);
                if running < schedule.config.max_instances {
                    self.submit_execution(&schedule).await?;

                    // For window triggers, set window_end_time on the schedule
                    {
                        let triggers = self.inner.triggers.read().await;
                        if let Some(trigger) = triggers.get(&schedule.id)
                            && let Some(window_end) = trigger.window_end_time(&next_fire_time)
                        {
                            schedule.window_end_time = Some(window_end);
                        }
                    }
                } else {
                    tracing::warn!(schedule_id = %schedule.id, "max_instances reached, skipping");
                }
            }

            // Advance to next fire time
            self.advance_schedule(&mut schedule, &now).await?;
        }

        Ok(())
    }

    async fn check_window_ends(&self) -> Result<(), SchedulerError> {
        let now = Utc::now();
        let schedules = self.inner.store.list_schedules().await?;

        for schedule in schedules {
            if let Some(window_end) = schedule.window_end_time
                && window_end <= now
            {
                // Cancel running executions for this schedule
                {
                    let mut tokens = self.inner.cancellation_tokens.write().await;
                    if let Some(token_list) = tokens.remove(&schedule.id) {
                        for token in token_list {
                            token.cancel();
                        }
                    }
                }

                // Cancel remote executions if applicable
                #[cfg(feature = "remote")]
                if let Some(remote) = self.inner.executor.as_dispatchable()
                    && let Ok(execs) = self.inner.store.get_executions(&schedule.id).await
                {
                    for exec in execs.iter().filter(|e| e.state == ExecutionState::Running) {
                        let _ = remote
                            .cancel_remote(crate::executor::remote::CancelRequest {
                                execution_id: exec.id.clone(),
                                schedule_id: schedule.id.clone(),
                            })
                            .await;
                    }
                }

                // Clear window_end_time and advance
                let mut s = schedule.clone();
                s.window_end_time = None;
                s.updated_at = now;
                self.advance_schedule(&mut s, &now).await?;
            }
        }

        Ok(())
    }

    async fn submit_execution(&self, schedule: &ScheduleRecord) -> Result<(), SchedulerError> {
        tracing::info!(
            schedule_id = %schedule.id,
            type_name = %schedule.type_name,
            name = ?schedule.name,
            "submitting execution"
        );
        let execution_id = ExecutionId::new();
        let schedule_id = schedule.id.clone();
        let scheduled_fire_time = schedule.next_fire_time.unwrap_or_else(Utc::now);

        let execution = Execution {
            id: execution_id.clone(),
            schedule_id: schedule_id.clone(),
            state: ExecutionState::Running,
            scheduled_fire_time,
            actual_fire_time: Some(Utc::now()),
            finished_at: None,
            error: None,
        };
        self.inner.store.add_execution(execution).await?;

        self.emit(SchedulerEvent::ExecutionStarted {
            schedule_id: schedule_id.clone(),
            execution_id: execution_id.clone(),
        });

        #[cfg(feature = "remote")]
        if let Some(remote) = self.inner.executor.as_dispatchable() {
            let request = crate::executor::remote::TaskDispatchRequest {
                execution_id,
                schedule_id,
                type_name: schedule.type_name.clone(),
                task_data: schedule.task_data.clone(),
            };
            return remote.dispatch(request).await;
        }

        // Get runner and apply middleware chain
        let mut runner = self
            .inner
            .registry
            .get(&schedule.type_name)
            .await
            .ok_or_else(|| SchedulerError::TaskTypeNotRegistered(schedule.type_name.clone()))?;

        // Apply global middleware (outermost)
        for mw in &self.inner.global_middleware {
            runner = mw.wrap(runner);
        }

        // Apply per-schedule middleware (innermost, closer to task)
        {
            let mw_map = self.inner.schedule_middleware.read().await;
            if let Some(mw_list) = mw_map.get(&schedule.id) {
                for mw in mw_list {
                    runner = mw.wrap(runner);
                }
            }
        }

        let task_data = schedule.task_data.clone();
        let cancel_token = CancellationToken::new();

        // Store cancellation token
        {
            let mut tokens = self.inner.cancellation_tokens.write().await;
            tokens
                .entry(schedule_id.clone())
                .or_default()
                .push(cancel_token.clone());
        }

        let ctx = TaskContext {
            schedule_id: schedule_id.clone(),
            execution_id: execution_id.clone(),
            cancellation_token: cancel_token.clone(),
        };

        let store = self.inner.store.clone();
        let event_listeners = self.inner.event_listeners.clone();
        let tokens = self.inner.cancellation_tokens.clone();
        let sid_clone = schedule_id.clone();
        let eid_clone = execution_id.clone();

        let task_fn: BoxedTaskFn = Box::new(move || {
            Box::pin(async move {
                let result = runner(task_data, ctx).await;

                let now = Utc::now();
                let (state, error) = match &result {
                    Ok(()) => (ExecutionState::Succeeded, None),
                    Err(e) => (ExecutionState::Failed, Some(e.to_string())),
                };

                let updated_exec = Execution {
                    id: eid_clone.clone(),
                    schedule_id: sid_clone.clone(),
                    state,
                    scheduled_fire_time,
                    actual_fire_time: Some(now),
                    finished_at: Some(now),
                    error: error.clone(),
                };
                let _ = store.update_execution(&updated_exec).await;

                // Emit completion event
                let event = if result.is_ok() {
                    SchedulerEvent::ExecutionSucceeded {
                        schedule_id: sid_clone.clone(),
                        execution_id: eid_clone.clone(),
                    }
                } else {
                    SchedulerEvent::ExecutionFailed {
                        schedule_id: sid_clone.clone(),
                        execution_id: eid_clone.clone(),
                        error: error.unwrap_or_default(),
                    }
                };
                let listeners = event_listeners.read().await;
                for listener in listeners.iter() {
                    listener(&event);
                }

                // Clean up cancellation token
                {
                    let mut t = tokens.write().await;
                    if let Some(token_list) = t.get_mut(&sid_clone) {
                        token_list.retain(|tok| !tok.is_cancelled());
                    }
                }

                result
            })
        });

        self.inner.executor.submit(&schedule.id, task_fn).await?;
        Ok(())
    }

    #[cfg(feature = "remote")]
    fn spawn_result_processor(&self) {
        if let Some(receiver) = &self.inner.result_receiver {
            let receiver = receiver.clone();
            let scheduler = self.clone();
            tokio::spawn(async move {
                while let Some(report) = receiver.recv().await {
                    scheduler.handle_remote_result(report).await;
                }
            });
        }
    }

    #[cfg(feature = "remote")]
    async fn handle_remote_result(&self, report: crate::executor::remote::TaskResultReport) {
        let now = Utc::now();
        let (state, error) = match &report.outcome {
            crate::executor::remote::TaskOutcome::Succeeded => (ExecutionState::Succeeded, None),
            crate::executor::remote::TaskOutcome::Failed { error } => {
                (ExecutionState::Failed, Some(error.clone()))
            }
        };

        // Update execution record
        if let Ok(execs) = self.inner.store.get_executions(&report.schedule_id).await
            && let Some(exec) = execs.iter().find(|e| e.id == report.execution_id)
        {
            let updated = Execution {
                id: exec.id.clone(),
                schedule_id: exec.schedule_id.clone(),
                state,
                scheduled_fire_time: exec.scheduled_fire_time,
                actual_fire_time: exec.actual_fire_time,
                finished_at: Some(now),
                error: error.clone(),
            };
            let _ = self.inner.store.update_execution(&updated).await;
        }

        // Decrement running count on the remote executor
        if let Some(remote) = self.inner.executor.as_dispatchable() {
            remote.decrement_running_count(&report.schedule_id);
        }

        // Emit completion event
        match report.outcome {
            crate::executor::remote::TaskOutcome::Succeeded => {
                self.emit(SchedulerEvent::ExecutionSucceeded {
                    schedule_id: report.schedule_id,
                    execution_id: report.execution_id,
                });
            }
            crate::executor::remote::TaskOutcome::Failed { error } => {
                self.emit(SchedulerEvent::ExecutionFailed {
                    schedule_id: report.schedule_id,
                    execution_id: report.execution_id,
                    error,
                });
            }
        }
    }

    fn emit(&self, event: SchedulerEvent) {
        let listeners = self.inner.event_listeners.clone();
        let event_clone = event;
        // Fire-and-forget emit to avoid blocking
        tokio::spawn(async move {
            let l = listeners.read().await;
            for listener in l.iter() {
                listener(&event_clone);
            }
        });
    }

    async fn advance_schedule(
        &self,
        schedule: &mut ScheduleRecord,
        reference: &DateTime<Utc>,
    ) -> Result<(), SchedulerError> {
        let triggers = self.inner.triggers.read().await;
        if let Some(trigger) = triggers.get(&schedule.id) {
            schedule.next_fire_time = trigger.next_fire_time(reference);
        } else {
            schedule.next_fire_time = None;
        }
        drop(triggers);

        if schedule.next_fire_time.is_none() {
            schedule.state = ScheduleState::Completed;
            self.emit(SchedulerEvent::ScheduleExpired {
                id: schedule.id.clone(),
            });
        }

        schedule.updated_at = Utc::now();
        self.inner.store.update_schedule(schedule).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SchedulerBuilder
// ---------------------------------------------------------------------------

pub struct SchedulerBuilder {
    store: Option<Arc<dyn DataStore>>,
    executor: Option<Arc<dyn Executor>>,
    config: SchedulerConfig,
    global_middleware: Vec<Box<dyn TaskMiddleware>>,
    #[cfg(feature = "remote")]
    result_receiver: Option<Arc<dyn crate::executor::remote::ResultReceiver>>,
}

impl SchedulerBuilder {
    pub fn data_store(mut self, store: Arc<dyn DataStore>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn executor(mut self, executor: Arc<dyn Executor>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn poll_interval(mut self, interval: std::time::Duration) -> Self {
        self.config.poll_interval = interval;
        self
    }

    pub fn default_misfire_policy(mut self, policy: MisfirePolicy) -> Self {
        self.config.default_misfire_policy = policy;
        self
    }

    pub fn max_concurrent_jobs(mut self, max: u32) -> Self {
        self.config.max_concurrent_jobs = max;
        self
    }

    pub fn execution_retention(mut self, retention: chrono::Duration) -> Self {
        self.config.execution_retention = Some(retention);
        self
    }

    /// Add a global middleware applied to all task executions.
    pub fn middleware(mut self, mw: impl TaskMiddleware + 'static) -> Self {
        self.global_middleware.push(Box::new(mw));
        self
    }

    /// Configure a remote executor with a dispatcher and result receiver.
    #[cfg(feature = "remote")]
    pub fn remote_executor(
        mut self,
        dispatcher: Arc<dyn crate::executor::remote::TaskDispatcher>,
        receiver: Arc<dyn crate::executor::remote::ResultReceiver>,
    ) -> Self {
        self.executor = Some(Arc::new(crate::executor::remote::RemoteExecutor::new(dispatcher)));
        self.result_receiver = Some(receiver);
        self
    }

    pub fn build(self) -> Result<Scheduler, SchedulerError> {
        let store = self
            .store
            .unwrap_or_else(|| Arc::new(MemoryStore::new()));
        let executor = self
            .executor
            .unwrap_or_else(|| Arc::new(TokioExecutor::new()));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let inner = SchedulerInner {
            config: self.config,
            store,
            executor,
            triggers: RwLock::new(HashMap::new()),
            cancellation_tokens: Arc::new(RwLock::new(HashMap::new())),
            registry: TaskRegistry::new(),
            event_listeners: Arc::new(RwLock::new(Vec::new())),
            global_middleware: self.global_middleware,
            schedule_middleware: RwLock::new(HashMap::new()),
            notify: Notify::new(),
            shutdown_tx,
            shutdown_rx,
            #[cfg(feature = "remote")]
            result_receiver: self.result_receiver,
        };

        Ok(Scheduler {
            inner: Arc::new(inner),
        })
    }
}

// ---------------------------------------------------------------------------
// ScheduleBuilder
// ---------------------------------------------------------------------------

pub struct ScheduleBuilder<'a, T: Task> {
    scheduler: &'a Scheduler,
    task: T,
    trigger: Option<Box<dyn Trigger>>,
    config: ScheduleConfig,
    name: Option<String>,
    middleware: Vec<Box<dyn TaskMiddleware>>,
}

impl<'a, T: Task> ScheduleBuilder<'a, T> {
    pub fn trigger(mut self, trigger: impl Trigger + 'static) -> Self {
        self.trigger = Some(Box::new(trigger));
        self
    }

    pub fn misfire_policy(mut self, policy: MisfirePolicy) -> Self {
        self.config.misfire_policy = policy;
        self
    }

    pub fn misfire_grace_time(mut self, grace: chrono::Duration) -> Self {
        self.config.misfire_grace_time = Some(grace);
        self
    }

    pub fn max_instances(mut self, max: u32) -> Self {
        self.config.max_instances = max;
        self
    }

    pub fn recovery_policy(mut self, policy: RecoveryPolicy) -> Self {
        self.config.recovery_policy = policy;
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set task priority. Higher values = higher priority. Default is 0.
    pub fn priority(mut self, priority: i32) -> Self {
        self.config.priority = priority;
        self
    }

    /// Add a per-schedule middleware.
    pub fn middleware(mut self, mw: impl TaskMiddleware + 'static) -> Self {
        self.middleware.push(Box::new(mw));
        self
    }

    pub async fn submit(self) -> Result<ScheduleId, SchedulerError> {
        let trigger = self
            .trigger
            .ok_or_else(|| SchedulerError::InvalidTrigger("No trigger specified".to_string()))?;
        self.scheduler
            .add_schedule_internal(self.task, trigger, self.config, self.name, self.middleware)
            .await
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TaskResult;
    use crate::trigger::IntervalTrigger;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestTask {
        counter_id: String,
    }

    // Static counter for test tasks
    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    #[async_trait]
    impl Task for TestTask {
        const TYPE_NAME: &'static str = "test_task";
        async fn run(&self, _ctx: &TaskContext) -> TaskResult {
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn make_scheduler() -> Scheduler {
        Scheduler::builder()
            .poll_interval(Duration::from_millis(50))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_add_and_list_schedules() {
        let scheduler = make_scheduler();
        let task = TestTask {
            counter_id: "t1".into(),
        };
        let trigger = IntervalTrigger::every(Duration::from_secs(60));

        let id = scheduler.add(task, trigger).await.unwrap();

        let schedules = scheduler.list_schedules().await.unwrap();
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].id, id);
        assert_eq!(schedules[0].state, ScheduleState::Active);
        assert_eq!(schedules[0].type_name, "test_task");
    }

    #[tokio::test]
    async fn test_pause_and_resume() {
        let scheduler = make_scheduler();
        let task = TestTask {
            counter_id: "t2".into(),
        };
        let trigger = IntervalTrigger::every(Duration::from_secs(60));
        let id = scheduler.add(task, trigger).await.unwrap();

        // Pause
        scheduler.pause(&id).await.unwrap();
        let schedules = scheduler.list_schedules().await.unwrap();
        assert_eq!(schedules[0].state, ScheduleState::Paused);

        // Resume
        scheduler.resume(&id).await.unwrap();
        let schedules = scheduler.list_schedules().await.unwrap();
        assert_eq!(schedules[0].state, ScheduleState::Active);
    }

    #[tokio::test]
    async fn test_remove() {
        let scheduler = make_scheduler();
        let task = TestTask {
            counter_id: "t3".into(),
        };
        let trigger = IntervalTrigger::every(Duration::from_secs(60));
        let id = scheduler.add(task, trigger).await.unwrap();

        scheduler.remove(&id).await.unwrap();
        let schedules = scheduler.list_schedules().await.unwrap();
        assert_eq!(schedules.len(), 0);
    }

    #[tokio::test]
    async fn test_schedule_builder_api() {
        let scheduler = make_scheduler();
        let task = TestTask {
            counter_id: "t4".into(),
        };

        let id = scheduler
            .schedule(task)
            .name("my-schedule")
            .trigger(IntervalTrigger::every(Duration::from_secs(30)))
            .max_instances(5)
            .misfire_policy(MisfirePolicy::FireNow)
            .priority(10)
            .submit()
            .await
            .unwrap();

        let schedules = scheduler.list_schedules().await.unwrap();
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].id, id);
        assert_eq!(schedules[0].name, Some("my-schedule".to_string()));
        assert_eq!(schedules[0].config.max_instances, 5);
        assert_eq!(schedules[0].config.misfire_policy, MisfirePolicy::FireNow);
        assert_eq!(schedules[0].config.priority, 10);
    }

    #[tokio::test]
    async fn test_basic_execution() {
        TEST_COUNTER.store(0, Ordering::SeqCst);

        let scheduler = make_scheduler();
        let task = TestTask {
            counter_id: "t5".into(),
        };
        // Fire immediately and repeatedly
        let trigger = IntervalTrigger::every(Duration::from_millis(100));
        scheduler.add(task, trigger).await.unwrap();

        let handle = scheduler.spawn();

        // Wait for some executions
        tokio::time::sleep(Duration::from_millis(350)).await;
        handle.shutdown().await.unwrap();

        let count = TEST_COUNTER.load(Ordering::SeqCst);
        assert!(count >= 1, "Expected at least 1 execution, got {count}");
    }

    #[tokio::test]
    async fn test_metrics() {
        let scheduler = make_scheduler();
        let task = TestTask { counter_id: "m1".into() };
        scheduler.add(task, IntervalTrigger::every(Duration::from_secs(60))).await.unwrap();

        let metrics = scheduler.metrics().await.unwrap();
        assert_eq!(metrics.total_schedules, 1);
        assert_eq!(metrics.active_schedules, 1);
    }

    #[tokio::test]
    async fn test_health() {
        let scheduler = make_scheduler();
        let health = scheduler.health().await;
        assert!(health.healthy);
        assert!(health.store_accessible);
    }
}
