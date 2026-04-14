use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::SchedulerError;
use crate::schedule::{MisfirePolicy, RecoveryPolicy, ScheduleConfig, ScheduleId};
use crate::scheduler::Scheduler;
use crate::trigger::{CronTrigger, ImmediateTrigger, IntervalTrigger, OnceTrigger, Trigger};

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobsFile {
    #[serde(default)]
    pub jobs: Vec<JobDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobDef {
    pub name: String,
    pub type_name: String,
    pub task_data: String,
    pub trigger: TriggerDef,
    #[serde(default)]
    pub config: JobConfigDef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerDef {
    Cron { expression: String },
    Interval { interval_secs: u64 },
    Once { at: String },
    Immediate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct JobConfigDef {
    pub max_instances: Option<u32>,
    pub priority: Option<i32>,
    pub misfire_policy: Option<String>,
    pub recovery_policy: Option<String>,
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

impl TriggerDef {
    pub fn into_trigger(self) -> Result<Box<dyn Trigger>, SchedulerError> {
        match self {
            TriggerDef::Cron { expression } => {
                let trigger = CronTrigger::new(&expression)?;
                Ok(Box::new(trigger))
            }
            TriggerDef::Interval { interval_secs } => {
                let trigger = IntervalTrigger::every(std::time::Duration::from_secs(interval_secs));
                Ok(Box::new(trigger))
            }
            TriggerDef::Once { at } => {
                let dt = at.parse::<chrono::DateTime<chrono::Utc>>().map_err(|e| {
                    SchedulerError::InvalidTrigger(format!("invalid ISO 8601 datetime: {e}"))
                })?;
                let trigger = OnceTrigger::at(dt);
                Ok(Box::new(trigger))
            }
            TriggerDef::Immediate => Ok(Box::new(ImmediateTrigger::new())),
        }
    }
}

impl JobConfigDef {
    pub fn into_schedule_config(self) -> Result<ScheduleConfig, SchedulerError> {
        let mut config = ScheduleConfig::default();

        if let Some(max) = self.max_instances {
            config.max_instances = max;
        }
        if let Some(p) = self.priority {
            config.priority = p;
        }
        if let Some(ref mp) = self.misfire_policy {
            config.misfire_policy = match mp.as_str() {
                "do_nothing" => MisfirePolicy::DoNothing,
                "fire_now" => MisfirePolicy::FireNow,
                "coalesce" => MisfirePolicy::Coalesce,
                other => {
                    return Err(SchedulerError::InvalidTrigger(format!(
                        "unknown misfire_policy: {other}"
                    )));
                }
            };
        }
        if let Some(ref rp) = self.recovery_policy {
            config.recovery_policy = match rp.as_str() {
                "resubmit" => RecoveryPolicy::Resubmit,
                "skip" => RecoveryPolicy::Skip,
                "mark_failed" => RecoveryPolicy::MarkFailed,
                other => {
                    return Err(SchedulerError::InvalidTrigger(format!(
                        "unknown recovery_policy: {other}"
                    )));
                }
            };
        }

        Ok(config)
    }
}

// ---------------------------------------------------------------------------
// JobLoader
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct JobLoader {
    scheduler: Scheduler,
    path: PathBuf,
    loaded: Arc<RwLock<HashMap<String, (ScheduleId, JobDef)>>>,
}

impl JobLoader {
    pub fn new(scheduler: Scheduler, path: impl Into<PathBuf>) -> Self {
        Self {
            scheduler,
            path: path.into(),
            loaded: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Read the jobs file, parse it, and reconcile with the scheduler.
    pub async fn load(&self) -> Result<(), SchedulerError> {
        let path = self.path.clone();
        let contents = tokio::task::spawn_blocking(move || std::fs::read_to_string(&path))
            .await
            .map_err(|e| {
                tracing::error!("spawn_blocking join error: {e}");
                SchedulerError::InvalidTrigger(format!("spawn_blocking join error: {e}"))
            })?
            .map_err(|e| {
                tracing::error!("failed to read jobs file {:?}: {e}", self.path);
                SchedulerError::InvalidTrigger(format!("failed to read jobs file: {e}"))
            })?;

        let jobs_file: JobsFile = toml::from_str(&contents).map_err(|e| {
            tracing::error!("failed to parse jobs file {:?}: {e}", self.path);
            SchedulerError::InvalidTrigger(format!("failed to parse jobs file: {e}"))
        })?;

        self.reconcile(jobs_file.jobs).await
    }

    /// Reload the jobs file (re-reads and reconciles).
    pub async fn reload(&self) -> Result<(), SchedulerError> {
        self.load().await
    }

    /// Start watching the jobs file for changes.
    /// Returns a `JoinHandle` for the background watcher task.
    /// The caller can stop the watcher by cancelling the provided `CancellationToken`.
    pub fn watch(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let loader = self.clone();
        tokio::task::spawn(async move {
            if let Err(e) = run_watcher(loader, cancel).await {
                tracing::error!("file watcher error: {e}");
            }
        })
    }

    async fn reconcile(&self, new_jobs: Vec<JobDef>) -> Result<(), SchedulerError> {
        let new_map: HashMap<&str, &JobDef> =
            new_jobs.iter().map(|j| (j.name.as_str(), j)).collect();

        let mut loaded = self.loaded.write().await;

        // Collect names to remove or update.
        let existing_names: Vec<String> = loaded.keys().cloned().collect();

        for name in &existing_names {
            match new_map.get(name.as_str()) {
                None => {
                    // Job removed from file — remove from scheduler.
                    let (id, _) = loaded.remove(name).unwrap();
                    if let Err(e) = self.scheduler.remove(&id).await {
                        tracing::warn!("failed to remove schedule {id} for job {name}: {e}");
                    } else {
                        tracing::info!("removed job {name}");
                    }
                }
                Some(new_def) => {
                    let (old_id, old_def) = loaded.get(name).unwrap();
                    if *new_def != old_def {
                        // Job changed — remove old, add new.
                        let old_id = old_id.clone();
                        if let Err(e) = self.scheduler.remove(&old_id).await {
                            tracing::warn!(
                                "failed to remove old schedule {old_id} for updated job {name}: {e}"
                            );
                        }

                        match self.add_job(new_def).await {
                            Ok(new_id) => {
                                tracing::info!("updated job {name} (new id: {new_id})");
                                loaded.insert(name.clone(), (new_id, (*new_def).clone()));
                            }
                            Err(SchedulerError::TaskTypeNotRegistered(ref t)) => {
                                tracing::warn!(
                                    "skipping updated job {name}: task type {t} not registered"
                                );
                                loaded.remove(name);
                            }
                            Err(e) => {
                                tracing::warn!("failed to re-add updated job {name}: {e}");
                                loaded.remove(name);
                            }
                        }
                    }
                    // else: same definition, skip
                }
            }
        }

        // Add new jobs that weren't previously loaded.
        for (name, def) in &new_map {
            if loaded.contains_key(*name) {
                continue;
            }
            match self.add_job(def).await {
                Ok(id) => {
                    tracing::info!("loaded job {name} (id: {id})");
                    loaded.insert(name.to_string(), (id, (*def).clone()));
                }
                Err(SchedulerError::TaskTypeNotRegistered(ref t)) => {
                    tracing::warn!("skipping job {name}: task type {t} not registered");
                }
                Err(e) => {
                    tracing::warn!("failed to add job {name}: {e}");
                }
            }
        }

        Ok(())
    }

    async fn add_job(&self, def: &JobDef) -> Result<ScheduleId, SchedulerError> {
        let task_data: serde_json::Value = serde_json::from_str(&def.task_data).map_err(|e| {
            SchedulerError::SerializationError(format!("invalid task_data JSON: {e}"))
        })?;
        let trigger = def.trigger.clone().into_trigger()?;
        let config = def.config.clone().into_schedule_config()?;

        self.scheduler
            .add_raw(
                def.type_name.clone(),
                task_data,
                trigger,
                config,
                Some(def.name.clone()),
            )
            .await
    }
}

// ---------------------------------------------------------------------------
// File watcher (runs in spawned task)
// ---------------------------------------------------------------------------

async fn run_watcher(
    loader: JobLoader,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), SchedulerError> {
    use notify::{EventKind, RecursiveMode, Watcher};

    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(16);

    // Determine the parent directory and file name so we can watch correctly.
    let watch_path = std::fs::canonicalize(&loader.path).unwrap_or_else(|_| loader.path.clone());
    let watch_dir = watch_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    let file_name = watch_path.file_name().map(|n| n.to_os_string());

    let mut watcher =
        notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {
            match res {
                Ok(event) => {
                    match event.kind {
                        EventKind::Modify(_) | EventKind::Create(_) => {
                            // If we know the file name, filter to only that file.
                            let dominated = match &file_name {
                                Some(fname) => event.paths.iter().any(|p| {
                                    p.file_name()
                                        .map(|n| n == fname.as_os_str())
                                        .unwrap_or(false)
                                }),
                                None => true,
                            };
                            if dominated {
                                let _ = tx.try_send(());
                            }
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    tracing::warn!("File watcher error: {e}");
                }
            }
        })
        .map_err(|e| {
            SchedulerError::InvalidTrigger(format!("failed to create file watcher: {e}"))
        })?;

    watcher
        .watch(&watch_dir, RecursiveMode::NonRecursive)
        .map_err(|e| SchedulerError::InvalidTrigger(format!("failed to watch directory: {e}")))?;

    tracing::info!("watching {:?} for changes", loader.path);

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(()) => {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        while rx.try_recv().is_ok() {}

                        tracing::info!("jobs file changed, reloading");
                        if let Err(e) = loader.reload().await {
                            tracing::error!("reload failed: {e}");
                        }
                    }
                    None => break,
                }
            }
            _ = cancel.cancelled() => {
                tracing::info!("Job loader watcher stopped");
                break;
            }
        }
    }

    // Keep watcher alive — it is dropped when the loop exits.
    drop(watcher);
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_toml() {
        let toml_str = r#"
[[jobs]]
name = "cleanup"
type_name = "CleanupTask"
task_data = '{"max_age_days": 30}'
[jobs.trigger]
type = "cron"
expression = "0 0 * * * *"
[jobs.config]
max_instances = 2
priority = 5
misfire_policy = "fire_now"
recovery_policy = "resubmit"

[[jobs]]
name = "ping"
type_name = "PingTask"
task_data = '{}'
[jobs.trigger]
type = "immediate"
"#;

        let jobs_file: JobsFile = toml::from_str(toml_str).unwrap();
        assert_eq!(jobs_file.jobs.len(), 2);

        let cleanup = &jobs_file.jobs[0];
        assert_eq!(cleanup.name, "cleanup");
        assert_eq!(cleanup.type_name, "CleanupTask");
        assert_eq!(
            cleanup.trigger,
            TriggerDef::Cron {
                expression: "0 0 * * * *".into()
            }
        );
        assert_eq!(cleanup.config.max_instances, Some(2));
        assert_eq!(cleanup.config.priority, Some(5));
        assert_eq!(cleanup.config.misfire_policy, Some("fire_now".into()));
        assert_eq!(cleanup.config.recovery_policy, Some("resubmit".into()));

        let ping = &jobs_file.jobs[1];
        assert_eq!(ping.name, "ping");
        assert_eq!(ping.trigger, TriggerDef::Immediate);
        assert_eq!(ping.config, JobConfigDef::default());
    }

    #[test]
    fn trigger_def_cron() {
        let def = TriggerDef::Cron {
            expression: "0 30 9 * * *".into(),
        };
        let trigger = def.into_trigger().unwrap();
        assert!(trigger.description().contains("0 30 9 * * *"));
    }

    #[test]
    fn trigger_def_interval() {
        let def = TriggerDef::Interval { interval_secs: 60 };
        let trigger = def.into_trigger().unwrap();
        // IntervalTrigger should have a description mentioning the interval.
        let desc = trigger.description();
        assert!(!desc.is_empty());
    }

    #[test]
    fn trigger_def_once() {
        let def = TriggerDef::Once {
            at: "2030-01-01T00:00:00Z".into(),
        };
        let trigger = def.into_trigger().unwrap();
        let desc = trigger.description();
        assert!(!desc.is_empty());
    }

    #[test]
    fn config_def_conversion() {
        let def = JobConfigDef {
            max_instances: Some(3),
            priority: Some(10),
            misfire_policy: Some("do_nothing".into()),
            recovery_policy: Some("mark_failed".into()),
        };
        let config = def.into_schedule_config().unwrap();
        assert_eq!(config.max_instances, 3);
        assert_eq!(config.priority, 10);
        assert_eq!(config.misfire_policy, MisfirePolicy::DoNothing);
        assert_eq!(config.recovery_policy, RecoveryPolicy::MarkFailed);
    }

    #[test]
    fn config_def_defaults() {
        let def = JobConfigDef::default();
        let config = def.into_schedule_config().unwrap();
        let default_config = ScheduleConfig::default();
        assert_eq!(config.max_instances, default_config.max_instances);
        assert_eq!(config.priority, default_config.priority);
        assert_eq!(config.misfire_policy, default_config.misfire_policy);
        assert_eq!(config.recovery_policy, default_config.recovery_policy);
    }

    #[test]
    fn config_def_invalid_misfire_policy() {
        let def = JobConfigDef {
            misfire_policy: Some("explode".into()),
            ..Default::default()
        };
        let err = def.into_schedule_config().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown misfire_policy"),
            "expected 'unknown misfire_policy' in error, got: {msg}"
        );
    }

    #[test]
    fn config_def_invalid_recovery_policy() {
        let def = JobConfigDef {
            recovery_policy: Some("yolo".into()),
            ..Default::default()
        };
        let err = def.into_schedule_config().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown recovery_policy"),
            "expected 'unknown recovery_policy' in error, got: {msg}"
        );
    }

    #[test]
    fn trigger_def_once_invalid_datetime() {
        let def = TriggerDef::Once {
            at: "not-a-date".into(),
        };
        let err = def.into_trigger().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid ISO 8601"),
            "expected 'invalid ISO 8601' in error, got: {msg}"
        );
    }
}
