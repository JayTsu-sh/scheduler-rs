use std::collections::HashMap;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use crate::error::SchedulerError;
use crate::execution::{Execution, ExecutionState};
use crate::schedule::{ScheduleId, ScheduleRecord, ScheduleState};
use super::DataStore;

struct Inner {
    schedules: HashMap<ScheduleId, ScheduleRecord>,
    executions: Vec<Execution>,
}

pub struct MemoryStore {
    inner: RwLock<Inner>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                schedules: HashMap::new(),
                executions: Vec::new(),
            }),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataStore for MemoryStore {
    async fn add_schedule(&self, schedule: ScheduleRecord) -> Result<(), SchedulerError> {
        let mut inner = self.inner.write().await;
        if inner.schedules.contains_key(&schedule.id) {
            return Err(SchedulerError::ScheduleAlreadyExists(schedule.id));
        }
        inner.schedules.insert(schedule.id.clone(), schedule);
        Ok(())
    }

    async fn get_schedule(&self, id: &ScheduleId) -> Result<Option<ScheduleRecord>, SchedulerError> {
        let inner = self.inner.read().await;
        Ok(inner.schedules.get(id).cloned())
    }

    async fn get_due_schedules(&self, now: &DateTime<Utc>) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        let inner = self.inner.read().await;
        let mut due: Vec<ScheduleRecord> = inner
            .schedules
            .values()
            .filter(|s| {
                s.state == ScheduleState::Active
                    && s.next_fire_time.is_some()
                    && s.next_fire_time.unwrap() <= *now
            })
            .cloned()
            .collect();
        due.sort_by_key(|s| s.next_fire_time);
        Ok(due)
    }

    async fn get_next_fire_time(&self) -> Result<Option<DateTime<Utc>>, SchedulerError> {
        let inner = self.inner.read().await;
        let min = inner
            .schedules
            .values()
            .filter(|s| s.state == ScheduleState::Active && s.next_fire_time.is_some())
            .filter_map(|s| s.next_fire_time)
            .min();
        Ok(min)
    }

    async fn update_schedule(&self, schedule: &ScheduleRecord) -> Result<(), SchedulerError> {
        let mut inner = self.inner.write().await;
        if !inner.schedules.contains_key(&schedule.id) {
            return Err(SchedulerError::ScheduleNotFound(schedule.id.clone()));
        }
        inner.schedules.insert(schedule.id.clone(), schedule.clone());
        Ok(())
    }

    async fn remove_schedule(&self, id: &ScheduleId) -> Result<bool, SchedulerError> {
        let mut inner = self.inner.write().await;
        Ok(inner.schedules.remove(id).is_some())
    }

    async fn list_schedules(&self) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        let inner = self.inner.read().await;
        Ok(inner.schedules.values().cloned().collect())
    }

    async fn add_execution(&self, execution: Execution) -> Result<(), SchedulerError> {
        let mut inner = self.inner.write().await;
        inner.executions.push(execution);
        Ok(())
    }

    async fn update_execution(&self, execution: &Execution) -> Result<(), SchedulerError> {
        let mut inner = self.inner.write().await;
        if let Some(existing) = inner.executions.iter_mut().find(|e| e.id == execution.id) {
            *existing = execution.clone();
            Ok(())
        } else {
            Err(SchedulerError::StoreError(format!(
                "Execution not found: {}",
                execution.id
            )))
        }
    }

    async fn get_executions(&self, schedule_id: &ScheduleId) -> Result<Vec<Execution>, SchedulerError> {
        let inner = self.inner.read().await;
        Ok(inner
            .executions
            .iter()
            .filter(|e| e.schedule_id == *schedule_id)
            .cloned()
            .collect())
    }

    async fn get_executions_by_state(&self, state: ExecutionState) -> Result<Vec<Execution>, SchedulerError> {
        let inner = self.inner.read().await;
        Ok(inner
            .executions
            .iter()
            .filter(|e| e.state == state)
            .cloned()
            .collect())
    }

    async fn cleanup_executions_before(&self, before: DateTime<Utc>) -> Result<u64, SchedulerError> {
        let mut inner = self.inner.write().await;
        let before_len = inner.executions.len();
        inner.executions.retain(|e| {
            let terminal = matches!(
                e.state,
                ExecutionState::Succeeded
                    | ExecutionState::Failed
                    | ExecutionState::Missed
                    | ExecutionState::Interrupted
            );
            let old = e.finished_at.is_some_and(|f| f < before);
            !(terminal && old)
        });
        Ok((before_len - inner.executions.len()) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::ExecutionId;
    use crate::schedule::{ScheduleConfig, ScheduleState};
    use chrono::Duration;

    fn make_schedule(id: ScheduleId, next: Option<DateTime<Utc>>, state: ScheduleState) -> ScheduleRecord {
        let now = Utc::now();
        ScheduleRecord {
            id,
            name: None,
            type_name: "test".to_string(),
            task_data: serde_json::Value::Null,
            state,
            next_fire_time: next,
            window_end_time: None,
            config: ScheduleConfig::default(),
            created_at: now,
            updated_at: now,
        }
    }

    fn make_execution(
        schedule_id: ScheduleId,
        state: ExecutionState,
        finished_at: Option<DateTime<Utc>>,
    ) -> Execution {
        Execution {
            id: ExecutionId::new(),
            schedule_id,
            state,
            scheduled_fire_time: Utc::now(),
            actual_fire_time: None,
            finished_at,
            error: None,
        }
    }

    #[tokio::test]
    async fn add_and_get_schedule() {
        let store = MemoryStore::new();
        let id = ScheduleId::new();
        let sched = make_schedule(id.clone(), None, ScheduleState::Active);
        store.add_schedule(sched.clone()).await.unwrap();

        let got = store.get_schedule(&id).await.unwrap().unwrap();
        assert_eq!(got.id, id);

        // duplicate should error
        let result = store.add_schedule(sched).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_due_schedules_returns_only_due() {
        let store = MemoryStore::new();
        let now = Utc::now();

        let id1 = ScheduleId::new();
        let s1 = make_schedule(id1.clone(), Some(now - Duration::seconds(10)), ScheduleState::Active);
        store.add_schedule(s1).await.unwrap();

        let id2 = ScheduleId::new();
        let s2 = make_schedule(id2.clone(), Some(now + Duration::seconds(10)), ScheduleState::Active);
        store.add_schedule(s2).await.unwrap();

        // paused schedule with past time should NOT be returned
        let id3 = ScheduleId::new();
        let s3 = make_schedule(id3.clone(), Some(now - Duration::seconds(5)), ScheduleState::Paused);
        store.add_schedule(s3).await.unwrap();

        let due = store.get_due_schedules(&now).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, id1);
    }

    #[tokio::test]
    async fn get_next_fire_time_returns_earliest() {
        let store = MemoryStore::new();
        let now = Utc::now();

        let t1 = now + Duration::seconds(100);
        let t2 = now + Duration::seconds(50);

        store.add_schedule(make_schedule(ScheduleId::new(), Some(t1), ScheduleState::Active)).await.unwrap();
        store.add_schedule(make_schedule(ScheduleId::new(), Some(t2), ScheduleState::Active)).await.unwrap();
        // paused should be ignored
        store.add_schedule(make_schedule(ScheduleId::new(), Some(now + Duration::seconds(10)), ScheduleState::Paused)).await.unwrap();

        let next = store.get_next_fire_time().await.unwrap().unwrap();
        assert_eq!(next, t2);
    }

    #[tokio::test]
    async fn update_schedule_works() {
        let store = MemoryStore::new();
        let id = ScheduleId::new();
        let mut sched = make_schedule(id.clone(), None, ScheduleState::Active);
        store.add_schedule(sched.clone()).await.unwrap();

        sched.state = ScheduleState::Paused;
        store.update_schedule(&sched).await.unwrap();

        let got = store.get_schedule(&id).await.unwrap().unwrap();
        assert_eq!(got.state, ScheduleState::Paused);

        // update non-existent
        let mut fake = sched.clone();
        fake.id = ScheduleId::new();
        assert!(store.update_schedule(&fake).await.is_err());
    }

    #[tokio::test]
    async fn remove_schedule_works() {
        let store = MemoryStore::new();
        let id = ScheduleId::new();
        store.add_schedule(make_schedule(id.clone(), None, ScheduleState::Active)).await.unwrap();

        assert!(store.remove_schedule(&id).await.unwrap());
        assert!(!store.remove_schedule(&id).await.unwrap());
        assert!(store.get_schedule(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn execution_crud() {
        let store = MemoryStore::new();
        let sid = ScheduleId::new();

        let mut exec = make_execution(sid.clone(), ExecutionState::Running, None);
        store.add_execution(exec.clone()).await.unwrap();

        let execs = store.get_executions(&sid).await.unwrap();
        assert_eq!(execs.len(), 1);

        exec.state = ExecutionState::Succeeded;
        exec.finished_at = Some(Utc::now());
        store.update_execution(&exec).await.unwrap();

        let execs = store.get_executions(&sid).await.unwrap();
        assert_eq!(execs[0].state, ExecutionState::Succeeded);
    }

    #[tokio::test]
    async fn get_executions_by_state_filtering() {
        let store = MemoryStore::new();
        let sid = ScheduleId::new();

        store.add_execution(make_execution(sid.clone(), ExecutionState::Running, None)).await.unwrap();
        store.add_execution(make_execution(sid.clone(), ExecutionState::Succeeded, Some(Utc::now()))).await.unwrap();
        store.add_execution(make_execution(sid.clone(), ExecutionState::Running, None)).await.unwrap();

        let running = store.get_executions_by_state(ExecutionState::Running).await.unwrap();
        assert_eq!(running.len(), 2);

        let succeeded = store.get_executions_by_state(ExecutionState::Succeeded).await.unwrap();
        assert_eq!(succeeded.len(), 1);
    }

    #[tokio::test]
    async fn cleanup_executions_before_works() {
        let store = MemoryStore::new();
        let sid = ScheduleId::new();
        let now = Utc::now();

        // old succeeded - should be cleaned
        store.add_execution(make_execution(sid.clone(), ExecutionState::Succeeded, Some(now - Duration::hours(2)))).await.unwrap();
        // old failed - should be cleaned
        store.add_execution(make_execution(sid.clone(), ExecutionState::Failed, Some(now - Duration::hours(2)))).await.unwrap();
        // recent succeeded - should remain
        store.add_execution(make_execution(sid.clone(), ExecutionState::Succeeded, Some(now))).await.unwrap();
        // running - should remain (not terminal)
        store.add_execution(make_execution(sid.clone(), ExecutionState::Running, None)).await.unwrap();

        let removed = store.cleanup_executions_before(now - Duration::hours(1)).await.unwrap();
        assert_eq!(removed, 2);

        let all = store.get_executions(&sid).await.unwrap();
        assert_eq!(all.len(), 2);
    }
}
