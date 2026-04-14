use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;

use crate::error::SchedulerError;
use crate::execution::{Execution, ExecutionState};
use crate::schedule::{ScheduleId, ScheduleRecord, ScheduleState};
use super::DataStore;

pub struct RedisStore {
    conn: redis::aio::ConnectionManager,
    prefix: String,
}

impl RedisStore {
    pub async fn new(client: redis::Client) -> Result<Self, SchedulerError> {
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
        Ok(Self {
            conn,
            prefix: "scheduler".to_string(),
        })
    }

    pub async fn with_prefix(
        client: redis::Client,
        prefix: impl Into<String>,
    ) -> Result<Self, SchedulerError> {
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
        Ok(Self {
            conn,
            prefix: prefix.into(),
        })
    }

    fn key(&self, parts: &[&str]) -> String {
        format!("{}:{}", self.prefix, parts.join(":"))
    }

    fn schedule_key(&self, id: &ScheduleId) -> String {
        self.key(&["schedule", &id.to_string()])
    }

    fn schedule_ids_key(&self) -> String {
        self.key(&["schedule_ids"])
    }

    fn fire_times_key(&self) -> String {
        self.key(&["fire_times"])
    }

    fn execution_key(&self, id: &crate::execution::ExecutionId) -> String {
        self.key(&["execution", &id.to_string()])
    }

    fn schedule_execs_key(&self, schedule_id: &ScheduleId) -> String {
        self.key(&["schedule_execs", &schedule_id.to_string()])
    }

    fn exec_state_key(&self, state: ExecutionState) -> String {
        let state_str = serde_json::to_value(state)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        self.key(&["exec_state", &state_str])
    }

    fn map_err(e: redis::RedisError) -> SchedulerError {
        SchedulerError::StoreError(e.to_string())
    }

    fn serialize<T: serde::Serialize>(value: &T) -> Result<String, SchedulerError> {
        serde_json::to_string(value)
            .map_err(|e| SchedulerError::SerializationError(e.to_string()))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(data: &str) -> Result<T, SchedulerError> {
        serde_json::from_str(data)
            .map_err(|e| SchedulerError::SerializationError(e.to_string()))
    }
}

#[async_trait]
impl DataStore for RedisStore {
    async fn add_schedule(&self, schedule: ScheduleRecord) -> Result<(), SchedulerError> {
        let mut conn = self.conn.clone();
        let id_str = schedule.id.to_string();
        let schedule_key = self.schedule_key(&schedule.id);
        let schedule_ids_key = self.schedule_ids_key();
        let fire_times_key = self.fire_times_key();

        // Check if already exists
        let exists: bool = conn.exists(&schedule_key).await.map_err(Self::map_err)?;
        if exists {
            return Err(SchedulerError::ScheduleAlreadyExists(schedule.id));
        }

        let json = Self::serialize(&schedule)?;

        let mut pipe = redis::pipe();
        pipe.atomic()
            .set(&schedule_key, &json)
            .sadd(&schedule_ids_key, &id_str);

        if let Some(next) = schedule.next_fire_time {
            pipe.zadd(&fire_times_key, &id_str, next.timestamp_millis() as f64);
        }

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(Self::map_err)?;

        Ok(())
    }

    async fn get_schedule(
        &self,
        id: &ScheduleId,
    ) -> Result<Option<ScheduleRecord>, SchedulerError> {
        let mut conn = self.conn.clone();
        let key = self.schedule_key(id);

        let data: Option<String> = conn.get(&key).await.map_err(Self::map_err)?;
        match data {
            Some(json) => Ok(Some(Self::deserialize(&json)?)),
            None => Ok(None),
        }
    }

    async fn get_due_schedules(
        &self,
        now: &DateTime<Utc>,
    ) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        let mut conn = self.conn.clone();
        let fire_times_key = self.fire_times_key();
        let now_score = now.timestamp_millis() as f64;

        let ids: Vec<String> = conn
            .zrangebyscore(&fire_times_key, f64::NEG_INFINITY, now_score)
            .await
            .map_err(Self::map_err)?;

        let mut schedules = Vec::new();
        for id_str in ids {
            let key = self.key(&["schedule", &id_str]);
            let data: Option<String> = conn.get(&key).await.map_err(Self::map_err)?;
            if let Some(json) = data {
                let record: ScheduleRecord = Self::deserialize(&json)?;
                if record.state == ScheduleState::Active {
                    schedules.push(record);
                }
            }
        }

        schedules.sort_by_key(|s| s.next_fire_time);
        Ok(schedules)
    }

    async fn get_next_fire_time(&self) -> Result<Option<DateTime<Utc>>, SchedulerError> {
        let mut conn = self.conn.clone();
        let fire_times_key = self.fire_times_key();

        // Get all entries from sorted set with scores, sorted ascending
        // We need to find the first one whose schedule is Active
        let results: Vec<(String, f64)> = conn
            .zrangebyscore_withscores(&fire_times_key, f64::NEG_INFINITY, f64::INFINITY)
            .await
            .map_err(Self::map_err)?;

        for (id_str, score) in results {
            let key = self.key(&["schedule", &id_str]);
            let data: Option<String> = conn.get(&key).await.map_err(Self::map_err)?;
            if let Some(json) = data {
                let record: ScheduleRecord = Self::deserialize(&json)?;
                if record.state == ScheduleState::Active {
                    let ts_millis = score as i64;
                    let dt = DateTime::from_timestamp_millis(ts_millis)
                        .ok_or_else(|| {
                            SchedulerError::StoreError(format!(
                                "Invalid timestamp millis: {}",
                                ts_millis
                            ))
                        })?;
                    return Ok(Some(dt));
                }
            }
        }

        Ok(None)
    }

    async fn update_schedule(&self, schedule: &ScheduleRecord) -> Result<(), SchedulerError> {
        let mut conn = self.conn.clone();
        let schedule_key = self.schedule_key(&schedule.id);
        let fire_times_key = self.fire_times_key();
        let id_str = schedule.id.to_string();

        // Check existence
        let exists: bool = conn.exists(&schedule_key).await.map_err(Self::map_err)?;
        if !exists {
            return Err(SchedulerError::ScheduleNotFound(schedule.id.clone()));
        }

        let json = Self::serialize(schedule)?;

        let mut pipe = redis::pipe();
        pipe.atomic()
            .set(&schedule_key, &json)
            .zrem(&fire_times_key, &id_str);

        if let Some(next) = schedule.next_fire_time {
            pipe.zadd(&fire_times_key, &id_str, next.timestamp_millis() as f64);
        }

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(Self::map_err)?;

        Ok(())
    }

    async fn remove_schedule(&self, id: &ScheduleId) -> Result<bool, SchedulerError> {
        let mut conn = self.conn.clone();
        let schedule_key = self.schedule_key(id);
        let schedule_ids_key = self.schedule_ids_key();
        let fire_times_key = self.fire_times_key();
        let schedule_execs_key = self.schedule_execs_key(id);
        let id_str = id.to_string();

        let exists: bool = conn.exists(&schedule_key).await.map_err(Self::map_err)?;
        if !exists {
            return Ok(false);
        }

        // Get all execution IDs for this schedule to clean up
        let exec_ids: Vec<String> = conn
            .lrange(&schedule_execs_key, 0, -1)
            .await
            .map_err(Self::map_err)?;

        let mut pipe = redis::pipe();
        pipe.atomic()
            .del(&schedule_key)
            .srem(&schedule_ids_key, &id_str)
            .zrem(&fire_times_key, &id_str)
            .del(&schedule_execs_key);

        // Clean up each execution
        for exec_id in &exec_ids {
            let exec_key = self.key(&["execution", exec_id]);
            // Read the execution to find its state for state-set cleanup
            let data: Option<String> = conn.get(&exec_key).await.map_err(Self::map_err)?;
            if let Some(json) = data
                && let Ok(exec) = Self::deserialize::<Execution>(&json)
            {
                let state_key = self.exec_state_key(exec.state);
                pipe.srem(&state_key, exec_id);
            }
            pipe.del(&exec_key);
        }

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(Self::map_err)?;

        Ok(true)
    }

    async fn list_schedules(&self) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        let mut conn = self.conn.clone();
        let schedule_ids_key = self.schedule_ids_key();

        let ids: Vec<String> = conn.smembers(&schedule_ids_key).await.map_err(Self::map_err)?;

        let mut schedules = Vec::with_capacity(ids.len());
        for id_str in ids {
            let key = self.key(&["schedule", &id_str]);
            let data: Option<String> = conn.get(&key).await.map_err(Self::map_err)?;
            if let Some(json) = data {
                schedules.push(Self::deserialize(&json)?);
            }
        }

        Ok(schedules)
    }

    async fn add_execution(&self, execution: Execution) -> Result<(), SchedulerError> {
        let mut conn = self.conn.clone();
        let exec_key = self.execution_key(&execution.id);
        let schedule_execs_key = self.schedule_execs_key(&execution.schedule_id);
        let state_key = self.exec_state_key(execution.state);
        let exec_id_str = execution.id.to_string();

        let json = Self::serialize(&execution)?;

        redis::pipe()
            .atomic()
            .set(&exec_key, &json)
            .rpush(&schedule_execs_key, &exec_id_str)
            .sadd(&state_key, &exec_id_str)
            .query_async::<()>(&mut conn)
            .await
            .map_err(Self::map_err)?;

        Ok(())
    }

    async fn update_execution(&self, execution: &Execution) -> Result<(), SchedulerError> {
        let mut conn = self.conn.clone();
        let exec_key = self.execution_key(&execution.id);
        let exec_id_str = execution.id.to_string();

        // Get old execution to find previous state
        let old_data: Option<String> = conn.get(&exec_key).await.map_err(Self::map_err)?;
        let old_data = old_data.ok_or_else(|| {
            SchedulerError::StoreError(format!("Execution not found: {}", execution.id))
        })?;
        let old_exec: Execution = Self::deserialize(&old_data)?;

        let json = Self::serialize(execution)?;

        let mut pipe = redis::pipe();
        pipe.atomic().set(&exec_key, &json);

        // Update state sets if state changed
        if old_exec.state != execution.state {
            let old_state_key = self.exec_state_key(old_exec.state);
            let new_state_key = self.exec_state_key(execution.state);
            pipe.srem(&old_state_key, &exec_id_str)
                .sadd(&new_state_key, &exec_id_str);
        }

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(Self::map_err)?;

        Ok(())
    }

    async fn get_executions(
        &self,
        schedule_id: &ScheduleId,
    ) -> Result<Vec<Execution>, SchedulerError> {
        let mut conn = self.conn.clone();
        let schedule_execs_key = self.schedule_execs_key(schedule_id);

        let exec_ids: Vec<String> = conn
            .lrange(&schedule_execs_key, 0, -1)
            .await
            .map_err(Self::map_err)?;

        let mut executions = Vec::with_capacity(exec_ids.len());
        for exec_id in exec_ids {
            let key = self.key(&["execution", &exec_id]);
            let data: Option<String> = conn.get(&key).await.map_err(Self::map_err)?;
            if let Some(json) = data {
                executions.push(Self::deserialize(&json)?);
            }
        }

        Ok(executions)
    }

    async fn get_executions_by_state(
        &self,
        state: ExecutionState,
    ) -> Result<Vec<Execution>, SchedulerError> {
        let mut conn = self.conn.clone();
        let state_key = self.exec_state_key(state);

        let exec_ids: Vec<String> = conn.smembers(&state_key).await.map_err(Self::map_err)?;

        let mut executions = Vec::with_capacity(exec_ids.len());
        for exec_id in exec_ids {
            let key = self.key(&["execution", &exec_id]);
            let data: Option<String> = conn.get(&key).await.map_err(Self::map_err)?;
            if let Some(json) = data {
                executions.push(Self::deserialize(&json)?);
            }
        }

        Ok(executions)
    }

    async fn cleanup_executions_before(&self, before: DateTime<Utc>) -> Result<u64, SchedulerError> {
        let mut conn = self.conn.clone();
        let mut removed: u64 = 0;

        // Check terminal states for old executions
        let terminal_states = [
            ExecutionState::Succeeded,
            ExecutionState::Failed,
            ExecutionState::Missed,
            ExecutionState::Interrupted,
        ];

        for state in terminal_states {
            let state_key = self.exec_state_key(state);
            let exec_ids: Vec<String> = conn.smembers(&state_key).await.map_err(Self::map_err)?;

            for exec_id in exec_ids {
                let key = self.key(&["execution", &exec_id]);
                let data: Option<String> = conn.get(&key).await.map_err(Self::map_err)?;
                if let Some(json) = data {
                    let exec: Execution = Self::deserialize(&json)?;
                    if exec.finished_at.is_some_and(|f| f < before) {
                        let schedule_execs_key = self.schedule_execs_key(&exec.schedule_id);

                        redis::pipe()
                            .atomic()
                            .del(&key)
                            .srem(&state_key, &exec_id)
                            .lrem(&schedule_execs_key, 1, &exec_id)
                            .query_async::<()>(&mut conn)
                            .await
                            .map_err(Self::map_err)?;

                        removed += 1;
                    }
                }
            }
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::ExecutionId;
    use crate::schedule::{ScheduleConfig, ScheduleState};
    use chrono::Duration;

    async fn make_store() -> RedisStore {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let client = redis::Client::open(url).expect("Failed to create Redis client");
        let prefix = format!("test_scheduler_{}", uuid::Uuid::new_v4());
        RedisStore::with_prefix(client, prefix)
            .await
            .expect("Failed to connect to Redis")
    }

    async fn cleanup_store(store: &RedisStore) {
        let mut conn = store.conn.clone();
        let pattern = format!("{}:*", store.prefix);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .unwrap_or_default();
        if !keys.is_empty() {
            let _: () = redis::cmd("DEL")
                .arg(&keys)
                .query_async(&mut conn)
                .await
                .unwrap_or(());
        }
    }

    fn make_schedule(
        id: ScheduleId,
        next: Option<DateTime<Utc>>,
        state: ScheduleState,
    ) -> ScheduleRecord {
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
    #[ignore]
    async fn add_and_get_schedule() {
        let store = make_store().await;
        let id = ScheduleId::new();
        let sched = make_schedule(id.clone(), None, ScheduleState::Active);
        store.add_schedule(sched.clone()).await.unwrap();

        let got = store.get_schedule(&id).await.unwrap().unwrap();
        assert_eq!(got.id, id);

        // duplicate should error
        let result = store.add_schedule(sched).await;
        assert!(result.is_err());

        cleanup_store(&store).await;
    }

    #[tokio::test]
    #[ignore]
    async fn get_due_schedules_returns_only_due() {
        let store = make_store().await;
        let now = Utc::now();

        let id1 = ScheduleId::new();
        let s1 = make_schedule(
            id1.clone(),
            Some(now - Duration::seconds(10)),
            ScheduleState::Active,
        );
        store.add_schedule(s1).await.unwrap();

        let id2 = ScheduleId::new();
        let s2 = make_schedule(
            id2.clone(),
            Some(now + Duration::seconds(10)),
            ScheduleState::Active,
        );
        store.add_schedule(s2).await.unwrap();

        // paused schedule with past time should NOT be returned
        let id3 = ScheduleId::new();
        let s3 = make_schedule(
            id3.clone(),
            Some(now - Duration::seconds(5)),
            ScheduleState::Paused,
        );
        store.add_schedule(s3).await.unwrap();

        let due = store.get_due_schedules(&now).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, id1);

        cleanup_store(&store).await;
    }

    #[tokio::test]
    #[ignore]
    async fn get_next_fire_time_returns_earliest() {
        let store = make_store().await;
        let now = Utc::now();

        let t1 = now + Duration::seconds(100);
        let t2 = now + Duration::seconds(50);

        store
            .add_schedule(make_schedule(
                ScheduleId::new(),
                Some(t1),
                ScheduleState::Active,
            ))
            .await
            .unwrap();
        store
            .add_schedule(make_schedule(
                ScheduleId::new(),
                Some(t2),
                ScheduleState::Active,
            ))
            .await
            .unwrap();
        // paused should be ignored
        store
            .add_schedule(make_schedule(
                ScheduleId::new(),
                Some(now + Duration::seconds(10)),
                ScheduleState::Paused,
            ))
            .await
            .unwrap();

        let next = store.get_next_fire_time().await.unwrap().unwrap();
        // Compare at millisecond precision (Redis stores as millis)
        assert_eq!(next.timestamp_millis(), t2.timestamp_millis());

        cleanup_store(&store).await;
    }

    #[tokio::test]
    #[ignore]
    async fn update_schedule_works() {
        let store = make_store().await;
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

        cleanup_store(&store).await;
    }

    #[tokio::test]
    #[ignore]
    async fn remove_schedule_works() {
        let store = make_store().await;
        let id = ScheduleId::new();
        store
            .add_schedule(make_schedule(id.clone(), None, ScheduleState::Active))
            .await
            .unwrap();

        assert!(store.remove_schedule(&id).await.unwrap());
        assert!(!store.remove_schedule(&id).await.unwrap());
        assert!(store.get_schedule(&id).await.unwrap().is_none());

        cleanup_store(&store).await;
    }

    #[tokio::test]
    #[ignore]
    async fn execution_crud() {
        let store = make_store().await;
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

        cleanup_store(&store).await;
    }

    #[tokio::test]
    #[ignore]
    async fn get_executions_by_state_filtering() {
        let store = make_store().await;
        let sid = ScheduleId::new();

        store
            .add_execution(make_execution(
                sid.clone(),
                ExecutionState::Running,
                None,
            ))
            .await
            .unwrap();
        store
            .add_execution(make_execution(
                sid.clone(),
                ExecutionState::Succeeded,
                Some(Utc::now()),
            ))
            .await
            .unwrap();
        store
            .add_execution(make_execution(
                sid.clone(),
                ExecutionState::Running,
                None,
            ))
            .await
            .unwrap();

        let running = store
            .get_executions_by_state(ExecutionState::Running)
            .await
            .unwrap();
        assert_eq!(running.len(), 2);

        let succeeded = store
            .get_executions_by_state(ExecutionState::Succeeded)
            .await
            .unwrap();
        assert_eq!(succeeded.len(), 1);

        cleanup_store(&store).await;
    }

    #[tokio::test]
    #[ignore]
    async fn cleanup_executions_before_works() {
        let store = make_store().await;
        let sid = ScheduleId::new();
        let now = Utc::now();

        // old succeeded - should be cleaned
        store
            .add_execution(make_execution(
                sid.clone(),
                ExecutionState::Succeeded,
                Some(now - Duration::hours(2)),
            ))
            .await
            .unwrap();
        // old failed - should be cleaned
        store
            .add_execution(make_execution(
                sid.clone(),
                ExecutionState::Failed,
                Some(now - Duration::hours(2)),
            ))
            .await
            .unwrap();
        // recent succeeded - should remain
        store
            .add_execution(make_execution(
                sid.clone(),
                ExecutionState::Succeeded,
                Some(now),
            ))
            .await
            .unwrap();
        // running - should remain (not terminal)
        store
            .add_execution(make_execution(
                sid.clone(),
                ExecutionState::Running,
                None,
            ))
            .await
            .unwrap();

        let removed = store
            .cleanup_executions_before(now - Duration::hours(1))
            .await
            .unwrap();
        assert_eq!(removed, 2);

        let all = store.get_executions(&sid).await.unwrap();
        assert_eq!(all.len(), 2);

        cleanup_store(&store).await;
    }
}
