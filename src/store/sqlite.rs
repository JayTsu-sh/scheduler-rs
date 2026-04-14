use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use uuid::Uuid;

use crate::error::SchedulerError;
use crate::execution::{Execution, ExecutionId, ExecutionState};
use crate::schedule::{ScheduleConfig, ScheduleId, ScheduleRecord, ScheduleState};
use super::DataStore;

pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    pub async fn new(pool: SqlitePool) -> Result<Self, SchedulerError> {
        sqlx::query("PRAGMA foreign_keys = ON;")
            .execute(&pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS schedules (
                id TEXT PRIMARY KEY,
                name TEXT,
                type_name TEXT NOT NULL,
                task_data TEXT NOT NULL,
                state TEXT NOT NULL,
                next_fire_time TEXT,
                window_end_time TEXT,
                config TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                schedule_id TEXT NOT NULL,
                state TEXT NOT NULL,
                scheduled_fire_time TEXT NOT NULL,
                actual_fire_time TEXT,
                finished_at TEXT,
                error TEXT,
                FOREIGN KEY (schedule_id) REFERENCES schedules(id)
            );
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_schedules_next_fire ON schedules(next_fire_time);")
            .execute(&pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_schedules_state ON schedules(state);")
            .execute(&pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_executions_schedule ON executions(schedule_id);")
            .execute(&pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_executions_state ON executions(state);")
            .execute(&pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        Ok(Self { pool })
    }

    pub async fn in_memory() -> Result<Self, SchedulerError> {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
        Self::new(pool).await
    }
}

fn serialize_state<T: serde::Serialize>(state: &T) -> Result<String, SchedulerError> {
    serde_json::to_value(state)
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| SchedulerError::StoreError("Expected string enum variant".to_string()))
}

fn dt_to_string(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339()
}

fn opt_dt_to_string(dt: &Option<DateTime<Utc>>) -> Option<String> {
    dt.as_ref().map(|d| d.to_rfc3339())
}

fn parse_dt(s: &str) -> Result<DateTime<Utc>, SchedulerError> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| SchedulerError::StoreError(e.to_string()))
}

fn parse_opt_dt(s: Option<String>) -> Result<Option<DateTime<Utc>>, SchedulerError> {
    match s {
        Some(ref v) if !v.is_empty() => Ok(Some(parse_dt(v)?)),
        _ => Ok(None),
    }
}

fn row_to_schedule(row: &sqlx::sqlite::SqliteRow) -> Result<ScheduleRecord, SchedulerError> {
    let id_str: String = row.get("id");
    let id = ScheduleId(
        Uuid::parse_str(&id_str).map_err(|e| SchedulerError::StoreError(e.to_string()))?,
    );

    let name: Option<String> = row.get("name");
    let type_name: String = row.get("type_name");
    let task_data_str: String = row.get("task_data");
    let state_str: String = row.get("state");
    let next_fire_time_str: Option<String> = row.get("next_fire_time");
    let window_end_time_str: Option<String> = row.get("window_end_time");
    let config_str: String = row.get("config");
    let created_at_str: String = row.get("created_at");
    let updated_at_str: String = row.get("updated_at");

    let task_data: serde_json::Value = serde_json::from_str(&task_data_str)
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

    let state_json = format!("\"{}\"", state_str);
    let state: ScheduleState =
        serde_json::from_str(&state_json).map_err(|e| SchedulerError::StoreError(e.to_string()))?;

    let next_fire_time = parse_opt_dt(next_fire_time_str)?;
    let window_end_time = parse_opt_dt(window_end_time_str)?;

    let config: ScheduleConfig =
        serde_json::from_str(&config_str).map_err(|e| SchedulerError::StoreError(e.to_string()))?;

    let created_at = parse_dt(&created_at_str)?;
    let updated_at = parse_dt(&updated_at_str)?;

    Ok(ScheduleRecord {
        id,
        name,
        type_name,
        task_data,
        state,
        next_fire_time,
        window_end_time,
        config,
        created_at,
        updated_at,
    })
}

fn row_to_execution(row: &sqlx::sqlite::SqliteRow) -> Result<Execution, SchedulerError> {
    let id_str: String = row.get("id");
    let id = ExecutionId(
        Uuid::parse_str(&id_str).map_err(|e| SchedulerError::StoreError(e.to_string()))?,
    );

    let schedule_id_str: String = row.get("schedule_id");
    let schedule_id = ScheduleId(
        Uuid::parse_str(&schedule_id_str).map_err(|e| SchedulerError::StoreError(e.to_string()))?,
    );

    let state_str: String = row.get("state");
    let state_json = format!("\"{}\"", state_str);
    let state: ExecutionState =
        serde_json::from_str(&state_json).map_err(|e| SchedulerError::StoreError(e.to_string()))?;

    let scheduled_fire_time_str: String = row.get("scheduled_fire_time");
    let scheduled_fire_time = parse_dt(&scheduled_fire_time_str)?;

    let actual_fire_time_str: Option<String> = row.get("actual_fire_time");
    let actual_fire_time = parse_opt_dt(actual_fire_time_str)?;

    let finished_at_str: Option<String> = row.get("finished_at");
    let finished_at = parse_opt_dt(finished_at_str)?;

    let error: Option<String> = row.get("error");

    Ok(Execution {
        id,
        schedule_id,
        state,
        scheduled_fire_time,
        actual_fire_time,
        finished_at,
        error,
    })
}

#[async_trait]
impl DataStore for SqliteStore {
    async fn add_schedule(&self, schedule: ScheduleRecord) -> Result<(), SchedulerError> {
        let id = schedule.id.0.to_string();
        let state = serialize_state(&schedule.state)?;
        let next_fire_time = opt_dt_to_string(&schedule.next_fire_time);
        let window_end_time = opt_dt_to_string(&schedule.window_end_time);
        let config =
            serde_json::to_string(&schedule.config).map_err(|e| SchedulerError::StoreError(e.to_string()))?;
        let task_data = schedule.task_data.to_string();
        let created_at = dt_to_string(&schedule.created_at);
        let updated_at = dt_to_string(&schedule.updated_at);

        sqlx::query(
            "INSERT INTO schedules (id, name, type_name, task_data, state, next_fire_time, window_end_time, config, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&schedule.name)
        .bind(&schedule.type_name)
        .bind(&task_data)
        .bind(&state)
        .bind(&next_fire_time)
        .bind(&window_end_time)
        .bind(&config)
        .bind(&created_at)
        .bind(&updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        Ok(())
    }

    async fn get_schedule(&self, id: &ScheduleId) -> Result<Option<ScheduleRecord>, SchedulerError> {
        let id_str = id.0.to_string();
        let row = sqlx::query("SELECT * FROM schedules WHERE id = ?")
            .bind(&id_str)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        match row {
            Some(ref r) => Ok(Some(row_to_schedule(r)?)),
            None => Ok(None),
        }
    }

    async fn get_due_schedules(&self, now: &DateTime<Utc>) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        let now_str = dt_to_string(now);
        let rows = sqlx::query(
            "SELECT * FROM schedules WHERE state = 'Active' AND next_fire_time IS NOT NULL AND next_fire_time <= ? ORDER BY next_fire_time ASC",
        )
        .bind(&now_str)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        rows.iter().map(row_to_schedule).collect()
    }

    async fn get_next_fire_time(&self) -> Result<Option<DateTime<Utc>>, SchedulerError> {
        let row = sqlx::query(
            "SELECT MIN(next_fire_time) as min_time FROM schedules WHERE state = 'Active' AND next_fire_time IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        let min_str: Option<String> = row.get("min_time");
        parse_opt_dt(min_str)
    }

    async fn update_schedule(&self, schedule: &ScheduleRecord) -> Result<(), SchedulerError> {
        let id = schedule.id.0.to_string();
        let state = serialize_state(&schedule.state)?;
        let next_fire_time = opt_dt_to_string(&schedule.next_fire_time);
        let window_end_time = opt_dt_to_string(&schedule.window_end_time);
        let config =
            serde_json::to_string(&schedule.config).map_err(|e| SchedulerError::StoreError(e.to_string()))?;
        let task_data = schedule.task_data.to_string();
        let updated_at = dt_to_string(&schedule.updated_at);

        let result = sqlx::query(
            "UPDATE schedules SET name = ?, type_name = ?, task_data = ?, state = ?, next_fire_time = ?, window_end_time = ?, config = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&schedule.name)
        .bind(&schedule.type_name)
        .bind(&task_data)
        .bind(&state)
        .bind(&next_fire_time)
        .bind(&window_end_time)
        .bind(&config)
        .bind(&updated_at)
        .bind(&id)
        .execute(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(SchedulerError::ScheduleNotFound(schedule.id.clone()));
        }

        Ok(())
    }

    async fn remove_schedule(&self, id: &ScheduleId) -> Result<bool, SchedulerError> {
        let id_str = id.0.to_string();
        let result = sqlx::query("DELETE FROM schedules WHERE id = ?")
            .bind(&id_str)
            .execute(&self.pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        Ok(result.rows_affected() > 0)
    }

    async fn list_schedules(&self) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        let rows = sqlx::query("SELECT * FROM schedules")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        rows.iter().map(row_to_schedule).collect()
    }

    async fn add_execution(&self, execution: Execution) -> Result<(), SchedulerError> {
        let id = execution.id.0.to_string();
        let schedule_id = execution.schedule_id.0.to_string();
        let state = serialize_state(&execution.state)?;
        let scheduled_fire_time = dt_to_string(&execution.scheduled_fire_time);
        let actual_fire_time = opt_dt_to_string(&execution.actual_fire_time);
        let finished_at = opt_dt_to_string(&execution.finished_at);

        sqlx::query(
            "INSERT INTO executions (id, schedule_id, state, scheduled_fire_time, actual_fire_time, finished_at, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&schedule_id)
        .bind(&state)
        .bind(&scheduled_fire_time)
        .bind(&actual_fire_time)
        .bind(&finished_at)
        .bind(&execution.error)
        .execute(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        Ok(())
    }

    async fn update_execution(&self, execution: &Execution) -> Result<(), SchedulerError> {
        let id = execution.id.0.to_string();
        let state = serialize_state(&execution.state)?;
        let actual_fire_time = opt_dt_to_string(&execution.actual_fire_time);
        let finished_at = opt_dt_to_string(&execution.finished_at);

        let result = sqlx::query(
            "UPDATE executions SET state = ?, actual_fire_time = ?, finished_at = ?, error = ? WHERE id = ?",
        )
        .bind(&state)
        .bind(&actual_fire_time)
        .bind(&finished_at)
        .bind(&execution.error)
        .bind(&id)
        .execute(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(SchedulerError::StoreError(format!(
                "Execution not found: {}",
                execution.id
            )));
        }

        Ok(())
    }

    async fn get_executions(&self, schedule_id: &ScheduleId) -> Result<Vec<Execution>, SchedulerError> {
        let id_str = schedule_id.0.to_string();
        let rows = sqlx::query("SELECT * FROM executions WHERE schedule_id = ?")
            .bind(&id_str)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        rows.iter().map(row_to_execution).collect()
    }

    async fn get_executions_by_state(&self, state: ExecutionState) -> Result<Vec<Execution>, SchedulerError> {
        let state_str = serialize_state(&state)?;
        let rows = sqlx::query("SELECT * FROM executions WHERE state = ?")
            .bind(&state_str)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        rows.iter().map(row_to_execution).collect()
    }

    async fn cleanup_executions_before(&self, before: DateTime<Utc>) -> Result<u64, SchedulerError> {
        let before_str = dt_to_string(&before);
        let result = sqlx::query(
            "DELETE FROM executions WHERE state IN ('Succeeded', 'Failed', 'Missed', 'Interrupted') AND finished_at IS NOT NULL AND finished_at < ?",
        )
        .bind(&before_str)
        .execute(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        Ok(result.rows_affected())
    }
}

#[cfg(test)]
#[cfg(feature = "sqlx-sqlite")]
mod tests {
    use super::*;
    use crate::execution::ExecutionId;
    use crate::schedule::{ScheduleConfig, ScheduleState};
    use chrono::Duration;

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
    async fn add_and_get_schedule() {
        let store = SqliteStore::in_memory().await.unwrap();
        let id = ScheduleId::new();
        let sched = make_schedule(id.clone(), None, ScheduleState::Active);
        store.add_schedule(sched.clone()).await.unwrap();

        let got = store.get_schedule(&id).await.unwrap().unwrap();
        assert_eq!(got.id, id);
        assert_eq!(got.type_name, "test");

        // duplicate should error
        let result = store.add_schedule(sched).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_schedule_not_found() {
        let store = SqliteStore::in_memory().await.unwrap();
        let result = store.get_schedule(&ScheduleId::new()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn get_due_schedules_returns_only_due() {
        let store = SqliteStore::in_memory().await.unwrap();
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
    }

    #[tokio::test]
    async fn get_next_fire_time_returns_earliest() {
        let store = SqliteStore::in_memory().await.unwrap();
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
        assert_eq!(next, t2);
    }

    #[tokio::test]
    async fn get_next_fire_time_empty() {
        let store = SqliteStore::in_memory().await.unwrap();
        let result = store.get_next_fire_time().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn update_schedule_works() {
        let store = SqliteStore::in_memory().await.unwrap();
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
        let store = SqliteStore::in_memory().await.unwrap();
        let id = ScheduleId::new();
        store
            .add_schedule(make_schedule(id.clone(), None, ScheduleState::Active))
            .await
            .unwrap();

        assert!(store.remove_schedule(&id).await.unwrap());
        assert!(!store.remove_schedule(&id).await.unwrap());
        assert!(store.get_schedule(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_schedules_works() {
        let store = SqliteStore::in_memory().await.unwrap();
        store
            .add_schedule(make_schedule(
                ScheduleId::new(),
                None,
                ScheduleState::Active,
            ))
            .await
            .unwrap();
        store
            .add_schedule(make_schedule(
                ScheduleId::new(),
                None,
                ScheduleState::Paused,
            ))
            .await
            .unwrap();

        let all = store.list_schedules().await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn execution_crud() {
        let store = SqliteStore::in_memory().await.unwrap();
        let sid = ScheduleId::new();
        store
            .add_schedule(make_schedule(sid.clone(), None, ScheduleState::Active))
            .await
            .unwrap();

        let mut exec = make_execution(sid.clone(), ExecutionState::Running, None);
        store.add_execution(exec.clone()).await.unwrap();

        let execs = store.get_executions(&sid).await.unwrap();
        assert_eq!(execs.len(), 1);
        assert_eq!(execs[0].state, ExecutionState::Running);

        exec.state = ExecutionState::Succeeded;
        exec.finished_at = Some(Utc::now());
        store.update_execution(&exec).await.unwrap();

        let execs = store.get_executions(&sid).await.unwrap();
        assert_eq!(execs[0].state, ExecutionState::Succeeded);
        assert!(execs[0].finished_at.is_some());
    }

    #[tokio::test]
    async fn update_execution_not_found() {
        let store = SqliteStore::in_memory().await.unwrap();
        let exec = make_execution(ScheduleId::new(), ExecutionState::Running, None);
        // Note: FK constraint may cause error too, but we just check it's an error
        let result = store.update_execution(&exec).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_executions_by_state_filtering() {
        let store = SqliteStore::in_memory().await.unwrap();
        let sid = ScheduleId::new();
        store
            .add_schedule(make_schedule(sid.clone(), None, ScheduleState::Active))
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
    }

    #[tokio::test]
    async fn cleanup_executions_before_works() {
        let store = SqliteStore::in_memory().await.unwrap();
        let sid = ScheduleId::new();
        let now = Utc::now();
        store
            .add_schedule(make_schedule(sid.clone(), None, ScheduleState::Active))
            .await
            .unwrap();

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
    }
}
