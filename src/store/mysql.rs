use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::mysql::MySqlRow;
use sqlx::{MySqlPool, Row};
use uuid::Uuid;

use crate::error::SchedulerError;
use crate::execution::{Execution, ExecutionId, ExecutionState};
use crate::schedule::{ScheduleId, ScheduleRecord, ScheduleState};
use super::DataStore;

pub struct MysqlStore {
    pool: MySqlPool,
}

impl MysqlStore {
    pub async fn new(pool: MySqlPool) -> Result<Self, SchedulerError> {
        Self::migrate(&pool).await?;
        Ok(Self { pool })
    }

    async fn migrate(pool: &MySqlPool) -> Result<(), SchedulerError> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS schedules (
                id VARCHAR(36) PRIMARY KEY,
                name VARCHAR(255),
                type_name VARCHAR(255) NOT NULL,
                task_data JSON NOT NULL,
                state VARCHAR(20) NOT NULL,
                next_fire_time DATETIME(6),
                window_end_time DATETIME(6),
                config JSON NOT NULL,
                created_at DATETIME(6) NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                INDEX idx_next_fire (next_fire_time),
                INDEX idx_state (state)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#,
        )
        .execute(pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS executions (
                id VARCHAR(36) PRIMARY KEY,
                schedule_id VARCHAR(36) NOT NULL,
                state VARCHAR(20) NOT NULL,
                scheduled_fire_time DATETIME(6) NOT NULL,
                actual_fire_time DATETIME(6),
                finished_at DATETIME(6),
                error TEXT,
                INDEX idx_schedule (schedule_id),
                INDEX idx_exec_state (state),
                FOREIGN KEY (schedule_id) REFERENCES schedules(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#,
        )
        .execute(pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        Ok(())
    }
}

fn schedule_state_to_str(state: ScheduleState) -> &'static str {
    match state {
        ScheduleState::Active => "Active",
        ScheduleState::Paused => "Paused",
        ScheduleState::Completed => "Completed",
    }
}

fn schedule_state_from_str(s: &str) -> Result<ScheduleState, SchedulerError> {
    match s {
        "Active" => Ok(ScheduleState::Active),
        "Paused" => Ok(ScheduleState::Paused),
        "Completed" => Ok(ScheduleState::Completed),
        other => Err(SchedulerError::StoreError(format!(
            "Unknown schedule state: {other}"
        ))),
    }
}

fn execution_state_to_str(state: ExecutionState) -> &'static str {
    match state {
        ExecutionState::Scheduled => "Scheduled",
        ExecutionState::Running => "Running",
        ExecutionState::Succeeded => "Succeeded",
        ExecutionState::Failed => "Failed",
        ExecutionState::Missed => "Missed",
        ExecutionState::Interrupted => "Interrupted",
    }
}

fn execution_state_from_str(s: &str) -> Result<ExecutionState, SchedulerError> {
    match s {
        "Scheduled" => Ok(ExecutionState::Scheduled),
        "Running" => Ok(ExecutionState::Running),
        "Succeeded" => Ok(ExecutionState::Succeeded),
        "Failed" => Ok(ExecutionState::Failed),
        "Missed" => Ok(ExecutionState::Missed),
        "Interrupted" => Ok(ExecutionState::Interrupted),
        other => Err(SchedulerError::StoreError(format!(
            "Unknown execution state: {other}"
        ))),
    }
}

fn parse_uuid(s: &str) -> Result<Uuid, SchedulerError> {
    Uuid::parse_str(s).map_err(|e| SchedulerError::StoreError(e.to_string()))
}

fn schedule_from_row(row: &MySqlRow) -> Result<ScheduleRecord, SchedulerError> {
    let id_str: String = row
        .try_get("id")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let name: Option<String> = row
        .try_get("name")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let type_name: String = row
        .try_get("type_name")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    // MariaDB reports JSON/LONGTEXT as BLOB — read as bytes and convert.
    let task_data_str: String = row
        .try_get::<String, _>("task_data")
        .or_else(|_| {
            row.try_get::<Vec<u8>, _>("task_data")
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        })
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let state_str: String = row
        .try_get("state")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let next_fire_time: Option<DateTime<Utc>> = row
        .try_get("next_fire_time")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let window_end_time: Option<DateTime<Utc>> = row
        .try_get("window_end_time")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    // MariaDB reports JSON/LONGTEXT as BLOB — read as bytes and convert.
    let config_str: String = row
        .try_get::<String, _>("config")
        .or_else(|_| {
            row.try_get::<Vec<u8>, _>("config")
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        })
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let created_at: DateTime<Utc> = row
        .try_get("created_at")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let updated_at: DateTime<Utc> = row
        .try_get("updated_at")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

    Ok(ScheduleRecord {
        id: ScheduleId(parse_uuid(&id_str)?),
        name,
        type_name,
        task_data: serde_json::from_str(&task_data_str)
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?,
        state: schedule_state_from_str(&state_str)?,
        next_fire_time,
        window_end_time,
        config: serde_json::from_str(&config_str)
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?,
        created_at,
        updated_at,
    })
}

fn execution_from_row(row: &MySqlRow) -> Result<Execution, SchedulerError> {
    let id_str: String = row
        .try_get("id")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let schedule_id_str: String = row
        .try_get("schedule_id")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let state_str: String = row
        .try_get("state")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let scheduled_fire_time: DateTime<Utc> = row
        .try_get("scheduled_fire_time")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let actual_fire_time: Option<DateTime<Utc>> = row
        .try_get("actual_fire_time")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let finished_at: Option<DateTime<Utc>> = row
        .try_get("finished_at")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;
    let error: Option<String> = row
        .try_get("error")
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

    Ok(Execution {
        id: ExecutionId(parse_uuid(&id_str)?),
        schedule_id: ScheduleId(parse_uuid(&schedule_id_str)?),
        state: execution_state_from_str(&state_str)?,
        scheduled_fire_time,
        actual_fire_time,
        finished_at,
        error,
    })
}

#[async_trait]
impl DataStore for MysqlStore {
    async fn add_schedule(&self, schedule: ScheduleRecord) -> Result<(), SchedulerError> {
        let task_data_str = schedule.task_data.to_string();
        let config_str = serde_json::to_string(&schedule.config)
            .map_err(|e| SchedulerError::SerializationError(e.to_string()))?;

        sqlx::query(
            "INSERT INTO schedules (id, name, type_name, task_data, state, next_fire_time, window_end_time, config, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(schedule.id.0.to_string())
        .bind(&schedule.name)
        .bind(&schedule.type_name)
        .bind(&task_data_str)
        .bind(schedule_state_to_str(schedule.state))
        .bind(schedule.next_fire_time)
        .bind(schedule.window_end_time)
        .bind(&config_str)
        .bind(schedule.created_at)
        .bind(schedule.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        Ok(())
    }

    async fn get_schedule(
        &self,
        id: &ScheduleId,
    ) -> Result<Option<ScheduleRecord>, SchedulerError> {
        let row = sqlx::query("SELECT * FROM schedules WHERE id = ?")
            .bind(id.0.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        match row {
            Some(ref r) => Ok(Some(schedule_from_row(r)?)),
            None => Ok(None),
        }
    }

    async fn get_due_schedules(
        &self,
        now: &DateTime<Utc>,
    ) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        let rows = sqlx::query(
            "SELECT * FROM schedules WHERE state = 'Active' AND next_fire_time IS NOT NULL AND next_fire_time <= ? ORDER BY next_fire_time ASC",
        )
        .bind(*now)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        rows.iter().map(schedule_from_row).collect()
    }

    async fn get_next_fire_time(&self) -> Result<Option<DateTime<Utc>>, SchedulerError> {
        let row = sqlx::query(
            "SELECT MIN(next_fire_time) as min_time FROM schedules WHERE state = 'Active' AND next_fire_time IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        let min_time: Option<DateTime<Utc>> = row
            .try_get("min_time")
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        Ok(min_time)
    }

    async fn update_schedule(&self, schedule: &ScheduleRecord) -> Result<(), SchedulerError> {
        let task_data_str = schedule.task_data.to_string();
        let config_str = serde_json::to_string(&schedule.config)
            .map_err(|e| SchedulerError::SerializationError(e.to_string()))?;

        let result = sqlx::query(
            "UPDATE schedules SET name = ?, type_name = ?, task_data = ?, state = ?, next_fire_time = ?, window_end_time = ?, config = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&schedule.name)
        .bind(&schedule.type_name)
        .bind(&task_data_str)
        .bind(schedule_state_to_str(schedule.state))
        .bind(schedule.next_fire_time)
        .bind(schedule.window_end_time)
        .bind(&config_str)
        .bind(schedule.updated_at)
        .bind(schedule.id.0.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(SchedulerError::ScheduleNotFound(schedule.id.clone()));
        }

        Ok(())
    }

    async fn remove_schedule(&self, id: &ScheduleId) -> Result<bool, SchedulerError> {
        let result = sqlx::query("DELETE FROM schedules WHERE id = ?")
            .bind(id.0.to_string())
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

        rows.iter().map(schedule_from_row).collect()
    }

    async fn list_schedules_by_name_prefix(&self, prefix: &str) -> Result<Vec<ScheduleRecord>, SchedulerError> {
        let pattern = format!("{}%", prefix.replace('%', "\\%").replace('_', "\\_"));
        let rows = sqlx::query("SELECT * FROM schedules WHERE name LIKE ?")
            .bind(&pattern)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        rows.iter().map(schedule_from_row).collect()
    }

    async fn add_execution(&self, execution: Execution) -> Result<(), SchedulerError> {
        sqlx::query(
            "INSERT INTO executions (id, schedule_id, state, scheduled_fire_time, actual_fire_time, finished_at, error)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(execution.id.0.to_string())
        .bind(execution.schedule_id.0.to_string())
        .bind(execution_state_to_str(execution.state))
        .bind(execution.scheduled_fire_time)
        .bind(execution.actual_fire_time)
        .bind(execution.finished_at)
        .bind(&execution.error)
        .execute(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        Ok(())
    }

    async fn update_execution(&self, execution: &Execution) -> Result<(), SchedulerError> {
        let result = sqlx::query(
            "UPDATE executions SET state = ?, scheduled_fire_time = ?, actual_fire_time = ?, finished_at = ?, error = ? WHERE id = ?",
        )
        .bind(execution_state_to_str(execution.state))
        .bind(execution.scheduled_fire_time)
        .bind(execution.actual_fire_time)
        .bind(execution.finished_at)
        .bind(&execution.error)
        .bind(execution.id.0.to_string())
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

    async fn get_executions(
        &self,
        schedule_id: &ScheduleId,
    ) -> Result<Vec<Execution>, SchedulerError> {
        let rows = sqlx::query("SELECT * FROM executions WHERE schedule_id = ?")
            .bind(schedule_id.0.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        rows.iter().map(execution_from_row).collect()
    }

    async fn get_executions_by_state(
        &self,
        state: ExecutionState,
    ) -> Result<Vec<Execution>, SchedulerError> {
        let rows = sqlx::query("SELECT * FROM executions WHERE state = ?")
            .bind(execution_state_to_str(state))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        rows.iter().map(execution_from_row).collect()
    }

    async fn cleanup_executions_before(
        &self,
        before: DateTime<Utc>,
    ) -> Result<u64, SchedulerError> {
        let result = sqlx::query(
            "DELETE FROM executions WHERE state IN ('Succeeded', 'Failed', 'Missed', 'Interrupted') AND finished_at IS NOT NULL AND finished_at < ?",
        )
        .bind(before)
        .execute(&self.pool)
        .await
        .map_err(|e| SchedulerError::StoreError(e.to_string()))?;

        Ok(result.rows_affected())
    }
}

#[cfg(all(test, feature = "sqlx-mysql"))]
mod tests {
    use super::*;
    use crate::schedule::{ScheduleConfig, ScheduleState};
    use chrono::Duration;

    async fn create_store() -> MysqlStore {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/scheduler_test".to_string());
        let pool = MySqlPool::connect(&url).await.expect("Failed to connect to MySQL");

        // Clean up tables for a fresh test run
        sqlx::query("DROP TABLE IF EXISTS executions")
            .execute(&pool)
            .await
            .expect("Failed to drop executions");
        sqlx::query("DROP TABLE IF EXISTS schedules")
            .execute(&pool)
            .await
            .expect("Failed to drop schedules");

        MysqlStore::new(pool).await.expect("Failed to create MysqlStore")
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
    async fn mysql_add_and_get_schedule() {
        let store = create_store().await;
        let id = ScheduleId::new();
        let sched = make_schedule(id.clone(), None, ScheduleState::Active);
        store.add_schedule(sched.clone()).await.unwrap();

        let got = store.get_schedule(&id).await.unwrap().unwrap();
        assert_eq!(got.id, id);
        assert_eq!(got.type_name, "test");
    }

    #[tokio::test]
    #[ignore]
    async fn mysql_get_due_schedules() {
        let store = create_store().await;
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
    #[ignore]
    async fn mysql_get_next_fire_time() {
        let store = create_store().await;
        let now = Utc::now();

        let t1 = now + Duration::seconds(100);
        let t2 = now + Duration::seconds(50);

        store
            .add_schedule(make_schedule(ScheduleId::new(), Some(t1), ScheduleState::Active))
            .await
            .unwrap();
        store
            .add_schedule(make_schedule(ScheduleId::new(), Some(t2), ScheduleState::Active))
            .await
            .unwrap();

        let next = store.get_next_fire_time().await.unwrap().unwrap();
        // Compare with microsecond tolerance (MySQL DATETIME(6) precision)
        let diff = (next - t2).num_microseconds().unwrap_or(i64::MAX).abs();
        assert!(diff < 1000, "Times should be within 1ms: diff={diff}us");
    }

    #[tokio::test]
    #[ignore]
    async fn mysql_update_schedule() {
        let store = create_store().await;
        let id = ScheduleId::new();
        let mut sched = make_schedule(id.clone(), None, ScheduleState::Active);
        store.add_schedule(sched.clone()).await.unwrap();

        sched.state = ScheduleState::Paused;
        store.update_schedule(&sched).await.unwrap();

        let got = store.get_schedule(&id).await.unwrap().unwrap();
        assert_eq!(got.state, ScheduleState::Paused);
    }

    #[tokio::test]
    #[ignore]
    async fn mysql_remove_schedule() {
        let store = create_store().await;
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
    #[ignore]
    async fn mysql_execution_crud() {
        let store = create_store().await;
        let sid = ScheduleId::new();

        // Need a schedule first due to FK constraint
        store
            .add_schedule(make_schedule(sid.clone(), None, ScheduleState::Active))
            .await
            .unwrap();

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
    #[ignore]
    async fn mysql_get_executions_by_state() {
        let store = create_store().await;
        let sid = ScheduleId::new();

        store
            .add_schedule(make_schedule(sid.clone(), None, ScheduleState::Active))
            .await
            .unwrap();

        store
            .add_execution(make_execution(sid.clone(), ExecutionState::Running, None))
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
            .add_execution(make_execution(sid.clone(), ExecutionState::Running, None))
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
    #[ignore]
    async fn mysql_cleanup_executions() {
        let store = create_store().await;
        let sid = ScheduleId::new();
        let now = Utc::now();

        store
            .add_schedule(make_schedule(sid.clone(), None, ScheduleState::Active))
            .await
            .unwrap();

        store
            .add_execution(make_execution(
                sid.clone(),
                ExecutionState::Succeeded,
                Some(now - Duration::hours(2)),
            ))
            .await
            .unwrap();
        store
            .add_execution(make_execution(
                sid.clone(),
                ExecutionState::Failed,
                Some(now - Duration::hours(2)),
            ))
            .await
            .unwrap();
        store
            .add_execution(make_execution(
                sid.clone(),
                ExecutionState::Succeeded,
                Some(now),
            ))
            .await
            .unwrap();
        store
            .add_execution(make_execution(sid.clone(), ExecutionState::Running, None))
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
