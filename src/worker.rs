use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::execution::ExecutionId;
use crate::executor::remote::{CancelRequest, TaskDispatchRequest, TaskOutcome, TaskResultReport};
use crate::middleware::TaskRunner;
use crate::task::{Task, TaskContext};

/// Transport-agnostic worker that receives dispatched tasks, executes them,
/// and returns result reports.
///
/// Transport layers (HTTP, gRPC, message queues) call [`Worker::execute`] and
/// [`Worker::cancel`] to drive task execution.
pub struct Worker {
    runners: RwLock<HashMap<String, TaskRunner>>,
    tokens: RwLock<HashMap<ExecutionId, CancellationToken>>,
}

impl Worker {
    pub fn new() -> Self {
        Self {
            runners: RwLock::new(HashMap::new()),
            tokens: RwLock::new(HashMap::new()),
        }
    }

    /// Register a task type so the worker can deserialize and execute it.
    pub async fn register<T: Task>(&self) {
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

    /// Execute a dispatched task request. Returns the result report.
    ///
    /// This is the main entry point called by transport layers.
    pub async fn execute(&self, request: TaskDispatchRequest) -> TaskResultReport {
        let runner = {
            let runners = self.runners.read().await;
            runners.get(&request.type_name).cloned()
        };

        let runner = match runner {
            Some(r) => r,
            None => {
                return TaskResultReport {
                    execution_id: request.execution_id,
                    schedule_id: request.schedule_id,
                    outcome: TaskOutcome::Failed {
                        error: format!("Task type not registered: {}", request.type_name),
                    },
                };
            }
        };

        let token = CancellationToken::new();
        self.tokens
            .write()
            .await
            .insert(request.execution_id.clone(), token.clone());

        let ctx = TaskContext {
            schedule_id: request.schedule_id.clone(),
            execution_id: request.execution_id.clone(),
            cancellation_token: token,
        };

        let outcome = match runner(request.task_data, ctx).await {
            Ok(()) => TaskOutcome::Succeeded,
            Err(e) => TaskOutcome::Failed {
                error: e.to_string(),
            },
        };

        self.tokens.write().await.remove(&request.execution_id);

        TaskResultReport {
            execution_id: request.execution_id,
            schedule_id: request.schedule_id,
            outcome,
        }
    }

    /// Cancel a running task. Best-effort — the task may already be complete.
    pub fn cancel(&self, request: &CancelRequest) {
        // Use try_read to avoid blocking; if we can't get the lock the task
        // is likely being set up or torn down and the token will be short-lived.
        if let Ok(tokens) = self.tokens.try_read()
            && let Some(token) = tokens.get(&request.execution_id)
        {
            token.cancel();
        }
    }
}

impl Default for Worker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use crate::execution::ExecutionId;
    use crate::schedule::ScheduleId;

    // -- helpers ---------------------------------------------------------------

    fn make_schedule_id() -> ScheduleId {
        ScheduleId(Uuid::new_v4())
    }

    fn make_execution_id() -> ExecutionId {
        ExecutionId(Uuid::new_v4())
    }

    // -- test task types -------------------------------------------------------

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct GreetTask {
        name: String,
    }

    #[async_trait]
    impl Task for GreetTask {
        const TYPE_NAME: &'static str = "greet";

        async fn run(&self, _ctx: &TaskContext) -> crate::task::TaskResult {
            // Simulate work; just succeed.
            Ok(())
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct FailingTask;

    #[async_trait]
    impl Task for FailingTask {
        const TYPE_NAME: &'static str = "failing";

        async fn run(&self, _ctx: &TaskContext) -> crate::task::TaskResult {
            Err(SchedulerError::ExecutionError("intentional failure".into()))
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SlowTask;

    #[async_trait]
    impl Task for SlowTask {
        const TYPE_NAME: &'static str = "slow";

        async fn run(&self, ctx: &TaskContext) -> crate::task::TaskResult {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    Ok(())
                }
                _ = ctx.cancellation_token.cancelled() => {
                    Err(SchedulerError::ExecutionError("cancelled".into()))
                }
            }
        }
    }

    // -- tests -----------------------------------------------------------------

    #[tokio::test]
    async fn test_register_and_execute() {
        let worker = Worker::new();
        worker.register::<GreetTask>().await;

        let request = TaskDispatchRequest {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            type_name: "greet".into(),
            task_data: serde_json::json!({ "name": "world" }),
        };

        let report = worker.execute(request).await;
        assert_eq!(report.outcome, TaskOutcome::Succeeded);
    }

    #[tokio::test]
    async fn test_execute_unknown_type() {
        let worker = Worker::new();

        let request = TaskDispatchRequest {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            type_name: "nonexistent".into(),
            task_data: serde_json::Value::Null,
        };

        let report = worker.execute(request).await;
        match &report.outcome {
            TaskOutcome::Failed { error } => {
                assert!(error.contains("Task type not registered"));
                assert!(error.contains("nonexistent"));
            }
            _ => panic!("expected Failed outcome"),
        }
    }

    #[tokio::test]
    async fn test_cancel() {
        let worker = Arc::new(Worker::new());
        worker.register::<SlowTask>().await;

        let eid = make_execution_id();
        let sid = make_schedule_id();

        let request = TaskDispatchRequest {
            execution_id: eid.clone(),
            schedule_id: sid.clone(),
            type_name: "slow".into(),
            task_data: serde_json::json!(null),
        };

        let worker2 = worker.clone();
        let eid2 = eid.clone();
        let sid2 = sid.clone();

        let handle = tokio::spawn(async move { worker2.execute(request).await });

        // Give the task time to start and register its token.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        worker.cancel(&CancelRequest {
            execution_id: eid2,
            schedule_id: sid2,
        });

        let report = handle.await.unwrap();
        match &report.outcome {
            TaskOutcome::Failed { error } => {
                assert!(error.contains("cancelled"));
            }
            _ => panic!("expected Failed outcome after cancellation"),
        }

        // Token should be cleaned up.
        assert!(worker.tokens.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_execute_failing_task() {
        let worker = Worker::new();
        worker.register::<FailingTask>().await;

        let request = TaskDispatchRequest {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            type_name: "failing".into(),
            task_data: serde_json::json!(null),
        };

        let report = worker.execute(request).await;
        match &report.outcome {
            TaskOutcome::Failed { error } => {
                assert!(error.contains("intentional failure"));
            }
            _ => panic!("expected Failed outcome"),
        }
    }
}
