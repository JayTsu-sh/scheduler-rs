use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::SchedulerError;
use crate::execution::ExecutionId;
use crate::schedule::ScheduleId;

use super::{BoxedTaskFn, DispatchableExecutor, Executor};

// ---------------------------------------------------------------------------
// Wire‐protocol message types
// ---------------------------------------------------------------------------

/// Request sent to a remote worker to execute a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDispatchRequest {
    pub execution_id: ExecutionId,
    pub schedule_id: ScheduleId,
    pub type_name: String,
    pub task_data: serde_json::Value,
}

/// The outcome of a remotely‐executed task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskOutcome {
    Succeeded,
    Failed { error: String },
}

/// Report sent back from a remote worker after task completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResultReport {
    pub execution_id: ExecutionId,
    pub schedule_id: ScheduleId,
    pub outcome: TaskOutcome,
}

/// Request to cancel a running remote task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelRequest {
    pub execution_id: ExecutionId,
    pub schedule_id: ScheduleId,
}

// ---------------------------------------------------------------------------
// Transport abstraction traits
// ---------------------------------------------------------------------------

/// Sends task dispatch and cancel requests to remote workers.
#[async_trait]
pub trait TaskDispatcher: Send + Sync {
    async fn dispatch(&self, request: TaskDispatchRequest) -> Result<(), SchedulerError>;
    async fn cancel(&self, request: CancelRequest) -> Result<(), SchedulerError>;
}

/// Receives task result reports from remote workers.
#[async_trait]
pub trait ResultReceiver: Send + Sync {
    async fn recv(&self) -> Option<TaskResultReport>;
}

// ---------------------------------------------------------------------------
// RemoteExecutor
// ---------------------------------------------------------------------------

/// An [`Executor`] that delegates work to remote workers via a [`TaskDispatcher`].
pub struct RemoteExecutor {
    dispatcher: Arc<dyn TaskDispatcher>,
    running_counts: Mutex<HashMap<ScheduleId, u32>>,
}

impl RemoteExecutor {
    pub fn new(dispatcher: Arc<dyn TaskDispatcher>) -> Self {
        Self {
            dispatcher,
            running_counts: Mutex::new(HashMap::new()),
        }
    }

    /// Decrement the running count for a schedule (called when a result is received).
    pub fn decrement_running_count(&self, schedule_id: &ScheduleId) {
        let mut counts = self.running_counts.lock().unwrap();
        if let Some(count) = counts.get_mut(schedule_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(schedule_id);
            }
        }
    }
}

#[async_trait]
impl Executor for RemoteExecutor {
    async fn submit(
        &self,
        _schedule_id: &ScheduleId,
        _task: BoxedTaskFn,
    ) -> Result<(), SchedulerError> {
        Err(SchedulerError::DispatchError(
            "RemoteExecutor does not support submit(); use dispatch() instead".into(),
        ))
    }

    fn running_count(&self, schedule_id: &ScheduleId) -> u32 {
        let counts = self.running_counts.lock().unwrap();
        counts.get(schedule_id).copied().unwrap_or(0)
    }

    async fn shutdown(&self, _timeout: Duration) -> Result<(), SchedulerError> {
        // No-op: remote workers manage their own lifecycle.
        Ok(())
    }

    fn as_dispatchable(&self) -> Option<&dyn DispatchableExecutor> {
        Some(self)
    }
}

#[async_trait]
impl DispatchableExecutor for RemoteExecutor {
    async fn dispatch(&self, request: TaskDispatchRequest) -> Result<(), SchedulerError> {
        let schedule_id = request.schedule_id.clone();
        self.dispatcher.dispatch(request).await?;
        let mut counts = self.running_counts.lock().unwrap();
        *counts.entry(schedule_id).or_insert(0) += 1;
        Ok(())
    }

    async fn cancel_remote(&self, request: CancelRequest) -> Result<(), SchedulerError> {
        self.dispatcher.cancel(request).await
    }

    fn decrement_running_count(&self, schedule_id: &ScheduleId) {
        RemoteExecutor::decrement_running_count(self, schedule_id);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use uuid::Uuid;

    /// A mock dispatcher that records calls.
    struct MockDispatcher {
        dispatch_count: AtomicU32,
        cancel_count: AtomicU32,
        fail_dispatch: bool,
    }

    impl MockDispatcher {
        fn new() -> Self {
            Self {
                dispatch_count: AtomicU32::new(0),
                cancel_count: AtomicU32::new(0),
                fail_dispatch: false,
            }
        }

        fn failing() -> Self {
            Self {
                dispatch_count: AtomicU32::new(0),
                cancel_count: AtomicU32::new(0),
                fail_dispatch: true,
            }
        }
    }

    #[async_trait]
    impl TaskDispatcher for MockDispatcher {
        async fn dispatch(&self, _request: TaskDispatchRequest) -> Result<(), SchedulerError> {
            if self.fail_dispatch {
                return Err(SchedulerError::DispatchError("mock failure".into()));
            }
            self.dispatch_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn cancel(&self, _request: CancelRequest) -> Result<(), SchedulerError> {
            self.cancel_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn make_schedule_id() -> ScheduleId {
        ScheduleId(Uuid::new_v4())
    }

    fn make_execution_id() -> ExecutionId {
        ExecutionId(Uuid::new_v4())
    }

    fn make_dispatch_request(schedule_id: &ScheduleId) -> TaskDispatchRequest {
        TaskDispatchRequest {
            execution_id: make_execution_id(),
            schedule_id: schedule_id.clone(),
            type_name: "my_task".into(),
            task_data: serde_json::json!({"key": "value"}),
        }
    }

    #[tokio::test]
    async fn submit_returns_error() {
        let executor = RemoteExecutor::new(Arc::new(MockDispatcher::new()));
        let sid = make_schedule_id();
        let task: BoxedTaskFn = Box::new(|| Box::pin(async { Ok(()) }));
        let result = executor.submit(&sid, task).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SchedulerError::DispatchError(_)));
    }

    #[tokio::test]
    async fn dispatch_increments_running_count() {
        let mock = Arc::new(MockDispatcher::new());
        let executor = RemoteExecutor::new(mock.clone());
        let sid = make_schedule_id();

        assert_eq!(executor.running_count(&sid), 0);

        executor.dispatch(make_dispatch_request(&sid)).await.unwrap();
        assert_eq!(executor.running_count(&sid), 1);

        executor.dispatch(make_dispatch_request(&sid)).await.unwrap();
        assert_eq!(executor.running_count(&sid), 2);

        assert_eq!(mock.dispatch_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn dispatch_failure_does_not_increment_count() {
        let executor = RemoteExecutor::new(Arc::new(MockDispatcher::failing()));
        let sid = make_schedule_id();

        let result = executor.dispatch(make_dispatch_request(&sid)).await;
        assert!(result.is_err());
        assert_eq!(executor.running_count(&sid), 0);
    }

    #[tokio::test]
    async fn decrement_running_count_works() {
        let executor = RemoteExecutor::new(Arc::new(MockDispatcher::new()));
        let sid = make_schedule_id();

        executor.dispatch(make_dispatch_request(&sid)).await.unwrap();
        executor.dispatch(make_dispatch_request(&sid)).await.unwrap();
        assert_eq!(executor.running_count(&sid), 2);

        executor.decrement_running_count(&sid);
        assert_eq!(executor.running_count(&sid), 1);

        executor.decrement_running_count(&sid);
        assert_eq!(executor.running_count(&sid), 0);

        // Decrementing past zero should not panic.
        executor.decrement_running_count(&sid);
        assert_eq!(executor.running_count(&sid), 0);
    }

    #[tokio::test]
    async fn cancel_delegates_to_dispatcher() {
        let mock = Arc::new(MockDispatcher::new());
        let executor = RemoteExecutor::new(mock.clone());
        let sid = make_schedule_id();

        let request = CancelRequest {
            execution_id: make_execution_id(),
            schedule_id: sid,
        };

        executor.cancel_remote(request).await.unwrap();
        assert_eq!(mock.cancel_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_is_noop() {
        let executor = RemoteExecutor::new(Arc::new(MockDispatcher::new()));
        let result = executor.shutdown(Duration::from_secs(5)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn as_dispatchable_returns_some() {
        let executor = RemoteExecutor::new(Arc::new(MockDispatcher::new()));
        assert!(executor.as_dispatchable().is_some());
    }

    #[test]
    fn message_types_serialize_roundtrip() {
        let sid = make_schedule_id();
        let eid = make_execution_id();

        let dispatch_req = TaskDispatchRequest {
            execution_id: eid.clone(),
            schedule_id: sid.clone(),
            type_name: "test_task".into(),
            task_data: serde_json::json!({"foo": 42}),
        };
        let json = serde_json::to_string(&dispatch_req).unwrap();
        let decoded: TaskDispatchRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.type_name, "test_task");
        assert_eq!(decoded.execution_id, eid);
        assert_eq!(decoded.schedule_id, sid);

        let report_ok = TaskResultReport {
            execution_id: eid.clone(),
            schedule_id: sid.clone(),
            outcome: TaskOutcome::Succeeded,
        };
        let json = serde_json::to_string(&report_ok).unwrap();
        let decoded: TaskResultReport = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.outcome, TaskOutcome::Succeeded);

        let report_err = TaskResultReport {
            execution_id: eid.clone(),
            schedule_id: sid.clone(),
            outcome: TaskOutcome::Failed { error: "boom".into() },
        };
        let json = serde_json::to_string(&report_err).unwrap();
        let decoded: TaskResultReport = serde_json::from_str(&json).unwrap();
        assert_eq!(
            decoded.outcome,
            TaskOutcome::Failed { error: "boom".into() }
        );

        let cancel = CancelRequest {
            execution_id: eid,
            schedule_id: sid,
        };
        let json = serde_json::to_string(&cancel).unwrap();
        let _decoded: CancelRequest = serde_json::from_str(&json).unwrap();
    }

    #[tokio::test]
    async fn multiple_schedules_tracked_independently() {
        let executor = RemoteExecutor::new(Arc::new(MockDispatcher::new()));
        let sid1 = make_schedule_id();
        let sid2 = make_schedule_id();

        executor.dispatch(make_dispatch_request(&sid1)).await.unwrap();
        executor.dispatch(make_dispatch_request(&sid1)).await.unwrap();
        executor.dispatch(make_dispatch_request(&sid2)).await.unwrap();

        assert_eq!(executor.running_count(&sid1), 2);
        assert_eq!(executor.running_count(&sid2), 1);

        executor.decrement_running_count(&sid1);
        assert_eq!(executor.running_count(&sid1), 1);
        assert_eq!(executor.running_count(&sid2), 1);
    }
}
