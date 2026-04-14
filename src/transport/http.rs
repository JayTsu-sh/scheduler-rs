use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::error::SchedulerError;
use crate::executor::remote::{
    CancelRequest, ResultReceiver, TaskDispatchRequest, TaskDispatcher, TaskResultReport,
};
use crate::worker::Worker;

// ---------------------------------------------------------------------------
// Scheduler-side: HttpDispatcher
// ---------------------------------------------------------------------------

/// Sends task dispatch and cancel requests to a remote worker over HTTP.
pub struct HttpDispatcher {
    client: reqwest::Client,
    worker_url: String,
}

impl HttpDispatcher {
    pub fn new(worker_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            worker_url: worker_url.into(),
        }
    }
}

#[async_trait]
impl TaskDispatcher for HttpDispatcher {
    async fn dispatch(&self, request: TaskDispatchRequest) -> Result<(), SchedulerError> {
        let resp = self
            .client
            .post(format!("{}/tasks/dispatch", self.worker_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| SchedulerError::DispatchError(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(SchedulerError::DispatchError(format!(
                "Worker returned status {}",
                resp.status()
            )));
        }
        Ok(())
    }

    async fn cancel(&self, request: CancelRequest) -> Result<(), SchedulerError> {
        let resp = self
            .client
            .post(format!("{}/tasks/cancel", self.worker_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| SchedulerError::DispatchError(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(SchedulerError::DispatchError(format!(
                "Worker returned status {}",
                resp.status()
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Scheduler-side: HttpResultReceiver
// ---------------------------------------------------------------------------

/// Receives task result reports from remote workers via an HTTP endpoint.
///
/// Call [`HttpResultReceiver::new`] to obtain both the receiver and an
/// [`axum::Router`] that exposes `POST /results`. Mount that router on the
/// scheduler's HTTP server so workers can report outcomes.
pub struct HttpResultReceiver {
    rx: Mutex<tokio::sync::mpsc::UnboundedReceiver<TaskResultReport>>,
}

impl HttpResultReceiver {
    /// Creates a new receiver and an axum [`Router`] to mount on the scheduler.
    ///
    /// The router exposes `POST /results` for workers to report task outcomes.
    pub fn new() -> (Self, axum::Router) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let router = axum::Router::new().route(
            "/results",
            axum::routing::post(
                move |axum::extract::Json(report): axum::extract::Json<TaskResultReport>| async move {
                    let _ = tx.send(report);
                    axum::http::StatusCode::OK
                },
            ),
        );
        (Self { rx: Mutex::new(rx) }, router)
    }
}

#[async_trait]
impl ResultReceiver for HttpResultReceiver {
    async fn recv(&self) -> Option<TaskResultReport> {
        self.rx.lock().await.recv().await
    }
}

// ---------------------------------------------------------------------------
// Worker-side: WorkerHttpServer
// ---------------------------------------------------------------------------

/// Wraps a [`Worker`] and exposes it as an HTTP server using axum.
///
/// The server exposes:
/// - `POST /tasks/dispatch` — accept and execute a task (returns 202 immediately)
/// - `POST /tasks/cancel` — cancel a running task
///
/// After a task completes the worker sends the result report back to the
/// scheduler via `POST {callback_url}/results`.
pub struct WorkerHttpServer {
    worker: Arc<Worker>,
    callback_url: String,
}

impl WorkerHttpServer {
    pub fn new(worker: Arc<Worker>, callback_url: impl Into<String>) -> Self {
        Self {
            worker,
            callback_url: callback_url.into(),
        }
    }

    /// Returns an [`axum::Router`] with the worker endpoints.
    pub fn router(&self) -> axum::Router {
        let worker = self.worker.clone();
        let callback_url = self.callback_url.clone();
        let client = reqwest::Client::new();

        let dispatch_worker = worker.clone();
        let dispatch_callback = callback_url.clone();
        let dispatch_client = client.clone();

        axum::Router::new()
            .route(
                "/tasks/dispatch",
                axum::routing::post(
                    move |axum::extract::Json(request): axum::extract::Json<
                        TaskDispatchRequest,
                    >| {
                        let worker = dispatch_worker.clone();
                        let callback_url = dispatch_callback.clone();
                        let client = dispatch_client.clone();
                        async move {
                            // Spawn task execution in background, immediately return 202
                            tokio::spawn(async move {
                                let report = worker.execute(request).await;
                                // Best-effort callback
                                let _ = client
                                    .post(format!("{}/results", callback_url))
                                    .json(&report)
                                    .send()
                                    .await;
                            });
                            axum::http::StatusCode::ACCEPTED
                        }
                    },
                ),
            )
            .route(
                "/tasks/cancel",
                axum::routing::post(
                    move |axum::extract::Json(request): axum::extract::Json<CancelRequest>| async move {
                        worker.cancel(&request);
                        axum::http::StatusCode::OK
                    },
                ),
            )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::ExecutionId;
    use crate::executor::remote::TaskOutcome;
    use crate::schedule::ScheduleId;
    use uuid::Uuid;

    fn make_schedule_id() -> ScheduleId {
        ScheduleId(Uuid::new_v4())
    }

    fn make_execution_id() -> ExecutionId {
        ExecutionId(Uuid::new_v4())
    }

    #[tokio::test]
    async fn test_http_result_receiver() {
        let (receiver, router) = HttpResultReceiver::new();

        let report = TaskResultReport {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            outcome: TaskOutcome::Succeeded,
        };

        // Use axum's test utilities to send a request through the router
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}/results", addr))
            .json(&report)
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.outcome, TaskOutcome::Succeeded);
    }

    #[tokio::test]
    async fn test_http_integration_dispatch_and_result() {
        use async_trait::async_trait;
        use crate::task::{Task, TaskContext, TaskResult};
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct EchoTask {
            message: String,
        }

        #[async_trait]
        impl Task for EchoTask {
            const TYPE_NAME: &'static str = "echo";

            async fn run(&self, _ctx: &TaskContext) -> TaskResult {
                Ok(())
            }
        }

        // Set up worker
        let worker = Arc::new(Worker::new());
        worker.register::<EchoTask>().await;

        // Set up result receiver on scheduler side
        let (receiver, result_router) = HttpResultReceiver::new();

        let scheduler_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let scheduler_addr = scheduler_listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(scheduler_listener, result_router).await.unwrap();
        });

        // Set up worker HTTP server
        let callback_url = format!("http://{}", scheduler_addr);
        let worker_server = WorkerHttpServer::new(worker, &callback_url);
        let worker_router = worker_server.router();

        let worker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let worker_addr = worker_listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(worker_listener, worker_router).await.unwrap();
        });

        // Dispatch a task via HTTP
        let dispatcher = HttpDispatcher::new(format!("http://{}", worker_addr));

        let sid = make_schedule_id();
        let eid = make_execution_id();

        let request = TaskDispatchRequest {
            execution_id: eid.clone(),
            schedule_id: sid.clone(),
            type_name: "echo".into(),
            task_data: serde_json::json!({ "message": "hello" }),
        };

        dispatcher.dispatch(request).await.unwrap();

        // Wait for the result to come back through the callback
        let report = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            receiver.recv(),
        )
        .await
        .expect("timed out waiting for result")
        .expect("channel closed");

        assert_eq!(report.execution_id, eid);
        assert_eq!(report.schedule_id, sid);
        assert_eq!(report.outcome, TaskOutcome::Succeeded);
    }
}
