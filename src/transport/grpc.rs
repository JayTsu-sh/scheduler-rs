use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::error::SchedulerError;
use crate::execution::ExecutionId;
use crate::executor::remote::{
    self, CancelRequest, ResultReceiver, TaskDispatcher, TaskOutcome,
};
use crate::schedule::ScheduleId;
use crate::worker::Worker;

/// Proto-generated types.
mod proto {
    tonic::include_proto!("scheduler");
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

fn parse_uuid(s: &str, field: &str) -> Result<Uuid, SchedulerError> {
    Uuid::parse_str(s)
        .map_err(|e| SchedulerError::DispatchError(format!("invalid {field}: {e}")))
}

fn app_dispatch_to_proto(
    req: &remote::TaskDispatchRequest,
) -> proto::TaskDispatchRequest {
    proto::TaskDispatchRequest {
        execution_id: req.execution_id.0.to_string(),
        schedule_id: req.schedule_id.0.to_string(),
        type_name: req.type_name.clone(),
        task_data_json: req.task_data.to_string(),
    }
}

fn proto_dispatch_to_app(
    req: &proto::TaskDispatchRequest,
) -> Result<remote::TaskDispatchRequest, SchedulerError> {
    Ok(remote::TaskDispatchRequest {
        execution_id: ExecutionId(parse_uuid(&req.execution_id, "execution_id")?),
        schedule_id: ScheduleId(parse_uuid(&req.schedule_id, "schedule_id")?),
        type_name: req.type_name.clone(),
        task_data: serde_json::from_str(&req.task_data_json)
            .map_err(|e| SchedulerError::DispatchError(format!("invalid task_data_json: {e}")))?,
    })
}

fn app_cancel_to_proto(req: &CancelRequest) -> proto::CancelRequest {
    proto::CancelRequest {
        execution_id: req.execution_id.0.to_string(),
        schedule_id: req.schedule_id.0.to_string(),
    }
}

fn proto_cancel_to_app(
    req: &proto::CancelRequest,
) -> Result<CancelRequest, SchedulerError> {
    Ok(CancelRequest {
        execution_id: ExecutionId(parse_uuid(&req.execution_id, "execution_id")?),
        schedule_id: ScheduleId(parse_uuid(&req.schedule_id, "schedule_id")?),
    })
}

fn app_result_to_proto(
    report: &remote::TaskResultReport,
) -> proto::TaskResultReport {
    let outcome = match &report.outcome {
        TaskOutcome::Succeeded => {
            Some(proto::task_result_report::Outcome::Succeeded(true))
        }
        TaskOutcome::Failed { error } => {
            Some(proto::task_result_report::Outcome::FailedError(error.clone()))
        }
    };
    proto::TaskResultReport {
        execution_id: report.execution_id.0.to_string(),
        schedule_id: report.schedule_id.0.to_string(),
        outcome,
    }
}

fn proto_result_to_app(
    report: &proto::TaskResultReport,
) -> Result<remote::TaskResultReport, SchedulerError> {
    let outcome = match &report.outcome {
        Some(proto::task_result_report::Outcome::Succeeded(_)) => TaskOutcome::Succeeded,
        Some(proto::task_result_report::Outcome::FailedError(e)) => {
            TaskOutcome::Failed { error: e.clone() }
        }
        None => {
            return Err(SchedulerError::DispatchError(
                "TaskResultReport missing outcome".into(),
            ));
        }
    };
    Ok(remote::TaskResultReport {
        execution_id: ExecutionId(parse_uuid(&report.execution_id, "execution_id")?),
        schedule_id: ScheduleId(parse_uuid(&report.schedule_id, "schedule_id")?),
        outcome,
    })
}

// ---------------------------------------------------------------------------
// Scheduler-side: GrpcDispatcher
// ---------------------------------------------------------------------------

/// Sends task dispatch and cancel requests to a remote worker over gRPC.
pub struct GrpcDispatcher {
    client: Mutex<proto::worker_service_client::WorkerServiceClient<tonic::transport::Channel>>,
}

impl GrpcDispatcher {
    /// Connect to a remote worker's gRPC `WorkerService`.
    ///
    /// `worker_addr` should be a URI such as `"http://127.0.0.1:50051"`.
    pub async fn connect(worker_addr: impl Into<String>) -> Result<Self, SchedulerError> {
        let addr = worker_addr.into();
        let client = proto::worker_service_client::WorkerServiceClient::connect(addr)
            .await
            .map_err(|e| SchedulerError::DispatchError(format!("gRPC connect failed: {e}")))?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }
}

#[async_trait]
impl TaskDispatcher for GrpcDispatcher {
    async fn dispatch(
        &self,
        request: remote::TaskDispatchRequest,
    ) -> Result<(), SchedulerError> {
        let proto_req = app_dispatch_to_proto(&request);
        self.client
            .lock()
            .await
            .dispatch(tonic::Request::new(proto_req))
            .await
            .map_err(|e| SchedulerError::DispatchError(format!("gRPC dispatch failed: {e}")))?;
        Ok(())
    }

    async fn cancel(&self, request: CancelRequest) -> Result<(), SchedulerError> {
        let proto_req = app_cancel_to_proto(&request);
        self.client
            .lock()
            .await
            .cancel(tonic::Request::new(proto_req))
            .await
            .map_err(|e| SchedulerError::DispatchError(format!("gRPC cancel failed: {e}")))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Scheduler-side: GrpcResultReceiver
// ---------------------------------------------------------------------------

/// Receives task result reports from remote workers via a gRPC `ResultService`.
pub struct GrpcResultReceiver {
    rx: Mutex<tokio::sync::mpsc::UnboundedReceiver<remote::TaskResultReport>>,
}

impl GrpcResultReceiver {
    /// Start a gRPC server that listens for result reports from workers.
    ///
    /// Returns `(receiver, server_future)`. The caller must spawn or await the
    /// server future so it actually starts accepting connections.
    ///
    /// `addr` should be a socket address such as `"0.0.0.0:50052"` or `"127.0.0.1:50052"`.
    pub async fn bind(
        addr: impl Into<String>,
    ) -> Result<
        (
            Self,
            impl std::future::Future<Output = Result<(), tonic::transport::Error>>,
        ),
        SchedulerError,
    > {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let svc = ResultServiceImpl { tx };
        let addr: std::net::SocketAddr = addr
            .into()
            .parse()
            .map_err(|e| SchedulerError::DispatchError(format!("invalid bind address: {e}")))?;

        let server_future = tonic::transport::Server::builder()
            .add_service(proto::result_service_server::ResultServiceServer::new(svc))
            .serve(addr);

        Ok((Self { rx: Mutex::new(rx) }, server_future))
    }
}

#[async_trait]
impl ResultReceiver for GrpcResultReceiver {
    async fn recv(&self) -> Option<remote::TaskResultReport> {
        self.rx.lock().await.recv().await
    }
}

/// Internal implementation of the `ResultService` gRPC service.
struct ResultServiceImpl {
    tx: tokio::sync::mpsc::UnboundedSender<remote::TaskResultReport>,
}

#[tonic::async_trait]
impl proto::result_service_server::ResultService for ResultServiceImpl {
    async fn report_result(
        &self,
        request: tonic::Request<proto::TaskResultReport>,
    ) -> Result<tonic::Response<proto::Empty>, tonic::Status> {
        let proto_report = request.into_inner();
        let app_report = proto_result_to_app(&proto_report)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let _ = self.tx.send(app_report);
        Ok(tonic::Response::new(proto::Empty {}))
    }
}

// ---------------------------------------------------------------------------
// Worker-side: WorkerGrpcServer
// ---------------------------------------------------------------------------

/// Wraps a [`Worker`] and exposes it as a gRPC `WorkerService` server.
///
/// When a `Dispatch` RPC is received, the task is executed in a background
/// tokio task. Upon completion the result is reported back to the scheduler
/// via the `ResultService` gRPC service at `scheduler_addr`.
pub struct WorkerGrpcServer {
    worker: Arc<Worker>,
    scheduler_addr: String,
}

impl WorkerGrpcServer {
    pub fn new(worker: Arc<Worker>, scheduler_addr: impl Into<String>) -> Self {
        Self {
            worker,
            scheduler_addr: scheduler_addr.into(),
        }
    }

    /// Start serving the `WorkerService` on the given address.
    ///
    /// `addr` should be a socket address such as `"0.0.0.0:50051"`.
    pub async fn serve(self, addr: impl Into<String>) -> Result<(), SchedulerError> {
        let addr: std::net::SocketAddr = addr
            .into()
            .parse()
            .map_err(|e| SchedulerError::DispatchError(format!("invalid bind address: {e}")))?;

        let svc = WorkerServiceImpl {
            worker: self.worker,
            scheduler_addr: self.scheduler_addr,
        };

        tonic::transport::Server::builder()
            .add_service(proto::worker_service_server::WorkerServiceServer::new(svc))
            .serve(addr)
            .await
            .map_err(|e| SchedulerError::DispatchError(format!("gRPC server error: {e}")))?;

        Ok(())
    }
}

/// Internal implementation of the `WorkerService` gRPC service.
struct WorkerServiceImpl {
    worker: Arc<Worker>,
    scheduler_addr: String,
}

#[tonic::async_trait]
impl proto::worker_service_server::WorkerService for WorkerServiceImpl {
    async fn dispatch(
        &self,
        request: tonic::Request<proto::TaskDispatchRequest>,
    ) -> Result<tonic::Response<proto::Empty>, tonic::Status> {
        let proto_req = request.into_inner();
        let app_req = proto_dispatch_to_app(&proto_req)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        let worker = self.worker.clone();
        let scheduler_addr = self.scheduler_addr.clone();

        // Spawn task execution in background, immediately return OK.
        tokio::spawn(async move {
            let report = worker.execute(app_req).await;
            let proto_report = app_result_to_proto(&report);

            // Best-effort report back to scheduler.
            let client =
                proto::result_service_client::ResultServiceClient::connect(scheduler_addr).await;
            if let Ok(mut client) = client {
                let _ = client
                    .report_result(tonic::Request::new(proto_report))
                    .await;
            }
        });

        Ok(tonic::Response::new(proto::Empty {}))
    }

    async fn cancel(
        &self,
        request: tonic::Request<proto::CancelRequest>,
    ) -> Result<tonic::Response<proto::Empty>, tonic::Status> {
        let proto_req = request.into_inner();
        let app_req = proto_cancel_to_app(&proto_req)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        self.worker.cancel(&app_req);
        Ok(tonic::Response::new(proto::Empty {}))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::remote::TaskOutcome;
    use crate::task::{Task, TaskContext, TaskResult};
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    fn make_schedule_id() -> ScheduleId {
        ScheduleId(Uuid::new_v4())
    }

    fn make_execution_id() -> ExecutionId {
        ExecutionId(Uuid::new_v4())
    }

    // -- conversion tests -----------------------------------------------------

    #[test]
    fn test_dispatch_roundtrip() {
        let app_req = remote::TaskDispatchRequest {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            type_name: "my_task".into(),
            task_data: serde_json::json!({"key": "value"}),
        };
        let proto_req = app_dispatch_to_proto(&app_req);
        let back = proto_dispatch_to_app(&proto_req).unwrap();
        assert_eq!(back.execution_id, app_req.execution_id);
        assert_eq!(back.schedule_id, app_req.schedule_id);
        assert_eq!(back.type_name, app_req.type_name);
        assert_eq!(back.task_data, app_req.task_data);
    }

    #[test]
    fn test_cancel_roundtrip() {
        let app_req = CancelRequest {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
        };
        let proto_req = app_cancel_to_proto(&app_req);
        let back = proto_cancel_to_app(&proto_req).unwrap();
        assert_eq!(back.execution_id, app_req.execution_id);
        assert_eq!(back.schedule_id, app_req.schedule_id);
    }

    #[test]
    fn test_result_roundtrip_succeeded() {
        let report = remote::TaskResultReport {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            outcome: TaskOutcome::Succeeded,
        };
        let proto_report = app_result_to_proto(&report);
        let back = proto_result_to_app(&proto_report).unwrap();
        assert_eq!(back.outcome, TaskOutcome::Succeeded);
    }

    #[test]
    fn test_result_roundtrip_failed() {
        let report = remote::TaskResultReport {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            outcome: TaskOutcome::Failed {
                error: "boom".into(),
            },
        };
        let proto_report = app_result_to_proto(&report);
        let back = proto_result_to_app(&proto_report).unwrap();
        assert_eq!(
            back.outcome,
            TaskOutcome::Failed {
                error: "boom".into()
            }
        );
    }

    #[test]
    fn test_result_missing_outcome() {
        let proto_report = proto::TaskResultReport {
            execution_id: Uuid::new_v4().to_string(),
            schedule_id: Uuid::new_v4().to_string(),
            outcome: None,
        };
        assert!(proto_result_to_app(&proto_report).is_err());
    }

    #[test]
    fn test_invalid_uuid() {
        let proto_req = proto::TaskDispatchRequest {
            execution_id: "not-a-uuid".into(),
            schedule_id: Uuid::new_v4().to_string(),
            type_name: "t".into(),
            task_data_json: "{}".into(),
        };
        assert!(proto_dispatch_to_app(&proto_req).is_err());
    }

    // -- integration test -----------------------------------------------------

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

    #[tokio::test]
    async fn test_grpc_integration_dispatch_and_result() {
        let scheduler_port = portpicker::pick_unused_port().expect("no free port");
        let worker_port = portpicker::pick_unused_port().expect("no free port");

        let scheduler_addr = format!("127.0.0.1:{scheduler_port}");
        let worker_addr = format!("127.0.0.1:{worker_port}");

        let (receiver, server_future) = GrpcResultReceiver::bind(scheduler_addr.clone())
            .await
            .expect("failed to bind result receiver");

        tokio::spawn(server_future);

        // Give the server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // 2. Set up the worker
        let worker = Arc::new(Worker::new());
        worker.register::<EchoTask>().await;

        let worker_server = WorkerGrpcServer::new(
            worker,
            format!("http://127.0.0.1:{scheduler_port}"),
        );

        let worker_addr_clone = worker_addr.clone();
        tokio::spawn(async move {
            worker_server.serve(worker_addr_clone).await.unwrap();
        });

        // Give the worker server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // 3. Dispatch a task via GrpcDispatcher
        let dispatcher = GrpcDispatcher::connect(format!("http://{worker_addr}"))
            .await
            .expect("failed to connect dispatcher");

        let sid = make_schedule_id();
        let eid = make_execution_id();

        let request = remote::TaskDispatchRequest {
            execution_id: eid.clone(),
            schedule_id: sid.clone(),
            type_name: "echo".into(),
            task_data: serde_json::json!({"message": "hello gRPC"}),
        };

        dispatcher.dispatch(request).await.unwrap();

        // 4. Wait for the result
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
