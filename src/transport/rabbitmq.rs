use std::sync::Arc;

use async_trait::async_trait;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel};
use tokio::sync::Mutex;

use crate::error::SchedulerError;
use crate::executor::remote::{
    CancelRequest, ResultReceiver, TaskDispatchRequest, TaskDispatcher, TaskResultReport,
};
use crate::worker::Worker;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Scheduler-side: RabbitMqDispatcher
// ---------------------------------------------------------------------------

/// Sends task dispatch and cancel requests to remote workers via RabbitMQ.
pub struct RabbitMqDispatcher {
    channel: Channel,
    task_queue: String,
    cancel_queue: String,
}

impl RabbitMqDispatcher {
    /// Create a new dispatcher, declaring the task and cancel queues.
    pub async fn new(
        channel: Channel,
        task_queue: &str,
        cancel_queue: &str,
    ) -> Result<Self, SchedulerError> {
        channel
            .queue_declare(task_queue, QueueDeclareOptions::default(), FieldTable::default())
            .await
            .map_err(|e| SchedulerError::DispatchError(format!("Failed to declare task queue: {e}")))?;
        channel
            .queue_declare(cancel_queue, QueueDeclareOptions::default(), FieldTable::default())
            .await
            .map_err(|e| {
                SchedulerError::DispatchError(format!("Failed to declare cancel queue: {e}"))
            })?;
        Ok(Self {
            channel,
            task_queue: task_queue.to_string(),
            cancel_queue: cancel_queue.to_string(),
        })
    }
}

#[async_trait]
impl TaskDispatcher for RabbitMqDispatcher {
    async fn dispatch(&self, request: TaskDispatchRequest) -> Result<(), SchedulerError> {
        let payload = serde_json::to_vec(&request)
            .map_err(|e| SchedulerError::SerializationError(e.to_string()))?;
        self.channel
            .basic_publish(
                "",
                &self.task_queue,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default()
                    .with_content_type("application/json".into()),
            )
            .await
            .map_err(|e| SchedulerError::DispatchError(format!("Failed to publish task: {e}")))?
            .await
            .map_err(|e| {
                SchedulerError::DispatchError(format!("Publisher confirm error: {e}"))
            })?;
        Ok(())
    }

    async fn cancel(&self, request: CancelRequest) -> Result<(), SchedulerError> {
        let payload = serde_json::to_vec(&request)
            .map_err(|e| SchedulerError::SerializationError(e.to_string()))?;
        self.channel
            .basic_publish(
                "",
                &self.cancel_queue,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default()
                    .with_content_type("application/json".into()),
            )
            .await
            .map_err(|e| SchedulerError::DispatchError(format!("Failed to publish cancel: {e}")))?
            .await
            .map_err(|e| {
                SchedulerError::DispatchError(format!("Publisher confirm error: {e}"))
            })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Scheduler-side: RabbitMqResultReceiver
// ---------------------------------------------------------------------------

/// Receives task result reports from workers via a RabbitMQ queue.
pub struct RabbitMqResultReceiver {
    consumer: Mutex<lapin::Consumer>,
}

impl RabbitMqResultReceiver {
    /// Create a new result receiver, declaring the result queue and starting a consumer.
    pub async fn new(channel: &Channel, result_queue: &str) -> Result<Self, SchedulerError> {
        channel
            .queue_declare(result_queue, QueueDeclareOptions::default(), FieldTable::default())
            .await
            .map_err(|e| {
                SchedulerError::DispatchError(format!("Failed to declare result queue: {e}"))
            })?;
        let consumer = channel
            .basic_consume(
                result_queue,
                "scheduler-result-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                SchedulerError::DispatchError(format!("Failed to create result consumer: {e}"))
            })?;
        Ok(Self {
            consumer: Mutex::new(consumer),
        })
    }
}

#[async_trait]
impl ResultReceiver for RabbitMqResultReceiver {
    async fn recv(&self) -> Option<TaskResultReport> {
        use futures_lite::StreamExt;
        let mut consumer = self.consumer.lock().await;
        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    delivery.ack(BasicAckOptions::default()).await.ok();
                    match serde_json::from_slice(&delivery.data) {
                        Ok(report) => return Some(report),
                        Err(e) => {
                            tracing::warn!("Failed to deserialize result report: {e}");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Result consumer error: {e}");
                    return None;
                }
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Worker-side: RabbitMqWorkerRunner
// ---------------------------------------------------------------------------

/// Consumes task and cancel requests from RabbitMQ queues, executes them via
/// a [`Worker`], and publishes result reports back.
pub struct RabbitMqWorkerRunner {
    worker: Arc<Worker>,
    channel: Channel,
    task_queue: String,
    cancel_queue: String,
    result_queue: String,
}

impl RabbitMqWorkerRunner {
    /// Create a new worker runner, declaring all queues.
    pub async fn new(
        worker: Arc<Worker>,
        channel: Channel,
        task_queue: &str,
        cancel_queue: &str,
        result_queue: &str,
    ) -> Result<Self, SchedulerError> {
        for (name, queue) in [
            ("task", task_queue),
            ("cancel", cancel_queue),
            ("result", result_queue),
        ] {
            channel
                .queue_declare(queue, QueueDeclareOptions::default(), FieldTable::default())
                .await
                .map_err(|e| {
                    SchedulerError::DispatchError(format!("Failed to declare {name} queue: {e}"))
                })?;
        }
        Ok(Self {
            worker,
            channel,
            task_queue: task_queue.to_string(),
            cancel_queue: cancel_queue.to_string(),
            result_queue: result_queue.to_string(),
        })
    }

    /// Run the worker loop, consuming tasks and cancel requests until the
    /// cancellation token is triggered or the consumers close.
    pub async fn run(&self, cancel: CancellationToken) -> Result<(), SchedulerError> {
        let task_consumer = self
            .channel
            .basic_consume(
                &self.task_queue,
                "worker-task-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                SchedulerError::DispatchError(format!("Failed to create task consumer: {e}"))
            })?;
        let cancel_consumer = self
            .channel
            .basic_consume(
                &self.cancel_queue,
                "worker-cancel-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                SchedulerError::DispatchError(format!("Failed to create cancel consumer: {e}"))
            })?;

        tokio::select! {
            () = self.process_tasks(task_consumer) => {}
            () = self.process_cancels(cancel_consumer) => {}
            () = cancel.cancelled() => {}
        }
        Ok(())
    }

    async fn process_tasks(&self, mut consumer: lapin::Consumer) {
        use futures_lite::StreamExt;
        while let Some(delivery) = consumer.next().await {
            let delivery = match delivery {
                Ok(d) => d,
                Err(e) => {
                    tracing::error!("Task consumer error: {e}");
                    return;
                }
            };
            delivery.ack(BasicAckOptions::default()).await.ok();
            let request = match serde_json::from_slice::<TaskDispatchRequest>(&delivery.data) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("Failed to deserialize task dispatch request: {e}");
                    continue;
                }
            };
            let report = self.worker.execute(request).await;
            let payload = match serde_json::to_vec(&report) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("Failed to serialize task result report: {e}");
                    continue;
                }
            };
            if let Err(e) = self
                .channel
                .basic_publish(
                    "",
                    &self.result_queue,
                    BasicPublishOptions::default(),
                    &payload,
                    BasicProperties::default()
                        .with_content_type("application/json".into()),
                )
                .await
            {
                tracing::error!("Failed to publish result: {e}");
            }
        }
    }

    async fn process_cancels(&self, mut consumer: lapin::Consumer) {
        use futures_lite::StreamExt;
        while let Some(delivery) = consumer.next().await {
            let delivery = match delivery {
                Ok(d) => d,
                Err(e) => {
                    tracing::error!("Cancel consumer error: {e}");
                    return;
                }
            };
            delivery.ack(BasicAckOptions::default()).await.ok();
            match serde_json::from_slice::<CancelRequest>(&delivery.data) {
                Ok(request) => {
                    self.worker.cancel(&request);
                }
                Err(e) => {
                    tracing::warn!("Failed to deserialize cancel request: {e}");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::execution::ExecutionId;
    use crate::executor::remote::{
        CancelRequest, TaskDispatchRequest, TaskOutcome, TaskResultReport,
    };
    use crate::schedule::ScheduleId;
    use uuid::Uuid;

    fn make_schedule_id() -> ScheduleId {
        ScheduleId(Uuid::new_v4())
    }

    fn make_execution_id() -> ExecutionId {
        ExecutionId(Uuid::new_v4())
    }

    #[test]
    fn test_dispatch_request_json_roundtrip() {
        let request = TaskDispatchRequest {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            type_name: "my_task".into(),
            task_data: serde_json::json!({"key": "value", "count": 42}),
        };
        let bytes = serde_json::to_vec(&request).unwrap();
        let decoded: TaskDispatchRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.type_name, request.type_name);
        assert_eq!(decoded.execution_id, request.execution_id);
        assert_eq!(decoded.schedule_id, request.schedule_id);
        assert_eq!(decoded.task_data, request.task_data);
    }

    #[test]
    fn test_result_report_json_roundtrip() {
        let report = TaskResultReport {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            outcome: TaskOutcome::Succeeded,
        };
        let bytes = serde_json::to_vec(&report).unwrap();
        let decoded: TaskResultReport = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.outcome, TaskOutcome::Succeeded);

        let report_failed = TaskResultReport {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            outcome: TaskOutcome::Failed {
                error: "something went wrong".into(),
            },
        };
        let bytes = serde_json::to_vec(&report_failed).unwrap();
        let decoded: TaskResultReport = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            decoded.outcome,
            TaskOutcome::Failed {
                error: "something went wrong".into()
            }
        );
    }

    #[test]
    fn test_cancel_request_json_roundtrip() {
        let request = CancelRequest {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
        };
        let bytes = serde_json::to_vec(&request).unwrap();
        let decoded: CancelRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.execution_id, request.execution_id);
        assert_eq!(decoded.schedule_id, request.schedule_id);
    }

    #[test]
    fn test_payload_is_valid_json_object() {
        // Verify the wire format is a JSON object with expected top-level keys
        let request = TaskDispatchRequest {
            execution_id: make_execution_id(),
            schedule_id: make_schedule_id(),
            type_name: "test".into(),
            task_data: serde_json::json!(null),
        };
        let bytes = serde_json::to_vec(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value.is_object());
        let obj = value.as_object().unwrap();
        assert!(obj.contains_key("execution_id"));
        assert!(obj.contains_key("schedule_id"));
        assert!(obj.contains_key("type_name"));
        assert!(obj.contains_key("task_data"));
    }
}
