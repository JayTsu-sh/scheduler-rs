use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::SchedulerError;
use crate::task::{TaskContext, TaskResult};

/// Type-erased async task runner: takes serialized task data + context, returns result.
pub type TaskRunner = Arc<
    dyn Fn(serde_json::Value, TaskContext) -> Pin<Box<dyn Future<Output = TaskResult> + Send>>
        + Send
        + Sync,
>;

/// Middleware that wraps task execution.
///
/// Middleware transforms a `TaskRunner` into another `TaskRunner`, enabling
/// cross-cutting concerns like retry, timeout, and logging.
///
/// # Example
///
/// ```ignore
/// scheduler.schedule(task)
///     .trigger(trigger)
///     .middleware(TimeoutMiddleware::new(Duration::from_secs(30)))
///     .middleware(RetryMiddleware::new(3, Duration::from_secs(1)))
///     .submit().await?;
/// ```
pub trait TaskMiddleware: Send + Sync + Debug {
    fn wrap(&self, next: TaskRunner) -> TaskRunner;
}

// ---------------------------------------------------------------------------
// TimeoutMiddleware
// ---------------------------------------------------------------------------

/// Wraps task execution with a timeout. If the task doesn't complete within
/// the given duration, it returns `SchedulerError::ExecutionError`.
#[derive(Debug, Clone)]
pub struct TimeoutMiddleware {
    timeout: std::time::Duration,
}

impl TimeoutMiddleware {
    pub fn new(timeout: std::time::Duration) -> Self {
        Self { timeout }
    }
}

impl TaskMiddleware for TimeoutMiddleware {
    fn wrap(&self, next: TaskRunner) -> TaskRunner {
        let timeout = self.timeout;
        Arc::new(move |data, ctx| {
            let next = next.clone();
            Box::pin(async move {
                match tokio::time::timeout(timeout, next(data, ctx)).await {
                    Ok(result) => result,
                    Err(_) => Err(SchedulerError::ExecutionError(format!(
                        "Task timed out after {:?}",
                        timeout
                    ))),
                }
            })
        })
    }
}

// ---------------------------------------------------------------------------
// RetryMiddleware
// ---------------------------------------------------------------------------

/// Retries task execution on failure, up to `max_retries` additional attempts
/// with a configurable delay between attempts.
#[derive(Debug, Clone)]
pub struct RetryMiddleware {
    max_retries: u32,
    delay: std::time::Duration,
}

impl RetryMiddleware {
    pub fn new(max_retries: u32, delay: std::time::Duration) -> Self {
        Self { max_retries, delay }
    }
}

impl TaskMiddleware for RetryMiddleware {
    fn wrap(&self, next: TaskRunner) -> TaskRunner {
        let max_retries = self.max_retries;
        let delay = self.delay;
        Arc::new(move |data, ctx| {
            let next = next.clone();
            Box::pin(async move {
                let mut last_err = None;
                for attempt in 0..=max_retries {
                    if attempt > 0 {
                        tracing::warn!(attempt, "Retrying task after failure");
                        tokio::time::sleep(delay).await;
                    }
                    match next(data.clone(), ctx.clone()).await {
                        Ok(()) => return Ok(()),
                        Err(e) => {
                            tracing::warn!(attempt, error = %e, "Task attempt failed");
                            last_err = Some(e);
                        }
                    }
                }
                Err(last_err.unwrap_or_else(|| {
                    SchedulerError::ExecutionError("All retry attempts exhausted".to_string())
                }))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::ExecutionId;
    use crate::schedule::ScheduleId;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio_util::sync::CancellationToken;

    fn make_ctx() -> TaskContext {
        TaskContext {
            schedule_id: ScheduleId::new(),
            execution_id: ExecutionId::new(),
            cancellation_token: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn test_timeout_middleware_success() {
        let runner: TaskRunner = Arc::new(|_data, _ctx| {
            Box::pin(async { Ok(()) })
        });
        let mw = TimeoutMiddleware::new(std::time::Duration::from_secs(1));
        let wrapped = mw.wrap(runner);

        let result = wrapped(serde_json::Value::Null, make_ctx()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_timeout_middleware_timeout() {
        let runner: TaskRunner = Arc::new(|_data, _ctx| {
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                Ok(())
            })
        });
        let mw = TimeoutMiddleware::new(std::time::Duration::from_millis(50));
        let wrapped = mw.wrap(runner);

        let result = wrapped(serde_json::Value::Null, make_ctx()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn test_retry_middleware_eventual_success() {
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let runner: TaskRunner = Arc::new(move |_data, _ctx| {
            let cc = cc.clone();
            Box::pin(async move {
                let n = cc.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(SchedulerError::ExecutionError("transient".into()))
                } else {
                    Ok(())
                }
            })
        });

        let mw = RetryMiddleware::new(3, std::time::Duration::from_millis(10));
        let wrapped = mw.wrap(runner);

        let result = wrapped(serde_json::Value::Null, make_ctx()).await;
        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 3); // 2 failures + 1 success
    }

    #[tokio::test]
    async fn test_retry_middleware_all_fail() {
        let runner: TaskRunner = Arc::new(|_data, _ctx| {
            Box::pin(async { Err(SchedulerError::ExecutionError("permanent".into())) })
        });

        let mw = RetryMiddleware::new(2, std::time::Duration::from_millis(10));
        let wrapped = mw.wrap(runner);

        let result = wrapped(serde_json::Value::Null, make_ctx()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_middleware_chain() {
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let runner: TaskRunner = Arc::new(move |_data, _ctx| {
            let cc = cc.clone();
            Box::pin(async move {
                cc.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                Ok(())
            })
        });

        // Chain: retry wraps timeout
        let timeout_mw = TimeoutMiddleware::new(std::time::Duration::from_secs(1));
        let retry_mw = RetryMiddleware::new(2, std::time::Duration::from_millis(10));

        let wrapped = retry_mw.wrap(timeout_mw.wrap(runner));
        let result = wrapped(serde_json::Value::Null, make_ctx()).await;
        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 1); // succeeded on first try
    }
}
