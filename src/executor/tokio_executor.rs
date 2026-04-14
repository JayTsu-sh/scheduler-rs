use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;

use crate::error::SchedulerError;
use crate::schedule::ScheduleId;
use super::{BoxedTaskFn, Executor};

pub struct TokioExecutor {
    running: Arc<std::sync::Mutex<HashMap<ScheduleId, u32>>>,
    handles: Arc<tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl TokioExecutor {
    pub fn new() -> Self {
        Self {
            running: Arc::new(std::sync::Mutex::new(HashMap::new())),
            handles: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }
}

impl Default for TokioExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Executor for TokioExecutor {
    async fn submit(&self, schedule_id: &ScheduleId, task: BoxedTaskFn) -> Result<(), SchedulerError> {
        let running = self.running.clone();
        let sid = schedule_id.clone();

        // Increment running count
        {
            let mut map = running.lock().unwrap();
            *map.entry(sid.clone()).or_insert(0) += 1;
        }

        let handle = tokio::spawn(async move {
            let fut = task();
            let _ = fut.await;

            // Decrement running count
            let mut map = running.lock().unwrap();
            if let Some(count) = map.get_mut(&sid) {
                *count -= 1;
                if *count == 0 {
                    map.remove(&sid);
                }
            }
        });

        self.handles.lock().await.push(handle);
        Ok(())
    }

    fn running_count(&self, schedule_id: &ScheduleId) -> u32 {
        let map = self.running.lock().unwrap();
        map.get(schedule_id).copied().unwrap_or(0)
    }

    async fn shutdown(&self, timeout: Duration) -> Result<(), SchedulerError> {
        let handles: Vec<_> = {
            let mut h = self.handles.lock().await;
            std::mem::take(&mut *h)
        };

        let wait_all = async {
            for handle in handles {
                let _ = handle.await;
            }
        };

        if tokio::time::timeout(timeout, wait_all).await.is_err() {
            // Abort remaining handles
            let remaining: Vec<_> = {
                let mut h = self.handles.lock().await;
                std::mem::take(&mut *h)
            };
            for handle in remaining {
                handle.abort();
            }
            return Err(SchedulerError::ExecutionError(
                "Shutdown timed out, aborted remaining tasks".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn submit_and_running_count() {
        let executor = TokioExecutor::new();
        let sid = ScheduleId::new();
        let counter = Arc::new(AtomicU32::new(0));

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let counter_clone = counter.clone();

        let task: BoxedTaskFn = Box::new(move || {
            Box::pin(async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                // Wait until signaled so we can observe running_count
                let _ = rx.await;
                Ok(())
            })
        });

        executor.submit(&sid, task).await.unwrap();

        // Give the task a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(executor.running_count(&sid), 1);
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Signal the task to finish
        let _ = tx.send(());
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(executor.running_count(&sid), 0);
    }

    #[tokio::test]
    async fn shutdown_waits_for_tasks() {
        let executor = TokioExecutor::new();
        let sid = ScheduleId::new();
        let finished = Arc::new(AtomicU32::new(0));
        let finished_clone = finished.clone();

        let task: BoxedTaskFn = Box::new(move || {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                finished_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });

        executor.submit(&sid, task).await.unwrap();
        executor.shutdown(Duration::from_secs(5)).await.unwrap();

        assert_eq!(finished.load(Ordering::SeqCst), 1);
    }
}
