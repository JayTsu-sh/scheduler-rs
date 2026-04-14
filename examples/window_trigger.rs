//! WindowTrigger 示例
//!
//! 演示:
//! - 工作日时间窗口（周一至周五 09:00-17:00）
//! - 跨午夜窗口（每天 22:00 至次日 06:00）
//! - Repeat 模式（窗口内每隔 N 秒重复执行）
//! - CancellationToken 配合窗口结束自动取消
//!
//! 运行: cargo run --example window_trigger

use std::time::Duration;

use chrono::NaiveTime;
use chrono::Timelike;
use scheduler_rs::prelude::*;
use scheduler_rs::trigger::{DayPattern, WindowTrigger};

// ---------------------------------------------------------------------------
// 任务定义
// ---------------------------------------------------------------------------

/// 数据同步任务 — 在时间窗口内持续运行，窗口结束时自动取消
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataSyncTask {
    source: String,
    destination: String,
}

#[async_trait]
impl Task for DataSyncTask {
    const TYPE_NAME: &'static str = "data_sync";

    async fn run(&self, ctx: &TaskContext) -> TaskResult {
        println!(
            "  [DataSync] Syncing {} -> {} (will run until window closes)",
            self.source, self.destination
        );

        let mut batch = 0u64;
        loop {
            if ctx.cancellation_token.is_cancelled() {
                println!("  [DataSync] Window closed, stopping after {batch} batches");
                return Ok(());
            }

            batch += 1;
            println!("  [DataSync] Processed batch {batch}");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

/// 监控采集任务 — 窗口内每隔固定间隔触发一次
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetricsCollectTask {
    endpoint: String,
}

#[async_trait]
impl Task for MetricsCollectTask {
    const TYPE_NAME: &'static str = "metrics_collect";

    async fn run(&self, _ctx: &TaskContext) -> TaskResult {
        println!("  [Metrics] Collected from {}", self.endpoint);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== WindowTrigger 示例 ===\n");

    let scheduler = Scheduler::builder()
        .poll_interval(Duration::from_millis(500))
        .build()?;

    scheduler.register::<DataSyncTask>().await;
    scheduler.register::<MetricsCollectTask>().await;

    // ---------------------------------------------------------------
    // 1. Once 模式 — 工作日 09:00-17:00，窗口开始时触发一次
    //    任务持续运行直到窗口结束（CancellationToken 被取消）
    // ---------------------------------------------------------------
    let weekday_window = WindowTrigger::daily()
        .start_at(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
        .end_at(NaiveTime::from_hms_opt(17, 0, 0).unwrap())
        .on_days(DayPattern::Weekdays(vec![1, 2, 3, 4, 5])); // Mon-Fri

    let sync_id = scheduler
        .schedule(DataSyncTask {
            source: "primary-db".into(),
            destination: "replica-db".into(),
        })
        .trigger(weekday_window)
        .name("weekday-data-sync")
        .max_instances(1)
        .submit()
        .await?;
    println!("Added weekday data sync: {sync_id}");

    // ---------------------------------------------------------------
    // 2. Repeat 模式 — 每天 22:00-06:00（跨午夜），每 2 秒采集一次
    // ---------------------------------------------------------------
    let night_window = WindowTrigger::daily()
        .start_at(NaiveTime::from_hms_opt(22, 0, 0).unwrap())
        .end_at(NaiveTime::from_hms_opt(6, 0, 0).unwrap())
        .repeat(Duration::from_secs(2));

    let metrics_id = scheduler
        .schedule(MetricsCollectTask {
            endpoint: "http://internal:9090/metrics".into(),
        })
        .trigger(night_window)
        .name("night-metrics")
        .submit()
        .await?;
    println!("Added night metrics: {metrics_id}");

    // ---------------------------------------------------------------
    // 3. 为了演示效果，再加一个"当前时间 ± 几秒"的窗口
    //    这样运行示例时能实际看到触发
    // ---------------------------------------------------------------
    let now = chrono::Utc::now().time();
    let start = NaiveTime::from_hms_opt(now.hour(), now.minute(), 0).unwrap();
    let end = start + chrono::Duration::seconds(10);

    let demo_window = WindowTrigger::daily()
        .start_at(start)
        .end_at(end)
        .repeat(Duration::from_secs(2));

    let demo_id = scheduler
        .schedule(MetricsCollectTask {
            endpoint: "http://localhost:8080/health".into(),
        })
        .trigger(demo_window)
        .name("demo-window")
        .submit()
        .await?;
    println!("Added demo window (next ~10s): {demo_id}");

    // ---------------------------------------------------------------
    // 启动并运行 15 秒
    // ---------------------------------------------------------------
    println!("\n--- 启动调度器 (运行 15 秒) ---\n");
    let handle = scheduler.spawn();

    tokio::time::sleep(Duration::from_secs(15)).await;

    // 查看执行历史
    let execs = scheduler.get_executions(&demo_id).await?;
    println!("\n--- demo-window 执行记录: {} 次 ---", execs.len());
    for e in &execs {
        println!(
            "  {} | {:?} | {}",
            e.id,
            e.state,
            e.scheduled_fire_time.format("%H:%M:%S")
        );
    }

    println!("\n--- 停止 ---");
    handle.shutdown().await?;
    println!("=== 完成 ===");
    Ok(())
}
