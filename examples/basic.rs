//! scheduler-rs 完整使用示例
//!
//! 运行: cargo run --example basic

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveTime;
use scheduler_rs::prelude::*;
use scheduler_rs::trigger::{
    CronTrigger, DayOfMonth, IntervalTrigger, MonthlyTrigger, OnceTrigger,
};

// ============================================================================
// 1. 定义任务 —— struct 即参数，trait 即行为
// ============================================================================

/// 发送邮件任务
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendEmailTask {
    to: String,
    subject: String,
    body: String,
}

#[async_trait]
impl Task for SendEmailTask {
    const TYPE_NAME: &'static str = "send_email";

    async fn run(&self, _ctx: &TaskContext) -> TaskResult {
        println!("  [SendEmail] To: {}, Subject: {}", self.to, self.subject);
        // 模拟发送耗时
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("  [SendEmail] Sent successfully!");
        Ok(())
    }
}

/// 数据清理任务（带计数器，展示可观测性）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CleanupTask {
    table: String,
    retention_days: u32,
}

#[async_trait]
impl Task for CleanupTask {
    const TYPE_NAME: &'static str = "cleanup";

    async fn run(&self, ctx: &TaskContext) -> TaskResult {
        println!(
            "  [Cleanup] Cleaning table '{}', retention: {} days",
            self.table, self.retention_days
        );

        // 展示如何配合 CancellationToken（用于 WindowTrigger 或 shutdown）
        for i in 0..3 {
            if ctx.cancellation_token.is_cancelled() {
                println!("  [Cleanup] Cancelled, stopping gracefully");
                return Ok(());
            }
            println!("  [Cleanup] Processing batch {}...", i + 1);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        println!("  [Cleanup] Done!");
        Ok(())
    }
}

/// 心跳任务（高频，用于展示 IntervalTrigger）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HeartbeatTask {
    service_name: String,
}

#[async_trait]
impl Task for HeartbeatTask {
    const TYPE_NAME: &'static str = "heartbeat";

    async fn run(&self, _ctx: &TaskContext) -> TaskResult {
        println!("  [Heartbeat] {} is alive", self.service_name);
        Ok(())
    }
}

// ============================================================================
// 2. 主程序
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== scheduler-rs 示例 ===\n");

    // ------------------------------------------------------------------
    // 2.1 构建调度器
    // ------------------------------------------------------------------
    let scheduler = Scheduler::builder()
        .default_misfire_policy(MisfirePolicy::Coalesce)
        .max_concurrent_jobs(10)
        .poll_interval(Duration::from_millis(500))
        .build()?;

    // 注册任务类型（持久化恢复时需要知道如何反序列化）
    scheduler.register::<SendEmailTask>().await;
    scheduler.register::<CleanupTask>().await;
    scheduler.register::<HeartbeatTask>().await;

    // ------------------------------------------------------------------
    // 2.2 注册事件监听器
    // ------------------------------------------------------------------
    let event_count = Arc::new(AtomicU32::new(0));
    let event_count_clone = event_count.clone();
    scheduler.on_event(move |event| {
        event_count_clone.fetch_add(1, Ordering::Relaxed);
        match event {
            SchedulerEvent::ExecutionStarted { schedule_id, .. } => {
                println!("  [Event] Execution started for schedule {}", schedule_id);
            }
            SchedulerEvent::ExecutionSucceeded { schedule_id, .. } => {
                println!("  [Event] Execution succeeded for schedule {}", schedule_id);
            }
            SchedulerEvent::ExecutionFailed { schedule_id, error, .. } => {
                println!("  [Event] Execution FAILED for schedule {}: {}", schedule_id, error);
            }
            _ => {}
        }
    });

    // ------------------------------------------------------------------
    // 2.3 添加任务 —— 简洁 API
    // ------------------------------------------------------------------
    println!("--- 添加任务 ---\n");

    // 每 2 秒执行一次心跳
    let heartbeat_id = scheduler
        .add(
            HeartbeatTask {
                service_name: "api-server".into(),
            },
            IntervalTrigger::every(Duration::from_secs(2)),
        )
        .await?;
    println!("Added heartbeat schedule: {}", heartbeat_id);

    // 一次性任务：1 秒后发送欢迎邮件
    let welcome_id = scheduler
        .add(
            SendEmailTask {
                to: "newuser@example.com".into(),
                subject: "Welcome!".into(),
                body: "Welcome to our platform.".into(),
            },
            OnceTrigger::after(Duration::from_secs(1)),
        )
        .await?;
    println!("Added one-shot welcome email: {}", welcome_id);

    // ------------------------------------------------------------------
    // 2.4 添加任务 —— Builder API（精细控制）
    // ------------------------------------------------------------------

    // 每 3 秒执行清理任务，misfire 时跳过，最大并发 1
    let cleanup_id = scheduler
        .schedule(CleanupTask {
            table: "user_sessions".into(),
            retention_days: 30,
        })
        .trigger(IntervalTrigger::every(Duration::from_secs(3)))
        .name("session-cleanup")
        .misfire_policy(MisfirePolicy::DoNothing)
        .max_instances(1)
        .recovery_policy(RecoveryPolicy::Skip)
        .submit()
        .await?;
    println!("Added cleanup schedule: {}", cleanup_id);

    // ------------------------------------------------------------------
    // 2.5 同一个 Task 类型，多个 Schedule（不同参数、不同触发器）
    // ------------------------------------------------------------------

    // 日报邮件 —— 使用 cron 触发（这里用每 5 秒模拟）
    let daily_report_id = scheduler
        .add(
            SendEmailTask {
                to: "team@example.com".into(),
                subject: "Daily Report".into(),
                body: "Here is your daily report.".into(),
            },
            CronTrigger::new("*/5 * * * * *")?, // 每 5 秒（模拟每日）
        )
        .await?;
    println!("Added daily report email: {}", daily_report_id);

    // 月报邮件 —— 使用 MonthlyTrigger（每月最后一天）
    // 注意：这个在示例运行期间不会触发，仅展示 API
    let monthly_report_id = scheduler
        .add(
            SendEmailTask {
                to: "finance@example.com".into(),
                subject: "Monthly Report".into(),
                body: "End of month financial summary.".into(),
            },
            MonthlyTrigger::new()
                .on_last_day()
                .on_day(DayOfMonth::Last(-3)) // 也在倒数第 3 天触发
                .at(NaiveTime::from_hms_opt(18, 0, 0).unwrap()),
        )
        .await?;
    println!("Added monthly report email: {}", monthly_report_id);

    // ------------------------------------------------------------------
    // 2.6 查看所有 Schedule
    // ------------------------------------------------------------------

    let schedules = scheduler.list_schedules().await?;
    println!("\n--- 当前 {} 个 Schedule ---", schedules.len());
    for s in &schedules {
        println!(
            "  {} | {} | {:?} | next: {:?}",
            s.id,
            s.name.as_deref().unwrap_or(&s.type_name),
            s.state,
            s.next_fire_time.map(|t| t.format("%H:%M:%S").to_string())
        );
    }

    // ------------------------------------------------------------------
    // 2.7 启动调度器（后台运行 8 秒）
    // ------------------------------------------------------------------
    println!("\n--- 启动调度器（运行 8 秒）---\n");

    let handle = scheduler.spawn();

    // 让调度器运行一段时间
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ------------------------------------------------------------------
    // 2.8 运行中管理：暂停和恢复
    // ------------------------------------------------------------------
    println!("\n--- 暂停心跳任务 ---");
    scheduler.pause(&heartbeat_id).await?;
    println!("Heartbeat paused. Waiting 3 seconds...\n");

    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("\n--- 恢复心跳任务 ---");
    scheduler.resume(&heartbeat_id).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // ------------------------------------------------------------------
    // 2.9 查看执行历史
    // ------------------------------------------------------------------
    let heartbeat_execs = scheduler.get_executions(&heartbeat_id).await?;
    println!(
        "\n--- 心跳任务执行历史: {} 次 ---",
        heartbeat_execs.len()
    );
    for exec in heartbeat_execs.iter().take(5) {
        println!(
            "  {} | {:?} | {}",
            exec.id,
            exec.state,
            exec.scheduled_fire_time.format("%H:%M:%S")
        );
    }

    // ------------------------------------------------------------------
    // 2.10 停止调度器
    // ------------------------------------------------------------------
    println!("\n--- 停止调度器 ---");
    handle.shutdown().await?;

    println!(
        "\nTotal events received: {}",
        event_count.load(Ordering::Relaxed)
    );
    println!("\n=== 示例完成 ===");

    Ok(())
}
