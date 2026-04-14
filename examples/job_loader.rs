//! jobs.toml 动态加载示例
//!
//! 演示:
//! - 从 TOML 文件加载预定义的 job
//! - 文件修改后自动热重载（增删改）
//! - type_name 未注册时优雅跳过
//!
//! 运行: cargo run --example job_loader --features job-loader

use std::time::Duration;

use scheduler_rs::job_loader::JobLoader;
use scheduler_rs::prelude::*;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// 任务定义
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CleanupTask {
    max_age_days: u32,
}

#[async_trait]
impl Task for CleanupTask {
    const TYPE_NAME: &'static str = "cleanup_task";

    async fn run(&self, _ctx: &TaskContext) -> TaskResult {
        println!(
            "  [Cleanup] Cleaning up records older than {} days",
            self.max_age_days
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HeartbeatTask;

#[async_trait]
impl Task for HeartbeatTask {
    const TYPE_NAME: &'static str = "heartbeat_task";

    async fn run(&self, _ctx: &TaskContext) -> TaskResult {
        println!("  [Heartbeat] ping!");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志（可以看到 JobLoader 的 info/warn 日志）
    tracing_subscriber::fmt::init();

    println!("=== jobs.toml 动态加载示例 ===\n");

    // ---------------------------------------------------------------
    // 1. 创建 jobs.toml 文件
    // ---------------------------------------------------------------
    let jobs_path = std::env::temp_dir().join("scheduler_example_jobs.toml");

    std::fs::write(
        &jobs_path,
        r#"
[[jobs]]
name = "cleanup"
type_name = "cleanup_task"
task_data = '{"max_age_days": 30}'

[jobs.trigger]
type = "interval"
interval_secs = 3

[jobs.config]
max_instances = 1
priority = 5
misfire_policy = "coalesce"

[[jobs]]
name = "heartbeat"
type_name = "heartbeat_task"
task_data = '{}'

[jobs.trigger]
type = "interval"
interval_secs = 2
"#,
    )?;
    println!("Created jobs file: {}\n", jobs_path.display());

    // ---------------------------------------------------------------
    // 2. 构建 Scheduler，注册任务类型
    // ---------------------------------------------------------------
    let scheduler = Scheduler::builder()
        .poll_interval(Duration::from_millis(500))
        .build()?;

    scheduler.register::<CleanupTask>().await;
    scheduler.register::<HeartbeatTask>().await;

    // ---------------------------------------------------------------
    // 3. 创建 JobLoader，首次加载
    // ---------------------------------------------------------------
    let loader = JobLoader::new(scheduler.clone(), &jobs_path);
    loader.load().await?;

    let schedules = scheduler.list_schedules().await?;
    println!("--- 加载后有 {} 个 Schedule ---", schedules.len());
    for s in &schedules {
        println!("  {} | {}", s.name.as_deref().unwrap_or("?"), s.type_name);
    }

    // ---------------------------------------------------------------
    // 4. 启动文件监听 + 调度器
    // ---------------------------------------------------------------
    let cancel = CancellationToken::new();
    let _watcher_handle = loader.watch(cancel.clone());
    let scheduler_handle = scheduler.spawn();

    println!("\n--- 调度器运行中 (6 秒后修改 jobs.toml) ---\n");
    tokio::time::sleep(Duration::from_secs(6)).await;

    // ---------------------------------------------------------------
    // 5. 修改 jobs.toml — 删除 heartbeat，修改 cleanup 的参数
    // ---------------------------------------------------------------
    println!("\n--- 修改 jobs.toml: 删除 heartbeat，修改 cleanup ---\n");
    std::fs::write(
        &jobs_path,
        r#"
[[jobs]]
name = "cleanup"
type_name = "cleanup_task"
task_data = '{"max_age_days": 7}'

[jobs.trigger]
type = "interval"
interval_secs = 2

[jobs.config]
max_instances = 2
priority = 10
misfire_policy = "fire_now"
"#,
    )?;

    // 等待热重载（debounce 500ms + reload）
    tokio::time::sleep(Duration::from_secs(3)).await;

    let schedules = scheduler.list_schedules().await?;
    println!("--- 重载后有 {} 个 Schedule ---", schedules.len());
    for s in &schedules {
        println!("  {} | {}", s.name.as_deref().unwrap_or("?"), s.type_name);
    }

    // 再运行 4 秒观察效果
    tokio::time::sleep(Duration::from_secs(4)).await;

    // ---------------------------------------------------------------
    // 6. 停止
    // ---------------------------------------------------------------
    println!("\n--- 停止 ---");
    cancel.cancel(); // 停止文件监听
    scheduler_handle.shutdown().await?;

    // 清理临时文件
    let _ = std::fs::remove_file(&jobs_path);
    println!("=== 完成 ===");
    Ok(())
}
