//! HTTP Remote Executor 示例
//!
//! 演示 Scheduler + Worker 通过 HTTP 通信:
//! - Scheduler 侧: HttpDispatcher + HttpResultReceiver
//! - Worker 侧: WorkerHttpServer
//! - 任务在 Worker 进程中执行，结果通过回调返回
//!
//! 运行: cargo run --example remote_http --features remote-http

use std::sync::Arc;
use std::time::Duration;

use scheduler_rs::prelude::*;
use scheduler_rs::transport::http::{HttpDispatcher, HttpResultReceiver, WorkerHttpServer};
use scheduler_rs::trigger::IntervalTrigger;

// ---------------------------------------------------------------------------
// 任务定义（Scheduler 和 Worker 都需要知道）
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReportTask {
    report_name: String,
    format: String,
}

#[async_trait]
impl Task for ReportTask {
    const TYPE_NAME: &'static str = "generate_report";

    async fn run(&self, _ctx: &TaskContext) -> TaskResult {
        println!(
            "  [Worker] Generating {} report in {} format...",
            self.report_name, self.format
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        println!("  [Worker] Report generated!");
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HeavyComputeTask {
    input_size: u64,
}

#[async_trait]
impl Task for HeavyComputeTask {
    const TYPE_NAME: &'static str = "heavy_compute";

    async fn run(&self, ctx: &TaskContext) -> TaskResult {
        println!(
            "  [Worker] Heavy compute started (input_size={})",
            self.input_size
        );
        for i in 0..5 {
            if ctx.cancellation_token.is_cancelled() {
                println!("  [Worker] Compute cancelled at step {i}");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        println!("  [Worker] Heavy compute finished!");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== HTTP Remote Executor 示例 ===\n");

    // ---------------------------------------------------------------
    // 1. 启动 Worker HTTP 服务器
    // ---------------------------------------------------------------
    let worker = Arc::new(Worker::new());
    worker.register::<ReportTask>().await;
    worker.register::<HeavyComputeTask>().await;

    // Scheduler 的回调地址（Worker 执行完会 POST 结果到这里）
    let scheduler_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let scheduler_addr = scheduler_listener.local_addr()?;
    let callback_url = format!("http://{scheduler_addr}");
    println!("Scheduler result endpoint: {callback_url}/results");

    // Worker 服务器
    let worker_server = WorkerHttpServer::new(worker, &callback_url);
    let worker_router = worker_server.router();

    let worker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let worker_addr = worker_listener.local_addr()?;
    println!("Worker HTTP server: http://{worker_addr}");

    tokio::spawn(async move {
        axum::serve(worker_listener, worker_router).await.unwrap();
    });

    // ---------------------------------------------------------------
    // 2. 创建 Scheduler（使用 RemoteExecutor）
    // ---------------------------------------------------------------
    let dispatcher = Arc::new(HttpDispatcher::new(format!("http://{worker_addr}")));
    let (result_receiver, result_router) = HttpResultReceiver::new();
    let result_receiver = Arc::new(result_receiver);

    // 挂载结果回调路由
    tokio::spawn(async move {
        axum::serve(scheduler_listener, result_router)
            .await
            .unwrap();
    });

    let scheduler = Scheduler::builder()
        .remote_executor(dispatcher, result_receiver)
        .poll_interval(Duration::from_millis(500))
        .build()?;

    // Scheduler 侧也需要注册（用于 type_name 验证和持久化恢复）
    scheduler.register::<ReportTask>().await;
    scheduler.register::<HeavyComputeTask>().await;

    // ---------------------------------------------------------------
    // 3. 添加远程任务
    // ---------------------------------------------------------------
    let report_id = scheduler
        .schedule(ReportTask {
            report_name: "sales-q4".into(),
            format: "pdf".into(),
        })
        .trigger(IntervalTrigger::every(Duration::from_secs(3)))
        .name("quarterly-report")
        .max_instances(1)
        .submit()
        .await?;
    println!("\nAdded remote report task: {report_id}");

    let compute_id = scheduler
        .schedule(HeavyComputeTask {
            input_size: 1_000_000,
        })
        .trigger(IntervalTrigger::every(Duration::from_secs(5)))
        .name("heavy-compute")
        .submit()
        .await?;
    println!("Added remote compute task: {compute_id}");

    // ---------------------------------------------------------------
    // 4. 运行
    // ---------------------------------------------------------------
    println!("\n--- 启动调度器 (运行 12 秒) ---\n");
    let handle = scheduler.spawn();

    tokio::time::sleep(Duration::from_secs(12)).await;

    // 查看执行记录
    println!("\n--- 执行记录 ---");
    for (name, id) in [("report", &report_id), ("compute", &compute_id)] {
        let execs = scheduler.get_executions(id).await?;
        println!("{name}: {} 次执行", execs.len());
        for e in &execs {
            println!(
                "  {} | {:?} | {}",
                e.id,
                e.state,
                e.scheduled_fire_time.format("%H:%M:%S")
            );
        }
    }

    println!("\n--- 停止 ---");
    handle.shutdown().await?;
    println!("=== 完成 ===");
    Ok(())
}
