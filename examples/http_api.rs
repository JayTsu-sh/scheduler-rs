//! HTTP API 创建 Schedule 示例
//!
//! 演示:
//! - 启动 Scheduler 的监控/管理 API
//! - 通过 HTTP POST 创建 schedule
//! - 通过 HTTP GET 查询 schedules 和执行记录
//!
//! 运行: cargo run --example http_api --features api

use std::time::Duration;

use scheduler_rs::prelude::*;

// ---------------------------------------------------------------------------
// 任务定义（必须预先注册才能通过 API 创建）
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NotifyTask {
    channel: String,
    message: String,
}

#[async_trait]
impl Task for NotifyTask {
    const TYPE_NAME: &'static str = "notify";

    async fn run(&self, _ctx: &TaskContext) -> TaskResult {
        println!("  [Notify] #{}: {}", self.channel, self.message);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackupTask {
    database: String,
}

#[async_trait]
impl Task for BackupTask {
    const TYPE_NAME: &'static str = "backup";

    async fn run(&self, _ctx: &TaskContext) -> TaskResult {
        println!("  [Backup] Backing up {}...", self.database);
        tokio::time::sleep(Duration::from_millis(200)).await;
        println!("  [Backup] Done!");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== HTTP API 创建 Schedule 示例 ===\n");

    // ---------------------------------------------------------------
    // 1. 构建 Scheduler，注册任务类型
    // ---------------------------------------------------------------
    let scheduler = Scheduler::builder()
        .poll_interval(Duration::from_millis(500))
        .build()?;

    scheduler.register::<NotifyTask>().await;
    scheduler.register::<BackupTask>().await;

    // ---------------------------------------------------------------
    // 2. 启动 API 服务器
    // ---------------------------------------------------------------
    let router = scheduler.api_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    let api_addr = listener.local_addr()?;
    println!("API server: http://{api_addr}");
    println!("  GET  /api/health");
    println!("  GET  /api/schedules");
    println!("  POST /api/schedules");
    println!("  GET  /api/schedules/:id/executions");

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // 启动调度器
    let handle = scheduler.spawn();

    // ---------------------------------------------------------------
    // 3. 通过 HTTP 创建 Schedule
    // ---------------------------------------------------------------
    let client = reqwest::Client::new();
    println!("\n--- 通过 HTTP API 创建 Schedule ---\n");

    // 3a. 创建一个 interval 触发的通知任务
    let resp = client
        .post(format!("http://{api_addr}/api/schedules"))
        .json(&serde_json::json!({
            "type_name": "notify",
            "task_data": {
                "channel": "alerts",
                "message": "System health check"
            },
            "trigger": {
                "type": "interval",
                "interval_secs": 3
            },
            "name": "health-alert",
            "max_instances": 1,
            "priority": 5
        }))
        .send()
        .await?;
    println!(
        "POST /api/schedules (notify): {} {}",
        resp.status(),
        resp.text().await?
    );

    // 3b. 创建一个 cron 触发的备份任务
    let resp = client
        .post(format!("http://{api_addr}/api/schedules"))
        .json(&serde_json::json!({
            "type_name": "backup",
            "task_data": {
                "database": "production"
            },
            "trigger": {
                "type": "cron",
                "expression": "*/5 * * * * *"
            },
            "name": "db-backup",
            "misfire_policy": "DoNothing"
        }))
        .send()
        .await?;
    println!(
        "POST /api/schedules (backup): {} {}",
        resp.status(),
        resp.text().await?
    );

    // 3c. 创建一个即时触发的一次性任务
    let resp = client
        .post(format!("http://{api_addr}/api/schedules"))
        .json(&serde_json::json!({
            "type_name": "notify",
            "task_data": {
                "channel": "deploy",
                "message": "Deployment complete!"
            },
            "trigger": {
                "type": "immediate"
            },
            "name": "deploy-notification"
        }))
        .send()
        .await?;
    println!(
        "POST /api/schedules (immediate): {} {}",
        resp.status(),
        resp.text().await?
    );

    // 3d. 尝试创建未注册类型的任务 — 应返回 400
    let resp = client
        .post(format!("http://{api_addr}/api/schedules"))
        .json(&serde_json::json!({
            "type_name": "unknown_task",
            "task_data": {},
            "trigger": { "type": "immediate" }
        }))
        .send()
        .await?;
    println!(
        "POST /api/schedules (unknown): {} {}",
        resp.status(),
        resp.text().await?
    );

    // ---------------------------------------------------------------
    // 4. 查询 API
    // ---------------------------------------------------------------
    println!("\n--- 等待 8 秒后查询 ---\n");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // 查看健康状态
    let health: serde_json::Value = client
        .get(format!("http://{api_addr}/api/health"))
        .send()
        .await?
        .json()
        .await?;
    println!(
        "GET /api/health: {}",
        serde_json::to_string_pretty(&health)?
    );

    // 列出所有 schedule
    let schedules: Vec<serde_json::Value> = client
        .get(format!("http://{api_addr}/api/schedules"))
        .send()
        .await?
        .json()
        .await?;
    println!("\nGET /api/schedules: {} schedules", schedules.len());
    for s in &schedules {
        println!(
            "  {} | {} | {}",
            s["id"].as_str().unwrap_or("?"),
            s["name"].as_str().unwrap_or(&s["type_name"].to_string()),
            s["state"].as_str().unwrap_or("?"),
        );
    }

    // 查看某个 schedule 的执行记录
    if let Some(first) = schedules.first() {
        let id = first["id"].as_str().unwrap();
        let execs: Vec<serde_json::Value> = client
            .get(format!("http://{api_addr}/api/schedules/{id}/executions"))
            .send()
            .await?
            .json()
            .await?;
        let name = first["name"].as_str().unwrap_or("?");
        println!(
            "\nGET /api/schedules/{id}/executions ({name}): {} 条",
            execs.len()
        );
        for e in execs.iter().take(5) {
            println!(
                "  {} | {}",
                e["id"].as_str().unwrap_or("?"),
                e["state"].as_str().unwrap_or("?"),
            );
        }
    }

    // ---------------------------------------------------------------
    // 5. 通过 API 暂停/恢复/删除
    // ---------------------------------------------------------------
    if let Some(s) = schedules.iter().find(|s| s["name"] == "health-alert") {
        let id = s["id"].as_str().unwrap();
        println!("\n--- 暂停 health-alert ---");
        let resp = client
            .post(format!("http://{api_addr}/api/schedules/{id}/pause"))
            .send()
            .await?;
        println!("POST .../pause: {}", resp.status());

        tokio::time::sleep(Duration::from_secs(2)).await;

        println!("--- 恢复 health-alert ---");
        let resp = client
            .post(format!("http://{api_addr}/api/schedules/{id}/resume"))
            .send()
            .await?;
        println!("POST .../resume: {}", resp.status());
    }

    // ---------------------------------------------------------------
    // 6. 停止
    // ---------------------------------------------------------------
    println!("\n--- 停止调度器 ---");
    handle.shutdown().await?;
    println!("=== 完成 ===");
    Ok(())
}
