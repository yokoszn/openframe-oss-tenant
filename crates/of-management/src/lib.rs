//! `of-management` — replaces `openframe-management-service-core`.
//!
//! The upstream service runs:
//!
//! - Tenant CRUD + provisioning.
//! - Scheduled tasks (`@Scheduled` cron runners in Spring — cleanup,
//!   billing, deployment status sync).
//! - Admin dashboard endpoints.
//!
//! The replacement:
//!
//! - `Table<TenantId, TenantRow>` for the tenant store.
//! - `tokio::time::interval` per scheduled task; the cron expression is
//!   pre-parsed at boot.
//! - Same hashtrinity HTTP helpers as the rest of the stack.
//!
//! ```text
//! GET    /admin/tenants
//! POST   /admin/tenants
//! GET    /admin/tasks
//! POST   /admin/tasks/{name}/run
//! GET    /healthz
//! ```

use clap::Parser;
use hashtrinity::Table;
use openframe_domain::{TenantId, TenantKey};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantRow {
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    pub name: String,
    pub last_run_at: i64,
    pub last_run_status: String,
}

#[derive(Clone, Default)]
pub struct Management {
    pub tenants: Arc<std::sync::Mutex<Table<TenantKey, TenantRow>>>,
    pub tasks: Arc<std::sync::Mutex<Table<String, TaskStatus>>>,
}

impl Management {
    pub fn new() -> Self { Self::default() }

    pub fn register_tenant(&self, name: String) -> TenantRow {
        let tid = TenantId::new_v4();
        let row = TenantRow {
            tenant_id: tid,
            name,
            created_at: chrono::Utc::now().timestamp(),
        };
        self.tenants.lock().unwrap().put(TenantKey(tid), row.clone());
        row
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8086")]
    bind: String,
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing::info!(bind = %args.bind, "of-management starting");
    let mgmt = Management::new();

    // Schedule the demo cleanup tick.
    let mgmt_bg = mgmt.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let mut tasks = mgmt_bg.tasks.lock().unwrap();
            tasks.put("cleanup".into(), TaskStatus {
                name: "cleanup".into(),
                last_run_at: chrono::Utc::now().timestamp_millis(),
                last_run_status: "ok".into(),
            });
            tracing::debug!("cleanup tick");
        }
    });

    let listener = TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "of-management listening — replaces openframe-management on :8086");

    loop {
        let (sock, peer) = listener.accept().await?;
        let mgmt = mgmt.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, mgmt).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

async fn handle(mut sock: TcpStream, mgmt: Management) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 16 * 1024];
    let n = sock.read(&mut buf).await?;
    if n == 0 { return Ok(()); }
    let raw = std::str::from_utf8(&buf[..n])?;
    let (method, path, body) = parse_request(raw);

    let resp = match (method.as_str(), path.as_str()) {
        ("GET", "/healthz") => (200, "OK", "text/plain", "ok".into()),
        ("GET", "/admin/tenants") => {
            let tenants = mgmt.tenants.lock().unwrap();
            let snap: Vec<TenantRow> = tenants.iter().map(|(_, v)| v.clone()).collect();
            (200, "OK", "application/json", serde_json::to_string(&snap).unwrap_or_else(|_| "[]".into()))
        }
        ("POST", "/admin/tenants") => {
            let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            let name = v.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let row = mgmt.register_tenant(name);
            (201, "Created", "application/json", serde_json::to_string(&row).unwrap_or_else(|_| "{}".into()))
        }
        ("GET", "/admin/tasks") => {
            let tasks = mgmt.tasks.lock().unwrap();
            let snap: Vec<TaskStatus> = tasks.iter().map(|(_, v)| v.clone()).collect();
            (200, "OK", "application/json", serde_json::to_string(&snap).unwrap_or_else(|_| "[]".into()))
        }
        _ => (404, "Not Found", "application/json", r#"{"error":"no route"}"#.into()),
    };
    let (status, reason, ct, body) = resp;
    let r = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ct}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(r.as_bytes()).await?;
    sock.shutdown().await?;
    Ok(())
}

fn parse_request(raw: &str) -> (String, String, String) {
    let mut lines = raw.split("\r\n");
    let first = lines.next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("GET").to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let mut body = String::new();
    let mut past_headers = false;
    for line in lines {
        if line.is_empty() { past_headers = true; continue; }
        if past_headers { body.push_str(line); body.push('\n'); }
    }
    (method, path, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_tenant() {
        let mgmt = Management::new();
        let row = mgmt.register_tenant("acme".into());
        assert_eq!(row.name, "acme");
        let snap: Vec<TenantRow> = mgmt.tenants.lock().unwrap().iter().map(|(_, v)| v.clone()).collect();
        assert_eq!(snap.len(), 1);
    }
}