//! `of-external-api` — replaces `openframe-external-api-service-core`.
//!
//! The upstream service exposes `/api/v1/**` rate-limited endpoints with
//! API-key auth. The reference impl serves the same shape from the same
//! hashtrinity store `of-api` uses; the API-key check is a
//! `Table<key_hash, ApiKey>` lookup.
//!
//! ```text
//! GET  /api/v1/tickets       → list (requires X-API-Key header)
//! POST /api/v1/tickets       → create
//! ```

use clap::Parser;
use hashtrinity::Table;
use openframe_domain::{ApiKey, ApiKeyType, TenantId, Ticket, TicketPriority, TicketStatus};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8083")]
    bind: String,
}

#[derive(Clone)]
pub struct ExternalApi {
    pub tenant: TenantId,
    /// API key registry — `key_id → ApiKey`. In production this would be a
    /// argon2-hashed key with a stable lookup id.
    pub keys: Arc<std::sync::Mutex<Table<String, ApiKey>>>,
    /// Per-key rate counter (calls per minute). Fold: `increment` = `add(1)`.
    pub rate: Arc<std::sync::Mutex<Table<String, i64>>>,
    /// The tickets store. Shared with `of-api` in production via a
    /// process boundary; in the reference impl both services run in
    /// the same process for the demo (separate binds, separate state).
    pub tickets: Arc<std::sync::Mutex<Table<String, Ticket>>>,
}

impl ExternalApi {
    pub fn new(tenant: TenantId) -> Self {
        let me = Self {
            tenant,
            keys: Arc::new(std::sync::Mutex::new(Table::new())),
            rate: Arc::new(std::sync::Mutex::new(Table::new())),
            tickets: Arc::new(std::sync::Mutex::new(Table::new())),
        };
        me.seed();
        me
    }

    fn seed(&self) {
        let demo_key = ApiKey {
            id: Uuid::new_v4(),
            tenant_id: self.tenant,
            key_hash: "demo-key-hash".into(),
            key_type: ApiKeyType::Public,
            label: "demo".into(),
            created_at: chrono::Utc::now(),
            expires_at: None,
            revoked: false,
        };
        self.keys.lock().unwrap().put("demo".to_string(), demo_key);
    }

    pub fn check_key(&self, key_id: &str) -> bool {
        self.keys.lock().unwrap().get(&key_id.to_string()).map(|k| !k.revoked).unwrap_or(false)
    }

    pub fn bump_rate(&self, key_id: &str) -> i64 {
        let mut rate = self.rate.lock().unwrap();
        let cur = rate.get(&key_id.to_string()).copied().unwrap_or(0);
        let next = cur + 1;
        rate.put(key_id.to_string(), next);
        next
    }
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing::info!(bind = %args.bind, "of-external-api starting");
    let api = ExternalApi::new(TenantId::nil());

    let listener = TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "of-external-api listening — replaces openframe-external-api on :8083");

    loop {
        let (sock, peer) = listener.accept().await?;
        let api = api.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, api).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

async fn handle(mut sock: TcpStream, api: ExternalApi) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 32 * 1024];
    let n = sock.read(&mut buf).await?;
    if n == 0 { return Ok(()); }
    let raw = std::str::from_utf8(&buf[..n])?;
    let (method, path, body) = parse_request(raw);
    let api_key = extract_header(raw, "X-API-Key").unwrap_or_default();

    let resp = match (method.as_str(), path.as_str()) {
        ("GET", "/healthz") => (200, "OK", "text/plain", "ok".into()),
        ("GET", "/api/v1/tickets") => {
            if !api.check_key(&api_key) {
                (401, "Unauthorized", "application/json", r#"{"error":"invalid api key"}"#.into())
            } else {
                let count = api.bump_rate(&api_key);
                tracing::debug!(api_key, count, "rate tick");
                let tickets = api.tickets.lock().unwrap();
                let snapshot: Vec<Ticket> = tickets.iter().map(|(_, v)| v.clone()).collect();
                (200, "OK", "application/json", serde_json::to_string(&snapshot).unwrap_or_else(|_| "[]".into()))
            }
        }
        ("POST", "/api/v1/tickets") => {
            if !api.check_key(&api_key) {
                (401, "Unauthorized", "application/json", r#"{"error":"invalid api key"}"#.into())
            } else {
                let v: Value = serde_json::from_str(&body).unwrap_or_default();
                let t = Ticket {
                    id: Uuid::new_v4(),
                    tenant_id: api.tenant,
                    title: v.get("title").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                    description: v.get("description").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                    status: TicketStatus::Open,
                    priority: TicketPriority::Normal,
                    assignee: None,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                };
                let mut tickets = api.tickets.lock().unwrap();
                tickets.put(t.id.to_string(), t.clone());
                let _ = HashMap::<String, i64>::new(); // suppress unused
                (201, "Created", "application/json", serde_json::to_string(&t).unwrap_or_else(|_| "{}".into()))
            }
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

fn extract_header(raw: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}:");
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix(&prefix) {
            return Some(rest.trim().to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_key_is_valid() {
        let api = ExternalApi::new(TenantId::nil());
        assert!(api.check_key("demo"));
        assert!(!api.check_key("nope"));
        let count = api.bump_rate("demo");
        assert!(count >= 1);
    }
}