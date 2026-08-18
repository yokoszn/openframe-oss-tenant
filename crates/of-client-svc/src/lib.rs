//! `of-client-svc` — replaces `openframe-client-core` (the Java agent
//! control plane).
//!
//! ## The collapse
//!
//! The upstream service:
//!
//! - Manages agent registration secrets (`AgentRegistrationSecret`).
//! - Ingests agent heartbeats (`InstalledAgent` updates) — the
//!   `openframe-data-nats` JetStream subscription is the read path.
//! - Schedules RMM scripts (`ScriptScheduled` + `ScriptExecution`).
//! - Publishes live commands (`agent.command.{machine_id}` NATS subject).
//! - Reads the agent MongoDB collection via `TenantAwareMongoTemplate`.
//!
//! All of those reduce to:
//!
//! - `Table<key, V>` for the MongoDB-shaped state.
//! - `sangdb-bindings-nats` for the NATS-shaped subjects.
//! - One process = one hashtrinity store + one JetStream shim.
//!
//! ## Wire surface
//!
//! ```text
//! POST /agents/register                      → mint registration secret
//! POST /agents/{id}/heartbeat                → ingest a heartbeat
//! GET  /agents                                → list installed agents
//! POST /agents/{id}/command                   → enqueue a live command
//! ```

use clap::Parser;
use hashtrinity::Table;
use openframe_domain::{
    AgentRegistrationSecret, InstalledAgent, TenantId,
};
use sangdb_bindings_nats::JetStream;
use serde_json::Value;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8084")]
    bind: String,
}

#[derive(Clone)]
pub struct ClientSvc {
    pub tenant: TenantId,
    /// `secret_id → AgentRegistrationSecret`
    pub secrets: Arc<RwLock<Table<String, AgentRegistrationSecret>>>,
    /// `agent_id → InstalledAgent`
    pub agents: Arc<RwLock<Table<String, InstalledAgent>>>,
    pub js: Arc<JetStream>,
}

impl ClientSvc {
    pub fn new(tenant: TenantId) -> Self {
        Self {
            tenant,
            secrets: Arc::new(RwLock::new(Table::new())),
            agents: Arc::new(RwLock::new(Table::new())),
            js: Arc::new(JetStream::new()),
        }
    }

    pub async fn mint_secret(&self) -> AgentRegistrationSecret {
        let s = AgentRegistrationSecret {
            id: Uuid::new_v4(),
            tenant_id: self.tenant,
            secret_hash: Uuid::new_v4().to_string(),
            expires_at: chrono::Utc::now() + chrono::Duration::hours(24),
            used: false,
        };
        self.secrets.write().await.put(s.id.to_string(), s.clone());
        s
    }

    pub async fn ingest_heartbeat(&self, agent_id: Uuid, device_id: Uuid, version: String) {
        let agent = InstalledAgent {
            id: agent_id,
            tenant_id: self.tenant,
            device_id,
            agent_version: version,
            last_heartbeat: chrono::Utc::now(),
            registered_at: chrono::Utc::now(),
        };
        self.agents.write().await.put(agent_id.to_string(), agent);
        // Publish on the NATS-shaped subject so the dashboard live-tile
        // surface picks it up via its subscription.
        let subject = self.js.for_tenant(self.tenant, "agent.heartbeat");
        let _ = self.js.publish(&subject, serde_json::json!({
            "agent_id": agent_id,
            "device_id": device_id,
            "ts": chrono::Utc::now().timestamp_millis(),
        }).to_string().into_bytes());
    }

    pub async fn enqueue_command(&self, machine_id: Uuid, command: &str) -> anyhow::Result<()> {
        let subject = self.js.for_tenant(self.tenant, &format!("agent.command.{machine_id}"));
        self.js.publish(&subject, command.as_bytes().to_vec()).map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing::info!(bind = %args.bind, "of-client-svc starting");
    let svc = ClientSvc::new(TenantId::nil());

    let listener = TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "of-client-svc listening — replaces openframe-client on :8084");

    loop {
        let (sock, peer) = listener.accept().await?;
        let svc = svc.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, svc).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

async fn handle(mut sock: TcpStream, svc: ClientSvc) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 16 * 1024];
    let n = sock.read(&mut buf).await?;
    if n == 0 { return Ok(()); }
    let raw = std::str::from_utf8(&buf[..n])?;
    let (method, path, body) = parse_request(raw);

    let resp = match (method.as_str(), path.as_str()) {
        ("GET", "/healthz") => (200, "OK", "text/plain", "ok".into()),
        ("POST", "/agents/register") => {
            let s = svc.mint_secret().await;
            (201, "Created", "application/json", serde_json::to_string(&s).unwrap_or_else(|_| "{}".into()))
        }
        ("GET", "/agents") => {
            let agents = svc.agents.read().await;
            let snap: Vec<InstalledAgent> = agents.iter().map(|(_, v)| v.clone()).collect();
            (200, "OK", "application/json", serde_json::to_string(&snap).unwrap_or_else(|_| "[]".into()))
        }
        ("POST", p) if p.starts_with("/agents/") && p.ends_with("/heartbeat") => {
            let agent_id_str = p.trim_start_matches("/agents/").trim_end_matches("/heartbeat");
            let agent_id = Uuid::parse_str(agent_id_str).map_err(|e| anyhow::anyhow!(e))?;
            let v: Value = serde_json::from_str(&body).unwrap_or_default();
            let device_id = v.get("deviceId").and_then(|x| x.as_str())
                .and_then(|s| Uuid::parse_str(s).ok())
                .unwrap_or_else(Uuid::new_v4);
            let version = v.get("version").and_then(|x| x.as_str()).unwrap_or("0.0.0").to_string();
            svc.ingest_heartbeat(agent_id, device_id, version).await;
            (202, "Accepted", "text/plain", "ok".into())
        }
        ("POST", p) if p.starts_with("/agents/") && p.ends_with("/command") => {
            let machine_id_str = p.trim_start_matches("/agents/").trim_end_matches("/command");
            let machine_id = Uuid::parse_str(machine_id_str).map_err(|e| anyhow::anyhow!(e))?;
            svc.enqueue_command(machine_id, &body).await?;
            (202, "Accepted", "text/plain", "ok".into())
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

    #[tokio::test]
    async fn mint_and_heartbeat() {
        let svc = ClientSvc::new(TenantId::nil());
        let secret = svc.mint_secret().await;
        assert!(!secret.used);
        let agent_id = Uuid::new_v4();
        let device_id = Uuid::new_v4();
        svc.ingest_heartbeat(agent_id, device_id, "0.0.1".into()).await;
        let agents = svc.agents.read().await;
        assert!(agents.get(&agent_id.to_string()).is_some());
    }
}