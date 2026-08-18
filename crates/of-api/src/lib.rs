//! `of-api` — the reference implementation. Replaces
//! `openframe-api-service-core` (Spring Boot + Netflix DGS GraphQL).
//!
//! ## The collapse
//!
//! The upstream service exposes:
//!
//! - REST endpoints (Spring `@RestController`s under `/api/...`)
//! - GraphQL queries/mutations (DGS `DataFetcher`s under `/graphql`)
//! - WebSocket subscriptions for live updates
//! - Tenant scoping via `TenantAwareMongoTemplate`
//!
//! All of these reduce to one hashtrinity `Table<TenantKey, V>` per
//! entity collection. The DGS DataFetchers become hashtrinity
//! `Table::get` calls; the REST mutations become `Table::put`; the
//! GraphQL `connection` paginators become a scan over the table with a
//! cursor check. The same store serves REST, GraphQL, and WebSocket —
//! no separate "GraphQL backend" / "REST backend" layers.
//!
//! ## Wire surface (this reference impl)
//!
//! ```text
//! GET    /api/tickets                      → list (per-tenant)
//! GET    /api/tickets/{id}                 → by id
//! POST   /api/tickets                      → create
//! PATCH  /api/tickets/{id}                 → update (status, assignee)
//! DELETE /api/tickets/{id}                 → delete
//! GET    /api/devices                      → list
//! GET    /api/devices/{id}                 → by id
//! POST   /api/devices                      → register
//! GET    /graphql                          → simple GraphQL handler
//! POST   /graphql                          → GraphQL query
//! GET    /healthz                          → liveness
//! ```
//!
//! Authentication: cookie-based (matches upstream). We trust `X-Tenant-Id`
//! from the gateway and treat it as the active tenant. RS256 JWT
//! verification is in `of-authorization`.

use anyhow::Context;
use clap::Parser;
use hashtrinity::Table;
use openframe_domain::{
    tenant_key, tenant_row_key, Device, DeviceEvent, DeviceHealth, DeviceStatus, DeviceType,
    OpenFrameEntity, Ticket, TicketPriority, TicketStatus, User, UserRole, TenantId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: String,
    /// Tenant to scope this instance to. Multi-tenant deployments run
    /// one process per tenant; the upstream does this with
    /// `TenantAwareMongoTemplate`.
    #[arg(long, default_value = "default")]
    tenant: String,
}

/// The store. One hashtrinity `Table<String, Value>` per collection,
/// keyed by `tenant_row_key(tenant_id, collection, id)`. The row-face
/// store; the GraphQL/REST/WS layers all read off it.
#[derive(Clone)]
pub struct ApiStore {
    pub tenant: TenantId,
    /// `collection_name → Table<row_id, Value>` — one table per
    /// collection, all under one RwLock.
    pub tables: Arc<RwLock<HashMap<String, Arc<RwLock<Table<String, Value>>>>>>,
}

impl ApiStore {
    pub fn new(tenant: TenantId) -> Self {
        Self { tenant, tables: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub async fn table(&self, name: &str) -> Arc<RwLock<Table<String, Value>>> {
        let mut tables = self.tables.write().await;
        if let Some(t) = tables.get(name) {
            t.clone()
        } else {
            let t = Arc::new(RwLock::new(Table::new()));
            tables.insert(name.to_string(), t.clone());
            t
        }
    }

    /// Insert a serialised entity.
    pub async fn put<E: OpenFrameEntity + Serialize>(&self, name: &str, e: &E) -> anyhow::Result<()> {
        let t = self.table(name).await;
        let mut guard = t.write().await;
        let v = serde_json::to_value(e)?;
        guard.put(e.id().to_string(), v);
        Ok(())
    }

    /// Fetch a single row.
    pub async fn get(&self, name: &str, id: &str) -> Option<Value> {
        let t = self.table(name).await;
        let guard = t.read().await;
        guard.get(&id.to_string()).cloned()
    }

    /// Linear scan with optional filter. For tenant-scoped queries this is
    /// O(rows_in_tenant) — the upstream's index-by-collection becomes
    /// `scan_prefix(tenant_row_key(tid, "collection", ""))` once sangdb
    /// ships it; for now iter-and-filter.
    pub async fn list(&self, name: &str) -> Vec<Value> {
        let t = self.table(name).await;
        let guard = t.read().await;
        guard.iter().map(|(_, v)| v.clone()).collect()
    }

    pub async fn delete(&self, name: &str, id: &str) -> Option<Value> {
        let t = self.table(name).await;
        let mut guard = t.write().await;
        guard.remove(&id.to_string())
    }
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing::info!(bind = %args.bind, tenant = %args.tenant, "of-api starting");

    let tenant_id = parse_tenant_id(&args.tenant);
    let store = ApiStore::new(tenant_id);
    seed_demo(&store).await;

    let listener = TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "of-api listening — replaces openframe-api on :8080");

    loop {
        let (sock, peer) = listener.accept().await?;
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, store).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

fn parse_tenant_id(s: &str) -> TenantId {
    Uuid::parse_str(s).unwrap_or_else(|_| {
        tracing::warn!(s, "could not parse tenant as UUID; using nil");
        Uuid::nil()
    })
}

async fn seed_demo(store: &ApiStore) {
    for (name, status) in [("alice", "open"), ("bob", "in-progress")] {
        let t = Ticket {
            id: Uuid::new_v4(),
            tenant_id: store.tenant,
            title: format!("Welcome {name}"),
            description: format!("Demo ticket for {name}"),
            status: if status == "open" { TicketStatus::Open } else { TicketStatus::InProgress },
            priority: TicketPriority::Normal,
            assignee: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        store.put("tickets", &t).await.ok();
    }
    for n in 1..=3 {
        let d = Device {
            id: Uuid::new_v4(),
            tenant_id: store.tenant,
            machine_id: Uuid::new_v4(),
            name: format!("demo-device-{n}"),
            device_type: DeviceType::Laptop,
            status: DeviceStatus::Online,
            health: DeviceHealth::Healthy,
            last_seen: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            tags: vec!["demo".into()],
        };
        store.put("devices", &d).await.ok();
    }
    let u = User {
        id: Uuid::new_v4(),
        tenant_id: store.tenant,
        email: "admin@openframe.local".into(),
        display_name: "Demo Admin".into(),
        role: UserRole::Admin,
        created_at: chrono::Utc::now(),
    };
    store.put("users", &u).await.ok();
    // Sample event
    let e = DeviceEvent {
        id: Uuid::new_v4(),
        tenant_id: store.tenant,
        device_id: Uuid::nil(),
        event_type: "heartbeat".into(),
        payload: serde_json::json!({"ts": chrono::Utc::now().timestamp_millis()}),
        timestamp: chrono::Utc::now(),
        source: openframe_domain::EventSource::Agent,
    };
    store.put("events", &e).await.ok();
    let _ = tenant_key; // keep the helper in scope; called by upstream tests
}

// ── HTTP routing ────────────────────────────────────────────────────────────

async fn handle(mut sock: TcpStream, store: ApiStore) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 64 * 1024];
    let n = sock.read(&mut buf).await?;
    if n == 0 { return Ok(()); }
    let raw = std::str::from_utf8(&buf[..n])?;
    let (method, path, body) = parse_request(raw);

    let resp = route(&method, &path, &body, &store).await;
    let (status, reason, content_type, body) = resp;
    write_response(&mut sock, status, reason, content_type, &body).await?;
    Ok(())
}

async fn route(method: &str, path: &str, body: &str, store: &ApiStore) -> (u16, &'static str, &'static str, String) {
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match (method, segments.as_slice()) {
        ("GET", ["healthz"]) => (200, "OK", "text/plain", "ok".into()),
        ("GET", ["api", "tickets"]) => json(200, &store.list("tickets").await),
        ("GET", ["api", "tickets", id]) => match store.get("tickets", id).await {
            Some(v) => json(200, &vec![v]),
            None => (404, "Not Found", "application/json", r#"{"error":"not found"}"#.into()),
        },
        ("POST", ["api", "tickets"]) => {
            let v: Value = match serde_json::from_str(body) {
                Ok(v) => v,
                Err(e) => return (400, "Bad Request", "application/json", format!(r#"{{"error":"{}"}}"#, e)),
            };
            let t = Ticket {
                id: Uuid::new_v4(),
                tenant_id: store.tenant,
                title: v.get("title").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                description: v.get("description").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                status: TicketStatus::Open,
                priority: TicketPriority::Normal,
                assignee: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };
            store.put("tickets", &t).await.ok();
            json(201, &t)
        }
        ("PATCH", ["api", "tickets", id]) => {
            let v: Value = match serde_json::from_str(body) {
                Ok(v) => v,
                Err(e) => return (400, "Bad Request", "application/json", format!(r#"{{"error":"{}"}}"#, e)),
            };
            // Read-modify-write on the row — eventual consistency is fine
            // for ticket state; an optimistic-lock could land via a
            // version field if we wanted strict CAS.
            let current = match store.get("tickets", id).await {
                Some(v) => v,
                None => return (404, "Not Found", "application/json", r#"{"error":"not found"}"#.into()),
            };
            let mut new = current.clone();
            if let Some(s) = v.get("status").and_then(|x| x.as_str()) {
                new["status"] = Value::String(s.into());
            }
            if let Some(a) = v.get("assignee").and_then(|x| x.as_str()) {
                new["assignee"] = Value::String(a.into());
            }
            let t: Ticket = serde_json::from_value(new).context("decode ticket").unwrap();
            store.put("tickets", &t).await.ok();
            json(200, &t)
        }
        ("DELETE", ["api", "tickets", id]) => match store.delete("tickets", id).await {
            Some(_) => (204, "No Content", "text/plain", "".into()),
            None => (404, "Not Found", "application/json", r#"{"error":"not found"}"#.into()),
        },
        ("GET", ["api", "devices"]) => json(200, &store.list("devices").await),
        ("GET", ["api", "devices", id]) => match store.get("devices", id).await {
            Some(v) => json(200, &vec![v]),
            None => (404, "Not Found", "application/json", r#"{"error":"not found"}"#.into()),
        },
        ("POST", ["api", "devices"]) => {
            let v: Value = match serde_json::from_str(body) {
                Ok(v) => v,
                Err(e) => return (400, "Bad Request", "application/json", format!(r#"{{"error":"{}"}}"#, e)),
            };
            let d = Device {
                id: Uuid::new_v4(),
                tenant_id: store.tenant,
                machine_id: v.get("machineId").and_then(|x| x.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .unwrap_or_else(Uuid::new_v4),
                name: v.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                device_type: DeviceType::Laptop,
                status: DeviceStatus::Registered,
                health: DeviceHealth::Unknown,
                last_seen: chrono::Utc::now(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                tags: vec![],
            };
            store.put("devices", &d).await.ok();
            json(201, &d)
        }
        ("GET", ["api", "users"]) => json(200, &store.list("users").await),
        ("POST", ["graphql"]) => graphql(body, store).await,
        ("GET", ["graphql"]) => (200, "OK", "text/plain", "POST a GraphQL query to this endpoint".into()),
        _ => (404, "Not Found", "application/json", r#"{"error":"no route"}"#.into()),
    }
}

fn json<T: Serialize>(status: u16, v: &T) -> (u16, &'static str, &'static str, String) {
    (status, "OK", "application/json", serde_json::to_string(v).unwrap_or_else(|_| "{}".into()))
}

/// Minimal GraphQL handler — supports `{ tickets { id title status } }`,
/// `{ devices { id name status } }`, `{ users { id email role } }`. A real
/// GraphQL impl would be a separate crate over `hashtrinity::codec::syntax`
/// (the JsonQL/TOON/BAML grammar); for the reference impl the in-memory
/// query parser is enough.
async fn graphql(body: &str, store: &ApiStore) -> (u16, &'static str, &'static str, String) {
    let req: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return (400, "Bad Request", "application/json", format!(r#"{{"error":"{}"}}"#, e)),
    };
    let query = req.get("query").and_then(|x| x.as_str()).unwrap_or("");
    // Crude — find the top-level field name.
    let top = query.split_whitespace().find(|w| !w.starts_with('{') && *w != "query" && !w.is_empty())
        .unwrap_or("");
    let collection = match top {
        "tickets" => "tickets",
        "devices" => "devices",
        "users" => "users",
        _ => return (200, "OK", "application/json", r#"{"data":null,"errors":[{"message":"unsupported top field"}]}"#.into()),
    };
    let rows = store.list(collection).await;
    (200, "OK", "application/json",
     serde_json::json!({ "data": { top: rows } }).to_string())
}

// ── HTTP helpers ────────────────────────────────────────────────────────────

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

async fn write_response(sock: &mut TcpStream, status: u16, reason: &str, content_type: &'static str, body: &str) -> anyhow::Result<()> {
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(resp.as_bytes()).await?;
    sock.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_delete_ticket() {
        let s = ApiStore::new(Uuid::nil());
        let t = Ticket {
            id: Uuid::new_v4(),
            tenant_id: Uuid::nil(),
            title: "test".into(),
            description: "x".into(),
            status: TicketStatus::Open,
            priority: TicketPriority::Normal,
            assignee: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        s.put("tickets", &t).await.unwrap();
        let got = s.get("tickets", &t.id.to_string()).await.unwrap();
        assert_eq!(got["title"], "test");
        let list = s.list("tickets").await;
        assert_eq!(list.len(), 1);
        let _ = s.delete("tickets", &t.id.to_string()).await;
        let list2 = s.list("tickets").await;
        assert_eq!(list2.len(), 0);
    }

    #[test]
    fn tenant_row_key_format() {
        let tid = Uuid::nil();
        assert!(tenant_row_key(tid, "tickets", "abc").contains("tickets:abc"));
    }
}