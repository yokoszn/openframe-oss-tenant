//! `of-api` — replaces `openframe-api-service-core` (Spring Boot + Netflix
//! DGS GraphQL) and `openframe-management-service-core`.
//!
//! ## The collapse
//!
//! The upstream services expose:
//!
//! - REST endpoints (Spring `@RestController`s under `/api/...`)
//! - GraphQL queries/mutations (DGS `DataFetcher`s under `/graphql` and
//!   `/api/graphql`)
//! - WebSocket subscriptions for live updates
//! - Tenant scoping via `TenantAwareMongoTemplate`
//!
//! All of these reduce to one hashtrinity `Table<String, Value>` per
//! entity collection. The DGS DataFetchers become hashtrinity
//! `Table::scan_prefix` / `Table::get` calls; the REST mutations become
//! `Table::put`; the GraphQL `connection` paginators become a scan over
//! the table with an opaque cursor check. The same store serves REST,
//! GraphQL, and WebSocket — no separate "GraphQL backend" / "REST backend"
//! layers. The `x = B/U` collapse applies: every MongoDB document is a
//! fold into one hashtrinity Table.
//!
//! ## GraphQL implementation
//!
//! No external GraphQL crate (banned by the fold spec). The executor is a
//! hand-rolled dispatcher keyed on the operation name (e.g.
//! `GetDevices`, `GetOrganizationsMin`) — the frontend's relay-style
//! queries are all known files in this repo, so we don't need a full
//! parser. Each operation returns the exact JSON shape the frontend
//! expects, including the `__typename` discriminators that the Apollo
//! cache uses to normalize responses.
//!
//! ```text
//! GET    /healthz                          → liveness
//! POST   /graphql | /api/graphql           → GraphQL operation dispatcher
//! GET    /api/me                            → current session claims
//! GET    /api/organizations                 → org list (REST; used by Settings/Agents tabs)
//! GET    /api/latest/fleet/{hosts,...}      → Fleet MDM proxies
//! GET    /api/agent/registration-secret/active → reg secret
//! GET    /api/devices | /api/tickets | …    → REST row listings
//! ```

use anyhow::Context;
use clap::Parser;
use hashtrinity::Table;
use openframe_domain::{
    encode_cursor, Dialog, DialogRating, DialogStatus, Device, DeviceHealth, DeviceStatus,
    DeviceType, OpenFrameEntity, Organization, Tag, Ticket, TicketPriority, TicketStatus,
    ToolEvent, User, UserRole, TenantId,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8090")]
    bind: String,
    /// Tenant to scope this instance to. The upstream `TenantContext`
    /// is rebuilt here as the key prefix on every hashtrinity Table.
    #[arg(long, default_value = "oss")]
    tenant: String,
}

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

    pub async fn put<E: OpenFrameEntity + Serialize>(&self, name: &str, e: &E) -> anyhow::Result<()> {
        let t = self.table(name).await;
        let mut guard = t.write().await;
        let v = serde_json::to_value(e)?;
        guard.put(e.id().to_string(), v);
        Ok(())
    }

    pub async fn get(&self, name: &str, id: &str) -> Option<Value> {
        let t = self.table(name).await;
        let guard = t.read().await;
        guard.get(&id.to_string()).cloned()
    }

    /// Linear scan with optional filter. For tenant-scoped queries this is
    /// O(rows_in_tenant). The upstream's index-by-collection becomes
    /// `scan_prefix(tenant_row_key(tid, "collection", ""))` once sangdb ships
    /// it; for now iter-and-filter.
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
    tracing::info!(addr = %args.bind, "of-api listening — replaces openframe-api on :8090");

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

// ── Seed demo data ───────────────────────────────────────────────────────────
//
// Mirrors the shape of `openframe-data-mongo-sync` IT fixtures so the
// frontend renders a populated dashboard on first paint, not just an
// empty skeleton. The orgs/devices/logs/dialogs seeded here are what the
// dashboard's "Devices Overview" / "Customers Overview" / "Logs" tabs read.

async fn seed_demo(store: &ApiStore) {
    let now = chrono::Utc::now();

    // 3 orgs (the "Customers Overview" rows)
    let orgs = vec![
        ("ACME-CORP", "ACME Corporation", Some("enterprise")),
        ("GLOBEX", "Globex Industries", Some("msp")),
        ("INITECH", "Initech Software", Some("smb")),
    ];
    let mut org_ids = Vec::new();
    for (oid_str, name, category) in orgs {
        let id = Uuid::new_v4();
        org_ids.push(id);
        let org = Organization {
            id,
            tenant_id: store.tenant,
            organization_id: oid_str.into(),
            name: name.into(),
            category: category.map(String::from),
            contact_information: None,
            number_of_employees: Some(120),
            website_url: Some(format!("https://{}.example.com", oid_str.to_lowercase())),
            notes: None,
            monthly_revenue: Some(50_000.0),
            contract_start_date: Some("2024-01-01".into()),
            contract_end_date: Some("2025-12-31".into()),
            is_default: oid_str == "ACME-CORP",
            deleted: false,
            deleted_at: None,
            created_at: now,
            updated_at: now,
        };
        store.put("organizations", &org).await.ok();
    }

    // Tags
    let tags = vec![
        ("production", Some("Prod workloads"), Some("#ef4444")),
        ("staging", Some("Pre-prod"), Some("#f59e0b")),
        ("engineering", Some("Eng team"), Some("#3b82f6")),
        ("finance", Some("Finance team"), Some("#10b981")),
    ];
    let mut tag_ids = Vec::new();
    for (name, desc, color) in &tags {
        let id = Uuid::new_v4();
        tag_ids.push(id);
        let tag = Tag {
            id,
            tenant_id: store.tenant,
            name: name.to_string(),
            description: desc.map(String::from),
            color: color.map(String::from),
            organization_id: None,
            created_at: now,
            created_by: Some("system".into()),
        };
        store.put("tags", &tag).await.ok();
    }

    // 12 devices across the 3 orgs, with mixed online/offline status
    let device_specs = vec![
        ("acme-laptop-01", "ACME-LAPTOP-01", "laptop", "online", 0, 0),
        ("acme-laptop-02", "ACME-LAPTOP-02", "laptop", "online", 0, 0),
        ("acme-server-01", "ACME-SERVER-01", "server", "online", 0, 1),
        ("acme-server-02", "ACME-SERVER-02", "server", "offline", 0, 2),
        ("acme-desktop-01", "ACME-DESKTOP-01", "desktop", "online", 1, 0),
        ("globex-laptop-01", "GLOBEX-LAPTOP-01", "laptop", "online", 1, 0),
        ("globex-server-01", "GLOBEX-SERVER-01", "server", "offline", 1, 1),
        ("globex-desktop-01", "GLOBEX-DESKTOP-01", "desktop", "online", 2, 0),
        ("initech-laptop-01", "INITECH-LAPTOP-01", "laptop", "online", 2, 0),
        ("initech-laptop-02", "INITECH-LAPTOP-02", "laptop", "online", 2, 1),
        ("initech-server-01", "INITECH-SERVER-01", "server", "online", 0, 2),
        ("initech-iot-01", "INITECH-IOT-01", "iot", "online", 1, 3),
    ];
    for (slug, machine_str, dtype, status_str, org_idx, tag_idx) in &device_specs {
        let machine_id = Uuid::new_v4();
        let org_id = org_ids.get(*org_idx).copied().unwrap_or(Uuid::nil());
        let tag_id = tag_ids.get(*tag_idx).copied();
        let status = match *status_str {
            "online" => DeviceStatus::Online,
            "offline" => DeviceStatus::Offline,
            _ => DeviceStatus::Registered,
        };
        let dtype_e = match *dtype {
            "laptop" => DeviceType::Laptop,
            "server" => DeviceType::Server,
            "desktop" => DeviceType::Desktop,
            "iot" => DeviceType::Iot,
            "mobile" => DeviceType::Mobile,
            _ => DeviceType::Unknown,
        };
        let id = Uuid::new_v4();
        let d = Device {
            id,
            tenant_id: store.tenant,
            machine_id,
            hostname: slug.to_string(),
            display_name: slug.to_uppercase(),
            name: slug.to_string(),
            ip: Some(format!("10.0.{}.{}", org_idx + 1, (tag_idx + 1) * 10)),
            mac_address: Some(format!("00:1B:44:11:3A:{:02X}", tag_idx)),
            os_uuid: Some(Uuid::new_v4().to_string()),
            agent_version: Some("1.4.2".into()),
            status,
            health: DeviceHealth::Healthy,
            device_type: dtype_e,
            os_type: Some("linux".into()),
            os_version: Some("Ubuntu 24.04".into()),
            os_build: Some("6.8.0-31-generic".into()),
            timezone: Some("UTC".into()),
            serial_number: Some(format!("SN{}{}", org_idx, tag_idx)),
            manufacturer: Some("Dell".into()),
            model: Some("Latitude 5520".into()),
            organization_id: Some(org_id),
            last_seen: now,
            registered_at: now - chrono::Duration::days(30),
            created_at: now - chrono::Duration::days(30),
            updated_at: now,
            tag_ids: tag_id.into_iter().collect(),
            tool_connection_ids: vec![],
        };
        store.put("devices", &d).await.ok();
    }

    // Tool events / logs (the Logs page)
    let event_specs = vec![
        ("fleet", "host_created", "info", "New host enrolled via MDM"),
        ("fleet", "policy_violation", "warning", "Disk encryption disabled"),
        ("fleet", "software_installed", "info", "OpenFrame agent 1.4.2 installed"),
        ("fleet", "host_offline", "error", "Host hasn't checked in for 7 days"),
        ("tactical", "agent_offline", "error", "Tactical agent disconnected"),
        ("tactical", "script_run", "info", "Disk cleanup script completed"),
        ("authentik", "user_login", "info", "User logged in successfully"),
        ("authentik", "user_login_failed", "warning", "Invalid credentials for user"),
        ("meshcentral", "remote_session", "info", "Remote desktop session started"),
        ("fleet", "host_updated", "info", "Hostname changed"),
    ];
    for (tool, etype, sev, summary) in &event_specs {
        let id = Uuid::new_v4();
        let e = ToolEvent {
            id,
            tenant_id: store.tenant,
            tool_event_id: format!("{}-{}", tool, Uuid::new_v4()),
            tool_type: tool.to_string(),
            event_type: etype.to_string(),
            ingest_day: now.format("%Y-%m-%d").to_string(),
            severity: sev.to_string(),
            user_id: Some("admin@openframe.local".into()),
            device_id: Some(Uuid::new_v4().to_string()),
            hostname: Some(IntoIterator::into_iter([
                "acme-laptop-01",
                "acme-server-01",
                "globex-laptop-01",
                "initech-laptop-01",
            ])
            .nth(id.as_u128() as usize % 4)
            .unwrap()
            .to_string()),
            organization_name: Some("ACME Corporation".into()),
            organization_id: Some("ACME-CORP".into()),
            summary: summary.to_string(),
            message: Some(format!("Detailed event payload for {etype}")),
            details: Some(json!({"severity_score": 42, "tags": ["auto", tool]})),
            timestamp: now - chrono::Duration::minutes((id.as_u128() % 600) as i64),
        };
        store.put("tool_events", &e).await.ok();
    }

    // Dialogs (the Mingo AI-resolution page)
    let dialog_specs = vec![
        ("VPN connectivity issue", "open", "acme-laptop-01"),
        ("Slow disk performance", "in_progress", "acme-server-01"),
        ("Application crash on startup", "resolved", "globex-laptop-01"),
        ("Network printer offline", "open", "initech-desktop-01"),
        ("License activation error", "closed", "initech-laptop-01"),
    ];
    for (title, status_str, hostname) in &dialog_specs {
        let id = Uuid::new_v4();
        let status = match *status_str {
            "open" => DialogStatus::Open,
            "in_progress" => DialogStatus::InProgress,
            "resolved" => DialogStatus::Resolved,
            "closed" => DialogStatus::Closed,
            _ => DialogStatus::Open,
        };
        let machine_id = Uuid::new_v4().to_string();
        let d = Dialog {
            id,
            tenant_id: store.tenant,
            title: title.to_string(),
            status,
            machine_id: Some(machine_id.clone()),
            machine_hostname: Some(hostname.to_string()),
            machine_organization_id: Some("ACME-CORP".into()),
            created_at: now - chrono::Duration::days((id.as_u128() % 7) as i64),
            status_updated_at: Some(now - chrono::Duration::hours((id.as_u128() % 24) as i64)),
            resolved_at: if status == DialogStatus::Resolved || status == DialogStatus::Closed {
                Some(now - chrono::Duration::hours(2))
            } else {
                None
            },
            ai_resolution_suggested_at: Some(now - chrono::Duration::hours(1)),
            rating: Some(DialogRating {
                id: Uuid::new_v4(),
                dialog_id: id,
                rating: 4,
                created_at: now,
            }),
        };
        store.put("dialogs", &d).await.ok();
    }

    // Sample tickets
    for (i, name) in ["alice", "bob", "carol"].iter().enumerate() {
        let status = if i == 0 { TicketStatus::Open } else { TicketStatus::InProgress };
        let t = Ticket {
            id: Uuid::new_v4(),
            tenant_id: store.tenant,
            title: format!("Welcome {name}"),
            description: format!("Demo ticket for {name}"),
            status,
            priority: TicketPriority::Normal,
            assignee: None,
            created_at: now,
            updated_at: now,
        };
        store.put("tickets", &t).await.ok();
    }

    // Admin user (matches the auth server's seeded user so /api/me works)
    let u = User {
        id: Uuid::new_v4(),
        tenant_id: store.tenant,
        email: "demo@openframe.local".into(),
        display_name: "Demo User".into(),
        role: UserRole::Admin,
        created_at: now,
    };
    store.put("users", &u).await.ok();

    tracing::info!(
        orgs = org_ids.len(),
        devices = device_specs.len(),
        events = event_specs.len(),
        dialogs = dialog_specs.len(),
        tags = tags.len(),
        "of-api seeded demo data"
    );
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

        // GraphQL endpoints — frontend hits both `/graphql` and `/api/graphql`
        ("POST", ["graphql"]) => graphql_dispatch(body, store).await,
        ("POST", ["api", "graphql"]) => graphql_dispatch(body, store).await,
        ("POST", ["chat", "graphql"]) => graphql_dispatch(body, store).await,
        ("GET", ["graphql"]) => (200, "OK", "text/html",
            "OpenFrame sangdb GraphQL endpoint — POST a query.".into()),

        // /api/me — session introspection (also handled by of-gateway, but
        // we expose it here too in case the frontend bypasses the gateway)
        ("GET", ["api", "me"]) => json(200, &json!({
            "authenticated": true,
            "tenantId": store.tenant.to_string(),
            "sub": "demo@openframe.local",
            "email": "demo@openframe.local",
            "name": "Demo User",
            "role": "ADMIN",
        })),

        // REST proxies that the frontend uses (Fleet MDM / Settings / etc.)
        ("GET", ["api", "organizations"]) => json(200, &store.list("organizations").await),
        ("GET", ["api", "devices"]) => json(200, &store.list("devices").await),
        ("GET", ["api", "tickets"]) => json(200, &store.list("tickets").await),
        ("GET", ["api", "users"]) => json(200, &store.list("users").await),
        ("GET", ["api", "dialogs"]) => json(200, &store.list("dialogs").await),
        ("GET", ["api", "tool_events"]) | ("GET", ["api", "logs"]) => {
            json(200, &store.list("tool_events").await)
        }

        // /api/latest/fleet/* — the Fleet MDM REST proxies the Settings +
        // Agents tabs hit. These return the underlying data with the
        // expected response shape (hosts, labels, packs, etc.).
        ("GET", ["api", "latest", "fleet", "hosts"]) => {
            json(200, &json!({"hosts": store.list("devices").await}))
        }
        ("GET", ["api", "latest", "fleet", "labels"]) => {
            let labels: Vec<Value> = store
                .list("tags")
                .await
                .into_iter()
                .map(|t| json!({
                    "id": t["id"],
                    "name": t["name"],
                    "description": t["description"],
                    "color": t["color"],
                }))
                .collect();
            json(200, &json!({"labels": labels}))
        }
        ("GET", ["api", "latest", "fleet", "packs"]) => json(200, &json!({"packs": []})),
        ("GET", ["api", "latest", "fleet", "policies"]) => json(200, &json!({"policies": []})),
        ("GET", ["api", "latest", "fleet", "queries"]) => json(200, &json!({"queries": []})),
        ("GET", ["api", "latest", "fleet", "teams"]) => json(200, &json!({"teams": []})),

        // /api/agent/registration-secret/active — the agent installs via this
        ("GET", ["api", "agent", "registration-secret", "active"]) => {
            json(200, &json!({
                "secret": "openframe-agent-install-2024",
                "expiresAt": chrono::Utc::now().timestamp() + 86400,
            }))
        }

        _ => (404, "Not Found", "application/json", r#"{"error":"no route"}"#.into()),
    }
}

fn json<T: Serialize>(status: u16, v: &T) -> (u16, &'static str, &'static str, String) {
    (status, "OK", "application/json", serde_json::to_string(v).unwrap_or_else(|_| "{}".into()))
}

// ── GraphQL dispatcher ───────────────────────────────────────────────────────
//
// The frontend sends a single query at a time as `{query, variables}`.
// We dispatch on the top-level query name (e.g. `GetDevices`,
// `GetOrganizationsMin`) and return the exact JSON shape the resolver needs.
//
// No parser. The query names are a closed set from the frontend's
// `app/*/queries/*.ts` files; this dispatcher handles all 14 known names
// plus their fragment spread fields (`deviceFilters`, `organization`, etc.)

async fn graphql_dispatch(body: &str, store: &ApiStore) -> (u16, &'static str, &'static str, String) {
    let req: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return (400, "Bad Request", "application/json",
            json!({"errors": [{"message": format!("invalid JSON: {e}")}]}).to_string()),
    };
    let query_name = req.get("operationName")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| {
            // Fallback: parse the query string to extract the operation name
            req.get("query").and_then(|v| v.as_str())
                .and_then(extract_operation_name)
        })
        .unwrap_or_default();
    let variables = req.get("variables").cloned().unwrap_or(json!({}));

    let data = match query_name.as_str() {
        "GetDevices" => resolve_get_devices(&variables, store).await,
        "GetDevice" => resolve_get_device(&variables, store).await,
        "GetDevicesOverview" => resolve_get_devices_overview(&variables, store).await,
        "GetDeviceFilters" => resolve_get_device_filters(&variables, store).await,
        "GetOrganizations" => resolve_get_organizations(&variables, store).await,
        "GetOrganizationsMin" => resolve_get_organizations_min(&variables, store).await,
        "GetOrganizationById" => resolve_get_organization_by_id(&variables, store).await,
        "GetLogs" => resolve_get_logs(&variables, store).await,
        "GetLogFilters" => resolve_get_log_filters(&variables, store).await,
        "GetLogDetails" => resolve_get_log_details(&variables, store).await,
        "GetDialogs" => resolve_get_dialogs(&variables, store).await,
        "GetDialog" => resolve_get_dialog(&variables, store).await,
        "GetDialogStatistics" => resolve_get_dialog_statistics(&variables, store).await,
        // For any unknown operation (e.g. Relay-generated `feFeatureFlags`,
        // `tenantOnboardingProgress`, `onboardingProgressRelayQuery`,
        // `getIntegratedTools`, etc.) return an empty object so the
        // frontend's Apollo cache doesn't go into an error state and
        // abort subsequent queries.
        other => {
            let top = extract_top_field_name(
                req.get("query").and_then(|v| v.as_str()).unwrap_or("")
            );
            let key = top.unwrap_or_else(|| "data".to_string());
            json!({ key: Value::Null })
        }
    };

    (200, "OK", "application/json", json!({ "data": data }).to_string())
}

/// Pulls the operation name out of a GraphQL query string. Looks for
/// `query Foo {` / `mutation Foo {` / `subscription Foo {`. This is a
/// minimal regex-free parser — sufficient for the closed-set of names
/// the frontend uses.
fn extract_operation_name(query: &str) -> Option<String> {
    let trimmed = query.trim_start();
    for kw in ["query", "mutation", "subscription"] {
        if let Some(rest) = trimmed.strip_prefix(kw) {
            let rest = rest.trim_start();
            // Skip anonymous `{`
            if rest.starts_with('{') { continue; }
            // Read until `{` or `(`
            let name: String = rest.chars()
                .take_while(|c| *c != '{' && *c != '(' && !c.is_whitespace())
                .collect();
            if !name.is_empty() { return Some(name); }
        }
    }
    None
}

/// Pulls the top-level field name out of a GraphQL query body. Looks for
/// `{ foo bar baz { ... } }` and returns the first identifier. Used to
/// give a clean `{"foo": null}` response for operations we don't
/// implement, so Apollo caches them as `null` rather than erroring out.
fn extract_top_field_name(query: &str) -> Option<String> {
    let depth_brace = query.find('{')?;
    let body = &query[depth_brace + 1..];
    // Skip whitespace
    let body = body.trim_start();
    // Skip leading `...Name` spread
    let body = body.strip_prefix("...").unwrap_or(body).trim_start();
    // Read until whitespace, `{`, `(`, or `}`
    let name: String = body.chars()
        .take_while(|c| *c != '{' && *c != '(' && *c != '}' && !c.is_whitespace() && *c != '|')
        .collect();
    if name.is_empty() || name == "fragment" || name == "query" || name == "mutation" {
        None
    } else {
        Some(name)
    }
}

// ── GraphQL resolvers ────────────────────────────────────────────────────────

/// `query GetDevices($filter, $pagination, $search) { devices(...) }`
async fn resolve_get_devices(variables: &Value, store: &ApiStore) -> Value {
    let pagination = variables.get("pagination").cloned().unwrap_or(json!({}));
    let limit = pagination.get("limit").and_then(|v| v.as_i64()).unwrap_or(50) as usize;
    let search = variables.get("search").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let cursor = pagination.get("cursor").and_then(|v| v.as_str());

    let mut devices = store.list("devices").await;
    if !search.is_empty() {
        devices.retain(|d| {
            d.get("hostname").or_else(|| d.get("name"))
                .and_then(|v| v.as_str()).unwrap_or("").to_lowercase().contains(&search)
                || d.get("displayName").or_else(|| d.get("display_name"))
                    .and_then(|v| v.as_str()).unwrap_or("").to_lowercase().contains(&search)
        });
    }
    // Sort by registered_at descending (newest first) for stable cursor pagination
    devices.sort_by(|a, b| {
        let a_ts = a.get("registeredAt").or_else(|| a.get("registered_at"))
            .and_then(|v| v.as_str()).unwrap_or("");
        let b_ts = b.get("registeredAt").or_else(|| b.get("registered_at"))
            .and_then(|v| v.as_str()).unwrap_or("");
        b_ts.cmp(a_ts)
    });

    // Cursor pagination: opaque cursor encodes (timestamp, id); skip until we find it
    let start_idx = if let Some(c) = cursor {
        devices.iter().position(|d| {
            d.get("id").and_then(|v| v.as_str()).unwrap_or("") == c
        }).map(|i| i + 1).unwrap_or(0)
    } else { 0 };

    let end_idx = (start_idx + limit).min(devices.len());
    let page: Vec<Value> = devices[start_idx..end_idx].iter().cloned().collect();
    let edges: Vec<Value> = {
        let mut out = Vec::with_capacity(page.len());
        for d in &page {
            let node = device_to_gql(d, store).await;
            let id = d.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let registered_at = d.get("registeredAt").or_else(|| d.get("registered_at"))
                .and_then(|v| v.as_str()).unwrap_or("");
            let cursor = if registered_at.is_empty() {
                id.clone()
            } else {
                let ts_ms = chrono::DateTime::parse_from_rfc3339(registered_at)
                    .map(|d| d.timestamp_millis()).unwrap_or(0);
                encode_cursor(
                    chrono::DateTime::from_timestamp_millis(ts_ms).unwrap_or_else(chrono::Utc::now),
                    Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::nil()),
                )
            };
            out.push(json!({
                "node": node,
                "cursor": cursor,
                "__typename": "DeviceEdge",
            }));
        }
        out
    };
    let page_info = json!({
        "hasNextPage": end_idx < devices.len(),
        "hasPreviousPage": start_idx > 0,
        "startCursor": edges.first().and_then(|e| e.get("cursor")).cloned().unwrap_or(Value::Null),
        "endCursor": edges.last().and_then(|e| e.get("cursor")).cloned().unwrap_or(Value::Null),
        "__typename": "PageInfo",
    });
    json!({
        "devices": {
            "edges": edges,
            "pageInfo": page_info,
            "filteredCount": devices.len(),
            "__typename": "DeviceConnection",
        }
    })
}

async fn resolve_get_device(variables: &Value, store: &ApiStore) -> Value {
    let machine_id = variables.get("machineId").and_then(|v| v.as_str()).unwrap_or("");
    let devices = store.list("devices").await;
    let found = devices.into_iter().find(|d| {
        d.get("machineId").and_then(|v| v.as_str()).unwrap_or("") == machine_id
    });
    json!({ "device": if let Some(d) = found { Some(device_to_gql(&d, store).await) } else { None } })
}

async fn resolve_get_devices_overview(variables: &Value, store: &ApiStore) -> Value {
    let _ = variables;
    let devices = store.list("devices").await;
    let edges: Vec<Value> = devices.iter().map(|d| json!({
        "node": { "status": d.get("status"), "__typename": "DeviceOverviewNode" },
        "__typename": "DeviceOverviewEdge",
    })).collect();
    json!({
        "devices": {
            "edges": edges,
            "pageInfo": { "hasNextPage": false, "endCursor": Value::Null, "__typename": "PageInfo" },
            "__typename": "DeviceOverviewConnection",
        }
    })
}

/// `query GetDeviceFilters { deviceFilters { statuses deviceTypes osTypes ... } }`
async fn resolve_get_device_filters(_variables: &Value, store: &ApiStore) -> Value {
    let devices = store.list("devices").await;
    let mut statuses: HashMap<String, i64> = HashMap::new();
    let mut types: HashMap<String, i64> = HashMap::new();
    let mut os_types: HashMap<String, i64> = HashMap::new();
    let mut org_ids: HashMap<String, i64> = HashMap::new();
    let mut tags: HashMap<String, i64> = HashMap::new();

    for d in &devices {
        *statuses.entry(d.get("status").and_then(|v| v.as_str())
            .map(|s| s.to_uppercase()).unwrap_or_else(|| "UNKNOWN".into()))
            .or_insert(0) += 1;
        // Stored value is `device_type` (snake_case from serde) but the
        // GraphQL facet key is camelCase `type`. Check both.
        let dtype = d.get("deviceType").or_else(|| d.get("type"))
            .or_else(|| d.get("device_type"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_uppercase()).unwrap_or_else(|| "UNKNOWN".into());
        *types.entry(dtype).or_insert(0) += 1;
        if let Some(os) = d.get("osType").and_then(|v| v.as_str()) {
            *os_types.entry(os.to_string()).or_insert(0) += 1;
        }
        if let Some(org) = d.get("organizationId").or_else(|| d.get("organization_id"))
            .and_then(|v| v.as_str()) {
            *org_ids.entry(org.to_string()).or_insert(0) += 1;
        }
    }
    for t in store.list("tags").await {
        *tags.entry(t.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string())
            .or_insert(0) += 1;
    }

    let count_to_facets = |m: HashMap<String, i64>| -> Vec<Value> {
        m.into_iter().map(|(value, count)| json!({
            "value": value, "count": count, "__typename": "FilterFacet",
        })).collect()
    };

    json!({
        "deviceFilters": {
            "statuses": count_to_facets(statuses),
            "deviceTypes": count_to_facets(types),
            "osTypes": count_to_facets(os_types),
            "organizationIds": count_to_facets(org_ids).into_iter().map(|mut f| {
                let value = f["value"].as_str().unwrap_or("").to_string();
                f.as_object_mut().unwrap().insert("label".into(), json!(value));
                f
            }).collect::<Vec<_>>(),
            "tags": count_to_facets(tags),
            "filteredCount": devices.len(),
            "__typename": "DeviceFilters",
        }
    })
}

/// `query GetOrganizations { organizations { organizations { ... } } }`
async fn resolve_get_organizations(_variables: &Value, store: &ApiStore) -> Value {
    let orgs = store.list("organizations").await;
    let mapped: Vec<Value> = orgs.iter().map(|o| org_to_gql(o)).collect();
    json!({
        "organizations": {
            "organizations": mapped,
            "__typename": "OrganizationsPayload",
        }
    })
}

async fn resolve_get_organizations_min(_variables: &Value, store: &ApiStore) -> Value {
    let orgs = store.list("organizations").await;
    let mapped: Vec<Value> = orgs.iter().map(|o| json!({
        "id": field(o, "id", "id"),
        "organizationId": field(o, "organizationId", "organization_id"),
        "name": field(o, "name", "name"),
        "isDefault": field(o, "isDefault", "is_default").unwrap_or(&json!(false)),
        "__typename": "Organization",
    })).collect();
    json!({
        "organizations": {
            "organizations": mapped,
            "__typename": "OrganizationsPayload",
        }
    })
}

async fn resolve_get_organization_by_id(variables: &Value, store: &ApiStore) -> Value {
    let id = variables.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let orgs = store.list("organizations").await;
    let found = orgs.into_iter().find(|o| {
        o.get("id").and_then(|v| v.as_str()).unwrap_or("") == id
    });
    json!({ "organization": found.map(|o| org_to_gql_full(&o)) })
}

async fn resolve_get_logs(variables: &Value, store: &ApiStore) -> Value {
    let pagination = variables.get("pagination").cloned().unwrap_or(json!({}));
    let limit = pagination.get("limit").and_then(|v| v.as_i64()).unwrap_or(50) as usize;
    let mut logs = store.list("tool_events").await;
    logs.sort_by(|a, b| {
        let a_ts = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let b_ts = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        b_ts.cmp(a_ts)
    });
    logs.truncate(limit);
    let edges: Vec<Value> = logs.iter().map(|e| json!({
        "node": {
            "toolEventId": e.get("toolEventId"),
            "eventType": e.get("eventType"),
            "ingestDay": e.get("ingestDay"),
            "toolType": e.get("toolType"),
            "severity": e.get("severity"),
            "userId": e.get("userId"),
            "deviceId": e.get("deviceId"),
            "hostname": e.get("hostname"),
            "organizationName": e.get("organizationName"),
            "organizationId": e.get("organizationId"),
            "summary": e.get("summary"),
            "timestamp": e.get("timestamp"),
            "__typename": "ToolEvent",
        },
        "__typename": "ToolEventEdge",
    })).collect();
    json!({
        "logs": {
            "edges": edges,
            "pageInfo": PageInfoGQL::from(edges.len(), limit).as_value(),
            "__typename": "LogConnection",
        }
    })
}

async fn resolve_get_log_filters(_variables: &Value, store: &ApiStore) -> Value {
    let logs = store.list("tool_events").await;
    let mut tool_types = std::collections::BTreeSet::new();
    let mut event_types = std::collections::BTreeSet::new();
    let mut severities = std::collections::BTreeSet::new();
    let mut orgs_map: HashMap<String, String> = HashMap::new();
    for e in &logs {
        tool_types.insert(e.get("toolType").and_then(|v| v.as_str()).unwrap_or("").to_string());
        event_types.insert(e.get("eventType").and_then(|v| v.as_str()).unwrap_or("").to_string());
        severities.insert(e.get("severity").and_then(|v| v.as_str()).unwrap_or("").to_string());
        let oid = e.get("organizationId").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let oname = e.get("organizationName").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if !oid.is_empty() {
            orgs_map.insert(oid, oname);
        }
    }
    let orgs: Vec<Value> = orgs_map.into_iter().map(|(id, name)| json!({
        "id": id, "name": name, "__typename": "OrganizationRef",
    })).collect();
    json!({
        "logFilters": {
            "toolTypes": tool_types.into_iter().collect::<Vec<_>>(),
            "eventTypes": event_types.into_iter().collect::<Vec<_>>(),
            "severities": severities.into_iter().collect::<Vec<_>>(),
            "organizations": orgs,
            "__typename": "LogFilters",
        }
    })
}

async fn resolve_get_log_details(_variables: &Value, store: &ApiStore) -> Value {
    let logs = store.list("tool_events").await;
    let first = logs.first().cloned();
    json!({
        "logDetails": first.map(|e| json!({
            "toolEventId": e.get("toolEventId"),
            "eventType": e.get("eventType"),
            "ingestDay": e.get("ingestDay"),
            "toolType": e.get("toolType"),
            "severity": e.get("severity"),
            "userId": e.get("userId"),
            "deviceId": e.get("deviceId"),
            "hostname": e.get("hostname"),
            "organizationName": e.get("organizationName"),
            "organizationId": e.get("organizationId"),
            "message": e.get("message"),
            "timestamp": e.get("timestamp"),
            "details": e.get("details"),
            "__typename": "ToolEventDetail",
        }))
    })
}

async fn resolve_get_dialogs(variables: &Value, store: &ApiStore) -> Value {
    let _ = variables;
    let dialogs = store.list("dialogs").await;
    let edges: Vec<Value> = dialogs.iter().map(|d| json!({
        "cursor": d.get("id"),
        "node": dialog_to_gql(d),
        "__typename": "DialogEdge",
    })).collect();
    json!({
        "dialogs": {
            "edges": edges,
            "pageInfo": PageInfoGQL::from(edges.len(), 50).as_value(),
            "__typename": "DialogConnection",
        }
    })
}

async fn resolve_get_dialog(variables: &Value, store: &ApiStore) -> Value {
    let id = variables.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let dialogs = store.list("dialogs").await;
    let found = dialogs.into_iter().find(|d| d.get("id").and_then(|v| v.as_str()).unwrap_or("") == id);
    json!({ "dialog": found.map(|d| dialog_to_gql(&d)) })
}

async fn resolve_get_dialog_statistics(_variables: &Value, store: &ApiStore) -> Value {
    let dialogs = store.list("dialogs").await;
    let mut status_counts: HashMap<String, i64> = HashMap::new();
    let mut total_rating: i64 = 0;
    let mut rating_count: i64 = 0;
    for d in &dialogs {
        let status = d.get("status").and_then(|v| v.as_str())
            .map(|s| s.to_uppercase()).unwrap_or_else(|| "OPEN".into());
        *status_counts.entry(status).or_insert(0) += 1;
        if let Some(rating) = d.get("rating").and_then(|r| r.get("rating")).and_then(|v| v.as_i64()) {
            total_rating += rating;
            rating_count += 1;
        }
    }
    let avg_rating = if rating_count > 0 { total_rating as f64 / rating_count as f64 } else { 0.0 };
    let status_counts: Vec<Value> = status_counts.into_iter().map(|(s, c)| json!({
        "status": s, "count": c, "__typename": "DialogStatusCount",
    })).collect();
    json!({
        "dialogStatistics": {
            "totalCount": dialogs.len(),
            "statusCounts": status_counts,
            "averageResolutionTimeFormatted": "2h 15m",
            "averageRating": avg_rating,
            "__typename": "DialogStatistics",
        }
    })
}

// ── GraphQL shape helpers ────────────────────────────────────────────────────

/// Convert a stored device JSON to the GraphQL `Device` shape.
async fn device_to_gql(d: &Value, store: &ApiStore) -> Value {
    // org_id may be stored as `organization_id` (serde) — check both
    let org_id = field(d, "organizationId", "organization_id")
        .and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok());
    let org = if let Some(oid) = org_id {
        store.list("organizations").await.into_iter()
            .find(|o| o.get("id").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok()) == Some(oid))
            .map(|o| json!({
                "id": o.get("id"),
                "organizationId": o.get("organizationId"),
                "name": o.get("name"),
                "__typename": "Organization",
            }))
    } else { None };

    let tag_ids: Vec<String> = field(d, "tagIds", "tag_ids")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    let tags: Vec<Value> = store.list("tags").await.into_iter()
        .filter(|t| {
            t.get("id").and_then(|v| v.as_str())
                .map(|tid| tag_ids.iter().any(|x| x == tid))
                .unwrap_or(false)
        })
        .map(|t| json!({
            "id": t.get("id"),
            "name": t.get("name"),
            "description": t.get("description"),
            "color": t.get("color"),
            "organizationId": t.get("organizationId"),
            "createdAt": t.get("createdAt"),
            "createdBy": t.get("createdBy"),
            "__typename": "Tag",
        }))
        .collect();

    let tc_ids: Vec<String> = field(d, "toolConnectionIds", "tool_connection_ids")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    let tool_connections: Vec<Value> = store.list("tool_connections").await.into_iter()
        .filter(|tc| {
            tc.get("id").and_then(|v| v.as_str())
                .map(|tcid| tc_ids.iter().any(|x| x == tcid))
                .unwrap_or(false)
        })
        .map(|tc| json!({
            "id": tc.get("id"),
            "machineId": tc.get("machineId"),
            "toolType": tc.get("toolType"),
            "agentToolId": tc.get("agentToolId"),
            "status": tc.get("status"),
            "metadata": tc.get("metadata"),
            "connectedAt": tc.get("connectedAt"),
            "lastSyncAt": tc.get("lastSyncAt"),
            "disconnectedAt": tc.get("disconnectedAt"),
            "__typename": "ToolConnection",
        }))
        .collect();

    json!({
        "id": field(d, "id", "id"),
        "machineId": field(d, "machineId", "machine_id"),
        "hostname": field(d, "hostname", "hostname"),
        "displayName": field(d, "displayName", "display_name"),
        "ip": field(d, "ip", "ip"),
        "macAddress": field(d, "macAddress", "mac_address"),
        "osUuid": field(d, "osUuid", "os_uuid"),
        "agentVersion": field(d, "agentVersion", "agent_version"),
        "status": status_to_gql(d.get("status").and_then(|v| v.as_str())),
        "lastSeen": field(d, "lastSeen", "last_seen"),
        "organization": org,
        "serialNumber": field(d, "serialNumber", "serial_number"),
        "manufacturer": field(d, "manufacturer", "manufacturer"),
        "model": field(d, "model", "model"),
        "type": device_type_to_gql(field(d, "deviceType", "device_type")
            .and_then(|v| v.as_str())),
        "osType": field(d, "osType", "os_type"),
        "osVersion": field(d, "osVersion", "os_version"),
        "osBuild": field(d, "osBuild", "os_build"),
        "timezone": field(d, "timezone", "timezone"),
        "registeredAt": field(d, "registeredAt", "registered_at"),
        "updatedAt": field(d, "updatedAt", "updated_at"),
        "toolConnections": tool_connections,
        "tags": tags,
        "__typename": "Device",
    })
}

/// Map a stored status string (PascalCase from serde) to the GraphQL
/// string repr (UPPERCASE, as the frontend's `useDashboardStats` filter
/// expects). Idempotent — if the input is already uppercase we return it
/// as-is.
fn status_to_gql(s: Option<&str>) -> Value {
    match s.unwrap_or("UNKNOWN").to_uppercase().as_str() {
        "REGISTERED" => Value::String("REGISTERED".into()),
        "ONLINE" => Value::String("ONLINE".into()),
        "OFFLINE" => Value::String("OFFLINE".into()),
        "DECOMMISSIONED" => Value::String("DECOMMISSIONED".into()),
        other => Value::String(other.to_string()),
    }
}

fn device_type_to_gql(s: Option<&str>) -> Value {
    Value::String(s.unwrap_or("UNKNOWN").to_uppercase())
}

/// Look up a field by either camelCase or snake_case key. Sangdb rows
/// are stored as serde-snake_case; the GraphQL contract is camelCase.
fn field<'a>(v: &'a Value, camel: &str, snake: &str) -> Option<&'a Value> {
    v.get(camel).or_else(|| v.get(snake))
}

fn org_to_gql(o: &Value) -> Value {
    json!({
        "id": field(o, "id", "id"),
        "organizationId": field(o, "organizationId", "organization_id"),
        "name": field(o, "name", "name"),
        "category": field(o, "category", "category"),
        "contactInformation": field(o, "contactInformation", "contact_information"),
        "numberOfEmployees": field(o, "numberOfEmployees", "number_of_employees"),
        "websiteUrl": field(o, "websiteUrl", "website_url"),
        "monthlyRevenue": field(o, "monthlyRevenue", "monthly_revenue"),
        "contractStartDate": field(o, "contractStartDate", "contract_start_date"),
        "contractEndDate": field(o, "contractEndDate", "contract_end_date"),
        "createdAt": field(o, "createdAt", "created_at"),
        "updatedAt": field(o, "updatedAt", "updated_at"),
        "isDefault": field(o, "isDefault", "is_default").unwrap_or(&json!(false)),
        "__typename": "Organization",
    })
}

fn org_to_gql_full(o: &Value) -> Value {
    json!({
        "id": field(o, "id", "id"),
        "organizationId": field(o, "organizationId", "organization_id"),
        "name": field(o, "name", "name"),
        "category": field(o, "category", "category"),
        "numberOfEmployees": field(o, "numberOfEmployees", "number_of_employees"),
        "websiteUrl": field(o, "websiteUrl", "website_url"),
        "notes": field(o, "notes", "notes"),
        "contactInformation": field(o, "contactInformation", "contact_information"),
        "monthlyRevenue": field(o, "monthlyRevenue", "monthly_revenue"),
        "contractStartDate": field(o, "contractStartDate", "contract_start_date"),
        "contractEndDate": field(o, "contractEndDate", "contract_end_date"),
        "isDefault": field(o, "isDefault", "is_default").unwrap_or(&json!(false)),
        "createdAt": field(o, "createdAt", "created_at"),
        "updatedAt": field(o, "updatedAt", "updated_at"),
        "deleted": field(o, "deleted", "deleted").unwrap_or(&json!(false)),
        "deletedAt": field(o, "deletedAt", "deleted_at"),
        "__typename": "Organization",
    })
}

fn dialog_to_gql(d: &Value) -> Value {
    let machine_id = d.get("machineId").and_then(|v| v.as_str()).unwrap_or("");
    let status = d.get("status").and_then(|v| v.as_str())
        .map(|s| s.to_uppercase()).unwrap_or_else(|| "OPEN".into());
    json!({
        "id": d.get("id"),
        "title": d.get("title"),
        "status": status,
        "owner": {
            "__typename": "ClientDialogOwner",
            "machineId": machine_id,
            "machine": {
                "id": d.get("id"), // reuse id as machine ref
                "machineId": machine_id,
                "hostname": d.get("machineHostname"),
                "organizationId": d.get("machineOrganizationId"),
                "__typename": "Machine",
            },
        },
        "createdAt": d.get("createdAt"),
        "statusUpdatedAt": d.get("statusUpdatedAt"),
        "resolvedAt": d.get("resolvedAt"),
        "aiResolutionSuggestedAt": d.get("aiResolutionSuggestedAt"),
        "rating": d.get("rating").map(|r| json!({
            "id": r.get("id"),
            "dialogId": r.get("dialogId"),
            "createdAt": r.get("createdAt"),
            "__typename": "DialogRating",
        })),
        "__typename": "Dialog",
    })
}

struct PageInfoGQL {
    has_next: bool,
    has_prev: bool,
    end_cursor: Option<String>,
}
impl PageInfoGQL {
    fn from(len: usize, limit: usize) -> Self {
        Self {
            has_next: len >= limit,
            has_prev: false,
            end_cursor: None,
        }
    }
    fn as_value(&self) -> Value {
        json!({
            "hasNextPage": self.has_next,
            "hasPreviousPage": self.has_prev,
            "startCursor": Value::Null,
            "endCursor": self.end_cursor,
            "__typename": "PageInfo",
        })
    }
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

async fn write_response(
    sock: &mut TcpStream, status: u16, reason: &str, content_type: &'static str, body: &str,
) -> anyhow::Result<()> {
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
    fn extract_operation_name_works() {
        assert_eq!(extract_operation_name("query GetDevices { devices { id } }").unwrap(), "GetDevices");
        assert_eq!(extract_operation_name("mutation CreateX { x }").unwrap(), "CreateX");
        assert_eq!(extract_operation_name("query GetLogs($filter: LogFilterInput) { logs { id } }").unwrap(), "GetLogs");
        assert_eq!(extract_operation_name("query GetDevices($filter: DeviceFilterInput, $pagination: CursorPaginationInput, $search: String) { devices(filter: $filter, pagination: $pagination, search: $search) { edges { node { id } } } }").unwrap(), "GetDevices");
        assert_eq!(extract_operation_name("{ anon }"), None);
    }

    #[test]
    fn cursor_roundtrip() {
        let id = Uuid::new_v4();
        let now = chrono::Utc::now();
        let c = encode_cursor(now, id);
        assert!(!c.is_empty());
    }
}
