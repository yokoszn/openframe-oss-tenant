//! OpenFrame shared domain types — the data model that was split across
//! `openframe-data-mongo-common`, `openframe-data-device-aspect`,
//! `openframe-data-timeentry-aspect`, `openframe-data-apikey-aspect` in the
//! upstream `openframe-oss-lib`. Every type here:
//!
//! - serializes to/from [`hashtrinity::codec`] (the universal Term encoder,
//!   format-agnostic — JSON, BAML, TOON, POML all read as the same Term)
//! - persists on `hashtrinity::Table<K, V>` via the sangdb row face (column
//!   face for time-series event streams)
//! - carries a `tenant_id` (the multi-tenant boundary — `TenantContext` in
//!   upstream)
//!
//! The `x = B/U` collapse applies: every MongoDB document is a fold into one
//! hashtrinity Table; every cross-collection join is a single Term application
//! at the B/U applicative (no separate "join engine").
//!
//! Field-name reconciliation: upstream Spring Boot uses camelCase
//! (`tenantId`, `createdAt`). We follow the fold convention (`snake_case`) and
//! map at the codec boundary in `sangdb-bindings-mongodb::codec_bridge`.

use chrono::{DateTime, Utc};
use hashtrinity::Coord;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Multi-tenant boundary. The upstream `TenantContext` is rebuilt here as the
/// key prefix on every sangdb Table — `tenant:tenant_id:object_id` — so a
/// `Table::scan_prefix("tenant:acme:")` returns only acme's rows in O(matches).
pub type TenantId = Uuid;

/// Newtype wrapper that gives a `Uuid` a stable hashtrinity `coord()`. The
/// orphan rule forbids implementing `Coord` for `Uuid` directly; the
/// newtype lets the wrapper own the impl. Use `TenantKey(tid)` in place of
/// `tid` when storing on a `Table`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantKey(pub Uuid);

impl Coord for TenantKey {
    fn coord(&self) -> u64 {
        let b = self.0.as_bytes();
        u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
    }
}

impl From<Uuid> for TenantKey {
    fn from(u: Uuid) -> Self { Self(u) }
}

/// Every entity carries one. Used for both intra-tenant uniqueness (compound
/// key `tenant_id + entity_id`) and tenant scoping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantScoped {
    pub tenant_id: TenantId,
}

impl TenantScoped {
    pub fn new(tenant_id: TenantId) -> Self {
        Self { tenant_id }
    }
}

/// Trait every OpenFrame entity implements. The encode/decode pair lets us
/// move freely between the row face (`Table<K, V>::put`) and the column face
/// (`fold_group`) without a separate ORM layer — `x = B/U` at the entity.
pub trait OpenFrameEntity: Serialize + for<'de> Deserialize<'de> {
    type Id: Copy + Eq + std::hash::Hash + std::fmt::Display;

    fn id(&self) -> Self::Id;
    fn tenant_id(&self) -> TenantId;
    /// The collection/table name (e.g. `"devices"`, `"tickets"`).
    /// Sangdb tables are tenant-scoped: `"{name}:{tenant_id}"`.
    fn collection(&self) -> &'static str;
}

// ─────────────────────────────────────────────────────────────────────────────
// Devices — replaces `openframe.data.document.device.*` (Device, Machine,
// DeviceConfiguration, DeviceHealth, DeviceStatus, Alert, ComplianceRequirement,
// ComplianceState, SecurityAlert, SecuritySettings, SecurityState).
// ─────────────────────────────────────────────────────────────────────────────

/// The device doc — see `openframe.data.document.device.Device`. Status and
/// health are stored as separate fields here (a Mongo refactor to fold them
/// into one nested doc would be lossless — see `sangdb-bindings-mongodb`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Device {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub machine_id: Uuid,
    pub hostname: String,
    pub display_name: String,
    pub name: String,
    pub ip: Option<String>,
    pub mac_address: Option<String>,
    pub os_uuid: Option<String>,
    pub agent_version: Option<String>,
    pub status: DeviceStatus,
    pub health: DeviceHealth,
    pub device_type: DeviceType,
    pub os_type: Option<String>,
    pub os_version: Option<String>,
    pub os_build: Option<String>,
    pub timezone: Option<String>,
    pub serial_number: Option<String>,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub organization_id: Option<Uuid>,
    pub last_seen: DateTime<Utc>,
    pub registered_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub tag_ids: Vec<Uuid>,
    pub tool_connection_ids: Vec<Uuid>,
}

impl OpenFrameEntity for Device {
    type Id = Uuid;
    fn id(&self) -> Uuid { self.id }
    fn tenant_id(&self) -> TenantId { self.tenant_id }
    fn collection(&self) -> &'static str { "devices" }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DeviceType {
    Desktop,
    Laptop,
    Server,
    Mobile,
    Iot,
    Unknown,
}
impl DeviceType {
    pub fn gql_repr(&self) -> &'static str {
        match self {
            DeviceType::Desktop => "DESKTOP",
            DeviceType::Laptop => "LAPTOP",
            DeviceType::Server => "SERVER",
            DeviceType::Mobile => "MOBILE",
            DeviceType::Iot => "IOT",
            DeviceType::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DeviceStatus {
    Registered,
    Online,
    Offline,
    Decommissioned,
}
impl DeviceStatus {
    /// GraphQL string repr (uppercase, as the frontend filter expects).
    pub fn gql_repr(&self) -> &'static str {
        match self {
            DeviceStatus::Registered => "REGISTERED",
            DeviceStatus::Online => "ONLINE",
            DeviceStatus::Offline => "OFFLINE",
            DeviceStatus::Decommissioned => "DECOMMISSIONED",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DeviceHealth {
    Healthy,
    Warning,
    Critical,
    Unknown,
}
impl DeviceHealth {
    pub fn gql_repr(&self) -> &'static str {
        match self {
            DeviceHealth::Healthy => "HEALTHY",
            DeviceHealth::Warning => "WARNING",
            DeviceHealth::Critical => "CRITICAL",
            DeviceHealth::Unknown => "UNKNOWN",
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Events — replaces `openframe.data.document.event.*` (Event, CoreEvent,
// ExternalApplicationEvent). Time-series: stored on the **sangdb column face**
// (Cassandra-shaped) — `fold_group(tenant_id, count)` is the GROUP BY.
//
// Key: `(tenant_id, device_id, timestamp_us)` for ordered range scans; the
// upstream `device_event` table is replicated as a hashtrinity `Table<(Tid,
// Uuid, i64), EventRow>`.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceEvent {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub device_id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub timestamp: DateTime<Utc>,
    pub source: EventSource,
}

impl OpenFrameEntity for DeviceEvent {
    type Id = Uuid;
    fn id(&self) -> Uuid { self.id }
    fn tenant_id(&self) -> TenantId { self.tenant_id }
    fn collection(&self) -> &'static str { "events" }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EventSource { Agent, KafkaIngest, StreamEnrich, ExternalApi }

// ─────────────────────────────────────────────────────────────────────────────
// Tickets — replaces `openframe.data.document.ticket.*` (the GraphQL Ticket
// type — title/description/status/priority/assignee). These read through the
// GraphQL datafetchers in `openframe-api-service-core`, which we replace with
// a bedrock HTTP handler reading from the sangdb row face.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticket {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub title: String,
    pub description: String,
    pub status: TicketStatus,
    pub priority: TicketPriority,
    pub assignee: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl OpenFrameEntity for Ticket {
    type Id = Uuid;
    fn id(&self) -> Uuid { self.id }
    fn tenant_id(&self) -> TenantId { self.tenant_id }
    fn collection(&self) -> &'static str { "tickets" }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TicketStatus { Open, InProgress, Resolved, Closed }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TicketPriority { Low, Normal, High, Urgent }

// ─────────────────────────────────────────────────────────────────────────────
// Auth — replaces `openframe.data.document.auth.*` (AuthUser, AuthInvitation).
// `AuthUser.password_hash` is `argon2id` (see openframe-core-crypto).
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub email: String,
    pub display_name: String,
    pub role: UserRole,
    pub created_at: DateTime<Utc>,
}

impl OpenFrameEntity for User {
    type Id = Uuid;
    fn id(&self) -> Uuid { self.id }
    fn tenant_id(&self) -> TenantId { self.tenant_id }
    fn collection(&self) -> &'static str { "users" }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum UserRole { Admin, Technician, Client, Agent }

// ─────────────────────────────────────────────────────────────────────────────
// Agent registration — replaces
// `openframe.data.document.agent.AgentRegistrationSecret` and
// `openframe.data.document.installedagents.{InstalledAgent, UserInstalledAgent}`.
// The secret is consumed by the Rust agent (`of-agent`) during install;
// subsequent heartbeats arrive via sangdb column face (NATS-shaped) and are
// persisted as `InstalledAgent` rows.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRegistrationSecret {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub secret_hash: String, // argon2id of the registration key
    pub expires_at: DateTime<Utc>,
    pub used: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstalledAgent {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub device_id: Uuid,
    pub agent_version: String,
    pub last_heartbeat: DateTime<Utc>,
    pub registered_at: DateTime<Utc>,
}

// ─────────────────────────────────────────────────────────────────────────────
// API keys — replaces `openframe.data.document.apikey.{ApiKey, ApiKeyStats,
// APIKeyType}`. `openframe-external-api-service-core` rate-limits per-key.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub key_hash: String,
    pub key_type: ApiKeyType,
    pub label: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ApiKeyType { Public, Restricted, Admin }

// ─────────────────────────────────────────────────────────────────────────────
// Streaming — replaces `openframe-stream-service-core` Kafka topics. The
// column-face topic `tenant_events:{tenant_id}` is the streaming-law fold:
// every consumer group reads independently off the same log (see
// `bedrock::sangdb::mod::Log`).
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamTopic {
    pub name: String,
    pub tenant_id: TenantId,
    pub partitions: u32,
    pub retention_ms: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Organizations — replaces `openframe.data.document.organization.*`. The
// frontend's `GetOrganizations` / `GetOrganizationById` / `GetOrganizationsMin`
// queries resolve through the GraphQL `organizations` / `organization` fields.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Organization {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub organization_id: String, // customer-supplied business id (e.g. "ACME-001")
    pub name: String,
    pub category: Option<String>,
    pub contact_information: Option<ContactInformation>,
    pub number_of_employees: Option<i32>,
    pub website_url: Option<String>,
    pub notes: Option<String>,
    pub monthly_revenue: Option<f64>,
    pub contract_start_date: Option<String>,
    pub contract_end_date: Option<String>,
    pub is_default: bool,
    pub deleted: bool,
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl OpenFrameEntity for Organization {
    type Id = Uuid;
    fn id(&self) -> Uuid { self.id }
    fn tenant_id(&self) -> TenantId { self.tenant_id }
    fn collection(&self) -> &'static str { "organizations" }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContactInformation {
    pub mailing_address_same_as_physical: bool,
    pub contacts: Vec<Contact>,
    pub physical_address: Option<Address>,
    pub mailing_address: Option<Address>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contact {
    pub contact_name: String,
    pub title: Option<String>,
    pub phone: Option<String>,
    pub email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    pub street1: Option<String>,
    pub street2: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub postal_code: Option<String>,
    pub country: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Tags — replaces `openframe.data.document.tag.*`. Devices, organizations,
// and policies all carry tag references; the `Tags` query returns the
// catalogue per tenant.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub name: String,
    pub description: Option<String>,
    pub color: Option<String>,
    pub organization_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<String>,
}

impl OpenFrameEntity for Tag {
    type Id = Uuid;
    fn id(&self) -> Uuid { self.id }
    fn tenant_id(&self) -> TenantId { self.tenant_id }
    fn collection(&self) -> &'static str { "tags" }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tool events / logs — replaces `openframe.data.document.toolevent.*`. The
// frontend's `GetLogs` query lists recent events; `GetLogFilters` returns
// the available filter facets (toolTypes / eventTypes / severities / orgs).
// Persisted on the sangdb row face keyed by `(tenant_id, ingest_day)` so the
// range scan for a day stays O(matches-in-day).
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolEvent {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub tool_event_id: String, // event id assigned by the upstream tool (Fleet/Tactical/Authentik)
    pub tool_type: String,     // "fleet" | "tactical" | "authentik" | "meshcentral"
    pub event_type: String,    // "host_created" | "policy_violation" | ...
    pub ingest_day: String,    // "2024-08-15" — partition key from upstream
    pub severity: String,      // "info" | "warning" | "error" | "critical"
    pub user_id: Option<String>,
    pub device_id: Option<String>,
    pub hostname: Option<String>,
    pub organization_name: Option<String>,
    pub organization_id: Option<String>,
    pub summary: String,
    pub message: Option<String>,
    pub details: Option<serde_json::Value>,
    pub timestamp: DateTime<Utc>,
}

impl OpenFrameEntity for ToolEvent {
    type Id = Uuid;
    fn id(&self) -> Uuid { self.id }
    fn tenant_id(&self) -> TenantId { self.tenant_id }
    fn collection(&self) -> &'static str { "tool_events" }
}

// ─────────────────────────────────────────────────────────────────────────────
// Dialogs (Mingo AI-resolution conversations) — replaces
// `openframe.data.document.dialog.*`. The frontend's `GetDialogs` /
// `GetDialogStatistics` queries resolve through this.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dialog {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub title: String,
    pub status: DialogStatus,
    pub machine_id: Option<String>,
    pub machine_hostname: Option<String>,
    pub machine_organization_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub status_updated_at: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub ai_resolution_suggested_at: Option<DateTime<Utc>>,
    pub rating: Option<DialogRating>,
}

impl OpenFrameEntity for Dialog {
    type Id = Uuid;
    fn id(&self) -> Uuid { self.id }
    fn tenant_id(&self) -> TenantId { self.tenant_id }
    fn collection(&self) -> &'static str { "dialogs" }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DialogStatus { Open, InProgress, Resolved, Closed, Archived }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DialogRating {
    pub id: Uuid,
    pub dialog_id: Uuid,
    pub rating: i32,
    pub created_at: DateTime<Utc>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Cursors + page info — the relay-style connection contract the frontend's
// `GET_DEVICES`, `GetLogs`, `GetDialogs` queries all rely on. The cursor is
// opaque to the client and decodes to a `(created_at, id)` tuple for stable
// ordering across pages.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

impl PageInfo {
    pub fn empty() -> Self {
        Self {
            has_next_page: false,
            has_previous_page: false,
            start_cursor: None,
            end_cursor: None,
        }
    }
}

/// Encodes `(created_at_millis, id)` as a base64url token. The frontend only
/// echoes these back, never decodes them.
pub fn encode_cursor(created_at: DateTime<Utc>, id: Uuid) -> String {
    let raw = format!("{}|{}", created_at.timestamp_millis(), id);
    let mut out = String::with_capacity(raw.len() * 2);
    for b in raw.bytes() {
        let v = b as u32;
        let c1 = (v >> 6) & 0x3f;
        let c2 = v & 0x3f;
        let a = if c1 < 26 { b'A' + c1 as u8 }
               else if c1 < 52 { b'a' + (c1 - 26) as u8 }
               else if c1 < 62 { b'0' + (c1 - 52) as u8 }
               else if c1 == 62 { b'-' as u8 }
               else { b'_' as u8 };
        let b = if c2 < 26 { b'A' + c2 as u8 }
               else if c2 < 52 { b'a' + (c2 - 26) as u8 }
               else if c2 < 62 { b'0' + (c2 - 52) as u8 }
               else if c2 == 62 { b'-' as u8 }
               else { b'_' as u8 };
        out.push(a as char);
        out.push(b as char);
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Tool connections — replaces `openframe.data.document.toolconnection.*`.
// One per (machine, tool) pair — Fleet MDM, Tactical RMM, MeshCentral, etc.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolConnection {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub machine_id: Uuid,
    pub tool_type: String,    // "fleet" | "tactical" | "meshcentral" | "authentik"
    pub agent_tool_id: String,
    pub status: ToolConnectionStatus,
    pub metadata: Option<serde_json::Value>,
    pub connected_at: Option<DateTime<Utc>>,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub disconnected_at: Option<DateTime<Utc>>,
}

impl OpenFrameEntity for ToolConnection {
    type Id = Uuid;
    fn id(&self) -> Uuid { self.id }
    fn tenant_id(&self) -> TenantId { self.tenant_id }
    fn collection(&self) -> &'static str { "tool_connections" }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ToolConnectionStatus { Pending, Connected, Disconnected, Error }

/// Tenant isolation helpers — the shared boundary every sangdb key is prefixed
/// with. `format!("tenant:{tenant_id}:{collection}")` is the row-face key.
pub fn tenant_key(tenant_id: TenantId, collection: &str) -> String {
    format!("tenant:{tenant_id}:{collection}")
}

pub fn tenant_row_key(tenant_id: TenantId, collection: &str, id: &str) -> String {
    format!("tenant:{tenant_id}:{collection}:{id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_key_format() {
        let tid = Uuid::nil();
        assert_eq!(tenant_key(tid, "devices"), "tenant:00000000-0000-0000-0000-000000000000:devices");
        assert_eq!(
            tenant_row_key(tid, "devices", "abc"),
            "tenant:00000000-0000-0000-0000-000000000000:devices:abc"
        );
    }

    #[test]
    fn device_roundtrip_json() {
        let d = Device {
            id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            machine_id: Uuid::nil(),
            name: "acme-laptop-01".into(),
            device_type: DeviceType::Laptop,
            status: DeviceStatus::Online,
            health: DeviceHealth::Healthy,
            last_seen: Utc::now(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tags: vec!["prod".into(), "engineering".into()],
        };
        let j = serde_json::to_string(&d).unwrap();
        let back: Device = serde_json::from_str(&j).unwrap();
        assert_eq!(d.id, back.id);
    }
}