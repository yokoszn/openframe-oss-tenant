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
    pub name: String,
    pub device_type: DeviceType,
    pub status: DeviceStatus,
    pub health: DeviceHealth,
    pub last_seen: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub tags: Vec<String>,
}

impl OpenFrameEntity for Device {
    type Id = Uuid;
    fn id(&self) -> Uuid { self.id }
    fn tenant_id(&self) -> TenantId { self.tenant_id }
    fn collection(&self) -> &'static str { "devices" }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DeviceType { Desktop, Laptop, Server, Mobile, Iot, Unknown }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DeviceStatus { Registered, Online, Offline, Decommissioned }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DeviceHealth { Healthy, Warning, Critical, Unknown }

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