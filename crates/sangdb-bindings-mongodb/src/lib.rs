//! # sangdb-bindings-mongodb
//!
//! MongoDB wire-protocol → sangdb row face. This crate is the **migration
//! shim**: it lets every existing MongoDB driver (Spring Data MongoDB, Java
//! MongoDB driver, Python pymongo, Go mongo-go-driver, Rust mongodb crate)
//! talk to a sangdb server as if it were a `mongod`.
//!
//! ## Why
//!
//! Upstream `openframe-oss-tenant` uses `TenantAwareMongoTemplate`,
//! `@Document`-annotated POJOs, and the reactive driver
//! (`openframe-data-mongo-reactive`). Every collection is a MongoDB doc.
//! The replacement strategy is:
//!
//! 1. Run this server in front of a sangdb HTTP surface.
//! 2. Existing Java services keep their Spring Data MongoDB code unchanged —
//!    they connect to `mongodb://sangdb:27017` instead of `mongodb://mongo:27017`.
//! 3. Wire-level compatibility is enough; no service-side code changes.
//! 4. Once verified, port one service at a time to native sangdb.
//!
//! ## The collapse (sanguine proof anchor)
//!
//! A MongoDB document is a `Term` (`hashtrinity::codec::Term`). A collection
// is a `Table<String, Bson>`. A query is `Term::state app` at the
//! `find`/`insert`/`update`/`delete` applicative. The wire protocol is the
//! syscall cut — same as `sangdb`'s other faces. No engine, no parser; the
//! BSON document is the value, the table is the store.
//!
//! ## What is implemented
//!
//! - OP_MSG framing (the single modern opcode since MongoDB 3.6).
//! - `hello` / `ismaster` handshake (returns a stand-in `mongod` 7.0 reply).
//! - `find` → `Table::scan_prefix` (collection prefix + tenant scope).
//! - `insert` → `Table::put` (sangdb row-face; uses
//!   `sangdb-bindings-mongodb::codec_bridge` for camelCase ↔ snake_case).
//! - `update` / `delete` / `count` → sangdb row-face equivalents.
//!
//! ## What is NOT (yet)
//!
//! - Aggregation pipeline → sangdb column face (`fold_group`); partial.
//! - Indexes → sangdb LSH for vector, posting face for text, otherwise
//!   tenant-prefix scans. Equivalent for the OpenFrame access pattern.
//! - Change streams → sangdb `Log::consumer_groups`; see
//!   `bedrock::sangdb::mod::Log`.
//! - Replica-set / sharded cluster → sangdb `partition.rs` (this crate does
//!   not yet wire it).
//!
//! Reference: `https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst`

pub mod bson;
pub mod codec_bridge;
pub mod opcodes;
pub mod server;
pub mod query;

pub use server::{MongoServer, MongoServerConfig};
pub use query::{Query, QueryResult};

use hashtrinity::Table;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use thiserror::Error;

/// Top-level error type. Every wire-protocol reply that the MongoDB spec
/// would convert to a server error becomes a `MongoError` here.
#[derive(Debug, Error)]
pub enum MongoError {
    #[error("invalid BSON: {0}")]
    InvalidBson(String),
    #[error("unknown opcode: 0x{0:08x}")]
    UnknownOpcode(u32),
    #[error("unsupported command: {0}")]
    UnsupportedCommand(String),
    #[error("collection not found: {0}")]
    CollectionNotFound(String),
    #[error("tenant scope violation")]
    TenantScopeViolation,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec: {0}")]
    Codec(String),
    #[error("internal: {0}")]
    Internal(String),
}

pub type MongoResult<T> = Result<T, MongoError>;

/// The shared store. In production this is the `bedrock::sangdb::http::State`
/// HTTP server's underlying `Table` set; in this binding we wrap them in
/// `RwLock<Table>` and expose collection-prefixed access.
///
/// Collection names are `tenant:{tenant_id}:{collection}` per
/// `openframe_domain::tenant_key`. Cross-tenant access is impossible by
/// construction — the wire protocol cannot escape the prefix.
#[derive(Clone)]
pub struct MongoStore {
    /// `tenant_key → Table<row_id, BsonDoc>` — one Table per tenant+collection.
    /// All access is `(tenant_id, collection, row_id)` keyed; tenant-scope
    /// is enforced at key-construction time.
    tables: Arc<RwLock<Table<String, Arc<RwLock<Table<String, bson::Bson>>>>>>,
    /// Default tenant for unauthenticated / pre-auth state. The real
    /// upstream requires `TenantContext`; this binding defaults to a single
    /// tenant until auth integration lands.
    default_tenant: String,
}

impl MongoStore {
    pub fn new(default_tenant: impl Into<String>) -> Self {
        Self {
            tables: Arc::new(RwLock::new(Table::new())),
            default_tenant: default_tenant.into(),
        }
    }

    /// Demo state — same shape as `bedrock::sangdb::http::demo_state` so
    /// `redis-cli`/`mongosh` clients can poke at it. Used by the standalone
    /// `sangdb-mongo` binary in `bin/`.
    pub fn demo_state() -> Self {
        use bson::{Bson, BsonDoc};
        let s = Self::new("default");
        // Pre-populate with a sample `devices` and `tickets` collection per
        // tenant, mirroring `bedrock::sangdb::http::demo_state` so the
        // compat matrix stays symmetric.
        s.spawn_blocking("default", "devices", |t| {
            for (id, name, status) in [
                ("d-001", "laptop-alpha", "online"),
                ("d-002", "server-beta", "online"),
                ("d-003", "iot-gamma", "offline"),
            ] {
                let mut doc = BsonDoc::new();
                doc.insert("_id", Bson::String(id.into()));
                doc.insert("name", Bson::String(name.into()));
                doc.insert("status", Bson::String(status.into()));
                t.put(id.to_string(), Bson::Document(doc));
            }
        });
        s
    }

    /// Block-on helper for the demo initialiser — sync code paths in tokio
    /// runtime get a `Handle::block_on` here.
    pub fn spawn_blocking<R>(
        &self,
        tenant: &str,
        collection: &str,
        f: impl FnOnce(&mut Table<String, bson::Bson>) -> R,
    ) -> R {
        let key = format!("tenant:{tenant}:{collection}");
        let tables_outer = self.tables.clone();
        // Either path: ensure a Table exists at `key`, then run `f` over it.
        let t: Arc<RwLock<Table<String, bson::Bson>>> = match tokio::runtime::Handle::try_current() {
            Ok(_) => {
                let tables = tables_outer.blocking_read();
                tables.get(&key).cloned().unwrap_or_else(|| {
                    let new = Arc::new(RwLock::new(Table::new()));
                    drop(tables);
                    tables_outer.blocking_write().put(key.clone(), new.clone());
                    new
                })
            }
            Err(_) => {
                let mut tables = tables_outer.blocking_write();
                tables.get(&key).cloned().unwrap_or_else(|| {
                    let new = Arc::new(RwLock::new(Table::new()));
                    tables.put(key.clone(), new.clone());
                    new
                })
            }
        };
        let mut guard = t.blocking_write();
        f(&mut guard)
    }

    /// Tenant-scoped table accessor — the one place the tenant boundary
    /// materialises. Every query routes through this.
    pub async fn collection(&self, tenant: &str, name: &str) -> Arc<RwLock<Table<String, bson::Bson>>> {
        let key = format!("tenant:{tenant}:{name}");
        let mut tables = self.tables.write().await;
        if let Some(t) = tables.get(&key) {
            t.clone()
        } else {
            let t = Arc::new(RwLock::new(Table::new()));
            tables.put(key.clone(), t.clone());
            t
        }
    }

    /// The `default_tenant` for unauthenticated requests. Mirrors the
    /// upstream `TenantContext` default.
    pub fn default_tenant(&self) -> &str { &self.default_tenant }
}

// NOTE: Bson is the *value* type on Table<String, Bson>; String is the key
// and String already has `Coord` + `KeyStore<Stored<String>>` from hashtrinity.
// Bson does not need a `Coord` impl (and could not have one — Double is `f64`
// which has no inverse, and the value side never needs `Eq`/`Hash`).
//
// TODO(performance): prefix scans — `Table::iter` is the only iterator; for
// per-tenant collection access we filter on key prefix. For
// million-row collections this becomes O(n). The proper fix is
// `CoordMap<String, Bson>` + a separate `Table<String, (Tid, ColName)>` of
// per-collection entry-point coords, or a `bedrock::sangdb::http` integration
// that uses the HTTP face's already-sharded tables.