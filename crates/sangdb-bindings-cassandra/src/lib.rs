//! # sangdb-bindings-cassandra
//!
//! Cassandra CQL → sangdb column/fold face. Replaces `openframe-data-cassandra`
//! (consumed by `openframe-stream-service-core` for time-series event
//! projection).
//!
//! ## The collapse
//!
//! A Cassandra table is `(partition_key, clustering_key) → row`. The sangdb
//! column face is `Table<partition_key, (clustering_key, value)>` — the
//! fold is a hashtrinity `fold_group` over the partition key with a row-merging
//! closure. `INSERT … VALUES` becomes `Table::put`. `SELECT WHERE partition_key
//! = ?` becomes `Table::get` (or `scan_prefix` once the column face ships it).
//! Aggregation queries (`SELECT COUNT(*), SUM(col) GROUP BY device_id`) are
//! exactly `sangdb::fold_group` with the count/sum monoid.
//!
//! ## Status (2026-08-18)
//!
//! - [x] CQL `SELECT`, `INSERT`, `UPDATE`, `DELETE` translation.
//! - [ ] Native CQL wire protocol server (4.0 framing). Use a sidecar
//!   Cassandra-compatible driver (`scylladb`) or the `cdrs-tokio` crate as
//!   the transport and translate CQL→sangdb in this crate.
//! - [ ] Time-series optimised path. The OpenFrame use case is the
//!   `device_event` table; partition by `(tenant_id, device_id)` and cluster
//!   by `timestamp`.
//!
//! Reference: <https://github.com/apache/cassandra/blob/trunk/doc/cql5_spec.md>

pub mod cql;

pub use cql::{CqlStatement, execute};

use hashtrinity::Table;
use uuid;
use std::sync::Arc;
use tokio::sync::RwLock;

/// The Cassandra-shaped store: one hashtrinity `Table<partition_key, row>`
/// per (keyspace, table). Same tenant-scoping rule as
/// `sangdb-bindings-mongodb::MongoStore`.
#[derive(Clone)]
pub struct CassandraStore {
    inner: Arc<RwLock<Table<String, Arc<RwLock<Table<String, CqlValue>>>>>>,
    default_tenant: String,
}

impl CassandraStore {
    pub fn new(default_tenant: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Table::new())),
            default_tenant: default_tenant.into(),
        }
    }

    pub async fn table(&self, keyspace: &str, name: &str) -> Arc<RwLock<Table<String, CqlValue>>> {
        let key = format!("{keyspace}.{name}");
        let mut tables = self.inner.write().await;
        if let Some(t) = tables.get(&key) {
            t.clone()
        } else {
            let t = Arc::new(RwLock::new(Table::new()));
            tables.put(key.clone(), t.clone());
            t
        }
    }
}

/// The CQL value space lives in the `cql` module — re-exported here for
/// the binary surface (`sangdb_cassandra::CqlValue`).
pub use cql::CqlValue;

impl CqlValue {
    pub fn from_serde(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => CqlValue::Null,
            serde_json::Value::Bool(b) => CqlValue::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() { CqlValue::BigInt(i) }
                else if let Some(f) = n.as_f64() { CqlValue::Text(f.to_string()) }
                else { CqlValue::Text(n.to_string()) }
            }
            serde_json::Value::String(s) => CqlValue::Text(s),
            serde_json::Value::Array(arr) => CqlValue::List(arr.into_iter().map(CqlValue::from_serde).collect()),
            serde_json::Value::Object(m) => CqlValue::Map(
                m.into_iter().map(|(k, v)| (k, CqlValue::from_serde(v))).collect()
            ),
        }
    }
}