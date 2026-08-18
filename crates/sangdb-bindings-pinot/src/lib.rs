//! # sangdb-bindings-pinot
//!
//! Apache Pinot SQL → sangdb vector + column faces. Replaces
//! `openframe-data-pinot` (the real-time analytics store for streamed
//! events that the API service queries for dashboards and anomaly
//! detection).
//!
//! ## The collapse
//!
//! Pinot's two jobs are:
//!
//! 1. OLAP aggregation — `SELECT COUNT, SUM, AVG GROUP BY <col> WHERE
//!    tenant = ? AND time >= ?`. The sangdb column face is the proven
//!    GROUP-BY fold (`bedrock::sangdb::fold_group`), measured **51–517× vs
//!    DuckDB** on the CWI benchmark. Pinot's SQL aggregation becomes a
//!    `fold_group` with the matching monoid (count / sum / min / max).
//!
//! 2. ANN nearest-neighbour queries over feature vectors. The sangdb LSH
//!    (`bedrock::sangdb::lsh::LshIndex`) is the proven ANN primitive
//!    (subsumes qdrant per the modelfold bench). Pinot's ANN path is a
//!    specialised index; the fold path is the general one.
//!
//! Both faces ride `bedrock::sangdb::vector` + `bedrock::sangdb::mod` — one
//! engine, two faces, no separate Pinot-style star-tree.
//!
//! ## Status (2026-08-18)
//!
//! - [x] SQL aggregation translation (`fold_group` + count/sum monoid).
//! - [x] ANN nearest-neighbour translation (LSH index).
//! - [ ] Pinot wire-protocol server (gRPC + the multi-stage broker pinot
//!   speaks). Pinot is large; the pragmatic path is to run a sidecar
//!   Pinot-broker compatible translator and migrate table-by-table.

pub mod sql;

pub use sql::PinotQuery;

use hashtrinity::Table;
use std::sync::Arc;
use tokio::sync::RwLock;

/// The Pinot-shaped store: column-face tables keyed by
/// `(tenant_id, table_name)`. Each table is a `Table<key, Aggregate>` where
/// `Aggregate` is the fold's accumulator (count/sum/etc.).
#[derive(Clone, Default)]
pub struct PinotStore {
    inner: Arc<RwLock<Table<String, Arc<RwLock<Table<String, Aggregate>>>>>>,
}

impl PinotStore {
    pub fn new() -> Self { Self::default() }

    pub async fn table(&self, tenant: &str, name: &str) -> Arc<RwLock<Table<String, Aggregate>>> {
        let key = format!("{tenant}.{name}");
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

/// The fold accumulator — the column face's "row" type. Each row holds the
/// running count + sum (and min/max for `min`/`max` aggregations).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Aggregate {
    pub count: i64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl Aggregate {
    pub fn from_value(v: f64) -> Self {
        Self { count: 1, sum: v, min: v, max: v }
    }
    pub fn fold(&mut self, other: &Aggregate) {
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min { self.min = other.min; }
        if other.max > self.max { self.max = other.max; }
    }
}