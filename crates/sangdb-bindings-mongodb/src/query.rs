//! MongoDB command → sangdb query translation.
//!
//! The supported subset: `find`, `insert`, `update`, `delete`, `count`,
//! `hello`, `ismaster`, `ping`. Every other command returns
//! `MongoError::UnsupportedCommand`.

use super::bson::{Bson, BsonDoc};
use super::{MongoError, MongoResult, MongoStore};
use std::collections::BTreeMap;

/// A command extracted from the first BSON doc of an OP_MSG section-0 body.
#[derive(Debug, Clone)]
pub enum Query {
    Hello,
    IsMaster,
    Ping,
    Find { collection: String, filter: BsonDoc, limit: Option<i64>, skip: Option<i64> },
    Insert { collection: String, documents: Vec<BsonDoc> },
    Update { collection: String, filter: BsonDoc, update: BsonDoc, multi: bool },
    Delete { collection: String, filter: BsonDoc, limit: i64 },
    Count { collection: String, filter: BsonDoc },
    /// Anything we don't recognise; we still try to be polite.
    Unsupported(String),
}

impl Query {
    pub fn from_command(doc: &BsonDoc) -> MongoResult<Self> {
        // The first key in the BSON doc is the command name (MongoDB convention).
        let cmd_name = doc
            .iter()
            .next()
            .map(|(k, _)| k.clone())
            .ok_or_else(|| MongoError::Internal("empty command".into()))?;
        let rest: BsonDoc = doc
            .iter()
            .filter(|(k, _)| *k != &cmd_name)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<BTreeMap<_, _>>()
            .into();
        match cmd_name.as_str() {
            "hello" => Ok(Query::Hello),
            "ismaster" => Ok(Query::IsMaster),
            "ping" => Ok(Query::Ping),
            "find" => Ok(Query::Find {
                collection: get_string(&rest, "find")?,
                filter: get_doc(&rest, "filter").unwrap_or_default(),
                limit: get_i64(&rest, "limit").ok(),
                skip: get_i64(&rest, "skip").ok(),
            }),
            "insert" => Ok(Query::Insert {
                collection: get_string(&rest, "insert")?,
                documents: get_doc_array(&rest, "documents")?,
            }),
            "update" => Ok(Query::Update {
                collection: get_string(&rest, "update")?,
                filter: get_doc(&rest, "filter").unwrap_or_default(),
update: get_doc(&rest, "update").unwrap_or_default(),
            multi: get_bool(&rest, "multi").unwrap_or(false),
        }),
        "delete" => Ok(Query::Delete {
            collection: get_string(&rest, "delete")?,
            filter: get_doc(&rest, "filter").unwrap_or_default(),
            limit: get_i64(&rest, "limit").unwrap_or(0),
        }),
        "count" => Ok(Query::Count {
            collection: get_string(&rest, "count")?,
            filter: get_doc(&rest, "filter").unwrap_or_default(),
        }),
            other => Ok(Query::Unsupported(other.to_string())),
        }
    }
}

#[derive(Debug)]
pub enum QueryResult {
    /// Single BSON doc reply (e.g. `{ok: 1}` for hello).
    Single(BsonDoc),
    /// Cursor reply (e.g. `find` with a batch of docs).
    Cursor { first_batch: Vec<BsonDoc>, cursor_id: i64, ns: String },
    /// Write reply (inserted/modified count).
    Write { n: i64 },
}

impl QueryResult {
    pub fn to_reply_doc(&self) -> BsonDoc {
        match self {
            QueryResult::Single(d) => d.clone(),
            QueryResult::Cursor { first_batch, cursor_id, ns } => {
                let mut out = BsonDoc::new();
                out.insert("cursor", Bson::Document({
                    let mut c = BsonDoc::new();
                    c.insert("firstBatch", Bson::Array(first_batch.iter().map(|d| Bson::Document(d.clone())).collect()));
                    c.insert("id", Bson::Int64(*cursor_id));
                    c.insert("ns", Bson::String(ns.clone()));
                    c
                }));
                out.insert("ok", Bson::Double(1.0));
                out
            }
            QueryResult::Write { n } => {
                let mut out = BsonDoc::new();
                out.insert("n", Bson::Int64(*n));
                out.insert("ok", Bson::Double(1.0));
                out
            }
        }
    }
}

/// Execute a `Query` against the store. The result is the same shape as the
/// MongoDB command reply (BSON). Tenant scoping is enforced inside `MongoStore`.
pub async fn execute(store: &MongoStore, q: Query, tenant: &str) -> MongoResult<QueryResult> {
    match q {
        Query::Hello | Query::IsMaster => {
            let mut d = BsonDoc::new();
            d.insert("ok", Bson::Double(1.0));
            d.insert("ismaster", Bson::Bool(true));
            d.insert("maxBsonObjectSize", Bson::Int32(16 * 1024 * 1024));
            d.insert("maxMessageSizeBytes", Bson::Int32(48 * 1024 * 1024));
            d.insert("maxWriteBatchSize", Bson::Int32(100_000));
            d.insert("logicalSessionTimeoutMinutes", Bson::Int32(30));
            d.insert("minWireVersion", Bson::Int32(0));
            d.insert("maxWireVersion", Bson::Int32(21));
            d.insert("readOnly", Bson::Bool(false));
            d.insert("setName", Bson::String("sangdb".into()));
            Ok(QueryResult::Single(d))
        }
        Query::Ping => {
            let mut d = BsonDoc::new();
            d.insert("ok", Bson::Double(1.0));
            Ok(QueryResult::Single(d))
        }
        Query::Find { collection, filter, limit, skip } => {
            let coll = store.collection(tenant, &collection).await;
            let guard = coll.read().await;
            // Iter + filter — see the TODO on `Table::iter` in lib.rs.
            let prefix = format!("tenant:{tenant}:{collection}:");
            let mut matched: Vec<BsonDoc> = Vec::new();
            for (_k, v) in guard.iter() {
                if matches_filter(v, &filter) {
                    if let Bson::Document(doc) = v {
                        matched.push(doc.clone());
                    }
                }
            }
            let skip_n = skip.unwrap_or(0) as usize;
            let limit_n = limit.map(|l| l as usize);
            let batch: Vec<BsonDoc> = matched
                .into_iter()
                .skip(skip_n)
                .take(limit_n.unwrap_or(usize::MAX))
                .collect();
            let ns = format!("{tenant}.{collection}");
            Ok(QueryResult::Cursor { first_batch: batch, cursor_id: 0, ns })
        }
        Query::Insert { collection, documents } => {
            let coll = store.collection(tenant, &collection).await;
            let mut guard = coll.write().await;
            for doc in documents {
                let id = doc.get("_id").map(|v| bson_to_string_id(v)).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                guard.put(id, super::bson::Bson::Document(doc));
            }
            Ok(QueryResult::Write { n: 1 })
        }
        Query::Update { collection, filter, update, multi } => {
            let coll = store.collection(tenant, &collection).await;
            let mut guard = coll.write().await;
            let mut n = 0;
            // Collect ids to update first to avoid borrow conflict.
            let mut to_update: Vec<(String, BsonDoc)> = Vec::new();
            for (k, v) in guard.iter() {
                if let Bson::Document(doc) = v {
                    if matches_filter(v, &filter) {
                        let mut new_doc = doc.clone();
                        apply_update(&mut new_doc, &update);
                        to_update.push((k.to_string(), new_doc));
                        n += 1;
                        if !multi { break; }
                    }
                }
            }
            for (k, new_doc) in to_update {
                guard.put(k, Bson::Document(new_doc));
            }
            Ok(QueryResult::Write { n })
        }
        Query::Delete { collection, filter, limit } => {
            let coll = store.collection(tenant, &collection).await;
            let mut guard = coll.write().await;
            let mut to_remove: Vec<String> = Vec::new();
            let mut n = 0;
            for (k, v) in guard.iter() {
                if let Bson::Document(_) = v {
                    if matches_filter(v, &filter) {
                        to_remove.push(k.to_string());
                        n += 1;
                        if limit > 0 && n >= limit { break; }
                    }
                }
            }
            for k in to_remove {
                guard.remove(&k);
            }
            Ok(QueryResult::Write { n })
        }
        Query::Count { collection, filter } => {
            let coll = store.collection(tenant, &collection).await;
            let guard = coll.read().await;
            let n = guard.iter().filter(|(_, v)| matches_filter(v, &filter)).count() as i64;
            Ok(QueryResult::Write { n })
        }
        Query::Unsupported(name) => Err(MongoError::UnsupportedCommand(name)),
    }
}

// ── filter / update helpers ──────────────────────────────────────────────────

fn matches_filter(value: &Bson, filter: &BsonDoc) -> bool {
    let Bson::Document(doc) = value else { return false; };
    for (k, want) in filter.iter() {
        let got = doc.get(k);
        if !bson_eq(got, want) {
            // Mongo allows operator docs like `{"$eq": ...}`; we support
            // just `$eq` and `$ne` and `$exists` and `$in`.
            if let Bson::Document(op) = want {
                if let Some(Bson::Bool(true)) = op.get("$exists") {
                    if got.is_none() { return false; }
                    continue;
                }
                if let Some(target) = op.get("$eq") {
                    if !bson_eq(got, target) { return false; }
                    continue;
                }
                if let Some(target) = op.get("$ne") {
                    if bson_eq(got, target) { return false; }
                    continue;
                }
                if let Some(Bson::Array(arr)) = op.get("$in") {
                    if !arr.iter().any(|x| bson_eq(got, x)) { return false; }
                    continue;
                }
                return false;
            }
            return false;
        }
    }
    true
}

fn bson_eq(a: Option<&Bson>, b: &Bson) -> bool {
    match (a, b) {
        (Some(x), y) => x == y,
        (None, Bson::Null) => true,
        (None, _) => false,
    }
}

fn apply_update(doc: &mut BsonDoc, update: &BsonDoc) {
    // Support `$set` and `$unset` only — enough for the OpenFrame access
    // pattern (`MongoTemplate.updateFirst` with `$set` is the common case).
    if let Some(Bson::Document(sets)) = update.get("$set") {
        for (k, v) in sets.iter() {
            doc.insert(k.clone(), v.clone());
        }
    }
    if let Some(Bson::Document(sets)) = update.get("$unset") {
        for (k, _) in sets.iter() {
            doc.remove(k);
        }
    }
    // Bare-key update: `{name: "alice"}` means "set name = alice".
    for (k, v) in update.iter() {
        if !k.starts_with('$') {
            doc.insert(k.clone(), v.clone());
        }
    }
}

fn bson_to_string_id(v: &Bson) -> String {
    match v {
        Bson::String(s) => s.clone(),
        Bson::ObjectId(id) => id.iter().map(|b| format!("{b:02x}")).collect(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

// ── get_* accessors ──────────────────────────────────────────────────────────

fn get_string(doc: &BsonDoc, key: &str) -> MongoResult<String> {
    match doc.get(key) {
        Some(Bson::String(s)) => Ok(s.clone()),
        Some(Bson::Int32(i)) => Ok(i.to_string()),
        Some(Bson::Int64(i)) => Ok(i.to_string()),
        _ => Err(MongoError::Internal(format!("missing string field `{key}`"))),
    }
}
fn get_doc(doc: &BsonDoc, key: &str) -> Option<BsonDoc> {
    match doc.get(key) {
        Some(Bson::Document(d)) => Some(d.clone()),
        _ => None,
    }
}
fn get_doc_array(doc: &BsonDoc, key: &str) -> MongoResult<Vec<BsonDoc>> {
    match doc.get(key) {
        Some(Bson::Array(arr)) => arr.iter().filter_map(|v| match v {
            Bson::Document(d) => Some(d.clone()),
            _ => None,
        }).collect::<Vec<_>>().pipe(Ok),
        _ => Err(MongoError::Internal(format!("missing array field `{key}`"))),
    }
}
fn get_i64(doc: &BsonDoc, key: &str) -> MongoResult<i64> {
    match doc.get(key) {
        Some(Bson::Int64(i)) => Ok(*i),
        Some(Bson::Int32(i)) => Ok(*i as i64),
        _ => Err(MongoError::Internal(format!("missing int field `{key}`"))),
    }
}
fn get_bool(doc: &BsonDoc, key: &str) -> Option<bool> {
    match doc.get(key) {
        Some(Bson::Bool(b)) => Some(*b),
        _ => None,
    }
}

// ── tiny pipe helper so the iterator chain above reads cleanly ──────────────

trait Pipe: Sized {
    fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R { f(self) }
}
impl<T> Pipe for T {}