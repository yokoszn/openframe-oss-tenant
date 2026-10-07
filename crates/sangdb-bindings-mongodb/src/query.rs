//! MongoDB command → sangdb query translation.
//!
//! The supported subset: `find`, `insert`, `update`, `delete`, `count`,
//! `hello`, `ismaster`, `ping`. Every other command returns
//! `MongoError::UnsupportedCommand`.

use super::bson::{BsonDoc, BsonValue};
use super::{MongoError, MongoResult, MongoStore, UniqueIndex};
use std::collections::BTreeMap;

/// A command extracted from the first BSON doc of an OP_MSG section-0 body.
#[derive(Debug, Clone)]
pub enum Query {
    Hello,
    IsMaster,
    Ping,
    /// `client` — driver tells server its name/version; server acks with `{ok:1}`.
    ClientMetadata,
    /// `saslStart` / `saslContinue` / `logout` etc. — auth handshake commands.
    /// We accept any sasl/logout/logout without doing the auth dance; reply `{ok:1}`.
    NoopAck(String),
    /// `createIndexes` — retain unique compound-key definitions so writes can
    /// apply the hashtrinity membership face before entering the map face.
    /// Raw specs (incl. non-unique) are kept for `listIndexes` replay.
    CreateIndexes { collection: String, indexes: Vec<UniqueIndex>, specs: Vec<BsonDoc> },
    /// `listIndexes` — cursor reply replaying the recorded spec docs.
    ListIndexes { collection: String },
    /// `drop` — collection-level delete. We ack (the data lives in our Table;
    /// Spring Data may issue drops in `BeforeEach` resetCollection patterns).
    Drop { collection: String },
    /// `distinct` — return unique values of a field over matching docs.
    Distinct { collection: String, field: String, filter: BsonDoc },
    /// `aggregate` — minimal pipeline support for Spring Data MongoDB:
    /// `$match`, `$count`, `$group` (with `_id`), `$unwind`, `$project`,
    /// `$facet`, `$sort`, `$limit`, `$skip`. Pipeline is the array of stages.
    Aggregate { collection: String, pipeline: Vec<BsonDoc>, cursor: Option<BsonDoc> },
    /// `find` with optional sort, projection, limit, skip, sort direction (1=ASC, -1=DESC).
    Find {
        collection: String,
        filter: BsonDoc,
        sort: Vec<(String, i32)>,
        limit: Option<i64>,
        skip: Option<i64>,
    },
    Insert { collection: String, documents: Vec<BsonDoc>, ordered: bool },
    Update { collection: String, filter: BsonDoc, update: BsonValue, multi: bool },
    Delete { collection: String, filter: BsonDoc, limit: i64 },
    Count { collection: String, filter: BsonDoc },
    /// Anything we don't recognise; we still try to be polite.
    Unsupported(String),
}

impl Query {
    pub fn from_command(doc: &BsonDoc) -> MongoResult<Self> {
        // The first NON-META key is the command name. Meta keys (`$db`,
        // `lsid`, `$clusterTime`, `txnNumber`, `readPreference`, `readConcern`,
        // `writeConcern`, `$readPreference`, etc.) sort first in BSON but
        // are not the command. Drivers put `$db` as the very first key.
        const META_KEYS: &[&str] = &[
            "$db",
            "$clusterTime",
            "$readPreference",
            "$readConcern",
            "$writeConcern",
            "lsid",
            "txnNumber",
            "readPreference",
            "readConcern",
            "writeConcern",
            "comment",
            "allowExtensions",
];
        // Pick the command name. For OP_QUERY handshake, the driver's doc is
        // `{isMaster:1, helloOk:true, client:{...}}` — three keys present, but
        // `isMaster` is the command. For OP_MSG CRUD, the doc is
        // `{find:"users", filter:{...}, $db:"...", ...}` — the command is `find`,
        // but BSON-sorted keys may have `filter` before `find`.
        // Strategy: enumerate every command we (or our noop-ack) handle and pick
        // the first one present, in priority order. Handshake commands first
        // (they live alongside `client`/`helloOk` fields).
        // Case matters: BSON preserves case. The driver sends `isMaster` (camel)
        // in OP_QUERY but `hello` (lowercase) in OP_MSG, so we must check both.
        for candidate in [
            // handshake (priority)
            "isMaster", "ismaster", "hello",
            // CRUD
            "find", "insert", "update", "delete", "count", "aggregate", "distinct", "drop",
            // admin (noop)
            "ping", "createIndexes", "listIndexes", "dropIndexes",
            "listCollections", "listDatabases", "dbStats", "collStats",
            // driver/connection
            "client",
            // auth (noop)
            "saslStart", "saslContinue", "logout", "getnonce",
            // other admin
            "buildInfo", "serverStatus", "connectionStatus",
            "endSessions", "startSession", "refreshSessions",
            "getLastError", "getlasterror", "resetError",
            "explain",
        ] {
            if doc.0.contains_key(candidate) {
                tracing::debug!(cmd = candidate, "from_command matched handshake candidate");
                return Self::from_command_name(candidate, doc);
            }
        }
        tracing::warn!(keys = ?doc.0.keys().collect::<Vec<_>>(), "from_command falling through to generic");
        let cmd_name = doc
            .iter()
            .map(|(k, _)| k.clone())
            .find(|k| !META_KEYS.contains(&k.as_str()))
            .ok_or_else(|| MongoError::Internal("empty command".into()))?;
        Self::from_command_name(&cmd_name, doc)
    }

    fn from_command_name(cmd_name: &str, doc: &BsonDoc) -> MongoResult<Self> {
        let rest: BsonDoc = doc
            .iter()
            .filter(|(k, _)| *k != cmd_name)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<BTreeMap<_, _>>()
            .into();
        match cmd_name {
            "hello" | "ismaster" | "isMaster" => {
                if cmd_name == "hello" {
                    Ok(Query::Hello)
                } else {
                    Ok(Query::IsMaster)
                }
            }
            "ping" => Ok(Query::Ping),
            "client" => Ok(Query::ClientMetadata),
            "createIndexes" => Ok(Query::CreateIndexes {
                collection: cmd_name_value_str(doc, "createIndexes")?,
                indexes: get_unique_indexes(&rest),
                specs: get_index_specs(&rest),
            }),
            "listIndexes" => Ok(Query::ListIndexes {
                collection: cmd_name_value_str(doc, "listIndexes")?,
            }),
            "saslStart" | "saslContinue" | "logout" | "getnonce" => {
                Ok(Query::NoopAck(cmd_name.to_string()))
            }
            "find" => Ok(Query::Find {
                collection: cmd_name_value_str(doc, "find")?,
                filter: get_doc(&rest, "filter").unwrap_or_default(),
                sort: get_sort(&rest),
                limit: get_i64(&rest, "limit").ok(),
                skip: get_i64(&rest, "skip").ok(),
            }),
            "insert" => Ok(Query::Insert {
                collection: cmd_name_value_str(doc, "insert")?,
                // Documents may live in a kind-1 document sequence
                // (Spring Data MongoDB bulkWrite path). Default to empty
                // and let the dispatch overlay it.
                documents: get_doc_array(&rest, "documents").unwrap_or_default(),
                // Unordered bulk inserts (BulkMode.UNORDERED) continue past
                // duplicate-key failures and report them as writeErrors.
                ordered: get_bool(&rest, "ordered").unwrap_or(true),
            }),
            "update" => Ok(Query::Update {
                collection: cmd_name_value_str(doc, "update")?,
                filter: extract_update_filter(&rest),
                update: extract_update_document(&rest),
                multi: extract_update_multi(&rest),
            }),
            "delete" => Ok(Query::Delete {
                collection: cmd_name_value_str(doc, "delete")?,
                // The Mongo driver puts the per-delete spec in `deletes: [{q, limit, ...}]`
                // (or `deletes_` in kind-1). Pull the first element's `q` field.
                filter: extract_delete_filter(&rest),
                limit: extract_delete_limit(&rest),
            }),
            "count" => Ok(Query::Count {
                collection: cmd_name_value_str(doc, "count")?,
                filter: get_doc(&rest, "filter").unwrap_or_default(),
            }),
            "drop" => Ok(Query::Drop {
                collection: cmd_name_value_str(doc, "drop")?,
            }),
            "distinct" => Ok(Query::Distinct {
                collection: cmd_name_value_str(doc, "distinct")?,
                field: get_string(&rest, "key")?,
                filter: get_doc(&rest, "query").unwrap_or_default(),
            }),
            "aggregate" => Ok(Query::Aggregate {
                collection: cmd_name_value_str(doc, "aggregate")?,
                pipeline: get_doc_array(&rest, "pipeline")?,
                cursor: get_doc(&rest, "cursor"),
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
    /// Write reply (inserted/modified count). `n_modified` feeds the reply's
    /// `nModified` — Spring Data's `long`-returning update methods read the
    /// modified count, not the matched one. `write_errors` mirrors
    /// MongoDB's per-document `writeErrors` array (unordered bulk inserts
    /// continue past failures and report them here with `ok: 1`).
    Write { n: i64, n_modified: i64, write_errors: Vec<WriteError> },
}

/// One entry of the reply's `writeErrors` array.
#[derive(Debug, Clone)]
pub struct WriteError {
    pub index: i32,
    pub code: i32,
    pub message: String,
}

impl QueryResult {
    pub fn to_reply_doc(&self) -> BsonDoc {
        match self {
            QueryResult::Single(d) => d.clone(),
            QueryResult::Cursor { first_batch, cursor_id, ns } => {
                let mut out = BsonDoc::new();
                out.insert("cursor", BsonValue::Document({
                    let mut c = BsonDoc::new();
                    c.insert("firstBatch", BsonValue::Array(first_batch.iter().map(|d| BsonValue::Document(d.clone())).collect()));
                    c.insert("id", BsonValue::Int64(*cursor_id));
                    c.insert("ns", BsonValue::String(ns.clone()));
                    c
                }));
                out.insert("ok", BsonValue::Double(1.0));
                out
            }
            QueryResult::Write { n, n_modified, write_errors } => {
                let mut out = BsonDoc::new();
                out.insert("n", BsonValue::Int64(*n));
                if *n_modified > 0 {
                    out.insert("nModified", BsonValue::Int64(*n_modified));
                }
                if !write_errors.is_empty() {
                    let errors = write_errors
                        .iter()
                        .map(|e| {
                            let mut d = BsonDoc::new();
                            d.insert("index", BsonValue::Int32(e.index));
                            d.insert("code", BsonValue::Int32(e.code));
                            d.insert("errmsg", BsonValue::String(e.message.clone()));
                            BsonValue::Document(d)
                        })
                        .collect();
                    out.insert("writeErrors", BsonValue::Array(errors));
                }
                out.insert("ok", BsonValue::Double(1.0));
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
            d.insert("ok", BsonValue::Double(1.0));
            d.insert("ismaster", BsonValue::Bool(true));
            d.insert("helloOk", BsonValue::Bool(true));
            d.insert("maxBsonObjectSize", BsonValue::Int32(16 * 1024 * 1024));
            d.insert("maxMessageSizeBytes", BsonValue::Int32(48 * 1024 * 1024));
            d.insert("maxWriteBatchSize", BsonValue::Int32(100_000));
            d.insert("logicalSessionTimeoutMinutes", BsonValue::Int32(30));
            d.insert("minWireVersion", BsonValue::Int32(0));
            d.insert("maxWireVersion", BsonValue::Int32(21));
            d.insert("readOnly", BsonValue::Bool(false));
            d.insert("me", BsonValue::String("127.0.0.1:27017".into()));
            d.insert("localTime", BsonValue::Int64(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0),
            ));
            Ok(QueryResult::Single(d))
        }
        Query::Ping => {
            let mut d = BsonDoc::new();
            d.insert("ok", BsonValue::Double(1.0));
            Ok(QueryResult::Single(d))
        }
        Query::ClientMetadata => {
            // The driver sends `client` after handshake to identify itself
            // (driver name, version, OS, platform). Server acks with `{ok:1}`
            // and may echo back. We just ack.
            let mut d = BsonDoc::new();
            d.insert("ok", BsonValue::Double(1.0));
            Ok(QueryResult::Single(d))
        }
        Query::NoopAck(_name) => {
            let mut d = BsonDoc::new();
            d.insert("ok", BsonValue::Double(1.0));
            Ok(QueryResult::Single(d))
        }
        Query::CreateIndexes { collection, indexes, specs } => {
            store.record_unique_indexes(tenant, &collection, indexes);
            store.record_index_specs(tenant, &collection, specs);
            let mut d = BsonDoc::new();
            d.insert("ok", BsonValue::Double(1.0));
            d.insert("createdCollectionAutomatically", BsonValue::Bool(false));
            Ok(QueryResult::Single(d))
        }
        Query::ListIndexes { collection } => {
            // Replay the recorded spec docs (name/key/flags) verbatim with a
            // version marker, matching mongod's listIndexes cursor shape.
            let batch: Vec<BsonValue> = store
                .index_specs(tenant, &collection)
                .into_iter()
                .map(|mut spec| {
                    spec.insert("v", BsonValue::Int32(2));
                    BsonValue::Document(spec)
                })
                .collect();
            let mut cursor = BsonDoc::new();
            cursor.insert("firstBatch", BsonValue::Array(batch));
            cursor.insert("id", BsonValue::Int64(0));
            cursor.insert("ns", BsonValue::String(format!("{tenant}.{collection}")));
            let mut d = BsonDoc::new();
            d.insert("cursor", BsonValue::Document(cursor));
            d.insert("ok", BsonValue::Double(1.0));
            Ok(QueryResult::Single(d))
        }
        Query::Find { collection, filter, sort, limit, skip } => {
            let coll = store.collection(tenant, &collection);
            let guard = coll.read().unwrap();
            let mut matched: Vec<BsonDoc> = Vec::new();
            let total = guard.iter().count();
            let mut in_iter = 0usize;
            for (_k, v) in guard.iter() {
                in_iter += 1;
                if matches_filter(v, &filter) {
                    if let BsonValue::Document(doc) = v {
                        matched.push(doc.clone());
                    }
                }
            }
            tracing::debug!(coll = %collection, tenant = %tenant, total, in_iter, matched = matched.len(), filter_keys = ?filter.0.keys().collect::<Vec<_>>(), "find");
            // Apply sort (in-memory; sufficient for the small tenant-scoped collections).
            if !sort.is_empty() {
                matched.sort_by(|a, b| {
                    for (k, dir) in &sort {
                        let av = a.0.get(k);
                        let bv = b.0.get(k);
                        let ord = bson_cmp_optional(av, bv);
                        let ord = if *dir < 0 { ord.reverse() } else { ord };
                        if ord != std::cmp::Ordering::Equal { return ord; }
                    }
                    std::cmp::Ordering::Equal
                });
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
        Query::Insert { collection, documents, ordered } => {
            let coll = store.collection(tenant, &collection);
            let mut guard = coll.write().unwrap();
            let indexes = store.unique_indexes(tenant, &collection);
            let mut n = 0i64;
            let mut write_errors: Vec<WriteError> = Vec::new();
            for (i, doc) in documents.into_iter().enumerate() {
                match enforce_unique_indexes(&guard, &indexes, &doc, None) {
                    Ok(()) => {
                        let id = doc.get("_id").map(|v| bson_to_string_id(v)).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                        guard.put(id, super::bson::BsonValue::Document(doc));
                        n += 1;
                    }
                    Err(MongoError::DuplicateKey(message)) if !ordered => {
                        // Unordered mode: skip the row, report per-document,
                        // keep inserting the rest (mongod semantics).
                        write_errors.push(WriteError { index: i as i32, code: 11000, message });
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(QueryResult::Write { n, n_modified: 0, write_errors })
        }
        Query::Update { collection, filter, update, multi } => {
            let coll = store.collection(tenant, &collection);
            let mut guard = coll.write().unwrap();
            let mut n = 0;
            // Collect ids to update first to avoid borrow conflict.
            let mut to_update: Vec<(String, BsonDoc)> = Vec::new();
            let indexes = store.unique_indexes(tenant, &collection);
            for (k, v) in guard.iter() {
                if let BsonValue::Document(doc) = v {
                    if matches_filter(&v, &filter) {
                        let mut new_doc = doc.clone();
                        apply_update(&mut new_doc, &update);
                        enforce_unique_indexes(&guard, &indexes, &new_doc, Some(k))?;
                        to_update.push((k.to_string(), new_doc));
                        n += 1;
                        if !multi { break; }
                    }
                }
            }
            for (k, new_doc) in to_update {
                guard.put(k, BsonValue::Document(new_doc));
            }
            Ok(QueryResult::Write { n, n_modified: n, write_errors: Vec::new() })
        }
        Query::Delete { collection, filter, limit } => {
            let coll = store.collection(tenant, &collection);
            let mut guard = coll.write().unwrap();
            let mut to_remove: Vec<String> = Vec::new();
            let mut n = 0;
            for (k, v) in guard.iter() {
                if let BsonValue::Document(_) = v {
                    if matches_filter(&v, &filter) {
                        to_remove.push(k.to_string());
                        n += 1;
                        if limit > 0 && n >= limit { break; }
                    }
                }
            }
            for k in to_remove {
                guard.remove(&k);
            }
            Ok(QueryResult::Write { n, n_modified: 0, write_errors: Vec::new() })
        }
        Query::Count { collection, filter } => {
            let coll = store.collection(tenant, &collection);
            let guard = coll.read().unwrap();
            let n = guard.iter().filter(|(_, v)| matches_filter(v, &filter)).count() as i64;
            tracing::debug!(coll = %collection, tenant = %tenant, n, filter_keys = ?filter.0.keys().collect::<Vec<_>>(), "count");
            Ok(QueryResult::Write { n, n_modified: 0, write_errors: Vec::new() })
        }
        Query::Drop { collection } => {
            // Clear the table; ack ok.
            let coll = store.collection(tenant, &collection);
            coll.write().unwrap().clear();
            let mut d = BsonDoc::new();
            d.insert("ok", BsonValue::Double(1.0));
            d.insert("dropped", BsonValue::String(collection));
            Ok(QueryResult::Single(d))
        }
        Query::Distinct { collection, field, filter } => {
            let coll = store.collection(tenant, &collection);
            let guard = coll.read().unwrap();
            let mut seen: Vec<BsonValue> = Vec::new();
            for (_k, v) in guard.iter() {
                if !matches_filter(v, &filter) { continue; }
                let BsonValue::Document(doc) = v else { continue; };
                if let Some(val) = doc.0.get(&field) {
                    if !seen.iter().any(|x| x == val) {
                        seen.push(val.clone());
                    }
                }
            }
            Ok(QueryResult::Single({
                let mut d = BsonDoc::new();
                d.insert("values", BsonValue::Array(seen));
                d.insert("ok", BsonValue::Double(1.0));
                d
            }))
        }
        Query::Aggregate { collection, pipeline, cursor } => {
            let coll = store.collection(tenant, &collection);
            let guard = coll.read().unwrap();
            // Materialise the collection as a working vector.
            let mut docs: Vec<BsonDoc> = Vec::new();
            for (_k, v) in guard.iter() {
                if let BsonValue::Document(d) = v { docs.push(d.clone()); }
            }
            // Apply pipeline.
            for stage in &pipeline {
                docs = apply_aggregate_stage(store, tenant, docs, stage);
            }
            // Apply cursor.batchSize if requested.
            let batch_size = cursor.as_ref()
                .and_then(|c| c.0.get("batchSize"))
                .and_then(|v| match v {
                    BsonValue::Int32(i) => Some(*i as usize),
                    BsonValue::Int64(i) => Some(*i as usize),
                    _ => None,
                });
            let first_batch: Vec<BsonDoc> = if let Some(n) = batch_size {
                docs.into_iter().take(n).collect()
            } else {
                docs
            };
            Ok(QueryResult::Cursor {
                first_batch,
                cursor_id: 0,
                ns: format!("{tenant}.{collection}"),
            })
        }
        Query::Unsupported(name) => Err(MongoError::UnsupportedCommand(name)),
    }
}

// ── filter / update helpers ──────────────────────────────────────────────────

fn matches_filter(value: &BsonValue, filter: &BsonDoc) -> bool {
    let BsonValue::Document(doc) = value else { return false; };
    let result = matches_filter_inner(value, doc, filter);
    let key = doc.0.get("tenantId").map(|v| format!("{:?}", v)).unwrap_or_default();
    let status = doc.0.get("status").map(|v| format!("{:?}", v)).unwrap_or_default();
    tracing::debug!(tenant = %key, status = %status, result, "matches_filter");
    result
}

fn matches_filter_inner(value: &BsonValue, doc: &BsonDoc, filter: &BsonDoc) -> bool {
    for (k, want) in filter.iter() {
        // Logical operators may appear at ANY nesting level (per MongoDB spec,
        // they're query operators not field operators). Match the wire.
        match k.as_str() {
            "$and" => {
                if let BsonValue::Array(arr) = want {
                    if !arr.iter().all(|x| matches_filter_inner(value, doc, as_doc(x))) { return false; }
                    continue;
                }
            }
            "$or" => {
                if let BsonValue::Array(arr) = want {
                    if !arr.iter().any(|x| matches_filter_inner(value, doc, as_doc(x))) { return false; }
                    continue;
                }
            }
            "$nor" => {
                if let BsonValue::Array(arr) = want {
                    if arr.iter().any(|x| matches_filter_inner(value, doc, as_doc(x))) { return false; }
                    continue;
                }
            }
            "$not" => {
                if let BsonValue::Document(op) = want {
                    if matches_filter_inner(value, doc, op) { return false; }
                    continue;
                }
            }
            _ => {}
        }
        let got = doc.get(k);
        if !match_field_value(got, want) { return false; }
    }
    true
}

/// Check whether `got` (the field value, or None if the field is absent)
/// matches `want` (the filter expression). Handles both:
///   * scalar match: `want` is a primitive (`BsonValue::String` etc.) — exact equality
///   * operator match: `want` is a `BsonValue::Document` whose keys are `$eq`/`$ne`/`$lt`/`...
///   * logical match: `want` is a `BsonValue::Document` whose only key is `$and`/`$or`/`$nor`/`$not`
///   * regex match: `want` is a `BsonValue::Document` whose key is `$regex` (with optional `$options`)
fn match_field_value(got: Option<&BsonValue>, want: &BsonValue) -> bool {
    if bson_eq(got, want) { return true; }
    if let Some(BsonValue::Array(values)) = got {
        if values.iter().any(|value| bson_eq(Some(value), want)) { return true; }
    }
    // `want` may be a regex/operator doc OR a `BsonValue::Regex` directly (the
    // shorthand `{field: /pattern/}` form, which the Spring Data driver emits for
    // substring queries). Unwrap the shorthand into the standard operator-doc shape.
    let op_owned;
    let op: &BsonDoc = match want {
        BsonValue::Document(d) => d,
        BsonValue::Regex { pattern, options } => {
            let mut m = std::collections::BTreeMap::new();
            m.insert("$regex".to_string(), BsonValue::Regex { pattern: pattern.clone(), options: options.clone() });
            op_owned = BsonDoc(m.into());
            &op_owned
        }
        _ => return false,
    };
    // Logical operators may also be inside field-value docs (rare, but valid per spec).
    if let Some(BsonValue::Array(arr)) = op.0.get("$and") {
        if !arr.iter().all(|x| match_field_value(got, x)) { return false; }
        return true;
    }
    if let Some(BsonValue::Array(arr)) = op.0.get("$or") {
        if !arr.iter().any(|x| match_field_value(got, x)) { return false; }
        return true;
    }
    if let Some(target) = op.0.get("$eq") {
        return bson_eq(got, target);
    }
    if let Some(target) = op.0.get("$ne") {
        return !bson_eq(got, target);
    }
    if let Some(target) = op.0.get("$lt") {
        let got = got.unwrap_or(&BsonValue::Null);
        return bson_lt(got, target);
    }
    if let Some(target) = op.0.get("$lte") {
        let got = got.unwrap_or(&BsonValue::Null);
        return bson_lt(got, target) || bson_eq(Some(got), target);
    }
    if let Some(target) = op.0.get("$gt") {
        let got = got.unwrap_or(&BsonValue::Null);
        return bson_lt(target, got);
    }
    if let Some(target) = op.0.get("$gte") {
        let got = got.unwrap_or(&BsonValue::Null);
        return bson_lt(target, got) || bson_eq(Some(got), target);
    }
    if let Some(BsonValue::Array(arr)) = op.0.get("$in") {
        return match got {
            Some(BsonValue::Array(values)) => values
                .iter()
                .any(|value| arr.iter().any(|target| bson_eq(Some(value), target))),
            Some(value) => arr.iter().any(|target| bson_eq(Some(value), target)),
            None => false,
        };
    }
    if let Some(BsonValue::Array(arr)) = op.0.get("$nin") {
        return match got {
            Some(BsonValue::Array(values)) => !values
                .iter()
                .any(|value| arr.iter().any(|target| bson_eq(Some(value), target))),
            Some(value) => !arr.iter().any(|target| bson_eq(Some(value), target)),
            None => true,
        };
    }
    if let Some(BsonValue::Bool(true)) = op.0.get("$exists") {
        return got.is_some();
    }
    // `$regex: pattern` with optional `$options: "i"` (and friends). The substring
    // queries the Spring Data driver emits all use `\Q...\E` (PCRE quote-meta) with
    // `i` flag, so the matcher only needs to honour that.
    // `$regex: pattern` with optional `$options: "i"` (and friends). The substring
    // queries the Spring Data driver emits all use `\Q...\E` (PCRE quote-meta) with
    // `i` flag, so the matcher only needs to honour that.
    //
    // The `$regex` value IS a `BsonValue::Regex(pattern, options)` (BSON tag 0x0b).
    // A top-level `$options` doc field overrides the regex's own options.
    if let Some(regex_value) = op.0.get("$regex") {
        let (pattern_str, regex_opts) = match regex_value {
            BsonValue::Regex { pattern, options } => (pattern.as_str(), options.as_str()),
            BsonValue::String(s) => (s.as_str(), ""),
            _ => return false,
        };
        let options = op.0.get("$options")
            .and_then(|v| if let BsonValue::String(s) = v { Some(s.as_str()) } else { None })
            .unwrap_or(regex_opts);
        return got.is_some() && regex_match(got.unwrap(), pattern_str, options);
    }
    false
}

/// Minimal regex matcher — handles the Spring Data MongoDB substring case:
// `pattern` is typically `\Q<literal>\E` (PCRE quote-meta) and `options` is typically `"i"`.
///
/// We do NOT implement a full regex engine — only the cases actually emitted by the
/// Spring Data MongoDB driver in the upstream test corpus. Adding more operators
/// (`.`, `*`, `+`, character classes) is mechanical when needed.
fn regex_match(value: &BsonValue, pattern_str: &str, options: &str) -> bool {
    let BsonValue::String(haystack) = value else { return false; };
    let case_insensitive = options.contains('i');
    let needle_norm = if case_insensitive { pattern_str.to_lowercase() } else { pattern_str.to_string() };
    let haystack_norm = if case_insensitive { haystack.to_lowercase() } else { haystack.clone() };
    // Unwrap PCRE `\Q...\E` quote-meta markers — the only form Spring Data emits.
    // After lowercasing, `\Q...\E` becomes `\q...\e`. The literal to match is everything
    // between the `\q` (2 bytes) and the `\e` (2 bytes).
    let needle_lit = if needle_norm.starts_with("\\q") || needle_norm.starts_with("\\Q") {
        match needle_norm[2..].find("\\e").or_else(|| needle_norm[2..].find("\\E")) {
            Some(rel_end) => needle_norm[2..2 + rel_end].to_string(),
            None => needle_norm.clone(),
        }
    } else {
        needle_norm.clone()
    };
    if needle_lit.is_empty() { return false; }
    haystack_norm.contains(&needle_lit)
}

fn as_doc(b: &BsonValue) -> &BsonDoc {
    match b {
        BsonValue::Document(d) => d,
        _ => panic!("expected embedded doc"),
    }
}

fn bson_lt(a: &BsonValue, b: &BsonValue) -> bool {
    match (a, b) {
        (BsonValue::Null, _) => false,
        (BsonValue::Int32(x), BsonValue::Int32(y)) => x < y,
        (BsonValue::Int32(x), BsonValue::Int64(y)) => (*x as i64) < *y,
        (BsonValue::Int64(x), BsonValue::Int32(y)) => *x < (*y as i64),
        (BsonValue::Int64(x), BsonValue::Int64(y)) => x < y,
        (BsonValue::Double(x), BsonValue::Double(y)) => x < y,
        (BsonValue::String(x), BsonValue::String(y)) => x < y,
        (BsonValue::ObjectId(x), BsonValue::ObjectId(y)) => x < y,
        (BsonValue::DateTime(x), BsonValue::DateTime(y)) => x < y,
        _ => false,
    }
}

fn bson_eq(a: Option<&BsonValue>, b: &BsonValue) -> bool {
    match (a, b) {
        (Some(x), y) => x == y,
        (None, BsonValue::Null) => true,
        (None, _) => false,
    }
}

fn enforce_unique_indexes(
    rows: &hashtrinity::Table<String, BsonValue>,
    indexes: &[UniqueIndex],
    candidate: &BsonDoc,
    current_id: Option<&str>,
) -> MongoResult<()> {
    for index in indexes {
        if index.sparse && index.fields.iter().any(|field| candidate.get(field).is_none()) {
            continue;
        }
        if let Some(filter) = &index.partial_filter {
            if !matches_filter(&BsonValue::Document(candidate.clone()), filter) {
                continue;
            }
        }
        for (row_id, value) in rows.iter() {
            if current_id == Some(row_id) {
                continue;
            }
            let BsonValue::Document(existing) = value else { continue; };
            if let Some(filter) = &index.partial_filter {
                if !matches_filter(value, filter) {
                    continue;
                }
            }
            if index
                .fields
                .iter()
                .all(|field| existing.get(field) == candidate.get(field))
            {
                return Err(MongoError::DuplicateKey(format!(
                    "index {} on {}",
                    index.name,
                    index.fields.join(",")
                )));
            }
        }
    }
    Ok(())
}

fn apply_aggregate_stage(
    store: &MongoStore,
    tenant: &str,
    docs: Vec<BsonDoc>,
    stage: &BsonDoc,
) -> Vec<BsonDoc> {
    // Each stage is `{ "$op": args }`. Pick the first key starting with `$`.
    for (op, args) in stage.iter() {
        if !op.starts_with('$') { continue; }
        return match op.as_str() {
            "$match" => match args {
                BsonValue::Document(m) => docs.into_iter().filter(|d| {
                    matches_filter(&BsonValue::Document(d.clone()), m)
                }).collect(),
                _ => docs,
            },
            "$count" => match args {
                BsonValue::String(name) => {
                    let n = docs.len() as i64;
                    let mut d = BsonDoc::new();
                    d.insert(name.clone(), BsonValue::Int32(n as i32));
                    vec![d]
                }
                _ => docs,
            },
            "$group" => apply_group(docs, args),
            "$unwind" => apply_unwind(docs, args),
            "$project" => apply_project(docs, args),
            "$lookup" => apply_lookup(store, tenant, docs, args),
            "$addFields" | "$set" => apply_add_fields(docs, args),
            "$sort" => apply_sort(docs, args),
            "$limit" => match args {
                BsonValue::Int32(n) => docs.into_iter().take(*n as usize).collect(),
                BsonValue::Int64(n) => docs.into_iter().take(*n as usize).collect(),
                _ => docs,
            },
            "$skip" => match args {
                BsonValue::Int32(n) => docs.into_iter().skip(*n as usize).collect(),
                BsonValue::Int64(n) => docs.into_iter().skip(*n as usize).collect(),
                _ => docs,
            },
            "$facet" => apply_facet(store, tenant, docs, args),
            // Unknown stage — pass through unchanged.
            _ => docs,
        };
    }
    docs
}

fn apply_group(docs: Vec<BsonDoc>, args: &BsonValue) -> Vec<BsonDoc> {
    // args is `{_id: <group-key-expr>, <field>: {$sum: 1} or {$first: "$x"}}`
    let BsonValue::Document(spec) = args else { return docs; };
    let id_expr = spec.0.get("_id");
    let id_expr = match id_expr {
        Some(e) => e,
        None => return docs,
    };
    // Build groups keyed by the computed group key (BsonValue).
    let mut groups: BTreeMap<String, (BsonValue, BsonDoc)> = BTreeMap::new();
    for d in &docs {
        let key_bson = match id_expr {
            BsonValue::Null => BsonValue::Null,
            BsonValue::String(s) if s == "$_id" => BsonValue::String(d.0.get("_id").map(|v| format!("{v:?}")).unwrap_or_default()),
            BsonValue::String(s) if s.starts_with('$') => {
                let k = &s[1..];
                d.0.get(k).cloned().unwrap_or(BsonValue::Null)
            }
            _ => id_expr.clone(),
        };
        let key_str = format!("{key_bson:?}");
        let entry = groups.entry(key_str).or_insert_with(|| (key_bson.clone(), BsonDoc::new()));
        // Apply accumulators.
        for (field, val) in spec.iter() {
            if field == "_id" { continue; }
            if let BsonValue::Document(op) = val {
                // `$sum: <expr>` — the driver emits `$sum: 1` to count per group key.
                // We support the integer-literal case (count semantics); field-path sums
                // (e.g. `$sum: "$amount"`) are not yet implemented.
                if let Some(sum_val) = op.0.get("$sum") {
                    let increment = match sum_val {
                        BsonValue::Int32(i) => *i as i64,
                        BsonValue::Int64(i) => *i,
                        BsonValue::Double(f) => *f as i64,
                        _ => 0, // not a literal int → unsupported
                    };
                    let cur = entry.1.0.get(field).and_then(|v| match v {
                        BsonValue::Int32(i) => Some(*i as i64),
                        BsonValue::Int64(i) => Some(*i),
                        _ => None,
                    }).unwrap_or(0);
                    entry.1.0.insert(field.clone(), BsonValue::Int64(cur + increment));
                } else if let Some(target) = op.get("$first") {
                    // $first: "$x" — first non-null value
                    if let BsonValue::String(field_path) = target {
                        if field_path.starts_with('$') {
                            let k = &field_path[1..];
                            if !entry.1.0.contains_key(field) {
                                if let Some(v) = d.0.get(k) {
                                    entry.1.0.insert(field.clone(), v.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let mut out = Vec::new();
    for (_k, (id, acc)) in groups {
        let mut result = BsonDoc::new();
        result.insert("_id".to_string(), id);
        for (k, v) in acc.0.iter() {
            result.0.insert(k.clone(), v.clone());
        }
        out.push(result);
    }
    out
}

fn apply_unwind(docs: Vec<BsonDoc>, args: &BsonValue) -> Vec<BsonDoc> {
    // `{$unwind: "$path"}` or `{$unwind: {path: "$path", preserveNullAndEmptyArrays: bool}}`
    let path = match args {
        BsonValue::String(s) if s.starts_with('$') => &s[1..],
        BsonValue::Document(d) => match d.0.get("path") {
            Some(BsonValue::String(s)) if s.starts_with('$') => &s[1..],
            _ => return docs,
        },
        _ => return docs,
    };
    let mut out = Vec::new();
    for d in docs {
        match d.0.get(path) {
            Some(BsonValue::Array(arr)) => {
                for item in arr {
                    let mut copy = d.clone();
                    copy.0.insert(path.to_string(), item.clone());
                    out.push(copy);
                }
            }
            _ => out.push(d),
        }
    }
    out
}

fn apply_project(docs: Vec<BsonDoc>, args: &BsonValue) -> Vec<BsonDoc> {
    let BsonValue::Document(spec) = args else { return docs; };
    docs.into_iter().map(|mut d| {
        let mut new_doc = BsonDoc::new();
        for (k, v) in spec.iter() {
            if k == "_id" {
                new_doc.0.insert("_id".to_string(), d.0.get("_id").cloned().unwrap_or(BsonValue::Null));
                continue;
            }
            if let BsonValue::Int32(0) | BsonValue::Int64(0) = v {
                continue; // exclusion
            }
            if let BsonValue::String(s) = v {
                if s == "$$REMOVE" { continue; }
                if s.starts_with('$') {
                    let path = &s[1..];
                    if let Some(val) = d.0.get(path) {
                        new_doc.0.insert(k.clone(), val.clone());
                        continue;
                    }
                }
            }
            new_doc.0.insert(k.clone(), v.clone());
        }
        new_doc
    }).collect()
}

fn apply_lookup(
    store: &MongoStore,
    tenant: &str,
    docs: Vec<BsonDoc>,
    args: &BsonValue,
) -> Vec<BsonDoc> {
    let BsonValue::Document(spec) = args else { return docs; };
    let Some(BsonValue::String(from)) = spec.get("from") else { return docs; };
    let Some(BsonValue::String(output_field)) = spec.get("as") else { return docs; };
    let variables = get_doc(spec, "let").unwrap_or_default();
    let pipeline = get_doc_array(spec, "pipeline").unwrap_or_default();
    let foreign = store.collection(tenant, from);
    let foreign_docs = foreign
        .read()
        .unwrap()
        .iter()
        .filter_map(|(_, value)| match value {
            BsonValue::Document(doc) => Some(doc.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    docs.into_iter()
        .map(|mut doc| {
            let vars = variables
                .iter()
                .map(|(name, expression)| {
                    (name.clone(), eval_expression(expression, &doc, &BsonDoc::new()))
                })
                .collect::<BTreeMap<_, _>>()
                .into();
            let mut joined = foreign_docs.clone();
            for stage in &pipeline {
                if let Some(BsonValue::Document(filter)) = stage.get("$match") {
                    joined.retain(|candidate| matches_filter_with_vars(candidate, filter, &vars));
                }
            }
            doc.insert(
                output_field.clone(),
                BsonValue::Array(joined.into_iter().map(BsonValue::Document).collect()),
            );
            doc
        })
        .collect()
}

fn apply_add_fields(docs: Vec<BsonDoc>, args: &BsonValue) -> Vec<BsonDoc> {
    let BsonValue::Document(spec) = args else { return docs; };
    docs.into_iter()
        .map(|mut doc| {
            for (field, expression) in spec.iter() {
                let value = eval_expression(expression, &doc, &BsonDoc::new());
                doc.insert(field.clone(), value);
            }
            doc
        })
        .collect()
}

fn matches_filter_with_vars(doc: &BsonDoc, filter: &BsonDoc, vars: &BsonDoc) -> bool {
    for (field, expression) in filter.iter() {
        if field == "$expr" {
            if !eval_bool_expression(expression, doc, vars) {
                return false;
            }
        } else {
            let one = BsonDoc(
                [(field.clone(), expression.clone())]
                    .into_iter()
                    .collect(),
            );
            if !matches_filter(&BsonValue::Document(doc.clone()), &one) {
                return false;
            }
        }
    }
    true
}

fn eval_expression(expression: &BsonValue, doc: &BsonDoc, vars: &BsonDoc) -> BsonValue {
    match expression {
        BsonValue::String(path) if path.starts_with("$$") => vars
            .get(&path[2..])
            .cloned()
            .unwrap_or(BsonValue::Null),
        BsonValue::String(path) if path.starts_with('$') => doc
            .get(&path[1..])
            .cloned()
            .unwrap_or(BsonValue::Null),
        BsonValue::Document(operator) => {
            if let Some(value) = operator.get("$toString") {
                return match eval_expression(value, doc, vars) {
                    BsonValue::String(value) => BsonValue::String(value),
                    BsonValue::ObjectId(value) => BsonValue::String(
                        value.iter().map(|byte| format!("{byte:02x}")).collect(),
                    ),
                    BsonValue::Int32(value) => BsonValue::String(value.to_string()),
                    BsonValue::Int64(value) => BsonValue::String(value.to_string()),
                    BsonValue::Double(value) => BsonValue::String(value.to_string()),
                    BsonValue::Bool(value) => BsonValue::String(value.to_string()),
                    BsonValue::Null => BsonValue::Null,
                    value => BsonValue::String(format!("{value:?}")),
                };
            }
            if let Some(value) = operator.get("$size") {
                return match eval_expression(value, doc, vars) {
                    BsonValue::Array(values) => BsonValue::Int32(values.len() as i32),
                    _ => BsonValue::Int32(0),
                };
            }
            if let Some(BsonValue::Array(parts)) = operator.get("$cond") {
                if parts.len() == 3 {
                    return if eval_bool_expression(&parts[0], doc, vars) {
                        eval_expression(&parts[1], doc, vars)
                    } else {
                        eval_expression(&parts[2], doc, vars)
                    };
                }
            }
            expression.clone()
        }
        _ => expression.clone(),
    }
}

fn eval_bool_expression(expression: &BsonValue, doc: &BsonDoc, vars: &BsonDoc) -> bool {
    let BsonValue::Document(operator) = expression else {
        return matches!(eval_expression(expression, doc, vars), BsonValue::Bool(true));
    };
    if let Some(BsonValue::Array(parts)) = operator.get("$and") {
        return parts.iter().all(|part| eval_bool_expression(part, doc, vars));
    }
    if let Some(BsonValue::Array(parts)) = operator.get("$or") {
        return parts.iter().any(|part| eval_bool_expression(part, doc, vars));
    }
    if let Some(BsonValue::Array(parts)) = operator.get("$eq") {
        return parts.len() == 2
            && eval_expression(&parts[0], doc, vars) == eval_expression(&parts[1], doc, vars);
    }
    false
}

fn apply_sort(docs: Vec<BsonDoc>, args: &BsonValue) -> Vec<BsonDoc> {
    let BsonValue::Document(spec) = args else { return docs; };
    let sort = ordered_sort(spec);
    let mut docs = docs;
    docs.sort_by(|a, b| {
        for (k, dir) in &sort {
            let av = a.0.get(k);
            let bv = b.0.get(k);
            let ord = bson_cmp_optional(av, bv);
            let ord = if *dir < 0 { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal { return ord; }
        }
        std::cmp::Ordering::Equal
    });
    docs
}

fn apply_facet(
    store: &MongoStore,
    tenant: &str,
    docs: Vec<BsonDoc>,
    args: &BsonValue,
) -> Vec<BsonDoc> {
    // args: {field1: [pipeline], field2: [pipeline], ...}
    let BsonValue::Document(spec) = args else { return vec![BsonDoc::new()]; };
    let mut result = BsonDoc::new();
    for (name, pipelines) in spec.iter() {
        let BsonValue::Array(stages) = pipelines else { continue; };
        let mut sub_docs = docs.clone();
        for stage in stages {
            if let BsonValue::Document(s) = stage {
                sub_docs = apply_aggregate_stage(store, tenant, sub_docs, s);
            }
        }
        result.0.insert(name.clone(), BsonValue::Array(sub_docs.into_iter().map(BsonValue::Document).collect()));
    }
    vec![result]
}

fn apply_update(doc: &mut BsonDoc, update: &BsonValue) {
    match update {
        BsonValue::Document(update) => apply_update_document(doc, update, false),
        BsonValue::Array(stages) => {
            for stage in stages {
                if let BsonValue::Document(stage) = stage {
                    apply_update_document(doc, stage, true);
                }
            }
        }
        _ => {}
    }
}

fn apply_update_document(doc: &mut BsonDoc, update: &BsonDoc, evaluate: bool) {
    // Support `$set` and `$unset` only — enough for the OpenFrame access
    // pattern (`MongoTemplate.updateFirst` with `$set` is the common case).
    if let Some(BsonValue::Document(sets)) = update.get("$set") {
        let mut vars = BsonDoc::new();
        vars.insert(
            "NOW",
            BsonValue::DateTime(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|value| value.as_millis() as i64)
                    .unwrap_or(0),
            ),
        );
        for (k, v) in sets.iter() {
            let value = if evaluate {
                eval_expression(v, doc, &vars)
            } else {
                v.clone()
            };
            doc.insert(k.clone(), value);
        }
    }
    if let Some(BsonValue::Document(sets)) = update.get("$unset") {
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

fn bson_to_string_id(v: &BsonValue) -> String {
    match v {
        BsonValue::String(s) => s.clone(),
        BsonValue::ObjectId(id) => id.iter().map(|b| format!("{b:02x}")).collect(),
        BsonValue::Int32(i) => i.to_string(),
        BsonValue::Int64(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

// ── get_* accessors ──────────────────────────────────────────────────────────

fn get_string(doc: &BsonDoc, key: &str) -> MongoResult<String> {
    match doc.get(key) {
        Some(BsonValue::String(s)) => Ok(s.clone()),
        Some(BsonValue::Int32(i)) => Ok(i.to_string()),
        Some(BsonValue::Int64(i)) => Ok(i.to_string()),
        _ => Err(MongoError::Internal(format!("missing string field `{key}`"))),
    }
}

/// The collection name for CRUD commands is the VALUE of the cmd_name key
/// (e.g. `{find: "users", filter: {...}}` → "users"). After `from_command_name`
/// strips cmd_name from `rest`, we look it up in the original doc instead.
fn cmd_name_value_str(doc: &BsonDoc, key: &str) -> MongoResult<String> {
    match doc.0.get(key) {
        Some(BsonValue::String(s)) => Ok(s.clone()),
        Some(v) => Ok(format!("{v:?}")),
        None => Err(MongoError::Internal(format!("missing string field `{key}`"))),
    }
}

/// Extract the filter from a `delete` command's `deletes` array. The driver
/// sends `deletes: [{q: {filter}, limit: 1, ...}]`. We take the first spec.
fn extract_delete_filter(rest: &BsonDoc) -> BsonDoc {
    if let Some(BsonValue::Array(arr)) = rest.0.get("deletes") {
        if let Some(BsonValue::Document(spec)) = arr.first() {
            if let Some(BsonValue::Document(q)) = spec.0.get("q") {
                return q.clone();
            }
        }
    }
    // Legacy: a top-level `filter` field (older driver versions).
    if let Some(BsonValue::Document(f)) = rest.0.get("filter") {
        return f.clone();
    }
    BsonDoc::new()
}

fn extract_delete_limit(rest: &BsonDoc) -> i64 {
    if let Some(BsonValue::Array(arr)) = rest.0.get("deletes") {
        if let Some(BsonValue::Document(spec)) = arr.first() {
            if let Some(BsonValue::Int32(n)) = spec.0.get("limit") {
                return *n as i64;
            }
            if let Some(BsonValue::Int64(n)) = spec.0.get("limit") {
                return *n;
            }
        }
    }
    0
}

fn update_spec(rest: &BsonDoc) -> Option<&BsonDoc> {
    let BsonValue::Array(updates) = rest.get("updates")? else { return None; };
    let BsonValue::Document(spec) = updates.first()? else { return None; };
    Some(spec)
}

fn extract_update_filter(rest: &BsonDoc) -> BsonDoc {
    if let Some(BsonValue::Document(filter)) = update_spec(rest).and_then(|spec| spec.get("q")) {
        return filter.clone();
    }
    get_doc(rest, "filter").unwrap_or_default()
}

fn extract_update_document(rest: &BsonDoc) -> BsonValue {
    if let Some(update) = update_spec(rest).and_then(|spec| spec.get("u")) {
        return update.clone();
    }
    rest.get("updateDocument")
        .cloned()
        .unwrap_or_else(|| BsonValue::Document(BsonDoc::new()))
}

fn extract_update_multi(rest: &BsonDoc) -> bool {
    update_spec(rest)
        .and_then(|spec| get_bool(spec, "multi"))
        .or_else(|| get_bool(rest, "multi"))
        .unwrap_or(false)
}
fn get_doc(doc: &BsonDoc, key: &str) -> Option<BsonDoc> {
    match doc.get(key) {
        Some(BsonValue::Document(d)) => Some(d.clone()),
        _ => None,
    }
}
fn get_doc_array(doc: &BsonDoc, key: &str) -> MongoResult<Vec<BsonDoc>> {
    match doc.get(key) {
        Some(BsonValue::Array(arr)) => arr.iter().filter_map(|v| match v {
            BsonValue::Document(d) => Some(d.clone()),
            _ => None,
        }).collect::<Vec<_>>().pipe(Ok),
        _ => Err(MongoError::Internal(format!("missing array field `{key}`"))),
    }
}
fn get_i64(doc: &BsonDoc, key: &str) -> MongoResult<i64> {
    match doc.get(key) {
        Some(BsonValue::Int64(i)) => Ok(*i),
        Some(BsonValue::Int32(i)) => Ok(*i as i64),
        _ => Err(MongoError::Internal(format!("missing int field `{key}`"))),
    }
}
fn get_bool(doc: &BsonDoc, key: &str) -> Option<bool> {
    match doc.get(key) {
        Some(BsonValue::Bool(b)) => Some(*b),
        _ => None,
    }
}

fn get_unique_indexes(doc: &BsonDoc) -> Vec<UniqueIndex> {
    let Some(BsonValue::Array(indexes)) = doc.get("indexes") else { return Vec::new(); };
    indexes
        .iter()
        .filter_map(|value| {
            let BsonValue::Document(spec) = value else { return None; };
            if !get_bool(spec, "unique").unwrap_or(false) {
                return None;
            }
            let BsonValue::Document(key) = spec.get("key")? else { return None; };
            let fields = key.iter().map(|(field, _)| field.clone()).collect::<Vec<_>>();
            if fields.is_empty() {
                return None;
            }
            Some(UniqueIndex {
                name: spec
                    .get("name")
                    .and_then(BsonValue::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| fields.join("_")),
                fields,
                sparse: get_bool(spec, "sparse").unwrap_or(false),
                partial_filter: get_doc(spec, "partialFilterExpression"),
            })
        })
        .collect()
}

/// Raw index spec docs (unique and non-unique) for the `listIndexes` replay
/// surface. Only entries with both a string `name` and a document `key`
/// are kept — those are the fields mongod guarantees in its replies.
fn get_index_specs(doc: &BsonDoc) -> Vec<BsonDoc> {
    let Some(BsonValue::Array(indexes)) = doc.get("indexes") else { return Vec::new(); };
    indexes
        .iter()
        .filter_map(|value| {
            let BsonValue::Document(spec) = value else { return None; };
            let named = matches!(spec.get("name"), Some(BsonValue::String(_)));
            let keyed = matches!(spec.get("key"), Some(BsonValue::Document(_)));
            (named && keyed).then(|| spec.clone())
        })
        .collect()
}

/// Extract a `sort` field as a list of (field, direction) pairs. Direction is
/// 1 for ASC, -1 for DESC (per MongoDB convention).
fn get_sort(doc: &BsonDoc) -> Vec<(String, i32)> {
    let Some(sort) = doc.get("sort") else { return Vec::new(); };
    let BsonValue::Document(s) = sort else { return Vec::new(); };
    ordered_sort(s)
}

fn ordered_sort(spec: &BsonDoc) -> Vec<(String, i32)> {
    let mut fields = spec
        .iter()
        .map(|(field, value)| {
            let direction = match value {
                BsonValue::Int32(value) => *value,
                BsonValue::Int64(value) => *value as i32,
                BsonValue::Double(value) => *value as i32,
                _ => 1,
            };
            (field.clone(), direction)
        })
        .collect::<Vec<_>>();
    if fields.len() > 1 {
        fields.sort_by_key(|(field, _)| field == "_id");
    }
    fields
}

fn bson_cmp_optional(a: Option<&BsonValue>, b: Option<&BsonValue>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(a), Some(b)) => bson_cmp(a, b),
        (Some(a), None) => bson_cmp(a, &BsonValue::Null),
        (None, Some(b)) => bson_cmp(&BsonValue::Null, b),
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn bson_cmp(a: &BsonValue, b: &BsonValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if matches!(a, BsonValue::Null) && !matches!(b, BsonValue::Null) {
        return Ordering::Less;
    }
    if !matches!(a, BsonValue::Null) && matches!(b, BsonValue::Null) {
        return Ordering::Greater;
    }
    if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
        return a.cmp(&b);
    }
    match (a, b) {
        (BsonValue::String(x), BsonValue::String(y)) => x.cmp(y),
        (BsonValue::ObjectId(x), BsonValue::ObjectId(y)) => x.cmp(y),
        (BsonValue::DateTime(x), BsonValue::DateTime(y)) => x.cmp(y),
        (BsonValue::Bool(x), BsonValue::Bool(y)) => x.cmp(y),
        (BsonValue::Null, BsonValue::Null) => Ordering::Equal,
        _ => Ordering::Equal,
    }
}

// ── tiny pipe helper so the iterator chain above reads cleanly ──────────────

trait Pipe: Sized {
    fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R { f(self) }
}
impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_command_keeps_collection_separate_from_replacement_document() {
        let mut filter = BsonDoc::new();
        filter.insert("_id", BsonValue::String("script-1".into()));
        let mut replacement = BsonDoc::new();
        replacement.insert("_id", BsonValue::String("script-1".into()));
        replacement.insert("status", BsonValue::String("DELETED".into()));
        let mut command = BsonDoc::new();
        command.insert("update", BsonValue::String("scripts".into()));
        command.insert("filter", BsonValue::Document(filter));
        command.insert("updateDocument", BsonValue::Document(replacement));

        match Query::from_command(&command).unwrap() {
            Query::Update { collection, filter, update, multi } => {
                assert_eq!(collection, "scripts");
                assert_eq!(filter.get("_id"), Some(&BsonValue::String("script-1".into())));
                assert_eq!(
                    update,
                    BsonValue::Document(BsonDoc(
                        [
                            ("_id".into(), BsonValue::String("script-1".into())),
                            ("status".into(), BsonValue::String("DELETED".into())),
                        ]
                        .into_iter()
                        .collect(),
                    )),
                );
                assert!(!multi);
            }
            other => panic!("expected update query, got {other:?}"),
        }
    }

    #[test]
    fn update_pipeline_evaluates_now() {
        let mut set = BsonDoc::new();
        set.insert("status", BsonValue::String("READ".into()));
        set.insert("readAt", BsonValue::String("$$NOW".into()));
        let mut stage = BsonDoc::new();
        stage.insert("$set", BsonValue::Document(set));
        let mut doc = BsonDoc::new();

        apply_update(
            &mut doc,
            &BsonValue::Array(vec![BsonValue::Document(stage)]),
        );

        assert_eq!(doc.get("status"), Some(&BsonValue::String("READ".into())));
        assert!(matches!(doc.get("readAt"), Some(BsonValue::DateTime(_))));
    }

    #[test]
    fn in_operator_intersects_array_fields() {
        let got = BsonValue::Array(vec![
            BsonValue::String("WINDOWS".into()),
            BsonValue::String("MAC_OS".into()),
        ]);
        let mut operator = BsonDoc::new();
        operator.insert(
            "$in",
            BsonValue::Array(vec![BsonValue::String("MAC_OS".into())]),
        );

        assert!(match_field_value(Some(&got), &BsonValue::Document(operator)));
        assert!(match_field_value(
            Some(&got),
            &BsonValue::String("WINDOWS".into())
        ));
    }

    #[tokio::test]
    async fn unique_compound_index_rejects_a_duplicate_seed() {
        let mut key = BsonDoc::new();
        key.insert("tenantId", BsonValue::Int32(1));
        key.insert("name", BsonValue::Int32(1));
        let mut index = BsonDoc::new();
        index.insert("key", BsonValue::Document(key));
        index.insert("name", BsonValue::String("tenant_name".into()));
        index.insert("unique", BsonValue::Bool(true));
        let store = MongoStore::new("test");
        execute(
            &store,
            Query::CreateIndexes {
                collection: "scripts".into(),
                indexes: get_unique_indexes(&BsonDoc(
                    [("indexes".into(), BsonValue::Array(vec![BsonValue::Document(index)]))]
                        .into_iter()
                        .collect(),
                )),
                specs: Vec::new(),
            },
            "test",
        )
        .await
        .unwrap();

        let document = || {
            let mut doc = BsonDoc::new();
            doc.insert("tenantId", BsonValue::String("tenant-a".into()));
            doc.insert("name", BsonValue::String("diagnostics".into()));
            doc
        };
        execute(
            &store,
            Query::Insert { collection: "scripts".into(), documents: vec![document()], ordered: true },
            "test",
        )
        .await
        .unwrap();
        assert!(matches!(
            execute(
                &store,
                Query::Insert { collection: "scripts".into(), documents: vec![document()], ordered: true },
                "test",
            )
            .await,
            Err(MongoError::DuplicateKey(_))
        ));
    }

    #[tokio::test]
    async fn unordered_insert_reports_duplicates_as_write_errors_and_continues() {
        let store = MongoStore::new("test");
        let mut key = BsonDoc::new();
        key.insert("tenantId", BsonValue::Int32(1));
        key.insert("recipientId", BsonValue::Int32(1));
        let mut index = BsonDoc::new();
        index.insert("name", BsonValue::String("recipient_notification_unique".into()));
        index.insert("key", BsonValue::Document(key));
        index.insert("unique", BsonValue::Bool(true));
        let mut command = BsonDoc::new();
        command.insert("createIndexes", BsonValue::String("read_state".into()));
        command.insert(
            "indexes",
            BsonValue::Array(vec![BsonValue::Document(index)]),
        );

        match Query::from_command(&command).unwrap() {
            Query::CreateIndexes { collection, indexes, specs } => {
                execute(&store, Query::CreateIndexes { collection, indexes, specs }, "test")
                    .await
                    .unwrap();
            }
            other => panic!("expected createIndexes, got {other:?}"),
        }

        let row = |recipient: &str| {
            let mut doc = BsonDoc::new();
            doc.insert("tenantId", BsonValue::String("t".into()));
            doc.insert("recipientId", BsonValue::String(recipient.into()));
            doc
        };
        let result = execute(
            &store,
            Query::Insert {
                collection: "read_state".into(),
                documents: vec![row("alice"), row("alice"), row("bob")],
                ordered: false,
            },
            "test",
        )
        .await
        .unwrap();

        match result {
            QueryResult::Write { n, write_errors, .. } => {
                assert_eq!(n, 2, "duplicate skipped, other rows inserted");
                assert_eq!(write_errors.len(), 1);
                assert_eq!(write_errors[0].index, 1);
                assert_eq!(write_errors[0].code, 11000);
            }
            other => panic!("expected write reply, got {other:?}"),
        }
    }

    #[test]
    fn compound_sort_reads_the_value_before_the_id_tiebreaker() {
        let spec = BsonDoc(
            [
                ("_id".into(), BsonValue::Int32(-1)),
                ("repeat".into(), BsonValue::Int32(-1)),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(ordered_sort(&spec), vec![("repeat".into(), -1), ("_id".into(), -1)]);
        assert_eq!(bson_cmp(&BsonValue::Null, &BsonValue::Int64(1)), std::cmp::Ordering::Less);
    }

    #[test]
    fn lookup_and_add_fields_fold_joined_rows_into_a_count() {
        let store = MongoStore::new("test");
        let schedule_id = "010101010101010101010101";
        store.spawn_blocking("test", "assignments", |table| {
            for (id, schedule_id) in [("a1", schedule_id), ("a2", schedule_id), ("a3", "other")] {
                let mut assignment = BsonDoc::new();
                assignment.insert("scheduleId", BsonValue::String(schedule_id.into()));
                table.put(id.into(), BsonValue::Document(assignment));
            }
        });
        let mut schedule = BsonDoc::new();
        schedule.insert("_id", BsonValue::ObjectId([1; 12]));
        let mut equality = BsonDoc::new();
        equality.insert(
            "$eq",
            BsonValue::Array(vec![
                BsonValue::String("$scheduleId".into()),
                BsonValue::String("$$schedule".into()),
            ]),
        );
        let mut match_stage = BsonDoc::new();
        match_stage.insert("$expr", BsonValue::Document(equality));
        let mut pipeline_stage = BsonDoc::new();
        pipeline_stage.insert("$match", BsonValue::Document(match_stage));
        let mut variables = BsonDoc::new();
        let mut to_string = BsonDoc::new();
        to_string.insert("$toString", BsonValue::String("$_id".into()));
        variables.insert("schedule", BsonValue::Document(to_string));
        let mut lookup = BsonDoc::new();
        lookup.insert("from", BsonValue::String("assignments".into()));
        lookup.insert("let", BsonValue::Document(variables));
        lookup.insert(
            "pipeline",
            BsonValue::Array(vec![BsonValue::Document(pipeline_stage)]),
        );
        lookup.insert("as", BsonValue::String("assigned".into()));
        let joined = apply_lookup(&store, "test", vec![schedule], &BsonValue::Document(lookup));
        let mut size = BsonDoc::new();
        size.insert("$size", BsonValue::String("$assigned".into()));
        let mut fields = BsonDoc::new();
        fields.insert("deviceCount", BsonValue::Document(size));
        let counted = apply_add_fields(joined, &BsonValue::Document(fields));
        assert_eq!(counted[0].get("deviceCount"), Some(&BsonValue::Int32(2)));
    }
}
