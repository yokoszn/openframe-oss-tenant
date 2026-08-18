//! CQL → sangdb query translation. The subset we support mirrors what
//! `openframe-stream-service-core` issues for the `device_event` table:
//! `INSERT INTO …`, `SELECT … FROM … WHERE partition = ?`, `UPDATE … SET`,
//! `DELETE FROM … WHERE …`, `TRUNCATE`.

use super::CassandraStore;
use hashtrinity::Table;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum CqlError {
    #[error("unsupported statement: {0}")]
    Unsupported(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("tenant scope violation")]
    TenantScopeViolation,
}

pub type CqlResult<T> = Result<T, CqlError>;

/// The CQL value space. The wire format serialises every variant as
/// `<int4 length><bytes>`. Re-exported through the parent crate.
#[derive(Debug, Clone)]
pub enum CqlValue {
    Text(String),
    Int(i32),
    BigInt(i64),
    Bool(bool),
    Timestamp(i64), // unix millis, like BSON DateTime
    Uuid(uuid::Uuid),
    List(Vec<CqlValue>),
    Map(Vec<(String, CqlValue)>),
    Null,
}

#[derive(Debug, Clone)]
pub enum CqlStatement {
    Insert { keyspace: String, table: String, columns: Vec<String>, values: Vec<CqlValue> },
    Select { keyspace: String, table: String, columns: Vec<String>, partition_eq: Vec<(String, CqlValue)>, limit: Option<usize> },
    Update { keyspace: String, table: String, partition_eq: Vec<(String, CqlValue)>, set_columns: Vec<(String, CqlValue)> },
    Delete { keyspace: String, table: String, partition_eq: Vec<(String, CqlValue)> },
    Truncate { keyspace: String, table: String },
}

pub async fn execute(store: &CassandraStore, stmt: CqlStatement) -> CqlResult<Vec<Vec<CqlValue>>> {
    match stmt {
        CqlStatement::Insert { keyspace, table, columns, values } => {
            let t = store.table(&keyspace, &table).await;
            let mut guard = t.write().await;
            let key = partition_key(&columns, &values, "id");
            let row = build_row(&columns, values);
            guard.put(key, CqlValue::Map(row));
            Ok(vec![])
        }
        CqlStatement::Select { keyspace, table, columns: _select_cols, partition_eq, limit } => {
            let t = store.table(&keyspace, &table).await;
            let guard = t.read().await;
            let limit = limit.unwrap_or(usize::MAX);
            let mut rows = Vec::new();
            for (k, v) in guard.iter() {
                if !matches_partition(k, &partition_eq) { continue; }
                if let CqlValue::Map(m) = v {
                    let mut out = Vec::new();
                    for (col, _) in &partition_eq {
                        out.push(m.iter().find(|(k, _)| k == col).map(|(_, v)| v.clone()).unwrap_or(CqlValue::Null));
                    }
                    rows.push(out);
                }
                if rows.len() >= limit { break; }
            }
            Ok(rows)
        }
        CqlStatement::Update { keyspace, table, partition_eq, set_columns } => {
            let t = store.table(&keyspace, &table).await;
            let mut guard = t.write().await;
            let key = partition_key_from_eq(&partition_eq);
            let mut row = guard.get(&key).cloned().unwrap_or(CqlValue::Map(vec![]));
            if let CqlValue::Map(ref mut m) = row {
                for (col, val) in set_columns {
                    if let Some(existing) = m.iter_mut().find(|(k, _)| k == &col) {
                        existing.1 = val;
                    } else {
                        m.push((col, val));
                    }
                }
            }
            guard.put(key, row);
            Ok(vec![])
        }
        CqlStatement::Delete { keyspace, table, partition_eq } => {
            let t = store.table(&keyspace, &table).await;
            let mut guard = t.write().await;
            let key = partition_key_from_eq(&partition_eq);
            guard.remove(&key);
            Ok(vec![])
        }
        CqlStatement::Truncate { keyspace, table } => {
            let t = store.table(&keyspace, &table).await;
            let mut guard = t.write().await;
            // Drop every key — hashtrinity doesn't have clear() yet, so we
            // collect first and remove in a second pass.
            let keys: Vec<String> = guard.iter().map(|(k, _)| k.to_string()).collect();
            for k in keys {
                guard.remove(&k);
            }
            Ok(vec![])
        }
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn partition_key(columns: &[String], values: &[CqlValue], partition_col: &str) -> String {
    let idx = columns.iter().position(|c| c == partition_col);
    match idx {
        Some(i) => format!("{}={:?}", &columns[i], &values[i]),
        None => "default".into(),
    }
}

fn partition_key_from_eq(eq: &[(String, CqlValue)]) -> String {
    eq.iter().map(|(k, v)| format!("{}={:?}", k, v)).collect::<Vec<_>>().join(",")
}

fn matches_partition(key: &str, eq: &[(String, CqlValue)]) -> bool {
    eq.iter().all(|(k, v)| key.contains(&format!("{}={:?}", k, v)))
}

fn build_row(columns: &[String], values: Vec<CqlValue>) -> Vec<(String, CqlValue)> {
    columns.iter().cloned().zip(values).collect()
}

// ── Skeleton parse entry-point. Real CQL parsing lives in
//    `cassandra-protocol` upstream; this stub is here so the rest of the
//    workspace compiles while we wire the wire protocol. ──────────────────────

pub fn parse_cql(sql: &str) -> CqlResult<CqlStatement> {
    let sql = sql.trim().trim_end_matches(';');
    if sql.to_ascii_uppercase().starts_with("INSERT") {
        // Naive parser: INSERT INTO <ks>.<tbl> (<cols>) VALUES (<vals>)
        let mut rest = sql["INSERT".len()..].trim_start().trim_start_matches("INTO").trim_start();
        let (table_part, after) = rest.split_once('(').ok_or_else(|| CqlError::Parse("missing cols".into()))?;
        let (cols_part, after) = after.split_once(')').ok_or_else(|| CqlError::Parse("missing cols close".into()))?;
        let after = after.trim_start().trim_start_matches("VALUES").trim_start();
        let (vals_part, _) = after.split_once(')').ok_or_else(|| CqlError::Parse("missing vals close".into()))?;
        let (keyspace, table) = split_table(table_part.trim())?;
        let columns = split_csv(cols_part).into_iter().map(|s| s.trim().to_string()).collect();
        let values = parse_value_list(vals_part)?;
        Ok(CqlStatement::Insert { keyspace, table, columns, values })
    } else if sql.to_ascii_uppercase().starts_with("SELECT") {
        let rest = sql["SELECT".len()..].trim_start();
        let (select_cols_part, rest) = rest.split_once("FROM").ok_or_else(|| CqlError::Parse("missing FROM".into()))?;
        let columns = if select_cols_part.trim() == "*" { vec![] } else { split_csv(select_cols_part) };
        let rest = rest.trim_start();
        let (table_part, rest) = rest.split_once("WHERE").unwrap_or((rest, ""));
        let (keyspace, table) = split_table(table_part.trim())?;
        let partition_eq = parse_where(rest.trim())?;
        let limit = rest.split_once("LIMIT").and_then(|(_, l)| l.trim().parse::<usize>().ok());
        Ok(CqlStatement::Select { keyspace, table, columns, partition_eq, limit })
    } else if sql.to_ascii_uppercase().starts_with("UPDATE") {
        Err(CqlError::Unsupported("UPDATE parse — wire me".into()))
    } else if sql.to_ascii_uppercase().starts_with("DELETE") {
        Err(CqlError::Unsupported("DELETE parse — wire me".into()))
    } else if sql.to_ascii_uppercase().starts_with("TRUNCATE") {
        let rest = sql["TRUNCATE".len()..].trim_start();
        let (keyspace, table) = split_table(rest)?;
        Ok(CqlStatement::Truncate { keyspace, table })
    } else {
        Err(CqlError::Unsupported(sql.to_string()))
    }
}

fn split_table(s: &str) -> CqlResult<(String, String)> {
    let (ks, tbl) = s.split_once('.').ok_or_else(|| CqlError::Parse(format!("expected ks.tbl, got {s}")))?;
    Ok((ks.trim().to_string(), tbl.trim().to_string()))
}

fn split_csv(s: &str) -> Vec<String> {
    s.split(',').map(|x| x.trim().to_string()).collect()
}

fn parse_value_list(s: &str) -> CqlResult<Vec<CqlValue>> {
    // Crude — handles 'string', 123, true, null, and bare identifiers as text.
    let s = s.trim().trim_start_matches('(').trim_end_matches(')');
    s.split(',').map(|raw| {
        let raw = raw.trim();
        if raw == "null" || raw == "NULL" { return Ok(CqlValue::Null); }
        if raw == "true" { return Ok(CqlValue::Bool(true)); }
        if raw == "false" { return Ok(CqlValue::Bool(false)); }
        if let Ok(i) = raw.parse::<i64>() { return Ok(CqlValue::BigInt(i)); }
        if raw.starts_with('\'') && raw.ends_with('\'') {
            return Ok(CqlValue::Text(raw[1..raw.len()-1].to_string()));
        }
        Ok(CqlValue::Text(raw.to_string()))
    }).collect()
}

fn parse_where(s: &str) -> CqlResult<Vec<(String, CqlValue)>> {
    if s.is_empty() { return Ok(vec![]); }
    let mut out = Vec::new();
    for clause in s.split("AND") {
        let clause = clause.trim();
        let (col, val) = clause.split_once('=').ok_or_else(|| CqlError::Parse(format!("bad WHERE: {clause}")))?;
        let mut vals = parse_value_list(&format!("({})", val.trim()))?;
        if vals.len() != 1 { return Err(CqlError::Parse(format!("bad WHERE value: {val}"))); }
        out.push((col.trim().to_string(), vals.remove(0)));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_insert() {
        let stmt = parse_cql("INSERT INTO openframe.device_event (id, device_id, event_type) VALUES ('e1', 'd1', 'heartbeat')").unwrap();
        match stmt {
            CqlStatement::Insert { keyspace, table, columns, values } => {
                assert_eq!(keyspace, "openframe");
                assert_eq!(table, "device_event");
                assert_eq!(columns, vec!["id", "device_id", "event_type"]);
                assert_eq!(values.len(), 3);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_select() {
        let stmt = parse_cql("SELECT id, device_id FROM openframe.device_event WHERE id = 'e1'").unwrap();
        match stmt {
            CqlStatement::Select { keyspace, table, columns, partition_eq, limit } => {
                assert_eq!(keyspace, "openframe");
                assert_eq!(table, "device_event");
                assert_eq!(columns, vec!["id", "device_id"]);
                assert_eq!(partition_eq.len(), 1);
                assert!(limit.is_none());
            }
            _ => panic!("expected Select"),
        }
    }
}