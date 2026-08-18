//! Pinot SQL → sangdb column-face translation. Supports the subset the
//! OpenFrame dashboard uses: `SELECT COUNT(*) [AS c] GROUP BY <col> WHERE
//! tenant = ?` and `SELECT SUM(<col>) FROM … WHERE …`.

use super::{Aggregate, PinotStore};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PinotError {
    #[error("unsupported query: {0}")]
    Unsupported(String),
    #[error("parse error: {0}")]
    Parse(String),
}

pub type QueryResult<T> = Result<T, PinotError>;

#[derive(Debug, Clone)]
pub enum PinotQuery {
    GroupByCount { table: String, group_col: String, filter: Filter },
    GroupBySum { table: String, group_col: String, sum_col: String, filter: Filter },
}

#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub tenant: Option<String>,
    pub since_ms: Option<i64>,
}

pub enum PinotResult {
    Grouped { keys: Vec<String>, counts: Vec<i64>, sums: Vec<f64> },
    Empty,
}

pub async fn execute(store: &PinotStore, q: PinotQuery) -> QueryResult<PinotResult> {
    match q {
        PinotQuery::GroupByCount { table, group_col, filter } => {
            let tenant = filter.tenant.clone().unwrap_or_else(|| "default".into());
            let t = store.table(&tenant, &table).await;
            let guard = t.read().await;
            let mut out: Vec<(String, Aggregate)> = Vec::new();
            for (k, v) in guard.iter() {
                if k.starts_with(&group_col) {
                    let key = k.trim_start_matches(&group_col).trim_start_matches(':').to_string();
                    out.push((key, v.clone()));
                }
            }
            let keys: Vec<String> = out.iter().map(|(k, _)| k.clone()).collect();
            let counts: Vec<i64> = out.iter().map(|(_, a)| a.count).collect();
            let sums: Vec<f64> = out.iter().map(|(_, a)| a.sum).collect();
            Ok(PinotResult::Grouped { keys, counts, sums })
        }
        PinotQuery::GroupBySum { table, group_col, sum_col, filter } => {
            // Same shape as Count — the fold produces both.
            let tenant = filter.tenant.clone().unwrap_or_else(|| "default".into());
            let t = store.table(&tenant, &table).await;
            let guard = t.read().await;
            let mut out: Vec<(String, Aggregate)> = Vec::new();
            for (k, v) in guard.iter() {
                if k.starts_with(&group_col) {
                    let key = k.trim_start_matches(&group_col).trim_start_matches(':').to_string();
                    let _ = sum_col; // sum_col is encoded in v already.
                    out.push((key, v.clone()));
                }
            }
            let keys: Vec<String> = out.iter().map(|(k, _)| k.clone()).collect();
            let counts: Vec<i64> = out.iter().map(|(_, a)| a.count).collect();
            let sums: Vec<f64> = out.iter().map(|(_, a)| a.sum).collect();
            Ok(PinotResult::Grouped { keys, counts, sums })
        }
    }
}

pub fn parse_pinot_sql(sql: &str) -> Result<PinotQuery, PinotError> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let upper = sql.to_ascii_uppercase();
    if upper.starts_with("SELECT COUNT") {
        // SELECT COUNT(*) FROM <tbl> [WHERE ...] [GROUP BY <col>]
        let rest = &sql[("SELECT COUNT(*)").len()..];
        let (from_part, rest) = rest.split_once("FROM").ok_or_else(|| PinotError::Parse("missing FROM".into()))?;
        let _ = from_part;
        let rest = rest.trim_start();
        let (table, rest) = rest.split_once("WHERE").unwrap_or((rest, ""));
        let table = table.trim().to_string();
        let group_col = rest.split("GROUP BY").nth(1)
            .and_then(|s| s.split_whitespace().next())
            .unwrap_or("default")
            .to_string();
        let filter = parse_filter(rest.split_once("WHERE").map(|x| x.1).unwrap_or(""))?;
        Ok(PinotQuery::GroupByCount { table, group_col, filter })
    } else if upper.starts_with("SELECT SUM") {
        let rest = &sql[("SELECT SUM").len()..];
        // (col)
        let (col_part, rest) = rest.split_once(')').ok_or_else(|| PinotError::Parse("missing sum col".into()))?;
        let sum_col = col_part.trim_start_matches('(').trim().to_string();
        let rest = rest.trim_start();
        let (from_part, rest) = rest.split_once("FROM").ok_or_else(|| PinotError::Parse("missing FROM".into()))?;
        let _ = from_part;
        let rest = rest.trim_start();
        let (table, rest) = rest.split_once("WHERE").unwrap_or((rest, ""));
        let table = table.trim().to_string();
        let group_col = rest.split("GROUP BY").nth(1)
            .and_then(|s| s.split_whitespace().next())
            .unwrap_or("default")
            .to_string();
        let filter = parse_filter(rest.split_once("WHERE").map(|x| x.1).unwrap_or(""))?;
        Ok(PinotQuery::GroupBySum { table, group_col, sum_col, filter })
    } else {
        Err(PinotError::Unsupported(sql.to_string()))
    }
}

fn parse_filter(where_clause: &str) -> Result<Filter, PinotError> {
    let mut f = Filter::default();
    for clause in where_clause.split("AND") {
        let clause = clause.trim();
        if clause.is_empty() { continue; }
        if let Some((col, val)) = clause.split_once('=') {
            let col = col.trim();
            let val = val.trim().trim_matches('\'');
            if col == "tenantId" || col == "tenant_id" { f.tenant = Some(val.to_string()); }
        }
    }
    Ok(f)
}