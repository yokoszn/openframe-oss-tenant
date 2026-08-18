//! `of-stream` — replaces `openframe-stream-service-core` (Kafka Streams
//! processor).
//!
//! ## The collapse
//!
//! The upstream pipeline:
//!
//! ```text
//!   external-tool → Kafka topic → KStream (enrichment) → KTable (state)
//!                  → Cassandra sink + Pinot sink
//! ```
//!
//! reduces to:
//!
//! ```text
//!   external-tool → sangdb Log → fold_group (enrichment) → hashtrinity Table (state)
//!                  → Cassandra-shape binding (sangdb-bindings-cassandra)
//!                  → Pinot-shape binding (sangdb-bindings-pinot)
//! ```
//!
//! Every step is the streaming-law fold (`Substrate/Collapse.streaming_law`,
//! `Boost.hsum_merge_order_free`): a commutative monoid merge over the
//! per-tenant log. A late shard merges the same way; there is no
//! re-scan, no offset ordering, no consumer-group coordinator.
//!
//! ## Status
//!
//! The reference impl exposes a single fold (count by device_id over
//! `device_event`) as the demonstration. Adding more processors follows
//! the same shape: declare the per-tenant topic, declare the fold
//! closure, declare the sink table.

use clap::Parser;
use hashtrinity::Table;
use openframe_domain::TenantId;
use sangdb_bindings_kafka::{Consumer, Producer};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8085")]
    bind: String,
}

#[derive(Clone)]
pub struct StreamProcessor {
    pub producer: Producer,
    pub tenant: TenantId,
    /// The fold: per-device event count over the `device_event` topic.
    /// Stored as `Table<device_id, count>` — one row per device.
    pub state: Arc<std::sync::Mutex<Table<String, i64>>>,
}

impl StreamProcessor {
    pub fn new(tenant: TenantId) -> Self {
        Self {
            producer: Producer::new(),
            tenant,
            state: Arc::new(std::sync::Mutex::new(Table::new())),
        }
    }

    /// Run one tick — drain the `device_event` log and fold events into
    /// the device-count table. Idempotent and order-free by construction.
    pub fn tick(&self) {
        let consumer = Consumer::new(&self.producer, "stream-svc");
        for rec in consumer.poll("device_event") {
            let v: Value = match serde_json::from_slice(&rec.value) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let device_id = v.get("device_id").and_then(|x| x.as_str()).unwrap_or("").to_string();
            if device_id.is_empty() { continue; }
            let mut state = self.state.lock().expect("state poisoned");
            let cur = state.get(&device_id).copied().unwrap_or(0);
            state.put(device_id, cur + 1);
        }
    }
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing::info!(bind = %args.bind, "of-stream starting");
    let proc = StreamProcessor::new(TenantId::nil());
    let listener = TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "of-stream listening — replaces openframe-stream on :8085");

    // Background ticker — every second, fold the latest shard.
    let bg = proc.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
                interval.tick().await;
                bg.tick();
                let state = bg.state.lock().unwrap();
                let snapshot: HashMap<String, i64> = state.iter().map(|(k, v)| (k.to_string(), *v)).collect();
                tracing::debug!(?snapshot, "stream fold tick");
            }
    });

    loop {
        let (sock, peer) = listener.accept().await?;
        let proc = proc.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, proc).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

async fn handle(mut sock: TcpStream, proc: StreamProcessor) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 16 * 1024];
    let n = sock.read(&mut buf).await?;
    if n == 0 { return Ok(()); }
    let raw = std::str::from_utf8(&buf[..n])?;
    let (method, path, body) = parse_request(raw);

    let resp = match (method.as_str(), path.as_str()) {
        ("GET", "/healthz") => (200, "OK", "text/plain", "ok".into()),
        ("GET", "/state") => {
            let state = proc.state.lock().expect("state poisoned");
            let snapshot: HashMap<String, i64> = state.iter().map(|(k, v)| (k.to_string(), *v)).collect();
            (200, "OK", "application/json", serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".into()))
        }
        ("POST", "/publish") => {
            proc.producer.send(sangdb_bindings_kafka::OutgoingRecord::new(
                "device_event", body.as_bytes().to_vec(),
            )).map_err(|e| anyhow::anyhow!(e))?;
            (202, "Accepted", "text/plain", "ok".into())
        }
        _ => (404, "Not Found", "application/json", r#"{"error":"no route"}"#.into()),
    };
    let (status, reason, ct, body) = resp;
    let r = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ct}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(r.as_bytes()).await?;
    sock.shutdown().await?;
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_counts_per_device() {
        let proc = StreamProcessor::new(TenantId::nil());
        for (_id, ev) in [
            ("d1", r#"{"device_id":"d1"}"#),
            ("d1", r#"{"device_id":"d1"}"#),
            ("d2", r#"{"device_id":"d2"}"#),
        ] {
            proc.producer.send(sangdb_bindings_kafka::OutgoingRecord::new(
                "device_event", ev.as_bytes().to_vec(),
            )).unwrap();
        }
        proc.tick();
        let state = proc.state.lock().unwrap();
        assert_eq!(state.get(&"d1".to_string()).copied(), Some(2));
        assert_eq!(state.get(&"d2".to_string()).copied(), Some(1));
    }
}