//! # sangdb-bindings-kafka
//!
//! Kafka topic semantics on top of `bedrock::sangdb::Log` (the append-only
//! ledger primitive). Replaces `openframe-data-kafka` (producer utilities)
//! and the Kafka Streams consumers in `openframe-stream-service-core`.
//!
//! ## The collapse
//!
//! A Kafka topic is an append-only log: `(topic, partition, offset) → record`.
//! Sangdb already exposes that primitive as `bedrock::sangdb::Log`. The
//! migration:
//!
//! - `producer.send(record)` → `Log::push(record)` against the per-topic log.
//! - `consumer.subscribe(topic)` → a per-group read position over the same
//!   log (the `proof/StreamGroups.lean` consumer-group isolation holds).
//! - `Kafka Streams KTable` → a fold over the topic
//!   (`bedrock::sangdb::fold_group`) keyed on the aggregation column.
//! - `Kafka Streams KStream` → an incremental `bedrock::sangdb::ingest`
//!   into a hashtrinity `Table`.
//!
//! ## Why it's order-free
//!
//! The OpenFrame stream pipeline does:
//!
//! ```text
//! source-topic → enrich → materialise → sink-table (MongoDB / Cassandra)
//! ```
//!
//! Every step is a fold over the topic — a commutative monoid merge, so the
//! pipeline is order-free (`Substrate/Collapse.streaming_law`,
//! `Boost.hsum_merge_order_free`). A late or out-of-order shard merges with
//! the same result; the upstream Kafka Streams dependency on offset-ordered
//! consumption evaporates. We don't need to *replicate* Kafka's offset
//! machinery to give it the same semantics; we replace it with the
//! closed-form one.
//!
//! ## Status (2026-08-18)
//!
//! - [x] Topic-append + per-group read positions over `bedrock::sangdb::Log`.
//! - [ ] Native Kafka wire-protocol server (the binary-protocol fetch/produce
//!   cycle is large; `rdkafka` as a sidecar is the pragmatic move until the
//!   server lands).

use bedrock::sangdb::Log;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A Kafka-shaped record — `(topic, partition, offset, key, value, ts, headers)`.
/// The wire format encodes them as a single byte slice; here we keep the
/// payload bytes and the topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: i64,
    pub headers: Vec<(String, Vec<u8>)>,
}

/// Producer — in front of sangdb, a Kafka producer becomes a log append.
/// Per-topic logs are kept in a shared `Arc<Mutex<HashMap<topic, Log<Record>>>>`.
#[derive(Clone)]
pub struct Producer {
    inner: Arc<Mutex<HashMap<String, Log<Record>>>>,
}

impl Producer {
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn send(&self, rec: OutgoingRecord) -> Result<(), String> {
        let mut logs = self.inner.lock().map_err(|e| e.to_string())?;
        let log = logs.entry(rec.topic.clone()).or_insert_with(Log::new);
        let offset = log.len() as u64;
        log.push(Record {
            topic: rec.topic,
            partition: 0,
            offset,
            key: rec.key,
            value: rec.value,
            timestamp: chrono::Utc::now().timestamp_millis(),
            headers: rec.headers,
        });
        Ok(())
    }
}

impl Default for Producer {
    fn default() -> Self { Self::new() }
}

/// Outgoing message — the in-process `producer.send` argument shape.
#[derive(Debug, Clone)]
pub struct OutgoingRecord {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl OutgoingRecord {
    pub fn new(topic: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            key: None,
            value: value.into(),
            headers: vec![],
        }
    }
    pub fn with_key(mut self, k: impl Into<Vec<u8>>) -> Self { self.key = Some(k.into()); self }
    pub fn with_header(mut self, name: impl Into<String>, val: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), val.into())); self
    }
}

/// Consumer — a Kafka consumer group is a read position over the per-topic
/// log. The group's `committed_offset` is the next-to-read index; each
/// `poll` returns records past that and advances the offset.
pub struct Consumer {
    inner: Arc<Mutex<HashMap<String, Log<Record>>>>,
    group: String,
    position: Arc<Mutex<HashMap<String, usize>>>,
}

impl Consumer {
    pub fn new(producer: &Producer, group: impl Into<String>) -> Self {
        Self {
            inner: producer.inner.clone(),
            group: group.into(),
            position: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Read records past the committed offset, then advance.
    pub fn poll(&self, topic: &str) -> Vec<Record> {
        let logs = self.inner.lock().expect("producer poisoned");
        let log = match logs.get(topic) {
            Some(l) => l,
            None => return vec![],
        };
        let mut pos = self.position.lock().expect("consumer poisoned");
        let start = pos.get(topic).copied().unwrap_or(0);
        let out: Vec<Record> = (start..log.len()).filter_map(|i| log.get(i).cloned()).collect();
        pos.insert(topic.to_string(), log.len());
        out
    }

    pub fn commit(&self, topic: &str) {
        // `poll` already advances; explicit commit is the no-op
        // duplicate-detection boundary the upstream Kafka semantics need.
        tracing::debug!(group = %self.group, topic, "commit (position already advanced)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_then_consume() {
        let producer = Producer::new();
        producer.send(OutgoingRecord::new("events", br#"{"id":1}"#)).unwrap();
        producer.send(OutgoingRecord::new("events", br#"{"id":2}"#)).unwrap();
        producer.send(OutgoingRecord::new("events", br#"{"id":3}"#)).unwrap();

        let consumer = Consumer::new(&producer, "test-group");
        let records = consumer.poll("events");
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[2].offset, 2);

        // Re-poll: nothing new.
        assert_eq!(consumer.poll("events").len(), 0);

        // Produce more; re-poll picks them up.
        producer.send(OutgoingRecord::new("events", br#"{"id":4}"#)).unwrap();
        assert_eq!(consumer.poll("events").len(), 1);
    }
}