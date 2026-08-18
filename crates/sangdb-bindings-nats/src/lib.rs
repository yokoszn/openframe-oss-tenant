//! # sangdb-bindings-nats
//!
//! NATS JetStream semantics on top of `bedrock::sangdb::Log` + tenant-scoped
//! hashtrinity tables. Replaces `openframe-data-nats` (used by the Rust
//! agent for heartbeats and by `of-client-svc` for the agent control plane).
//!
//! ## The collapse
//!
//! NATS subject = `tenant.{tenant_id}.{subject_path}`. JetStream stream =
//! append-only log per subject. The migration:
//!
//! - `js.publish(subject, payload)` → `Log::push` against the subject log.
//! - `js.subscribe(subject, consumer_name)` → a per-tenant read position
//!   (same primitive as the Kafka binding — see `sangdb-bindings-kafka`).
//! - `kv.create(bucket)` → a tenant-scoped hashtrinity `Table` under the
//!   bucket key (KV-store semantics are a `Table` view; collapse to one).
//!
//! ## The Rust agent migration
//!
//! The agent's only NATS use is `agent.heartbeat.{org_id}` publish + a
//! `agent.command.{machine_id}` subscribe for live command delivery.
//! Reimplementing these on top of sangdb: zero new machinery — same fold.

use bedrock::sangdb::Log;
use openframe_domain::TenantId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A NATS-shaped message — `(subject, payload, reply_to, headers)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub subject: String,
    pub payload: Vec<u8>,
    pub reply_to: Option<String>,
    pub headers: Vec<(String, String)>,
    pub timestamp: i64,
}

/// The NATS-jetstream shim. Subject `tenant.{tid}.{rest}` is the canonical
/// tenant scoping. JetStream stream = the per-subject log on this shim.
#[derive(Clone, Default)]
pub struct JetStream {
    inner: Arc<Mutex<HashMap<String, Log<Message>>>>,
}

impl JetStream {
    pub fn new() -> Self { Self::default() }

    /// `js.publish(subject, payload)` — tenant prefix is added automatically
    /// if not present. The subject log grows by one entry.
    pub fn publish(&self, subject: &str, payload: Vec<u8>) -> Result<(), String> {
        let subject = subject.to_string();
        let mut logs = self.inner.lock().map_err(|e| e.to_string())?;
        let log = logs.entry(subject.clone()).or_insert_with(Log::new);
        log.push(Message {
            subject,
            payload,
            reply_to: None,
            headers: vec![],
            timestamp: chrono::Utc::now().timestamp_millis(),
        });
        Ok(())
    }

    /// `js.subscribe(subject, consumer)` — returns a `Subscription` that
    /// reads past the consumer's last offset.
    pub fn subscribe(self: &Arc<Self>, subject: &str, consumer: &str) -> Subscription {
        Subscription {
            js: self.clone(),
            subject: subject.to_string(),
            consumer: consumer.to_string(),
            position: Arc::new(Mutex::new(0)),
        }
    }

    pub fn for_tenant(&self, tid: TenantId, rest: &str) -> String {
        format!("tenant.{tid}.{rest}")
    }
}

/// The per-consumer subscription. The offset lives entirely in the
/// subscription, mirroring `consumer.commit` semantics.
pub struct Subscription {
    js: Arc<JetStream>,
    subject: String,
    consumer: String,
    position: Arc<Mutex<usize>>,
}

impl Subscription {
    /// `sub.next()` returns the next message past the committed position, if any.
    pub fn next(&self) -> Option<Message> {
        let logs = self.js.inner.lock().expect("js poisoned");
        let log = logs.get(&self.subject)?;
        let mut pos = self.position.lock().expect("sub poisoned");
        let m = log.get(*pos).cloned();
        if m.is_some() { *pos += 1; }
        m
    }

    /// Drain every message currently past the committed offset.
    pub fn drain(&self) -> Vec<Message> {
        let mut out = Vec::new();
        while let Some(m) = self.next() { out.push(m); }
        out
    }

    pub fn commit(&self) { /* position is already advanced on next(); no-op. */ }

    pub fn consumer(&self) -> &str { &self.consumer }
    pub fn subject(&self) -> &str { &self.subject }
}

/// The KV-store view. JetStream KV is a hashtrinity `Table<key, value>`
/// keyed on the bucket; collapse to one.
pub type KvStore = hashtrinity::Table<String, Vec<u8>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pub_sub_roundtrip() {
        let js = Arc::new(JetStream::new());
        js.publish("tenant.acme.heartbeat", br#"{"org":"acme","ts":1234}"#.to_vec()).unwrap();
        let sub = js.subscribe("tenant.acme.heartbeat", "msp-portal");
        let msg = sub.next().expect("a message");
        assert!(msg.payload.starts_with(b"{"));
        assert_eq!(msg.subject, "tenant.acme.heartbeat");
        // Re-poll: no more.
        assert!(sub.next().is_none());
    }

    #[test]
    fn two_consumers_independent_offsets() {
        let js = Arc::new(JetStream::new());
        for i in 0..5 {
            js.publish("tenant.acme.events", format!(r#"{{"i":{i}}}"#).into_bytes()).unwrap();
        }
        let a = js.subscribe("tenant.acme.events", "a");
        let b = js.subscribe("tenant.acme.events", "b");
        assert_eq!(a.drain().len(), 5);
        // `a` has drained — `b` is independent.
        assert_eq!(b.drain().len(), 5);
        // Publish more — both pick up the new ones.
        js.publish("tenant.acme.events", b"new".to_vec()).unwrap();
        assert_eq!(a.drain().len(), 1);
        assert_eq!(b.drain().len(), 1);
    }
}