//! `of-config` — replaces Spring Cloud Config Server.
//!
//! Upstream services do:
//!
//! ```yaml
//! spring:
//!   config:
//!     import: "optional:configserver:http://openframe-config.microservices.svc.cluster.local:8888"
//! ```
//!
//! and pull `/<application>/<profile>` (YAML or properties) at boot. The
//! replacement surface:
//!
//! ```text
//! GET  /{application}/{profile}              → current config (YAML)
//! POST /{application}/{profile}              → publish new config (admin)
//! GET  /healthz                              → liveness
//! ```
//!
//! Storage is `hashtrinity::Table<ConfigKey, ConfigValue>` — one row per
//! `(tenant, app, profile, label)`. The collapse:
//!
//! - Spring Cloud Config Server's `EnvironmentRepository` (backed by
//!   `git`, `jdbc`, `svn`, `vault`, `native` profile backends) → one
//!   hashtrinity `Table<ConfigKey, ConfigValue>`. The four backends were
//!   four `Repository` impls; we have one `Table`.
//! - `@RefreshScope` bean rebuild → irrelevant: hashtrinity is mutable in
//!   place; the next `Table::get` returns the latest value.
//!
//! ## Five faces of config, one store
//!
//! Spring's `Environment` is a 4-axis key (active profile, label, application,
//! tenant). Same shape, same key, one hashtrinity row.

use clap::Parser;
use hashtrinity::{Coord, Table};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigKey {
    pub tenant: String,
    pub application: String,
    pub profile: String,
    pub label: String,
}

impl Coord for ConfigKey {
    fn coord(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut h);
        h.finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValue {
    pub body: String,
    pub source: ConfigSource,
    pub version: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigSource { File, Git, Database }

#[derive(Clone, Default)]
pub struct ConfigStore {
    inner: Arc<std::sync::Mutex<Table<ConfigKey, ConfigValue>>>,
}

impl ConfigStore {
    pub fn new() -> Self { Self::default() }
    pub fn put(&self, key: ConfigKey, value: ConfigValue) {
        let mut guard = self.inner.lock().expect("config store poisoned");
        guard.put(key, value);
    }
    pub fn get(&self, key: &ConfigKey) -> Option<ConfigValue> {
        let guard = self.inner.lock().expect("config store poisoned");
        guard.get(key).cloned()
    }
    pub fn latest(&self, tenant: &str, app: &str, profile: &str) -> Option<ConfigValue> {
        let guard = self.inner.lock().expect("config store poisoned");
        for (k, v) in guard.iter() {
            if k.tenant == tenant && k.application == app && k.profile == profile {
                return Some(v.clone());
            }
        }
        None
    }
}

/// CLI for the binary — kept minimal, more flags ride later.
#[derive(Parser, Debug)]
#[command(name = "of-config", version)]
struct Args {
    /// Bind address, e.g. `0.0.0.0:8888`.
    #[arg(long, default_value = "0.0.0.0:8888")]
    bind: String,
}

/// Entry point.
pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing::info!(bind = %args.bind, "of-config starting");

    let store = ConfigStore::new();
    seed_demo_configs(&store);

    let listener = TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "of-config listening (replaces Spring Cloud Config on :8888)");

    loop {
        let (sock, peer) = listener.accept().await?;
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, store).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

fn seed_demo_configs(store: &ConfigStore) {
    for (app, profile, body) in [
        ("openframe-api", "default", "spring.data.mongodb.uri: mongodb://sangdb:27017/default\n"),
        ("openframe-stream", "default", "kafka.bootstrap.servers: sangdb:9092\n"),
        ("openframe-gateway", "default", "server.port: 8081\n"),
    ] {
        store.put(
            ConfigKey {
                tenant: "default".into(),
                application: app.into(),
                profile: profile.into(),
                label: "main".into(),
            },
            ConfigValue {
                body: body.into(),
                source: ConfigSource::File,
                version: "v1".into(),
                updated_at: chrono::Utc::now().timestamp_millis(),
            },
        );
    }
}

async fn handle(mut sock: TcpStream, store: ConfigStore) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 16 * 1024];
    let n = sock.read(&mut buf).await?;
    if n == 0 { return Ok(()); }
    let req = std::str::from_utf8(&buf[..n])?;
    let (method, path, body) = parse_request(req);

    let response = match (method.as_str(), path.as_str()) {
        ("GET", "/healthz") => (200, "OK", "ok".to_string()),
        ("GET", p) if p.starts_with('/') => {
            // GET /{application}/{profile}
            let trimmed = p.trim_start_matches('/');
            let parts: Vec<&str> = trimmed.split('/').collect();
            let (app, prof) = match parts.as_slice() {
                [app, prof] => (*app, *prof),
                _ => return Ok(write_response(&mut sock, 400, "Bad Request", "bad path").await?),
            };
            match store.latest("default", app, prof) {
                Some(v) => (200, "OK", v.body),
                None => (404, "Not Found", format!("no config for {app}/{prof}")),
            }
        }
        ("POST", p) if p.starts_with('/') => {
            let trimmed = p.trim_start_matches('/');
            let parts: Vec<&str> = trimmed.split('/').collect();
            let (app, prof) = match parts.as_slice() {
                [app, prof] => (*app, *prof),
                _ => return Ok(write_response(&mut sock, 400, "Bad Request", "bad path").await?),
            };
            store.put(
                ConfigKey {
                    tenant: "default".into(),
                    application: app.into(),
                    profile: prof.into(),
                    label: "main".into(),
                },
                ConfigValue {
                    body,
                    source: ConfigSource::File,
                    version: uuid::Uuid::new_v4().to_string(),
                    updated_at: chrono::Utc::now().timestamp_millis(),
                },
            );
            (201, "Created", "ok".into())
        }
        _ => (405, "Method Not Allowed", "method not allowed".into()),
    };

    let (status, reason, body) = response;
    write_response(&mut sock, status, reason, &body).await?;
    Ok(())
}

/// Hand-rolled HTTP/1.1 request parser — just enough for the config server.
/// Full HTTP routing lives in `bedrock::httpsrv`; this minimal version keeps
/// `of-config` independent of any heavy framework.
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

async fn write_response(sock: &mut TcpStream, status: u16, reason: &str, body: &str) -> anyhow::Result<()> {
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(resp.as_bytes()).await?;
    sock.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get() {
        let store = ConfigStore::new();
        let key = ConfigKey {
            tenant: "default".into(),
            application: "openframe-api".into(),
            profile: "prod".into(),
            label: "main".into(),
        };
        let value = ConfigValue {
            body: "server.port: 8080\n".into(),
            source: ConfigSource::File,
            version: "v1".into(),
            updated_at: 0,
        };
        store.put(key.clone(), value.clone());
        assert_eq!(store.get(&key).unwrap().body, "server.port: 8080\n");
        assert!(store.latest("default", "openframe-api", "prod").is_some());
    }

    #[test]
    fn parse_basic_request() {
        let raw = "GET /openframe-api/default HTTP/1.1\r\nHost: x\r\n\r\n";
        let (m, p, b) = parse_request(raw);
        assert_eq!(m, "GET");
        assert_eq!(p, "/openframe-api/default");
        assert_eq!(b, "");
    }
}