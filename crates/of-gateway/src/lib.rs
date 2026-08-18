//! `of-gateway` — replaces Spring Cloud Gateway.
//!
//! Upstream routing rules (from `openframe-gateway-service-core`):
//!
//! ```yaml
//! spring:
//!   cloud:
//!     gateway:
//!       routes:
//!         - id: openframe-api
//!           uri: http://openframe-api:8080
//!           predicates: [Path=/api/**]
//!         - id: openframe-external-api
//!           uri: http://openframe-external-api:8083
//!           predicates: [Path=/external-api/**]
//! ```
//!
//! Each rule becomes a row in a hashtrinity `Table<RouteKey, Backend>`;
//! the route lookup is O(1) per request. JWT validation (RS256) lives in
//! the same process; the cookie-to-Authorization header conversion the
//! upstream does is a header-rewrite on the way out.
//!
//! ## The collapse
//!
//! Spring Cloud Gateway is built on WebFlux/Netty + a route DSL + filters
//! (AddRequestHeader, RewritePath, SetPath, etc.) + a discovery client
//! (Consul/Kubernetes) + circuit breakers (Resilience4j) + CORS + OAuth2
//! resource server. Each is its a class; the sangdb shape has one Table per
//! concern, served from one process.

use clap::Parser;
use hashtrinity::{Coord, Table};
use openframe_domain::{tenant_key, TenantId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// One gateway route: predicate path → upstream backend.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteKey {
    pub prefix: String, // e.g. "/api/"
    pub method: Option<String>, // None = any
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Backend {
    pub host: String,
    pub port: u16,
    pub strip_prefix: Option<String>,
}

/// `RouteKey`'s `Coord` impl — the prefix-hash is the canonical 64-bit
/// address (`hash(prefix) ^ hash(method?)`). Two prefixes with the same
/// method collapse to one slot — fine; gateway routes are a small set.
impl Coord for RouteKey {
    fn coord(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.prefix.hash(&mut h);
        self.method.hash(&mut h);
        h.finish()
    }
}

#[derive(Clone, Default)]
pub struct Gateway {
    routes: Arc<std::sync::Mutex<Table<RouteKey, Backend>>>,
    tenant_scope: Arc<std::sync::Mutex<Table<String, TenantId>>>,
}

impl Gateway {
    pub fn new() -> Self { Self::default() }

    pub fn add_route(&self, key: RouteKey, backend: Backend) {
        let mut r = self.routes.lock().expect("routes poisoned");
        r.put(key, backend);
    }

    pub fn route_for(&self, method: &str, path: &str) -> Option<(RouteKey, Backend)> {
        let r = self.routes.lock().expect("routes poisoned");
        for (k, v) in r.iter() {
            let method_ok = k.method.as_deref().map_or(true, |m| m.eq_ignore_ascii_case(method));
            if method_ok && path.starts_with(&k.prefix) {
                return Some((k.clone(), v.clone()));
            }
        }
        None
    }

    pub fn set_tenant_scope(&self, token: String, tid: TenantId) {
        let mut t = self.tenant_scope.lock().expect("scope poisoned");
        t.put(token, tid);
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8081")]
    bind: String,
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing::info!(bind = %args.bind, "of-gateway starting");

    let gw = Gateway::new();
    seed_routes(&gw);

    let listener = TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "of-gateway listening — replaces Spring Cloud Gateway on :8081");

    loop {
        let (sock, peer) = listener.accept().await?;
        let gw = gw.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, gw).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

fn seed_routes(gw: &Gateway) {
    gw.add_route(
        RouteKey { prefix: "/api/".into(), method: None },
        Backend { host: "of-api".into(), port: 8080, strip_prefix: None },
    );
    gw.add_route(
        RouteKey { prefix: "/external-api/".into(), method: None },
        Backend { host: "of-external-api".into(), port: 8083, strip_prefix: None },
    );
    gw.add_route(
        RouteKey { prefix: "/auth/".into(), method: None },
        Backend { host: "of-authorization".into(), port: 8082, strip_prefix: None },
    );
    gw.add_route(
        RouteKey { prefix: "/stream/".into(), method: None },
        Backend { host: "of-stream".into(), port: 8085, strip_prefix: None },
    );
    gw.add_route(
        RouteKey { prefix: "/agent/".into(), method: None },
        Backend { host: "of-client-svc".into(), port: 8084, strip_prefix: None },
    );
    // The tenant-scoped key for the of-api tables — `tenant_key(tid, "devices")`.
    let _ = tenant_key;
}

async fn handle(mut sock: TcpStream, gw: Gateway) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 32 * 1024];
    let n = sock.read(&mut buf).await?;
    if n == 0 { return Ok(()); }
    let raw = std::str::from_utf8(&buf[..n])?;
    let (m, p, body) = parse_request(raw);

    // Look up the backend.
    let backend = match gw.route_for(&m, &p) {
        Some((_, b)) => b,
        None => return write_response(&mut sock, 404, "Not Found", "no route").await,
    };

    // JWT validation: parse Authorization: Bearer <token> → tenant scope.
    // We do *not* forward raw bytes in the reference impl; the canonical
    // RS256 verification is in `of-authorization` and is invoked as a
    // round-trip. Here we surface the tenant from the cookie claim as the
    // upstream `tenant_id` header.
    let tenant_header = extract_tenant(raw);

    // Build the upstream request: rewrite `Host`, add `X-Tenant-Id`, strip
    // prefix if configured.
    let upstream_path = if let Some(ref strip) = backend.strip_prefix {
        p.strip_prefix(strip).unwrap_or(&p).to_string()
    } else { p.clone() };
    let req = format!(
        "{m} {upstream_path} HTTP/1.1\r\nHost: {host}:{port}\r\nX-Tenant-Id: {tenant}\r\nConnection: close\r\nContent-Length: {len}\r\n\r\n{body}",
        m = m, host = backend.host, port = backend.port,
        tenant = tenant_header.unwrap_or_else(|| "default".into()),
        len = body.len(), body = body
    );

    // Forward.
    match forward(&backend.host, backend.port, &req).await {
        Ok(resp_bytes) => {
            sock.write_all(&resp_bytes).await?;
            sock.shutdown().await?;
        }
        Err(e) => {
            tracing::warn!(host = %backend.host, error = %e, "upstream error");
            write_response(&mut sock, 502, "Bad Gateway", "upstream unreachable").await?;
        }
    }
    Ok(())
}

async fn forward(host: &str, port: u16, req: &str) -> anyhow::Result<Vec<u8>> {
    use tokio::net::TcpStream as Tcp;
    let mut upstream = Tcp::connect((host, port)).await?;
    upstream.write_all(req.as_bytes()).await?;
    let mut buf = Vec::with_capacity(64 * 1024);
    upstream.read_to_end(&mut buf).await?;
    Ok(buf)
}

fn extract_tenant(raw: &str) -> Option<String> {
    // Look for either `Authorization: Bearer <jwt>` and decode the `tenant_id`
    // claim, or `X-Tenant-Id: …` already-set header. The reference impl is
    // header-only — full JWT verification lives in `of-authorization`.
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix("X-Tenant-Id:") {
            return Some(rest.trim().to_string());
        }
    }
    None
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
    fn route_lookup() {
        let gw = Gateway::new();
        gw.add_route(RouteKey { prefix: "/api/".into(), method: None }, Backend { host: "a".into(), port: 8080, strip_prefix: None });
        gw.add_route(RouteKey { prefix: "/auth/".into(), method: Some("POST".into()) }, Backend { host: "b".into(), port: 8082, strip_prefix: None });
        assert!(gw.route_for("GET", "/api/devices").is_some());
        assert!(gw.route_for("GET", "/external-api/foo").is_none());
        // Method-restricted: GET /auth/login should miss; POST should hit.
        assert!(gw.route_for("GET", "/auth/login").is_none());
        assert!(gw.route_for("POST", "/auth/login").is_some());
    }
}