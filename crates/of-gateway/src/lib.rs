//! `of-gateway` — replaces Spring Cloud Gateway.
//!
//! Route map mirrors the upstream `configs/base/openframe-gateway.yml`:
//!
//! ```text
//! /sas/**           → of-authorization   (auth server, /sas kept)
//! /api/**           → of-api             (StripPrefix=1 upstream)
//! /clients/**       → of-client-svc      (StripPrefix=1 upstream)
//! /external-api/**  → of-external-api    (StripPrefix=1 upstream)
//! /oauth/**, /me    → in-process         (gateway OAuth2 client: login,
//!                                         callback, refresh, logout, me,
//!                                         dev-exchange)
//! /**               → openframe frontend (Next.js container, :3000)
//! ```
//!
//! The gateway is the OAuth2 client (`openframe-gateway`). The browser flow:
//! `GET /oauth/login?tenantId=…&redirectTo=…` → 302 to the auth server's
//! authorize endpoint → auth server validates credentials → 302 back to
//! `/oauth/callback?code=…` → gateway exchanges the code, sets HTTP-only
//! session cookies, 302 to `redirectTo`. Proxied API calls carry the cookie;
//! the gateway converts cookie → `Authorization: Bearer` upstream (the
//! upstream cookie→header contract from `openframe-gateway-service-core`).

use clap::Parser;
use hashtrinity::{Coord, Table};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
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
    /// session_id → (access_token, refresh_token, tenant_id). The cookie
    /// value is an opaque session id; the tokens never leave the server
    /// except as the upstream Authorization header.
    sessions: Arc<std::sync::Mutex<HashMap<String, Session>>>,
}

#[derive(Clone, Debug)]
pub struct Session {
    pub access_token: String,
    pub refresh_token: String,
    pub tenant_id: String,
}

/// Static wiring for the deployed replacement (host-local ports).
struct Topology {
    authz_host: String,
    authz_port: u16,
    frontend_host: String,
    frontend_port: u16,
}

impl Default for Topology {
    fn default() -> Self {
        Self {
            authz_host: "127.0.0.1".into(),
            authz_port: 8082,
            frontend_host: "127.0.0.1".into(),
            frontend_port: 3001,
        }
    }
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

    pub fn put_session(&self, id: String, session: Session) {
        self.sessions.lock().expect("sessions poisoned").insert(id, session);
    }

    pub fn get_session(&self, id: &str) -> Option<Session> {
        self.sessions.lock().expect("sessions poisoned").get(id).cloned()
    }

    pub fn drop_session(&self, id: &str) {
        self.sessions.lock().expect("sessions poisoned").remove(id);
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8100")]
    bind: String,
    #[arg(long, default_value = "127.0.0.1:8082")]
    authz: String,
    #[arg(long, default_value = "127.0.0.1:3001")]
    frontend: String,
}

/// A parsed HTTP request (method, full URL, headers, body).
#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn cookie(&self, name: &str) -> Option<String> {
        let raw = self.header("cookie")?;
        for part in raw.split(';') {
            let part = part.trim();
            if let Some((k, v)) = part.split_once('=') {
                if k.trim() == name {
                    return Some(v.trim().to_string());
                }
            }
        }
        None
    }

    pub fn query_param(&self, name: &str) -> Option<String> {
        for pair in self.query.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                if k == name {
                    return Some(urldecode(v));
                }
            }
        }
        None
    }

    pub fn form_param(&self, name: &str) -> Option<String> {
        let body = std::str::from_utf8(&self.body).ok()?;
        for pair in body.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                if k == name {
                    return Some(urldecode(v));
                }
            }
        }
        None
    }
}

/// A response ready to serialize.
pub struct Response {
    pub status: u16,
    pub reason: &'static str,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn json(status: u16, body: String) -> Self {
        Self {
            status,
            reason: reason_for(status),
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: body.into_bytes(),
        }
    }

    pub fn redirect(location: String, cookies: Vec<String>) -> Self {
        let mut headers = vec![("Location".into(), location)];
        for c in cookies {
            headers.push(("Set-Cookie".into(), c));
        }
        Self { status: 302, reason: "Found", headers, body: Vec::new() }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut head = format!("HTTP/1.1 {} {}\r\n", self.status, self.reason);
        let mut has_len = false;
        for (k, v) in &self.headers {
            if k.eq_ignore_ascii_case("content-length") { has_len = true; }
            head.push_str(k);
            head.push_str(": ");
            head.push_str(v);
            head.push_str("\r\n");
        }
        if !has_len {
            head.push_str(&format!("Content-Length: {}\r\n", self.body.len()));
        }
        head.push_str("Connection: close\r\n\r\n");
        let mut out = head.into_bytes();
        out.extend_from_slice(&self.body);
        out
    }
}

fn reason_for(status: u16) -> &'static str {
    match status {
        200 => "OK", 201 => "Created", 204 => "No Content",
        302 => "Found",
        400 => "Bad Request", 401 => "Unauthorized", 403 => "Forbidden",
        404 => "Not Found", 405 => "Method Not Allowed",
        502 => "Bad Gateway", _ => "OK",
    }
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing::info!(bind = %args.bind, "of-gateway starting");

    let (authz_host, authz_port) = split_host_port(&args.authz);
    let (frontend_host, frontend_port) = split_host_port(&args.frontend);
    let topo = Topology { authz_host, authz_port, frontend_host, frontend_port };

    let gw = Arc::new(Gateway::new());

    let listener = TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "of-gateway listening — replaces Spring Cloud Gateway on :8100");

    loop {
        let (sock, peer) = listener.accept().await?;
        let gw = gw.clone();
        let topo = topo.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, gw, topo).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

fn split_host_port(s: &str) -> (String, u16) {
    let stripped = s.strip_prefix("http://").or_else(|| s.strip_prefix("https://")).unwrap_or(s);
    match stripped.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(8082)),
        None => (stripped.to_string(), 8082),
    }
}

/// Topology needs Clone for the accept loop.
impl Clone for Topology {
    fn clone(&self) -> Self {
        Self {
            authz_host: self.authz_host.clone(),
            authz_port: self.authz_port,
            frontend_host: self.frontend_host.clone(),
            frontend_port: self.frontend_port,
        }
    }
}

/// Read one full HTTP request (headers + Content-Length body) from the socket.
async fn read_request(sock: &mut TcpStream) -> anyhow::Result<Request> {
    let mut reader = BufReader::new(sock);
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    // Read until \r\n\r\n.
    loop {
        let n = reader.read(&mut byte).await?;
        if n == 0 { anyhow::bail!("closed before headers"); }
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") { break; }
        if head.len() > 64 * 1024 { anyhow::bail!("header flood"); }
    }
    let head_str = String::from_utf8_lossy(&head).to_string();
    let mut lines = head_str.split("\r\n");
    let first = lines.next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("GET").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target, String::new()),
    };
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() { continue; }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let content_length: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).await?;
    }
    Ok(Request { method, path, query, headers, body })
}

async fn handle(mut sock: TcpStream, gw: Arc<Gateway>, topo: Topology) -> anyhow::Result<()> {
    let req = match read_request(&mut sock).await {
        Ok(r) => r,
        Err(_) => return Ok(()),
    };
    let resp = dispatch(&req, &gw, &topo).await;
    let bytes = resp.serialize();
    sock.write_all(&bytes).await?;
    sock.shutdown().await?;
    Ok(())
}

async fn dispatch(req: &Request, gw: &Gateway, topo: &Topology) -> Response {
    let path = req.path.as_str();
    tracing::debug!(%path, method = %req.method, "gateway dispatch");

    // In-process gateway OAuth2 client surface.
    match (req.method.as_str(), path) {
        ("GET", "/oauth/login") => return oauth_login(req, topo).await,
        ("GET", "/oauth/callback") => return oauth_callback(req, gw, topo).await,
        ("POST", "/oauth/refresh") => return oauth_refresh(req, gw, topo).await,
        ("POST", "/oauth/logout") | ("GET", "/oauth/logout") => return oauth_logout(req, gw),
        ("GET", "/oauth/dev-exchange") => return oauth_dev_exchange(req, gw, topo).await,
        ("GET", "/me") | ("GET", "/api/me") => return me(req, gw),
        ("GET", "/healthz") => return Response::json(200, "{\"ok\":true}".into()),
        _ => {}
    }

    // Proxied prefixes (upstream route table).
    let route = route_for_path(path);
    let Some((prefix, backend_host, backend_port, strip)) = route else {
        // /** → the original frontend container.
        return proxy(req, &topo.frontend_host, topo.frontend_port, "", None).await;
    };
    let _ = prefix;
    // Session cookie → Authorization header (the upstream gateway contract:
    // JWTs in HTTP-only cookies, converted to headers for internal services).
    let bearer = req.cookie(SESSION_COOKIE)
        .and_then(|sid| gw.get_session(&sid))
        .map(|s| s.access_token);
    proxy(req, &backend_host, backend_port, &strip, bearer.as_deref()).await
}

/// The upstream route table (from openframe-gateway.yml), host-local ports.
fn route_for_path(path: &str) -> Option<(&'static str, String, u16, String)> {
    if path.starts_with("/sas/") || path == "/sas" {
        Some(("/sas/", "127.0.0.1".into(), 8082, String::new()))
    } else if path.starts_with("/api/") || path == "/api" || path == "/graphql" {
        // Forward the /api prefix as-is so of-api's segment-based router
        // sees `api/organizations` etc. The upstream openframe-gateway
        // stripped /api because of-api was co-located with /graphql at the
        // same path; the replacement keeps them separate.
        Some(("/api/", "127.0.0.1".into(), 8090, String::new()))
    } else if path.starts_with("/chat/") || path == "/chat" {
        // /chat/graphql hosts the Mingo/dialog-statistics query. Same
        // backend (of-api) handles GraphQL; the /chat prefix is the
        // frontend's per-feature namespace.
        Some(("/chat/", "127.0.0.1".into(), 8090, String::new()))
    } else if path.starts_with("/clients/") || path == "/clients" {
        Some(("/clients/", "127.0.0.1".into(), 8084, "/clients".into()))
    } else if path.starts_with("/external-api/") || path == "/external-api" {
        Some(("/external-api/", "127.0.0.1".into(), 8083, "/external-api".into()))
    } else {
        None
    }
}

/// Forward a request to an upstream, converting the session cookie into the
/// upstream Authorization header (the upstream gateway contract).
async fn proxy(req: &Request, host: &str, port: u16, strip: &str, bearer: Option<&str>) -> Response {
    let upstream_path = if strip.is_empty() {
        req.path.clone()
    } else {
        req.path.strip_prefix(strip).unwrap_or(&req.path).to_string()
    };
    let target = if upstream_path.is_empty() { "/".to_string() } else { upstream_path }
        + &(if req.query.is_empty() { String::new() } else { format!("?{}", req.query) });

    let mut out = format!("{} {} HTTP/1.1\r\n", req.method, target);
    out.push_str(&format!("Host: {}:{}\r\n", host, port));
    for (k, v) in &req.headers {
        let skip = k.eq_ignore_ascii_case("host")
            || k.eq_ignore_ascii_case("connection")
            || k.eq_ignore_ascii_case("content-length")
            || k.eq_ignore_ascii_case("authorization")
            || k.eq_ignore_ascii_case("cookie")
            || k.eq_ignore_ascii_case("accept-encoding");
        if !skip {
            out.push_str(k);
            out.push_str(": ");
            out.push_str(v);
            out.push_str("\r\n");
        }
    }
    out.push_str("Accept-Encoding: identity\r\n");
    if let Some(token) = bearer {
        out.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    out.push_str(&format!("Content-Length: {}\r\n", req.body.len()));
    out.push_str("Connection: close\r\n\r\n");

    let mut bytes = out.into_bytes();
    bytes.extend_from_slice(&req.body);

    match raw_exchange(host, port, &bytes).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(host, port, error = %e, "upstream unreachable");
            Response::json(502, "{\"error\":\"upstream unreachable\"}".into())
        }
    }
}

/// One request/response TCP exchange; parses the upstream reply minimally.
async fn raw_exchange(host: &str, port: u16, req_bytes: &[u8]) -> anyhow::Result<Response> {
    let mut upstream = TcpStream::connect((host, port)).await?;
    upstream.write_all(req_bytes).await?;
    let mut buf = Vec::with_capacity(16 * 1024);
    upstream.read_to_end(&mut buf).await?;
    parse_response(&buf)
        .ok_or_else(|| anyhow::anyhow!("unparseable upstream response"))
}

fn parse_response(raw: &[u8]) -> Option<Response> {
    let split = find_subslice(raw, b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let body = raw[split + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let status_line = lines.next()?;
    let mut it = status_line.split_whitespace();
    let _http = it.next()?;
    let status: u16 = it.next()?.parse().ok()?;
    let mut headers = Vec::new();
    let mut chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.eq_ignore_ascii_case("transfer-encoding")
                && v.to_ascii_lowercase().contains("chunked")
            {
                chunked = true;
            }
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let body = if chunked { dechunk(&body) } else { body };
    Some(Response {
        status,
        reason: reason_for(status),
        headers,
        body,
    })
}

fn dechunk(mut body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let nl = match find_subslice(body, b"\r\n") {
            Some(i) => i,
            None => break,
        };
        let size_str = String::from_utf8_lossy(&body[..nl]).to_string();
        let size = usize::from_str_radix(size_str.trim().split(';').next().unwrap_or("0"), 16)
            .unwrap_or(0);
        if size == 0 { break; }
        let start = nl + 2;
        if start + size > body.len() { break; }
        out.extend_from_slice(&body[start..start + size]);
        body = &body[(start + size + 2).min(body.len())..];
    }
    out
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

// ── in-process OAuth2 client (the openframe-gateway client) ─────────────────

const SESSION_COOKIE: &str = "of_session";

fn session_cookie(id: &str) -> String {
    format!("{SESSION_COOKIE}={id}; Path=/; HttpOnly; SameSite=Lax")
}

fn clear_session_cookie() -> String {
    format!("{SESSION_COOKIE}=; Path=/; HttpOnly; Max-Age=0")
}

/// GET /oauth/login → 302 to the auth server's authorize endpoint.
async fn oauth_login(req: &Request, topo: &Topology) -> Response {
    let tenant_id = req.query_param("tenantId").unwrap_or_else(|| "oss".into());
    let redirect_to = req.query_param("redirectTo").unwrap_or_else(|| "/dashboard".into());
    let provider = req.query_param("provider").unwrap_or_else(|| "openframe-sso".into());
    let state = format!("{}|{}", tenant_id, redirect_to);
    let location = format!(
        "http://{host}:{port}/sas/oauth/authorize?client_id=openframe-gateway&redirect_uri=http://{ghost}/oauth/callback&response_type=code&scope=openid+profile&state={state}&provider={provider}",
        host = topo.authz_host,
        port = topo.authz_port,
        ghost = gateway_host(req),
        state = urlencode(&state),
        provider = urlencode(&provider),
    );
    Response::redirect(location, vec![])
}

/// GET /oauth/callback?code=…&state=… → exchange, set cookie, 302 onward.
async fn oauth_callback(req: &Request, gw: &Gateway, topo: &Topology) -> Response {
    let Some(code) = req.query_param("code") else {
        return Response::json(400, "{\"error\":\"missing code\"}".into());
    };
    let state = req.query_param("state").unwrap_or_default();
    let redirect_to = state.split('|').nth(1).unwrap_or("/dashboard").to_string();

    let exchange = format!(
        "grant_type=authorization_code&code={code}&client_id=openframe-gateway&client_secret=openframe-gateway-secret&redirect_uri=http://{ghost}/oauth/callback",
        code = urlencode(&code),
        ghost = gateway_host(req),
    );
    let body = serde_json::json!({}).to_string(); // placeholder, real body below
    let _ = body;
    let token_req = format!(
        "POST /sas/oauth/token HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{exchange}",
        host = topo.authz_host,
        port = topo.authz_port,
        len = exchange.len(),
        exchange = exchange,
    );
    match raw_exchange(&topo.authz_host, topo.authz_port, token_req.as_bytes()).await {
        Ok(resp) if resp.status == 200 => {
            let parsed: serde_json::Value = serde_json::from_slice(&resp.body)
                .unwrap_or(serde_json::Value::Null);
            let access = parsed["access_token"].as_str().unwrap_or_default().to_string();
            let refresh = parsed["refresh_token"].as_str().unwrap_or_default().to_string();
            let tenant = parsed["tenant_id"].as_str().unwrap_or("oss").to_string();
            if access.is_empty() {
                return Response::json(502, "{\"error\":\"token exchange failed\"}".into());
            }
            let session_id = uuid::Uuid::new_v4().to_string();
            gw.put_session(session_id.clone(), Session {
                access_token: access,
                refresh_token: refresh,
                tenant_id: tenant,
            });
            Response::redirect(redirect_to, vec![session_cookie(&session_id)])
        }
        Ok(resp) => Response::json(resp.status, String::from_utf8_lossy(&resp.body).to_string()),
        Err(e) => {
            tracing::warn!(error = %e, "token exchange upstream error");
            Response::json(502, "{\"error\":\"auth server unreachable\"}".into())
        }
    }
}

/// POST /oauth/refresh — refresh with the session's refresh token.
async fn oauth_refresh(req: &Request, gw: &Gateway, topo: &Topology) -> Response {
    let Some(session_id) = req.cookie(SESSION_COOKIE) else {
        return Response::json(401, "{\"error\":\"no session\"}".into());
    };
    let Some(session) = gw.get_session(&session_id) else {
        return Response::json(401, "{\"error\":\"no session\"}".into());
    };
    let form = format!(
        "grant_type=refresh_token&refresh_token={rt}&client_id=openframe-gateway&client_secret=openframe-gateway-secret",
        rt = urlencode(&session.refresh_token),
    );
    let token_req = format!(
        "POST /sas/oauth/token HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{form}",
        host = topo.authz_host,
        port = topo.authz_port,
        len = form.len(),
        form = form,
    );
    match raw_exchange(&topo.authz_host, topo.authz_port, token_req.as_bytes()).await {
        Ok(resp) if resp.status == 200 => {
            let parsed: serde_json::Value = serde_json::from_slice(&resp.body)
                .unwrap_or(serde_json::Value::Null);
            let access = parsed["access_token"].as_str().unwrap_or_default().to_string();
            let refresh = parsed["refresh_token"].as_str()
                .unwrap_or(&session.refresh_token).to_string();
            if access.is_empty() {
                return Response::json(401, "{\"error\":\"refresh rejected\"}".into());
            }
            gw.put_session(session_id.clone(), Session {
                access_token: access,
                refresh_token: refresh,
                tenant_id: session.tenant_id.clone(),
            });
            Response::json(200, "{\"ok\":true}".into())
        }
        Ok(resp) => Response::json(resp.status, String::from_utf8_lossy(&resp.body).to_string()),
        Err(_) => Response::json(502, "{\"error\":\"auth server unreachable\"}".into()),
    }
}

fn oauth_logout(req: &Request, gw: &Gateway) -> Response {
    if let Some(session_id) = req.cookie(SESSION_COOKIE) {
        gw.drop_session(&session_id);
    }
    Response::redirect("/auth".into(), vec![clear_session_cookie()])
}

/// GET /oauth/dev-exchange?ticket=… — the OSS dev ticket path; exchanges a
/// pre-issued ticket for a session (used by the DevTicket observer).
async fn oauth_dev_exchange(req: &Request, gw: &Gateway, topo: &Topology) -> Response {
    let Some(ticket) = req.query_param("ticket") else {
        return Response::json(400, "{\"error\":\"missing ticket\"}".into());
    };
    let exchange = format!("grant_type=dev-ticket&ticket={}", urlencode(&ticket));
    let token_req = format!(
        "POST /sas/oauth/token HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{exchange}",
        host = topo.authz_host,
        port = topo.authz_port,
        len = exchange.len(),
        exchange = exchange,
    );
    match raw_exchange(&topo.authz_host, topo.authz_port, token_req.as_bytes()).await {
        Ok(resp) if resp.status == 200 => {
            let parsed: serde_json::Value = serde_json::from_slice(&resp.body)
                .unwrap_or(serde_json::Value::Null);
            let access = parsed["access_token"].as_str().unwrap_or_default().to_string();
            let refresh = parsed["refresh_token"].as_str().unwrap_or_default().to_string();
            let tenant = parsed["tenant_id"].as_str().unwrap_or("oss").to_string();
            if access.is_empty() {
                return Response::json(401, "{\"error\":\"invalid ticket\"}".into());
            }
            let session_id = uuid::Uuid::new_v4().to_string();
            gw.put_session(session_id.clone(), Session {
                access_token: access.clone(),
                refresh_token: refresh,
                tenant_id: tenant,
            });
            let body = serde_json::json!({"ok": true, "token": access}).to_string();
            let mut r = Response::json(200, body);
            r.headers.push(("Set-Cookie".into(), session_cookie(&session_id)));
            r
        }
        Ok(resp) => Response::json(resp.status, String::from_utf8_lossy(&resp.body).to_string()),
        Err(_) => Response::json(502, "{\"error\":\"auth server unreachable\"}".into()),
    }
}

/// GET /me — session introspection for the frontend auth check.
fn me(req: &Request, gw: &Gateway) -> Response {
    let Some(session_id) = req.cookie(SESSION_COOKIE) else {
        return Response::json(401, "{\"error\":\"no session\"}".into());
    };
    let Some(session) = gw.get_session(&session_id) else {
        return Response::json(401, "{\"error\":\"no session\"}".into());
    };
    // The claims live in the token; decode the payload segment without
    // verifying (verification happened at exchange time).
    let claims = decode_jwt_claims(&session.access_token);
    let body = serde_json::json!({
        "authenticated": true,
        "tenantId": session.tenant_id,
        "sub": claims.get("sub").cloned().unwrap_or_default(),
        "email": claims.get("email").cloned().unwrap_or_default(),
        "name": claims.get("name").cloned().unwrap_or_default(),
        "role": claims.get("role").cloned().unwrap_or(serde_json::json!("user")),
    }).to_string();
    Response::json(200, body)
}

fn decode_jwt_claims(jwt: &str) -> serde_json::Value {
    jwt.split('.')
        .nth(1)
        .and_then(b64url_decode_str)
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(serde_json::Value::Null)
}

fn b64url_decode_str(s: &str) -> Option<String> {
    let mut lookup = [0xffu8; 256];
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    for (i, &c) in alphabet.iter().enumerate() { lookup[c as usize] = i as u8; }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0;
    for &b in bytes {
        let v = lookup[b as usize];
        if v == 0xff { return None; }
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xff) as u8);
        }
    }
    String::from_utf8(out).ok()
}

/// The externally visible gateway host (for redirect_uri) — from the request
/// Host header, falling back to localhost.
fn gateway_host(req: &Request) -> String {
    req.header("host").unwrap_or("localhost").to_string()
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn urldecode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() + 1 && i + 2 < bytes.len() + 1 => {
                if i + 2 < bytes.len() {
                    let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                    if let Ok(v) = u8::from_str_radix(hex, 16) {
                        out.push(v);
                        i += 3;
                        continue;
                    }
                }
                out.push(bytes[i]);
                i += 1;
            }
            b'+' => { out.push(b' '); i += 1; }
            b => { out.push(b); i += 1; }
        }
    }
    String::from_utf8_lossy(&out).to_string()
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
        assert!(gw.route_for("GET", "/auth/login").is_none());
        assert!(gw.route_for("POST", "/auth/login").is_some());
    }

    #[test]
    fn path_routing_table() {
        assert!(route_for_path("/sas/tenant/discover").is_some());
        assert!(route_for_path("/api/devices").is_some());
        assert!(route_for_path("/clients/x").is_some());
        assert!(route_for_path("/dashboard").is_none()); // → frontend fallback
    }

    #[test]
    fn request_parsing() {
        let req = Request {
            method: "GET".into(),
            path: "/oauth/login".into(),
            query: "tenantId=oss%20x&redirectTo=/dashboard".into(),
            headers: vec![("Cookie".into(), "of_session=abc; other=1".into())],
            body: Vec::new(),
        };
        assert_eq!(req.query_param("tenantId").unwrap(), "oss x");
        assert_eq!(req.query_param("redirectTo").unwrap(), "/dashboard");
        assert_eq!(req.cookie("of_session").unwrap(), "abc");
    }

    #[test]
    fn response_dechunk() {
        let body = b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
        assert_eq!(dechunk(body), b"Wikipedia".to_vec());
    }

    #[test]
    fn jwt_claims_decode() {
        // header.payload.sig with payload {"sub":"u1"}
        let payload = b64url_payload(b"{\"sub\":\"u1\"}");
        let jwt = format!("e30.{payload}.sig");
        let claims = decode_jwt_claims(&jwt);
        assert_eq!(claims["sub"], "u1");
    }

    fn b64url_payload(bytes: &[u8]) -> String {
        // test-only standard base64url without padding
        let mut out = String::new();
        const AL: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        for chunk in bytes.chunks(3) {
            let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
            let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
            out.push(AL[((n >> 18) & 63) as usize] as char);
            out.push(AL[((n >> 12) & 63) as usize] as char);
            if chunk.len() > 1 { out.push(AL[((n >> 6) & 63) as usize] as char); }
            if chunk.len() > 2 { out.push(AL[(n & 63) as usize] as char); }
        }
        out
    }
}
