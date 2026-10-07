//! `of-authorization` — replaces `openframe-authorization-service-core`
//! (Spring Authorization Server + MongoDB).
//!
//! Serves the `/sas` surface the frontend expects plus the legacy
//! `/oauth2` endpoints.  Users live in the `of_users` collection of the
//! default `test` database, accessed through a minimal OP_MSG wire client
//! that talks to `sangdb-mongo` on `127.0.0.1:37017`.

use clap::Parser;
use sangdb_bindings_mongodb::bson::{BsonDoc, BsonValue};
use sangdb_bindings_mongodb::wire::mongodb::{Header, OpMsg, OpMsgSection};
use sangdb_bindings_mongodb::wire::OP_MSG;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8082")]
    bind: String,
    #[arg(long, default_value = "127.0.0.1:37017")]
    mongo_uri: String,
    #[arg(long)]
    seed_demo_user: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: String,
    pub exp: i64,
    pub iat: i64,
    pub tenant_id: String,
    pub scope: String,
}

#[derive(Clone)]
pub struct Authz {
    pub issuer: String,
    pub secret_seed: [u8; 32],
}

impl Authz {
    pub fn new() -> Self {
        Self {
            issuer: "of-authorization".into(),
            secret_seed: [0x42; 32],
        }
    }

    pub fn issue(&self, claims: &Claims) -> String {
        let header = base64url_encode(b"{\"alg\":\"HS256\",\"typ\":\"JWT\"}");
        let payload = base64url_encode(serde_json::to_string(claims).unwrap().as_bytes());
        let signing_input = format!("{header}.{payload}");
        let sig = hmac_sign(&self.secret_seed, signing_input.as_bytes());
        format!("{signing_input}.{}", base64url_encode(&sig))
    }

    pub fn verify(&self, jwt: &str) -> Option<Claims> {
        let mut parts = jwt.split('.');
        let _h = parts.next()?;
        let payload = parts.next()?;
        let sig = parts.next()?;
        let signing_input = format!("{}.{}", _h, payload);
        let expected = hmac_sign(&self.secret_seed, signing_input.as_bytes());
        if base64url_encode(&expected) != sig {
            return None;
        }
        let bytes = base64url_decode(payload)?;
        serde_json::from_slice(&bytes).ok()
    }
}

// ── Mongo wire client ────────────────────────────────────────────────────────

#[derive(Clone)]
struct MongoClient {
    addr: String,
}

impl MongoClient {
    fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }

    async fn cmd(&self, doc: BsonDoc) -> anyhow::Result<BsonDoc> {
        let mut stream = TcpStream::connect(&self.addr).await?;
        let op = OpMsg { flags: 0, sections: vec![OpMsgSection::Body(vec![doc])] };
        let body = op.encode();
        let msg_len = (16 + body.len()) as i32;
        let mut wire = Vec::with_capacity(16 + body.len());
        Header { message_length: msg_len, request_id: 1, response_to: 0, op_code: OP_MSG }
            .encode(&mut wire);
        wire.extend_from_slice(&body);
        stream.write_all(&wire).await?;
        let mut hdr = [0u8; 16];
        stream.read_exact(&mut hdr).await?;
        let h = Header::decode(&hdr)?;
        let body_len = (h.message_length - 16) as usize;
        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body).await?;
        let resp = OpMsg::decode(&body)?;
        match resp.sections.into_iter().next() {
            Some(OpMsgSection::Body(mut docs)) => {
                docs.pop().ok_or_else(|| anyhow::anyhow!("empty body"))
            }
            _ => Err(anyhow::anyhow!("no body section in reply")),
        }
    }

    async fn find_user(&self, email: &str) -> anyhow::Result<Option<BsonDoc>> {
        let mut filter = BsonDoc::new();
        filter.insert("email", BsonValue::String(email.to_string()));
        let mut cmd = BsonDoc::new();
        cmd.insert("find", BsonValue::String("of_users".into()));
        cmd.insert("filter", BsonValue::Document(filter));
        cmd.insert("$db", BsonValue::String("test".into()));
        let resp = self.cmd(cmd).await?;
        let cursor = resp.get("cursor").and_then(|v| v.as_doc());
        let first_batch = cursor
            .and_then(|c| c.get("firstBatch"))
            .and_then(|v| v.as_array());
        match first_batch {
            Some(arr) if !arr.is_empty() => match &arr[0] {
                BsonValue::Document(d) => Ok(Some(d.clone())),
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    async fn insert_user(&self, doc: BsonDoc) -> anyhow::Result<()> {
        let mut cmd = BsonDoc::new();
        cmd.insert("insert", BsonValue::String("of_users".into()));
        cmd.insert("$db", BsonValue::String("test".into()));
        let op = OpMsg {
            flags: 0,
            sections: vec![
                OpMsgSection::Body(vec![cmd]),
                OpMsgSection::Type1 { size: 0, identifier: "documents".into(), docs: vec![doc] },
            ],
        };
        let body = op.encode();
        let msg_len = (16 + body.len()) as i32;
        let mut stream = TcpStream::connect(&self.addr).await?;
        let mut wire = Vec::with_capacity(16 + body.len());
        Header { message_length: msg_len, request_id: 2, response_to: 0, op_code: OP_MSG }
            .encode(&mut wire);
        wire.extend_from_slice(&body);
        stream.write_all(&wire).await?;
        let mut hdr = [0u8; 16];
        stream.read_exact(&mut hdr).await?;
        let h = Header::decode(&hdr)?;
        let mut body = vec![0u8; (h.message_length - 16) as usize];
        stream.read_exact(&mut body).await?;
        let _resp = OpMsg::decode(&body)?;
        Ok(())
    }

    async fn delete_user(&self, email: &str) -> anyhow::Result<()> {
        let mut filter = BsonDoc::new();
        filter.insert("email", BsonValue::String(email.to_string()));
        let mut cmd = BsonDoc::new();
        cmd.insert("delete", BsonValue::String("of_users".into()));
        cmd.insert("deletes", BsonValue::Array(vec![BsonValue::Document(BsonDoc::new())]));
        cmd.insert("$db", BsonValue::String("test".into()));
        // The above is incomplete — mongod expects `deletes: [{q: filter, limit: N}]`
        let mut delete_doc = BsonDoc::new();
        delete_doc.insert("q", BsonValue::Document(filter));
        delete_doc.insert("limit", BsonValue::Int64(0));
        cmd.insert("deletes", BsonValue::Array(vec![BsonValue::Document(delete_doc)]));
        let _ = self.cmd(cmd).await?;
        Ok(())
    }
}

// ── Password hashing ─────────────────────────────────────────────────────────

fn hash_password(password: &str, salt: &[u8]) -> String {
    let mut data = Vec::with_capacity(salt.len() + password.len());
    data.extend_from_slice(salt);
    data.extend_from_slice(password.as_bytes());
    hex_encode(&sha256(&data))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len() / 2);
    let mut chars = s.chars();
    while let (Some(h), Some(l)) = (chars.next(), chars.next()) {
        let hi = h.to_digit(16).unwrap_or(0) as u8;
        let lo = l.to_digit(16).unwrap_or(0) as u8;
        out.push((hi << 4) | lo);
    }
    out
}

fn make_salt() -> Vec<u8> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let mut salt = vec![0u8; 16];
    let nanos = t.as_nanos().to_le_bytes();
    let copy_len = nanos.len().min(16);
    salt[..copy_len].copy_from_slice(&nanos[..copy_len]);
    salt
}

// ── HTTP parsing ─────────────────────────────────────────────────────────────

struct ParsedRequest {
    method: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn parse_request(raw: &[u8]) -> ParsedRequest {
    let mut split_at = raw.len();
    for i in 0..raw.len().saturating_sub(3) {
        if &raw[i..i + 4] == b"\r\n\r\n" {
            split_at = i;
            break;
        }
    }
    let head_bytes = &raw[..split_at];
    let body_bytes = if split_at + 4 <= raw.len() { &raw[split_at + 4..] } else { b"" };
    let head = std::str::from_utf8(head_bytes).unwrap_or("");
    let mut lines = head.split("\r\n");
    let first = lines.next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("GET").to_string();
    let full_path = parts.next().unwrap_or("/").to_string();
    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let (path, query) = match full_path.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (full_path, String::new()),
    };
    ParsedRequest { method, path, query, headers, body: body_bytes.to_vec() }
}

fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return Some(urldecode(v));
            }
        }
    }
    None
}

fn hex_digit(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'A'..=b'F' => c - b'A' + 10,
        b'a'..=b'f' => c - b'a' + 10,
        _ => 0,
    }
}

fn urldecode(s: &str) -> String {
    let mut out = Vec::new();
    let mut chars = s.as_bytes().iter();
    while let Some(&b) = chars.next() {
        match b {
            b'%' => {
                let h = chars.next().copied().unwrap_or(b'0');
                let l = chars.next().copied().unwrap_or(b'0');
                out.push((hex_digit(h) << 4) | hex_digit(l));
            }
            b'+' => out.push(b' '),
            _ => out.push(b),
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_form_body(body: &[u8]) -> HashMap<String, String> {
    let s = std::str::from_utf8(body).unwrap_or("");
    let mut map = HashMap::new();
    for pair in s.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(urldecode(k), urldecode(v));
        }
    }
    map
}

fn parse_json_body(body: &[u8]) -> serde_json::Value {
    serde_json::from_slice(body).unwrap_or(serde_json::Value::Null)
}

fn extract_bearer(raw: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(raw).ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("Authorization: Bearer ") {
            return Some(rest.trim().to_string());
        }
    }
    None
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

fn base64url_encode(bytes: &[u8]) -> String {
    let mut buf = Vec::new();
    let alpha = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut i = 0;
    while i + 3 <= bytes.len() {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i+1] as u32) << 8) | (bytes[i+2] as u32);
        buf.push(alpha[((n >> 18) & 0x3f) as usize]);
        buf.push(alpha[((n >> 12) & 0x3f) as usize]);
        buf.push(alpha[((n >> 6) & 0x3f) as usize]);
        buf.push(alpha[(n & 0x3f) as usize]);
        i += 3;
    }
    let rem = bytes.len() - i;
    if rem == 1 {
        let n = (bytes[i] as u32) << 16;
        buf.push(alpha[((n >> 18) & 0x3f) as usize]);
        buf.push(alpha[((n >> 12) & 0x3f) as usize]);
    } else if rem == 2 {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i+1] as u32) << 8);
        buf.push(alpha[((n >> 18) & 0x3f) as usize]);
        buf.push(alpha[((n >> 12) & 0x3f) as usize]);
        buf.push(alpha[((n >> 6) & 0x3f) as usize]);
    }
    String::from_utf8(buf).unwrap()
}

fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    let mut lookup = [0xffu8; 256];
    let alpha = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    for (i, &c) in alpha.iter().enumerate() { lookup[c as usize] = i as u8; }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0;
    for &b in bytes {
        let v = lookup[b as usize];
        if v == 0xff { return None; }
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 { bits -= 8; out.push(((buf >> bits) & 0xff) as u8); }
    }
    Some(out)
}

fn hmac_sign(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let block_size = 64;
    let mut k = vec![0u8; block_size];
    if key.len() > block_size {
        let hash = sha256(key);
        k[..hash.len()].copy_from_slice(&hash);
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let ipad: Vec<u8> = k.iter().map(|x| x ^ 0x36).collect();
    let opad: Vec<u8> = k.iter().map(|x| x ^ 0x5c).collect();
    let mut inner = ipad;
    inner.extend_from_slice(msg);
    let inner_hash = sha256(&inner);
    let mut outer = opad;
    outer.extend_from_slice(&inner_hash);
    sha256(&outer)
}

fn sha256(data: &[u8]) -> Vec<u8> {
    const K: [u32; 64] = [
        0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
        0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
        0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
        0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
        0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
        0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
        0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
        0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    let len = (msg.len() as u64) * 8;
    msg.push(0x80);
    while msg.len() % 64 != 56 { msg.push(0); }
    msg.extend_from_slice(&len.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 { w[i] = u32::from_be_bytes(chunk[i*4..i*4+4].try_into().unwrap()); }
        for i in 16..64 {
            let s0 = w[i-15].rotate_right(7) ^ w[i-15].rotate_right(18) ^ (w[i-15] >> 3);
            let s1 = w[i-2].rotate_right(17) ^ w[i-2].rotate_right(19) ^ (w[i-2] >> 10);
            w[i] = w[i-16].wrapping_add(s0).wrapping_add(w[i-7]).wrapping_add(s1);
        }
        let (mut a,mut b,mut c,mut d) = (h[0],h[1],h[2],h[3]);
        let (mut e,mut f,mut g,mut hh) = (h[4],h[5],h[6],h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let mj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(mj);
            hh = g; g = f; f = e; e = d.wrapping_add(t1);
            d = c; c = b; b = a; a = t1.wrapping_add(t2);
        }
        h[0]=h[0].wrapping_add(a); h[1]=h[1].wrapping_add(b);
        h[2]=h[2].wrapping_add(c); h[3]=h[3].wrapping_add(d);
        h[4]=h[4].wrapping_add(e); h[5]=h[5].wrapping_add(f);
        h[6]=h[6].wrapping_add(g); h[7]=h[7].wrapping_add(hh);
    }
    h.iter().flat_map(|x| x.to_be_bytes()).collect()
}

fn login_form_html(
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    provider: &str,
    error: Option<&str>,
) -> String {
    let error_msg = match error {
        Some(e) => format!(r#"<div class="error">{}</div>"#, escape_html(e)),
        None => String::new(),
    };
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Sign In</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
           background: #f5f5f5; display: flex; align-items: center; justify-content: center;
           min-height: 100vh; color: #333; }}
    .card {{ background: #fff; border-radius: 8px; box-shadow: 0 2px 12px rgba(0,0,0,.12);
             padding: 40px; width: 100%; max-width: 400px; }}
    h1 {{ font-size: 22px; font-weight: 600; margin-bottom: 24px; text-align: center; }}
    label {{ display: block; font-size: 14px; font-weight: 500; margin-bottom: 6px; }}
    input[type="email"], input[type="password"] {{
      width: 100%; padding: 10px 12px; border: 1px solid #d0d0d0; border-radius: 6px;
      font-size: 15px; outline: none; transition: border-color .15s;
    }}
    input:focus {{ border-color: #4a90d9; box-shadow: 0 0 0 3px rgba(74,144,217,.15); }}
    .field {{ margin-bottom: 16px; }}
    button {{ width: 100%; padding: 10px; background: #4a90d9; color: #fff; border: none;
              border-radius: 6px; font-size: 15px; font-weight: 600; cursor: pointer;
              transition: background .15s; }}
    button:hover {{ background: #357abd; }}
    .error {{ background: #fef2f2; border: 1px solid #fecaca; color: #b91c1c;
              padding: 10px 12px; border-radius: 6px; margin-bottom: 16px; font-size: 14px; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>Sign In</h1>
    {error_msg}
    <form method="POST" action="/sas/oauth/authorize">
      <input type="hidden" name="client_id" value="{}"/>
      <input type="hidden" name="redirect_uri" value="{}"/>
      <input type="hidden" name="state" value="{}"/>
      <input type="hidden" name="provider" value="{}"/>
      <div class="field">
        <label for="email">Email</label>
        <input type="email" id="email" name="email" required autofocus/>
      </div>
      <div class="field">
        <label for="password">Password</label>
        <input type="password" id="password" name="password" required/>
      </div>
      <button type="submit">Sign In</button>
    </form>
  </div>
</body>
</html>"#,
        escape_html(client_id),
        escape_html(redirect_uri),
        escape_html(state),
        escape_html(provider),
    )
}

fn bson_str(s: &str) -> BsonValue { BsonValue::String(s.to_string()) }
fn bson_bool(b: bool) -> BsonValue { BsonValue::Bool(b) }
fn bson_i64(n: i64) -> BsonValue { BsonValue::Int64(n) }

async fn send_response(sock: &mut TcpStream, status: u16, reason: &str, ct: &str, body: &[u8]) {
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status, reason, ct, body.len()
    );
    let _ = sock.write_all(header.as_bytes()).await;
    let _ = sock.write_all(body).await;
}

async fn send_json(sock: &mut TcpStream, status: u16, reason: &str, val: &serde_json::Value) {
    let body = val.to_string();
    send_response(sock, status, reason, "application/json", body.as_bytes()).await;
}

async fn send_redirect(sock: &mut TcpStream, location: &str) {
    let header = format!(
        "HTTP/1.1 302 Found\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        location
    );
    let _ = sock.write_all(header.as_bytes()).await;
}

async fn send_html(sock: &mut TcpStream, html: &str) {
    send_response(sock, 200, "OK", "text/html; charset=utf-8", html.as_bytes()).await;
}

type CodeStore = RwLock<HashMap<String, Claims>>;
type RefreshStore = RwLock<HashMap<String, Claims>>;

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    let authz = Arc::new(Authz::new());
    let mongo = Arc::new(MongoClient::new(&args.mongo_uri));
    let codes: Arc<CodeStore> = Arc::new(RwLock::new(HashMap::new()));
    let refreshes: Arc<RefreshStore> = Arc::new(RwLock::new(HashMap::new()));

    if args.seed_demo_user {
        // Delete existing demo user (in case of stale fields from previous run)
        let _ = mongo.delete_user("demo@openframe.local").await;
        let existing = mongo.find_user("demo@openframe.local").await;
        if let Ok(None) = existing {
            let salt = make_salt();
            let hash = hash_password("demo1234", &salt);
            let mut doc = BsonDoc::new();
            doc.insert("_id", bson_str(&uuid::Uuid::new_v4().to_string()));
            doc.insert("email", bson_str("demo@openframe.local"));
            doc.insert("firstName", bson_str("Demo"));
            doc.insert("lastName", bson_str("User"));
            doc.insert("name", bson_str("Demo User"));
            doc.insert("passwordHash", bson_str(&hash));
            doc.insert("password_hash", bson_str(&hash));
            doc.insert("salt", bson_str(&hex_encode(&salt)));
            doc.insert("tenantId", bson_str("oss"));
            doc.insert("tenant_id", bson_str("oss"));
            doc.insert("role", bson_str("ADMIN"));
            doc.insert("enabled", bson_bool(true));
            doc.insert("createdAt", bson_i64(chrono::Utc::now().timestamp_millis()));
            if let Err(e) = mongo.insert_user(doc).await {
                eprintln!("Failed to seed demo user: {e}");
            } else {
                println!("Seeded demo user: demo@openframe.local");
            }
        }
    }

    let listener = TcpListener::bind(&args.bind).await.unwrap();
    println!("of-authorization listening on {}", args.bind);

    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let authz = authz.clone();
        let mongo = mongo.clone();
        let codes = codes.clone();
        let refreshes = refreshes.clone();
        tokio::spawn(async move {
            let mut buf = Vec::with_capacity(4096);
            let mut tmp = [0u8; 4096];
            // Read until we have the full request: headers + Content-Length body.
            'read: loop {
                match stream.read(&mut tmp).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        buf.extend_from_slice(&tmp[..n]);
                        // Find end of headers.
                        if let Some(hdr_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let body_start = hdr_end + 4;
                            let head = std::str::from_utf8(&buf[..hdr_end]).unwrap_or("");
                            // Find Content-Length.
                            let mut content_length: usize = 0;
                            for line in head.split("\r\n") {
                                if let Some(rest) = line.strip_prefix("Content-Length:") {
                                    content_length = rest.trim().parse().unwrap_or(0);
                                } else if let Some(rest) = line.strip_prefix("content-length:") {
                                    content_length = rest.trim().parse().unwrap_or(0);
                                }
                            }
                            if buf.len() >= body_start + content_length {
                                break 'read;
                            }
                        }
                    }
                }
            }
            handle(&mut stream, &buf, &*authz, &*mongo, &*codes, &*refreshes).await;
        });
    }
}

async fn handle(
    sock: &mut TcpStream,
    raw: &[u8],
    authz: &Authz,
    mongo: &MongoClient,
    codes: &CodeStore,
    refreshes: &RefreshStore,
) {
    let req = parse_request(raw);
    let m = req.method.as_str();
    let p = req.path.as_str();

    match (m, p) {
        ("GET", "/healthz") => {
            send_json(sock, 200, "OK", &serde_json::json!({"ok":true})).await;
        }

        ("GET", "/.well-known/openid-configuration") => {
            let issuer = &authz.issuer;
            send_json(sock, 200, "OK", &serde_json::json!({
                "issuer": issuer,
                "authorization_endpoint": format!("{}/sas/oauth/authorize", issuer),
                "token_endpoint": format!("{}/sas/oauth/token", issuer),
                "userinfo_endpoint": format!("{}/userinfo", issuer),
                "jwks_uri": format!("{}/oauth2/jwks", issuer),
                "response_types_supported": ["code"],
                "grant_types_supported": ["authorization_code", "refresh_token", "password"],
                "subject_types_supported": ["public"],
                "id_token_signing_alg_values_supported": ["HS256"],
                "scopes_supported": ["openid", "profile", "email"],
            })).await;
        }

        ("GET", "/oauth2/jwks") => {
            send_json(sock, 200, "OK", &serde_json::json!({"keys": []})).await;
        }

        ("GET", "/sas/tenant/discover") => {
            let email = query_param(&req.query, "email").unwrap_or_default();
            if email.is_empty() {
                send_json(sock, 400, "Bad Request", &serde_json::json!({"error":"missing email"})).await;
                return;
            }
            match mongo.find_user(&email).await {
                Ok(Some(user)) => {
                    let tenant_id = user.get("tenantId")
                        .or_else(|| user.get("tenant_id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("default");
                    send_json(sock, 200, "OK", &serde_json::json!({
                        "email": email,
                        "has_existing_accounts": true,
                        "tenant_id": tenant_id,
                        "auth_providers": ["openframe-sso"],
                    })).await;
                }
                Ok(None) => {
                    send_json(sock, 200, "OK", &serde_json::json!({
                        "email": email,
                        "has_existing_accounts": false,
                        "auth_providers": [],
                    })).await;
                }
                Err(e) => {
                    send_json(sock, 500, "Internal Server Error", &serde_json::json!({"error": e.to_string()})).await;
                }
            }
        }

        ("POST", "/sas/oauth/register") => {
            let body = parse_json_body(&req.body);
            let email = body.get("email").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let password = body.get("password").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let first_name = body.get("firstName").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let last_name = body.get("lastName").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let tenant_name = body.get("tenantName").and_then(|v| v.as_str()).unwrap_or("default").to_string();
            let name = format!("{} {}", first_name, last_name);
            let trimmed_name = name.trim().to_string();
            if email.is_empty() || password.is_empty() {
                send_json(sock, 400, "Bad Request", &serde_json::json!({"error":"email and password required"})).await;
                return;
            }
            if mongo.find_user(&email).await.ok().flatten().is_some() {
                send_json(sock, 409, "Conflict", &serde_json::json!({"error":"user already exists"})).await;
                return;
            }
            let salt = make_salt();
            let hash = hash_password(&password, &salt);
            let mut doc = BsonDoc::new();
            doc.insert("_id", bson_str(&uuid::Uuid::new_v4().to_string()));
            doc.insert("email", bson_str(&email));
            doc.insert("firstName", bson_str(&first_name));
            doc.insert("lastName", bson_str(&last_name));
            doc.insert("name", bson_str(&trimmed_name));
            doc.insert("passwordHash", bson_str(&hash));
            doc.insert("password_hash", bson_str(&hash));
            doc.insert("salt", bson_str(&hex_encode(&salt)));
            doc.insert("tenantId", bson_str(&tenant_name));
            doc.insert("tenant_id", bson_str(&tenant_name));
            doc.insert("role", bson_str("ADMIN"));
            doc.insert("enabled", bson_bool(true));
            doc.insert("createdAt", bson_i64(chrono::Utc::now().timestamp_millis()));
            match mongo.insert_user(doc).await {
                Ok(()) => {
                    send_json(sock, 200, "OK", &serde_json::json!({"ok": true, "tenantId": tenant_name})).await;
                }
                Err(e) => {
                    send_json(sock, 500, "Internal Server Error", &serde_json::json!({"error": e.to_string()})).await;
                }
            }
        }

        ("GET", "/sas/oauth/authorize") => {
            let client_id = query_param(&req.query, "client_id").unwrap_or_else(|| "openframe".into());
            let redirect_uri = query_param(&req.query, "redirect_uri").unwrap_or_else(|| "/".into());
            let state = query_param(&req.query, "state").unwrap_or_default();
            let provider = query_param(&req.query, "provider").unwrap_or_else(|| "local".into());
            let html = login_form_html(&client_id, &redirect_uri, &state, &provider, None);
            send_html(sock, &html).await;
        }

        ("POST", "/sas/oauth/authorize") => {
            let form = parse_form_body(&req.body);
            let email = form.get("email").map(|s| s.as_str()).unwrap_or("");
            let password = form.get("password").map(|s| s.as_str()).unwrap_or("");
            let client_id = form.get("client_id").map(|s| s.as_str()).unwrap_or("openframe");
            let redirect_uri = form.get("redirect_uri").map(|s| s.as_str()).unwrap_or("/");
            let state = form.get("state").map(|s| s.as_str()).unwrap_or("");
            let provider = form.get("provider").map(|s| s.as_str()).unwrap_or("local");

            match mongo.find_user(email).await {
                Ok(Some(user_doc)) => {
                    let stored_hash = user_doc.get("password_hash")
                        .or_else(|| user_doc.get("passwordHash"))
                        .and_then(|v| v.as_str()).unwrap_or("");
                    let stored_salt = user_doc.get("salt")
                        .and_then(|v| v.as_str()).unwrap_or("");
                    eprintln!("DEBUG authorize: email={email:?} stored_hash={stored_hash:?} stored_salt={stored_salt:?} password={password:?}");
                    let salt_bytes = hex_decode(stored_salt);
                    let computed = hash_password(password, &salt_bytes);
                    eprintln!("DEBUG authorize: computed={computed:?} match={}", computed == stored_hash);
                    if computed == stored_hash {
                        let tenant_id = user_doc.get("tenant_id")
                            .and_then(|v| v.as_str()).unwrap_or("default");
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
                        let claims = Claims {
                            sub: email.to_string(),
                            iss: authz.issuer.clone(),
                            aud: client_id.to_string(),
                            exp: now + 3600,
                            iat: now,
                            tenant_id: tenant_id.to_string(),
                            scope: "openid profile email".into(),
                        };
                        let code = {
                            use std::fmt::Write;
                            let raw = sha256(
                                format!("{email}{now}{}", rand_u64()).as_bytes()
                            );
                            raw.iter().fold(String::new(), |mut s, b| { let _ = write!(s, "{b:02x}"); s })
                        };
                        codes.write().await.insert(code.clone(), claims);
                        let sep = if redirect_uri.contains('?') { '&' } else { '?' };
                        let mut loc = format!("{redirect_uri}{sep}code={code}");
                        if !state.is_empty() {
                            loc.push_str(&format!("&state={state}"));
                        }
                        send_redirect(sock, &loc).await;
                    } else {
                        let html = login_form_html(client_id, redirect_uri, state, provider, Some("Invalid email or password"));
                        send_response(sock, 401, "Unauthorized", "text/html; charset=utf-8", html.as_bytes()).await;
                    }
                }
                _ => {
                    let html = login_form_html(client_id, redirect_uri, state, provider, Some("Invalid email or password"));
                    send_response(sock, 401, "Unauthorized", "text/html; charset=utf-8", html.as_bytes()).await;
                }
            }
        }

        ("POST", "/sas/oauth/token") | ("POST", "/oauth2/token") => {
            let form = parse_form_body(&req.body);
            let grant_type = form.get("grant_type").map(|s| s.as_str()).unwrap_or("");

            match grant_type {
                "authorization_code" => {
                    let code = form.get("code").map(|s| s.as_str()).unwrap_or("");
                    let claims = codes.write().await.remove(code);
                    match claims {
                        Some(mut c) => {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
                            c.iat = now;
                            c.exp = now + 3600;
                            let access_token = authz.issue(&c);
                            let refresh_token = {
                                use std::fmt::Write;
                                let raw = sha256(format!("refresh{now}{}", rand_u64()).as_bytes());
                                raw.iter().fold(String::new(), |mut s, b| { let _ = write!(s, "{b:02x}"); s })
                            };
                            refreshes.write().await.insert(refresh_token.clone(), c.clone());
                            send_json(sock, 200, "OK", &serde_json::json!({
                                "access_token": access_token,
                                "token_type": "Bearer",
                                "expires_in": 3600,
                                "refresh_token": refresh_token,
                                "id_token": access_token,
                            })).await;
                        }
                        None => {
                            send_json(sock, 400, "Bad Request", &serde_json::json!({"error":"invalid_code"})).await;
                        }
                    }
                }
                "refresh_token" => {
                    let rt = form.get("refresh_token").map(|s| s.as_str()).unwrap_or("");
                    let claims = refreshes.write().await.remove(rt);
                    match claims {
                        Some(mut c) => {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
                            c.iat = now;
                            c.exp = now + 3600;
                            let access_token = authz.issue(&c);
                            let new_refresh = {
                                use std::fmt::Write;
                                let raw = sha256(format!("refresh{now}{}", rand_u64()).as_bytes());
                                raw.iter().fold(String::new(), |mut s, b| { let _ = write!(s, "{b:02x}"); s })
                            };
                            refreshes.write().await.insert(new_refresh.clone(), c);
                            send_json(sock, 200, "OK", &serde_json::json!({
                                "access_token": access_token,
                                "token_type": "Bearer",
                                "expires_in": 3600,
                                "refresh_token": new_refresh,
                            })).await;
                        }
                        None => {
                            send_json(sock, 400, "Bad Request", &serde_json::json!({"error":"invalid_refresh_token"})).await;
                        }
                    }
                }
                "password" => {
                    let email = form.get("username").map(|s| s.as_str()).unwrap_or("");
                    let password = form.get("password").map(|s| s.as_str()).unwrap_or("");
                    match mongo.find_user(email).await {
                        Ok(Some(user_doc)) => {
                            let stored_hash = user_doc.get("password_hash")
                                .and_then(|v| v.as_str()).unwrap_or("");
                            let stored_salt = user_doc.get("salt")
                                .and_then(|v| v.as_str()).unwrap_or("");
                            let salt_bytes = hex_decode(stored_salt);
                            let computed = hash_password(password, &salt_bytes);
                            if computed == stored_hash {
                                let tenant_id = user_doc.get("tenant_id")
                                    .and_then(|v| v.as_str()).unwrap_or("default");
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
                                let claims = Claims {
                                    sub: email.to_string(),
                                    iss: authz.issuer.clone(),
                                    aud: "openframe".into(),
                                    exp: now + 3600,
                                    iat: now,
                                    tenant_id: tenant_id.to_string(),
                                    scope: "openid profile email".into(),
                                };
                                let access_token = authz.issue(&claims);
                                send_json(sock, 200, "OK", &serde_json::json!({
                                    "access_token": access_token,
                                    "token_type": "Bearer",
                                    "expires_in": 3600,
                                    "id_token": access_token,
                                })).await;
                            } else {
                                send_json(sock, 401, "Unauthorized", &serde_json::json!({"error":"invalid_credentials"})).await;
                            }
                        }
                        _ => {
                            send_json(sock, 401, "Unauthorized", &serde_json::json!({"error":"invalid_credentials"})).await;
                        }
                    }
                }
                _ => {
                    send_json(sock, 400, "Bad Request", &serde_json::json!({"error":"unsupported_grant_type"})).await;
                }
            }
        }

        ("GET", "/userinfo") | ("POST", "/userinfo") => {
            let raw_auth = req.headers.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("Authorization"))
                .map(|(_, v)| v.as_str());
            let jwt = raw_auth.and_then(|h| h.strip_prefix("Bearer "));
            match jwt.and_then(|t| authz.verify(t)) {
                Some(claims) => {
                    send_json(sock, 200, "OK", &serde_json::json!({
                        "sub": claims.sub,
                        "iss": claims.iss,
                        "aud": claims.aud,
                        "exp": claims.exp,
                        "iat": claims.iat,
                        "tenant_id": claims.tenant_id,
                        "scope": claims.scope,
                    })).await;
                }
                None => {
                    send_json(sock, 401, "Unauthorized", &serde_json::json!({"error":"invalid_token"})).await;
                }
            }
        }

        ("POST", "/sas/password-reset/request") | ("POST", "/sas/password-reset/confirm") => {
            send_json(sock, 200, "OK", &serde_json::json!({"message":"stub"})).await;
        }

        ("GET", "/sas/sso/providers/registration") | ("GET", "/sas/sso/providers") => {
            send_json(sock, 200, "OK", &serde_json::json!({
                "providers": [],
                "authProviders": ["openframe-sso"],
            })).await;
        }

        ("POST", "/sas/invitations/accept") => {
            let body = parse_json_body(&req.body);
            let email = body.get("email").and_then(|v| v.as_str()).unwrap_or("");
            let password = body.get("password").and_then(|v| v.as_str()).unwrap_or("");
            let name = body.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if email.is_empty() || password.is_empty() {
                send_json(sock, 400, "Bad Request", &serde_json::json!({"error":"email and password required"})).await;
                return;
            }
            let salt = make_salt();
            let hash = hash_password(password, &salt);
            let mut doc = BsonDoc::new();
            doc.insert("email", bson_str(email));
            doc.insert("password_hash", bson_str(&hash));
            doc.insert("salt", bson_str(&hex_encode(&salt)));
            doc.insert("tenant_id", bson_str("default"));
            doc.insert("name", bson_str(name));
            match mongo.insert_user(doc).await {
                Ok(()) => {
                    send_json(sock, 201, "Created", &serde_json::json!({"email": email, "message": "registered"})).await;
                }
                Err(e) => {
                    send_json(sock, 500, "Internal Server Error", &serde_json::json!({"error": e.to_string()})).await;
                }
            }
        }

        _ => {
            send_json(sock, 404, "Not Found", &serde_json::json!({"error":"not found"})).await;
        }
    }
}

fn rand_u64() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let nanos = t.as_nanos();
    ((nanos ^ (nanos >> 31)) & 0xFFFF_FFFF_FFFF_FFFF) as u64
}
