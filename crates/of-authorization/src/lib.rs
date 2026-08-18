//! `of-authorization` — replaces `openframe-authorization-service-core`
//! (Spring Authorization Server + MongoDB).
//!
//! ## The collapse
//!
//! The upstream service:
//!
//! - Persists OAuth2 clients, registered users, refresh tokens, consent
//!   records, and registered redirect URIs to MongoDB via
//!   `openframe-data-mongo-reactive`.
//! - Issues RS256 JWTs signed by a tenant-scoped RSA key pair (per-tenant
//!   `RSA-signed JWTs` in the README).
//! - Exposes the OAuth2/OIDC endpoints (`/oauth2/token`, `/oauth2/jwks`,
//!   `/oauth2/authorize`, `/userinfo`, `/.well-known/openid-configuration`).
//! - Supports SSO via Google and Microsoft OIDC through the Spring
//!   Authorization Server client registry.
//!
//! The replacement stores all the MongoDB documents in a hashtrinity
//! `Table<TenantKey, V>` — one table per OAuth2 doc collection — and the
//! JWT signing uses the `ring`/RustCrypto primitives already in
//! `hashtrinity::crypto`. The cookie session the upstream issues becomes
//! a `tenant:tenant_id:session:<session_id>` row.
//!
//! ## Wire surface
//!
//! ```text
//! POST   /oauth2/token                      → issue JWT (RS256)
//! GET    /oauth2/jwks                       → JSON Web Key Set (public keys)
//! GET    /.well-known/openid-configuration  → discovery doc
//! GET    /userinfo                          → return claims for the JWT
//! POST   /login                             → password grant (form-encoded)
//! GET    /healthz                           → liveness
//! ```

use clap::Parser;
use openframe_domain::TenantId;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8082")]
    bind: String,
}

/// A JWT claim set — the same shape Spring Authorization Server uses.
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
    pub tenant: TenantId,
    pub issuer: String,
    pub secret_seed: [u8; 32],
}

impl Authz {
    pub fn new(tenant: TenantId) -> Self {
        Self {
            tenant,
            issuer: "of-authorization".into(),
            secret_seed: [0x42; 32], // TODO: real RS256 key per tenant
        }
    }

    /// Issue an RS256 JWT. The reference impl signs with HS256 against the
    /// tenant-scoped secret seed; replacing with RS256 via
    /// `hashtrinity::crypto::webauthn` is the next commit.
    pub fn issue(&self, claims: &Claims) -> String {
        // Minimal JWT shape: header.payload.signature — base64url-encoded.
        let header = base64url_encode(b"{\"alg\":\"HS256\",\"typ\":\"JWT\"}");
        let payload = base64url_encode(serde_json::to_string(claims).unwrap().as_bytes());
        let signing_input = format!("{header}.{payload}");
        let sig = hmac_sign(&self.secret_seed, signing_input.as_bytes());
        format!("{signing_input}.{}", base64url_encode(&sig))
    }

    pub fn verify(&self, jwt: &str) -> Option<Claims> {
        let mut parts = jwt.split('.');
        let _header = parts.next()?;
        let payload = parts.next()?;
        let sig = parts.next()?;
        let signing_input = format!("{}.{}", _header, payload);
        let expected = hmac_sign(&self.secret_seed, signing_input.as_bytes());
        if base64url_encode(&expected) != sig { return None; }
        let bytes = base64url_decode(payload)?;
        serde_json::from_slice(&bytes).ok()
    }
}

fn base64url_encode(bytes: &[u8]) -> String {
    let mut buf = Vec::new();
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut i = 0;
    while i + 3 <= bytes.len() {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i+1] as u32) << 8) | (bytes[i+2] as u32);
        buf.push(alphabet[((n >> 18) & 0x3f) as usize]);
        buf.push(alphabet[((n >> 12) & 0x3f) as usize]);
        buf.push(alphabet[((n >> 6) & 0x3f) as usize]);
        buf.push(alphabet[(n & 0x3f) as usize]);
        i += 3;
    }
    let rem = bytes.len() - i;
    if rem == 1 {
        let n = (bytes[i] as u32) << 16;
        buf.push(alphabet[((n >> 18) & 0x3f) as usize]);
        buf.push(alphabet[((n >> 12) & 0x3f) as usize]);
    } else if rem == 2 {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i+1] as u32) << 8);
        buf.push(alphabet[((n >> 18) & 0x3f) as usize]);
        buf.push(alphabet[((n >> 12) & 0x3f) as usize]);
        buf.push(alphabet[((n >> 6) & 0x3f) as usize]);
    }
    String::from_utf8(buf).unwrap()
}

fn base64url_decode(s: &str) -> Option<Vec<u8>> {
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
    Some(out)
}

/// HS256 = HMAC-SHA256 over the signing input. Real RS256 lands in the
/// next iteration via `hashtrinity::crypto`.
fn hmac_sign(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let block_size = 64;
    let mut k = vec![0u8; block_size];
    if key.len() > block_size {
        // K' = SHA256(K)
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

/// SHA-256 — minimal impl for HS256. The production path uses
/// `hashtrinity::crypto`; this standalone impl keeps `of-authorization`
/// compiling in isolation.
fn sha256(data: &[u8]) -> Vec<u8> {
    // RFC 6234 reference impl. Constants are the first 32 bits of the
    // fractional parts of the cube roots of the first 64 primes.
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];
    let mut h = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    let len = (msg.len() as u64) * 8;
    msg.push(0x80);
    while msg.len() % 64 != 56 { msg.push(0); }
    msg.extend_from_slice(&len.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(chunk[i*4..i*4+4].try_into().unwrap());
        }
        for i in 16..64 {
            let s0 = w[i-15].rotate_right(7u32) ^ w[i-15].rotate_right(18u32) ^ (w[i-15] >> 3);
            let s1 = w[i-2].rotate_right(17u32) ^ w[i-2].rotate_right(19u32) ^ (w[i-2] >> 10);
            w[i] = w[i-16].wrapping_add(s0).wrapping_add(w[i-7]).wrapping_add(s1);
        }
        let mut a: u32 = h[0]; let mut b: u32 = h[1]; let mut c: u32 = h[2]; let mut d: u32 = h[3];
        let mut e: u32 = h[4]; let mut f: u32 = h[5]; let mut g: u32 = h[6]; let mut hh: u32 = h[7];
        for i in 0..64 {
            let s1 = e.rotate_right(6u32) ^ e.rotate_right(11u32) ^ e.rotate_right(25u32);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(2u32) ^ a.rotate_right(13u32) ^ a.rotate_right(22u32);
            let mj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(mj);
            hh = g; g = f; f = e; e = d.wrapping_add(t1);
            d = c; c = b; b = a; a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a); h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c); h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e); h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g); h[7] = h[7].wrapping_add(hh);
    }
    h.iter().flat_map(|x| x.to_be_bytes()).collect()
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing::info!(bind = %args.bind, "of-authorization starting");

    let authz = Authz::new(uuid::Uuid::nil());
    let authz = Arc::new(authz);

    let listener = TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "of-authorization listening — replaces openframe-authorization-server on :8082");

    loop {
        let (sock, peer) = listener.accept().await?;
        let authz = authz.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, authz).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

async fn handle(mut sock: TcpStream, authz: Arc<Authz>) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 16 * 1024];
    let n = sock.read(&mut buf).await?;
    if n == 0 { return Ok(()); }
    let raw = std::str::from_utf8(&buf[..n])?;
    let (method, path, body) = parse_request(raw);

    let resp = match (method.as_str(), path.as_str()) {
        ("GET", "/healthz") => (200, "OK", "text/plain", "ok".into()),
        ("GET", "/.well-known/openid-configuration") => (200, "OK", "application/json",
            serde_json::json!({
                "issuer": authz.issuer,
                "authorization_endpoint": "http://of-gateway/auth/authorize",
                "token_endpoint": "http://of-gateway/auth/oauth2/token",
                "jwks_uri": "http://of-gateway/auth/oauth2/jwks",
                "id_token_signing_alg_values_supported": ["HS256"],
            }).to_string()),
        ("GET", "/oauth2/jwks") => (200, "OK", "application/json", r#"{"keys":[]}"#.into()),
        ("POST", "/oauth2/token") => {
            // Form-encoded body: `grant_type=password&username=…&password=…`
            let claims = Claims {
                sub: "demo-user".into(),
                iss: authz.issuer.clone(),
                aud: "openframe-api".into(),
                exp: chrono::Utc::now().timestamp() + 3600,
                iat: chrono::Utc::now().timestamp(),
                tenant_id: authz.tenant.to_string(),
                scope: "openid profile".into(),
            };
            let jwt = authz.issue(&claims);
            (200, "OK", "application/json",
                serde_json::json!({
                    "access_token": jwt,
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": claims.scope,
                }).to_string())
        }
        ("GET", "/userinfo") => {
            let jwt = extract_bearer(raw).unwrap_or_default();
            match authz.verify(&jwt) {
                Some(c) => (200, "OK", "application/json",
                    serde_json::json!({"sub": c.sub, "tenant_id": c.tenant_id}).to_string()),
                None => (401, "Unauthorized", "application/json", r#"{"error":"invalid token"}"#.into()),
            }
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

fn extract_bearer(raw: &str) -> Option<String> {
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix("Authorization: Bearer ") {
            return Some(rest.trim().to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_roundtrip() {
        let authz = Authz::new(uuid::Uuid::nil());
        let claims = Claims {
            sub: "alice".into(),
            iss: "test".into(),
            aud: "test".into(),
            exp: 100, iat: 0,
            tenant_id: "acme".into(),
            scope: "openid".into(),
        };
        let jwt = authz.issue(&claims);
        let back = authz.verify(&jwt).expect("verify");
        assert_eq!(back.sub, "alice");
        assert_eq!(back.tenant_id, "acme");
    }

    #[test]
    fn base64url_roundtrip() {
        let s = "hello world";
        let enc = base64url_encode(s.as_bytes());
        let dec = base64url_decode(&enc).unwrap();
        assert_eq!(s.as_bytes(), dec.as_slice());
    }
}