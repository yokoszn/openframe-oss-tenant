//! The TCP server that speaks the MongoDB wire protocol. Bind to
//! `0.0.0.0:27017` (the MongoDB default port — clients won't need to know
//! the difference) and forward every OP_MSG into the `Query` dispatcher.
//!
//! ```text
//! cargo run --bin sangdb-mongo -- 27017 default
//! mongosh --host 127.0.0.1 --port 27017 --eval 'db.devices.find().toArray()'
//! ```
//!
//! Run in front of `bedrock::sangdb::http::serve` (port 8080) and
//! `bedrock::sangdb::resp::serve` (port 6379) for the full sangdb surface.

use super::query;
use super::bson::BsonValue;
use super::wire::mongodb::{Header, OpMsg, OpQuery, OpReply};
use super::wire::{OP_MSG, OP_QUERY, OP_REPLY};
use super::{MongoError, MongoResult, MongoStore};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Debug)]
pub struct MongoServerConfig {
    pub bind_addr: String,
    pub default_tenant: String,
    pub request_id_seed: Arc<std::sync::atomic::AtomicI32>,
}

impl MongoServerConfig {
    pub fn new(bind_addr: impl Into<String>, default_tenant: impl Into<String>) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            default_tenant: default_tenant.into(),
            request_id_seed: Arc::new(std::sync::atomic::AtomicI32::new(1)),
        }
    }

    fn next_request_id(&self) -> i32 {
        self.request_id_seed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

pub struct MongoServer {
    config: MongoServerConfig,
    store: MongoStore,
}

impl MongoServer {
    pub fn new(config: MongoServerConfig, store: MongoStore) -> Self {
        Self { config, store }
    }

    pub async fn serve(self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        tracing::info!(addr = %self.config.bind_addr, "sangdb MongoDB wire-protocol listening");
        loop {
            let (sock, peer) = listener.accept().await?;
            tracing::debug!(%peer, "mongo client connected");
            let cfg = self.config.clone();
            let store = self.store.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(sock, store, cfg).await {
                    tracing::warn!(%peer, error = %e, "connection error");
                }
            });
        }
    }
}

async fn handle_connection(
    mut sock: TcpStream,
    store: MongoStore,
    cfg: MongoServerConfig,
) -> MongoResult<()> {
    // One TCP connection, many messages — keep reading until EOF.
    loop {
        let mut header_buf = [0u8; super::wire::HEADER_LEN];
        match sock.read_exact(&mut header_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(MongoError::Io(e)),
        }
        let header = Header::decode(&header_buf)?;
        let body_len = (header.message_length as usize).saturating_sub(super::wire::HEADER_LEN);
        let mut body = vec![0u8; body_len];
        sock.read_exact(&mut body).await?;
        // The reply header's `responseTo` field MUST equal the request's
        // `requestID` (the wire-protocol correlation field), NOT the
        // request's `responseTo` (which is 0 for fresh requests).
        let request_id = header.request_id;
        let response_to = header.request_id;

        let reply = dispatch(&cfg, &store, header.op_code, &body).await;

        let reply_bytes = match reply {
            Ok(QueryOk::OpMsg(reply_doc)) => {
                let op = OpMsg {
                    flags: 0,
                    sections: vec![super::wire::mongodb::OpMsgSection::Body(vec![reply_doc])],
                };
                let body = op.encode();
                let mut out = Vec::with_capacity(super::wire::HEADER_LEN + body.len());
                let total = (super::wire::HEADER_LEN + body.len()) as i32;
                Header {
                    message_length: total,
                    request_id: cfg.next_request_id(),
                    response_to,
                    op_code: OP_MSG,
                }.encode(&mut out);
                out.extend_from_slice(&body);
                out
            }
            Ok(QueryOk::OpReply(reply_doc)) => {
                let rep = OpReply {
                    response_flags: 0,
                    cursor_id: 0,
                    starting_from: 0,
                    docs: vec![reply_doc],
                };
                let body = rep.encode();
                let mut out = Vec::with_capacity(super::wire::HEADER_LEN + body.len());
                let total = (super::wire::HEADER_LEN + body.len()) as i32;
                Header {
                    message_length: total,
                    request_id: cfg.next_request_id(),
                    response_to,
                    op_code: OP_REPLY,
                }.encode(&mut out);
                out.extend_from_slice(&body);
                out
            }
            Ok(QueryOk::Empty) => {
                let mut out = Vec::new();
                Header {
                    message_length: super::wire::HEADER_LEN as i32,
                    request_id: cfg.next_request_id(),
                    response_to,
                    op_code: OP_MSG,
                }.encode(&mut out);
                out
            }
            Err(e) => {
                let reply = format_mongo_error(&e);
                let op = OpMsg {
                    flags: 0,
                    sections: vec![super::wire::mongodb::OpMsgSection::Body(vec![reply])],
                };
                let body = op.encode();
                let mut out = Vec::with_capacity(super::wire::HEADER_LEN + body.len());
                let total = (super::wire::HEADER_LEN + body.len()) as i32;
                Header {
                    message_length: total,
                    request_id: cfg.next_request_id(),
                    response_to,
                    op_code: OP_MSG,
                }.encode(&mut out);
                out.extend_from_slice(&body);
                out
            }
        };

        let _ = request_id;
        sock.write_all(&reply_bytes).await?;
    }
}

enum QueryOk {
    OpMsg(super::bson::BsonDoc),
    OpReply(super::bson::BsonDoc),
    Empty,
}

async fn dispatch(
    cfg: &MongoServerConfig,
    store: &MongoStore,
    op_code: u32,
    body: &[u8],
) -> MongoResult<QueryOk> {
    match op_code {
        OP_MSG => {
            let op = OpMsg::decode(body)?;
            let first_doc = section0(&op)?;
            // Pull the documents array out of any kind-1 section whose
            // identifier is "documents" — Spring Data MongoDB uses the
            // document-sequence form for insert bulkWrite payloads.
            // Likewise, the `deletes` array may live in a kind-1 section
            // for delete commands (identifier = "deletes_").
            let mut extra_docs: Vec<super::bson::BsonDoc> = Vec::new();
            let mut extra_deletes: Vec<super::bson::BsonDoc> = Vec::new();
            let mut extra_updates: Vec<super::bson::BsonDoc> = Vec::new();
            for section in &op.sections {
                if let super::wire::mongodb::OpMsgSection::Type1 { identifier, docs, .. } = section {
                    match identifier.as_str() {
                        "documents" => extra_docs.extend(docs.iter().cloned()),
                        "deletes_" | "deletes" => extra_deletes.extend(docs.iter().cloned()),
                        "updates_" | "updates" => extra_updates.extend(docs.iter().cloned()),
                        _ => {}
                    }
                }
            }
            // For delete with kind-1 deletes array, fold it back into a
            // synthetic body doc so from_command sees the `deletes` field.
            // For update with kind-1 updates array, fold the first spec's
            // `q` and `u` into the body so from_command sees the filter+update.
            let first_doc_for_cmd = if !extra_deletes.is_empty()
                && !first_doc.0.contains_key("deletes")
            {
                let mut augmented = first_doc.clone();
                augmented.0.insert(
                    "deletes".to_string(),
                    super::bson::BsonValue::Array(
                        extra_deletes.iter().cloned().map(super::bson::BsonValue::Document).collect(),
                    ),
                );
                augmented
            } else if !extra_updates.is_empty()
                && !first_doc.0.contains_key("filter")
            {
                // Each `updates` spec is `{q: <filter>, u: <$set/$unset/...>}`.
                // We synthesise a synthetic body that from_command reads as
                // a single update (Spring Data uses updateOne for save()).
                let mut augmented = first_doc.clone();
                if let Some(spec) = extra_updates.first() {
                    if let Some(q) = spec.0.get("q") {
                        augmented.0.insert("filter".to_string(), q.clone());
                    }
                    if let Some(u) = spec.0.get("u") {
                        // Keep the command's `update: <collection>` field intact.
                        augmented.0.insert("updateDocument".to_string(), u.clone());
                    }
                    if let Some(BsonValue::Bool(true)) = spec.0.get("multi") {
                        augmented.0.insert("multi".to_string(), BsonValue::Bool(true));
                    }
                }
                augmented
            } else {
                first_doc.clone()
            };
            let tenant = first_doc
                .get("$db")
                .and_then(|v| match v {
                    super::bson::BsonValue::String(s) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or(&cfg.default_tenant);
            let q = query::Query::from_command(&first_doc_for_cmd)?;
            // Override documents for `insert` from the kind-1 payload
            // (it lives in the body section key too, but we extracted it here).
            let q = match q {
                query::Query::Insert { collection, documents: _, ordered } if !extra_docs.is_empty() => {
                    query::Query::Insert { collection, documents: extra_docs, ordered }
                }
                other => other,
            };
            let result = query::execute(store, q, tenant).await?;
            Ok(QueryOk::OpMsg(result.to_reply_doc()))
        }
        OP_QUERY => {
            // Legacy OP_QUERY. Modern drivers only use it for the initial
            // `isMaster` / `hello` handshake. Treat it like an OP_MSG hello
            // and respond with OP_REPLY.
            let q = OpQuery::decode(body)?;
            // For the handshake, the query doc IS the command (`{isMaster:1,
            // helloOk:true, client:{...}}`). We map it through the same
            // `from_command` path so `hello`/`isMaster`/`ping`/anything-else
            // all behave consistently.
            let query_doc = q.query;
            let parsed = query::Query::from_command(&query_doc)?;
            let result = query::execute(store, parsed, &cfg.default_tenant).await?;
            Ok(QueryOk::OpReply(result.to_reply_doc()))
        }
        _ => Err(MongoError::UnknownOpcode(op_code)),
    }
}

fn section0<'a>(op: &'a OpMsg) -> MongoResult<&'a super::bson::BsonDoc> {
    use super::wire::mongodb::OpMsgSection;
    match op.sections.first().ok_or_else(|| MongoError::Internal("no section".into()))? {
        OpMsgSection::Body(docs) => docs.first().ok_or_else(|| MongoError::Internal("empty body".into())),
        OpMsgSection::Type1 { .. } => Err(MongoError::Internal("type1 section not supported".into())),
    }
}

fn format_mongo_error(e: &MongoError) -> super::bson::BsonDoc {
    use super::bson::{BsonDoc, BsonValue};
    let mut d = BsonDoc::new();
    d.insert("ok", BsonValue::Double(0.0));
    let code = match e {
        MongoError::UnsupportedCommand(_) => 115,
        MongoError::UnknownOpcode(_) => 11,
        MongoError::CollectionNotFound(_) => 26,
        MongoError::TenantScopeViolation => 13,
        MongoError::DuplicateKey(_) => 11000,
        _ => 1,
    };
    d.insert("code", BsonValue::Int32(code));
    d.insert("errmsg", BsonValue::String(e.to_string()));
    d
}
