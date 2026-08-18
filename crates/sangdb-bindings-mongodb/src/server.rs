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

use super::opcodes::{Header, OpMsg, OP_MSG};
use super::query;
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
        let mut header_buf = [0u8; super::opcodes::HEADER_LEN];
        match sock.read_exact(&mut header_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(MongoError::Io(e)),
        }
        let header = Header::decode(&header_buf)?;
        let body_len = (header.message_length as usize).saturating_sub(super::opcodes::HEADER_LEN);
        let mut body = vec![0u8; body_len];
        sock.read_exact(&mut body).await?;
        let request_id = header.request_id;
        let response_to = header.response_to;

        let reply = dispatch(&cfg, &store, header.op_code, &body).await;

        let reply_bytes = match reply {
            Ok(QueryOk::OpMsg(reply_doc)) => {
                let op = OpMsg {
                    flags: 0,
                    sections: vec![super::opcodes::OpMsgSection::Body(vec![reply_doc])],
                };
                let body = op.encode();
                let mut out = Vec::with_capacity(super::opcodes::HEADER_LEN + body.len());
                let total = (super::opcodes::HEADER_LEN + body.len()) as i32;
                Header {
                    message_length: total,
                    request_id: cfg.next_request_id(),
                    response_to,
                    op_code: OP_MSG,
                }.encode(&mut out);
                out.extend_from_slice(&body);
                out
            }
            Ok(QueryOk::Empty) => {
                let mut out = Vec::new();
                Header {
                    message_length: super::opcodes::HEADER_LEN as i32,
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
                    sections: vec![super::opcodes::OpMsgSection::Body(vec![reply])],
                };
                let body = op.encode();
                let mut out = Vec::with_capacity(super::opcodes::HEADER_LEN + body.len());
                let total = (super::opcodes::HEADER_LEN + body.len()) as i32;
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
            let tenant = first_doc
                .get("$db")
                .and_then(|v| match v {
                    super::bson::Bson::String(s) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or(&cfg.default_tenant);
            let q = query::Query::from_command(first_doc)?;
            let result = query::execute(store, q, tenant).await?;
            Ok(QueryOk::OpMsg(result.to_reply_doc()))
        }
        _ => Err(MongoError::UnknownOpcode(op_code)),
    }
}

fn section0<'a>(op: &'a OpMsg) -> MongoResult<&'a super::bson::BsonDoc> {
    use super::opcodes::OpMsgSection;
    match op.sections.first().ok_or_else(|| MongoError::Internal("no section".into()))? {
        OpMsgSection::Body(docs) => docs.first().ok_or_else(|| MongoError::Internal("empty body".into())),
        OpMsgSection::Type1(_) => Err(MongoError::Internal("type1 section not supported".into())),
    }
}

fn format_mongo_error(e: &MongoError) -> super::bson::BsonDoc {
    use super::bson::{Bson, BsonDoc};
    let mut d = BsonDoc::new();
    d.insert("ok", Bson::Double(0.0));
    let code = match e {
        MongoError::UnsupportedCommand(_) => 115,
        MongoError::UnknownOpcode(_) => 11,
        MongoError::CollectionNotFound(_) => 26,
        MongoError::TenantScopeViolation => 13,
        _ => 1,
    };
    d.insert("code", Bson::Int32(code));
    d.insert("errmsg", Bson::String(e.to_string()));
    d
}