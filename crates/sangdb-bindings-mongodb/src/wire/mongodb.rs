//! MongoDB wire-protocol message decoders — `Header`, `OpMsg`, `OpQuery`, `OpReply`.
//!
//! These are the opcode-specific body parsers. The 16-byte `Header` is shared by every opcode;
//! `OpMsg` is the modern command body (flags + sections); `OpQuery` is the legacy command / handshake
//! body; `OpReply` is the legacy reply body. Replies to OP_MSG commands use the OP_MSG reply shape
//! directly (a kind-0 section wrapping a single BSON doc), not `OpReply` — see `OpMsg::encode` for
//! that path.
//!
//! # Hashtrinity surface parity
//!
//! Mirrors `hashtrinity::codec::msgpack`'s shape:
//!
//! | hashtrinity::codec::msgpack       | here                                 |
//! |-----------------------------------|--------------------------------------|
//! | `pub fn decode(bytes) -> Result<…>` | `Header::decode`, `OpMsg::decode`, etc. |
//! | zero-copy `&[u8]` slicing         | same — `&buf[pos..pos+n]` everywhere   |
//! | `pub fn encode(term) -> Vec<u8>` | `Header::encode`, `OpReply::encode`     |
//!
//! When ported upstream to `hashtrinity::codec::wire::mongodb`, the type signatures stay
//! the same; the parent module (`hashtrinity::codec::wire`) would re-export this submodule.

use super::{HEADER_LEN, OP_MSG_SECTION_KIND_0, OP_MSG_SECTION_KIND_1};
use crate::bson::{BsonDoc, BsonValue};
use crate::MongoError;

// ── Header ─────────────────────────────────────────────────────────────────

/// 16-byte message header shared by every opcode.
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub message_length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: u32,
}

impl Header {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.message_length.to_le_bytes());
        buf.extend_from_slice(&self.request_id.to_le_bytes());
        buf.extend_from_slice(&self.response_to.to_le_bytes());
        buf.extend_from_slice(&self.op_code.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self, MongoError> {
        if buf.len() < HEADER_LEN {
            return Err(MongoError::Internal("header too short".into()));
        }
        let message_length = i32::from_le_bytes(buf[0..4].try_into().unwrap());
        let request_id = i32::from_le_bytes(buf[4..8].try_into().unwrap());
        let response_to = i32::from_le_bytes(buf[8..12].try_into().unwrap());
        let op_code = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        Ok(Self { message_length, request_id, response_to, op_code })
    }
}

// ── OP_MSG ─────────────────────────────────────────────────────────────────

/// An OP_MSG message body — flags (4 bytes) + sections.
#[derive(Debug, Clone)]
pub struct OpMsg {
    pub flags: u32,
    pub sections: Vec<OpMsgSection>,
}

#[derive(Debug, Clone)]
pub enum OpMsgSection {
    /// kind 0 — sequence of BSON docs; the first one is the command.
    Body(Vec<BsonDoc>),
    /// kind 1 — document sequence with identifier (e.g. `{...insert..., documents: <seq>}`).
    Type1 {
        size: usize,
        identifier: String,
        docs: Vec<BsonDoc>,
    },
}

impl OpMsg {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&self.flags.to_le_bytes());
        for section in &self.sections {
            match section {
                OpMsgSection::Body(docs) => {
                    body.push(OP_MSG_SECTION_KIND_0);
                    for d in docs {
                        let encoded = crate::bson::encode(d);
                        body.extend_from_slice(&encoded);
                    }
                }
                OpMsgSection::Type1 { size: _, identifier, docs } => {
                    body.push(OP_MSG_SECTION_KIND_1);
                    let mut payload: Vec<u8> = Vec::new();
                    payload.extend_from_slice(identifier.as_bytes());
                    payload.push(0x00);
                    for d in docs {
                        payload.extend_from_slice(&crate::bson::encode(d));
                    }
                    let size = (4 + payload.len()) as i32;
                    body.extend_from_slice(&size.to_le_bytes());
                    body.extend_from_slice(&payload);
                }
            }
        }
        body
    }

    pub fn decode(buf: &[u8]) -> Result<Self, MongoError> {
        if buf.len() < 4 {
            return Err(MongoError::Internal("op_msg flags missing".into()));
        }
        let flags = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let mut pos = 4;
        let mut sections = Vec::new();
        while pos < buf.len() {
            let kind = buf[pos];
            tracing::trace!(pos, kind = format!("0x{:02x}", kind), "OpMsg section kind byte");
            pos += 1;
            match kind {
                OP_MSG_SECTION_KIND_0 => {
                    if pos + 4 > buf.len() {
                        return Err(MongoError::Internal("kind-0 doc len missing".into()));
                    }
                    let len = i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
                    if len < 5 || pos + len > buf.len() {
                        return Err(MongoError::Internal(format!("kind-0 bad doc len {len} (buf={})", buf.len())));
                    }
                    let doc = crate::bson::decode(&buf[pos..pos + len])
                        .map_err(|e| MongoError::Codec(e.to_string()))?;
                    sections.push(OpMsgSection::Body(vec![doc]));
                    pos += len;
                }
                // MongoDB drivers use kind 0x02 for the document sequence in update
                // and delete commands (per the wire-protocol spec, kind 2 is "used for internal
                // purposes" but in practice it's the modern form of kind-1's payload here).
                // We treat it identically to kind 1: parse as a document sequence with
                // `[int32 size][cstring identifier][BSON docs...]`.
                0x02 | OP_MSG_SECTION_KIND_1 => {
                    if pos + 4 > buf.len() {
                        return Err(MongoError::Internal("kind-1 size missing".into()));
                    }
                    let section_size = i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
                    if section_size < 5 || pos + section_size > buf.len() {
                        return Err(MongoError::Internal(format!("kind-1 bad section_size {section_size} (buf={})", buf.len())));
                    }
                    let section_end = pos + section_size;
                    pos += 4;
                    let mut id_end = pos;
                    while id_end < buf.len() && buf[id_end] != 0 {
                        id_end += 1;
                    }
                    if id_end >= buf.len() {
                        return Err(MongoError::Internal("kind-1 identifier no NUL".into()));
                    }
                    let identifier = std::str::from_utf8(&buf[pos..id_end])
                        .map_err(|e| MongoError::Codec(e.to_string()))?
                        .to_string();
                    pos = id_end + 1;
                    let mut docs = Vec::new();
                    while pos < section_end {
                        if pos + 4 > section_end { break; }
                        let dlen = i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
                        if dlen < 5 || pos + dlen > section_end { break; }
                        let doc = crate::bson::decode(&buf[pos..pos + dlen])
                            .map_err(|e| MongoError::Codec(e.to_string()))?;
                        docs.push(doc);
                        pos += dlen;
                    }
                    sections.push(OpMsgSection::Type1 { size: section_size, identifier, docs });
                }
                other => return Err(MongoError::Internal(format!("unknown section kind {other}"))),
            }
        }
        Ok(Self { flags, sections })
    }
}

// ── OP_QUERY (handshake) ────────────────────────────────────────────────────

/// Decoded OP_QUERY request body. We only need the query doc for the handshake
/// (`{isMaster:1, helloOk:true, client:{...}}` on `admin.$cmd`).
#[derive(Debug)]
pub struct OpQuery {
    pub flags: i32,
    pub full_collection_name: String,
    pub number_to_skip: i32,
    pub number_to_return: i32,
    pub query: BsonDoc,
    pub return_fields_selector: Option<BsonDoc>,
}

impl OpQuery {
    pub fn decode(buf: &[u8]) -> Result<Self, MongoError> {
        if buf.len() < 12 {
            return Err(MongoError::Internal("op_query body too short".into()));
        }
        let flags = i32::from_le_bytes(buf[0..4].try_into().unwrap());
        let mut end = 4;
        while end < buf.len() && buf[end] != 0 {
            end += 1;
        }
        if end >= buf.len() {
            return Err(MongoError::Internal("op_query coll no NUL".into()));
        }
        let full_collection_name = std::str::from_utf8(&buf[4..end])
            .map_err(|e| MongoError::Codec(e.to_string()))?
            .to_string();
        let mut pos = end + 1;
        if pos + 8 > buf.len() {
            return Err(MongoError::Internal("op_query header fields short".into()));
        }
        let number_to_skip = i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());
        let number_to_return = i32::from_le_bytes(buf[pos + 4..pos + 8].try_into().unwrap());
        pos += 8;
        if pos + 4 > buf.len() {
            return Err(MongoError::Internal("op_query no query doc".into()));
        }
        let qlen = i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        if qlen < 5 || pos + qlen > buf.len() {
            return Err(MongoError::Internal("op_query bad query doc len".into()));
        }
        let query = crate::bson::decode(&buf[pos..pos + qlen])
            .map_err(|e| MongoError::Codec(e.to_string()))?;
        pos += qlen;
        let return_fields_selector = if pos + 4 <= buf.len() {
            let rlen = i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            if rlen >= 5 && pos + rlen <= buf.len() {
                let r = crate::bson::decode(&buf[pos..pos + rlen])
                    .map_err(|e| MongoError::Codec(e.to_string()))?;
                Some(r)
            } else {
                None
            }
        } else {
            None
        };
        Ok(Self {
            flags,
            full_collection_name,
            number_to_skip,
            number_to_return,
            query,
            return_fields_selector,
        })
    }
}

// ── OP_REPLY ───────────────────────────────────────────────────────────────

/// Encoded OP_REPLY body: responseFlags(4) + cursorId(8) + startingFrom(4) +
/// numberReturned(4) + BSON doc(s).
#[derive(Debug)]
pub struct OpReply {
    pub response_flags: i32,
    pub cursor_id: i64,
    pub starting_from: i32,
    pub docs: Vec<BsonDoc>,
}

impl OpReply {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&self.response_flags.to_le_bytes());
        body.extend_from_slice(&self.cursor_id.to_le_bytes());
        body.extend_from_slice(&self.starting_from.to_le_bytes());
        body.extend_from_slice(&(self.docs.len() as i32).to_le_bytes());
        for d in &self.docs {
            let encoded = crate::bson::encode(d);
            body.extend_from_slice(&encoded);
        }
        body
    }

    pub fn decode(buf: &[u8]) -> Result<Self, MongoError> {
        if buf.len() < 20 {
            return Err(MongoError::Internal("op_reply body too short".into()));
        }
        let response_flags = i32::from_le_bytes(buf[0..4].try_into().unwrap());
        let cursor_id = i64::from_le_bytes(buf[4..12].try_into().unwrap());
        let starting_from = i32::from_le_bytes(buf[12..16].try_into().unwrap());
        let number_returned = i32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;
        let mut pos = 20;
        let mut docs = Vec::with_capacity(number_returned);
        for _ in 0..number_returned {
            if pos + 4 > buf.len() {
                return Err(MongoError::Internal("op_reply truncated".into()));
            }
            let dlen = i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            if dlen < 5 || pos + dlen > buf.len() {
                return Err(MongoError::Internal("op_reply bad doc len".into()));
            }
            let doc = crate::bson::decode(&buf[pos..pos + dlen])
                .map_err(|e| MongoError::Codec(e.to_string()))?;
            docs.push(doc);
            pos += dlen;
        }
        Ok(Self { response_flags, cursor_id, starting_from, docs })
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let h = Header { message_length: 100, request_id: 7, response_to: 0, op_code: super::super::OP_MSG };
        let mut buf = Vec::new();
        h.encode(&mut buf);
        assert_eq!(buf.len(), HEADER_LEN);
        let back = Header::decode(&buf).unwrap();
        assert_eq!(h.message_length, back.message_length);
        assert_eq!(h.request_id, back.request_id);
        assert_eq!(h.response_to, back.response_to);
        assert_eq!(h.op_code, back.op_code);
    }

    #[test]
    fn op_reply_roundtrip() {
        let mut doc = BsonDoc::new();
        doc.insert("ok", BsonValue::Double(1.0));
        let r = OpReply { response_flags: 0, cursor_id: 0, starting_from: 0, docs: vec![doc] };
        let bytes = r.encode();
        let back = OpReply::decode(&bytes).unwrap();
        assert_eq!(back.docs.len(), 1);
        assert_eq!(back.docs[0].get("ok").unwrap().as_str(), None); // Double, not String
    }
}
