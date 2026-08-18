//! MongoDB wire-protocol opcodes (the `OP_*` numbers from
//! <https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/>).
//! We implement OP_MSG (the only opcode that matters since MongoDB 3.6 —
//! OP_QUERY, OP_INSERT, etc. were removed in 5.1).

/// OP_MSG — the modern single-opcode wire format. Every command rides it.
pub const OP_MSG: u32 = 2013;

/// OP_REPLY — server-to-client response (legacy opcode; OP_MSG uses its own
/// kind-0 reply so this is unused for us).
pub const OP_REPLY: u32 = 1;

/// Message header size (16 bytes): messageLength(4) + requestID(4) +
/// responseTo(4) + opCode(4).
pub const HEADER_LEN: usize = 16;

/// OP_MSG kind-0 body — sequence of BSON documents (each prefixed by a
/// 4-byte little-endian length). The first document carries the command;
/// optional subsequent docs are the payload sequence.
pub const OP_MSG_SECTION_KIND_0: u8 = 0x00;
/// OP_MSG kind-1 body — single document with a 32-bit type bit set in the
/// flags. Used for `bulkWrite` and similar; we don't need it yet.
pub const OP_MSG_SECTION_KIND_1: u8 = 0x01;

pub mod flags {
    pub const CHECKSUM_PRESENT: u32 = 1 << 0;
    pub const MORE_TO_COME: u32 = 1 << 1;
    pub const EXHAUST_ALLOWED: u32 = 1 << 16;
}

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

    pub fn decode(buf: &[u8]) -> Result<Self, super::MongoError> {
        if buf.len() < HEADER_LEN {
            return Err(super::MongoError::Internal("header too short".into()));
        }
        let message_length = i32::from_le_bytes(buf[0..4].try_into().unwrap());
        let request_id = i32::from_le_bytes(buf[4..8].try_into().unwrap());
        let response_to = i32::from_le_bytes(buf[8..12].try_into().unwrap());
        let op_code = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        Ok(Self { message_length, request_id, response_to, op_code })
    }
}

/// An OP_MSG message body — flags (4 bytes) + sections.
#[derive(Debug, Clone)]
pub struct OpMsg {
    pub flags: u32,
    pub sections: Vec<OpMsgSection>,
}

#[derive(Debug, Clone)]
pub enum OpMsgSection {
    /// kind 0 — sequence of BSON docs; the first one is the command.
    Body(Vec<super::bson::BsonDoc>),
    /// kind 1 — payload (typed via the 32-bit type bits). Not implemented.
    Type1(Vec<u8>),
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
                        let encoded = super::bson::encode(d);
                        body.extend_from_slice(&(encoded.len() as i32).to_le_bytes());
                        body.extend_from_slice(&encoded);
                    }
                }
                OpMsgSection::Type1(bytes) => {
                    body.push(OP_MSG_SECTION_KIND_1);
                    body.extend_from_slice(bytes);
                }
            }
        }
        body
    }

    pub fn decode(buf: &[u8]) -> Result<Self, super::MongoError> {
        if buf.len() < 4 {
            return Err(super::MongoError::Internal("op_msg flags missing".into()));
        }
        let flags = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let mut pos = 4;
        let mut sections = Vec::new();
        while pos < buf.len() {
            let kind = buf[pos];
            pos += 1;
            match kind {
                OP_MSG_SECTION_KIND_0 => {
                    let mut docs = Vec::new();
                    while pos < buf.len() {
                        if pos + 4 > buf.len() { break; }
                        let len = i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
                        if len < 5 || pos + 4 + len > buf.len() { break; }
                        let doc = super::bson::decode(&buf[pos + 4..pos + 4 + len])
                            .map_err(|e| super::MongoError::Codec(e.to_string()))?;
                        docs.push(doc);
                        pos += 4 + len;
                    }
                    sections.push(OpMsgSection::Body(docs));
                }
                OP_MSG_SECTION_KIND_1 => {
                    let bytes = buf[pos..].to_vec();
                    sections.push(OpMsgSection::Type1(bytes));
                    pos = buf.len();
                }
                other => return Err(super::MongoError::Internal(format!("unknown section kind {other}"))),
            }
        }
        Ok(Self { flags, sections })
    }
}