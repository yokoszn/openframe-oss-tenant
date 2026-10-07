//! MongoDB wire-protocol opcodes and constants — the framing layer that sits on top of
//! `crate::bson`.
//!
//! Per the spec at <https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/>,
//! every MongoDB message has a 16-byte header followed by an opcode-specific body.
//!
//! - `OP_MSG` (2013) is the only opcode drivers emit for commands since 3.6 — every command
//!   the shim processes rides this opcode.
//! - `OP_QUERY` (2004) is still emitted by modern drivers for the initial `isMaster`/`hello`
//!   connection handshake (the only MongoDB 5.1+ exception).
//! - `OP_REPLY` (1) is the reply opcode for OP_QUERY handshake.
//!
//! # Architecture note (temporary location)
//!
//! The canonical home for this framing layer is `hashtrinity::codec::wire::mongodb`,
//! as the MongoDB wire surface alongside `hashtrinity::codec::bson` (the BSON surface
//! itself) and `hashtrinity::codec::msgpack` (the message-pack surface). The two layers
//! would land as:
//!
//!   hashtrinity/src/codec/bson/{mod,value,codec}.rs        (currently openframe-sangdb's bson/)
//!   hashtrinity/src/codec/wire/mongodb.rs                  (currently openframe-sangdb's wire/)
//!
//! Per the user's instruction (2026-08-20), this landing in
//! `sangdb-bindings-mongodb` is **temporary** — the fold workspace gate is currently
//! locked. Port as soon as the gate is unstuck. See `bson/mod.rs` for the same note.

// OP_MSG — the modern single-opcode wire format. Every command rides it.
pub const OP_MSG: u32 = 2013;

// OP_QUERY — legacy request opcode. Still emitted by modern drivers for the initial
// `isMaster`/`hello` handshake (the only MongoDB 5.1+ exception).
pub const OP_QUERY: u32 = 2004;

// OP_REPLY — server-to-client reply to OP_QUERY.
pub const OP_REPLY: u32 = 1;

// Message header size (16 bytes): messageLength(4) + requestID(4) + responseTo(4) + opCode(4).
pub const HEADER_LEN: usize = 16;

// OP_MSG kind-0 body — a single BSON document (self-delimiting via its own 4-byte length
// prefix). Per the wire-protocol spec, no additional length wrapper is added between the kind
// byte and the BSON doc.
pub const OP_MSG_SECTION_KIND_0: u8 = 0x00;

// OP_MSG kind-1 document sequence — payload bulkWrite / type-1 sections.
pub const OP_MSG_SECTION_KIND_1: u8 = 0x01;

// OP_MSG kind-2 (per spec: "used for internal purposes"). In practice, modern MongoDB
// drivers (notably 5.0+) use kind 2 for the document sequence in update/delete commands,
// where kind 1 was used by older drivers. Treated identically to kind 1 in our decoder.
pub const OP_MSG_SECTION_KIND_2: u8 = 0x02;

pub mod flags {
    pub const CHECKSUM_PRESENT: u32 = 1 << 0;
    pub const MORE_TO_COME: u32 = 1 << 1;
    pub const EXHAUST_ALLOWED: u32 = 1 << 16;
}

pub mod mongodb;
pub use mongodb::*;
