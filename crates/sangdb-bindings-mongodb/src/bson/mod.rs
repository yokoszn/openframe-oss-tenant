//! Minimal BSON codec — MongoDB's binary JSON.
//!
//! # Architecture note (temporary location)
//!
//! The canonical home for this codec is `hashtrinity::codec::bson`, as a wire surface
//! over the universal `SyntaxTerm` (the same one-term-many-surfaces pattern that
//! `hashtrinity::codec::msgpack` already implements: `BsonValue` types would be
//! expressible as either the minimal `SyntaxTerm` (Bool/Num/Str) for simple cases or
//! as the richer BSON-specific surface for ObjectId/Binary/DateTime/Regex/etc.).
//!
//! Per the user's instruction (2026-08-20), this landing in
//! `sangdb-bindings-mongodb` is **temporary** — the fold workspace gate is currently
//! locked (the syndesi auth tool reports the harness as 8h23m old against a hard
//! 8h cap, even after regeneration). Once the gate is unstuck, port:
//!
//!   1. Move `bson/` and `wire/mongodb.rs` into
//!      `hashtrinity/src/codec/bson/{mod,value,codec}.rs` and
//!      `hashtrinity/src/codec/wire/mongodb.rs`.
//!   2. Re-export from `sangdb-bindings-mongodb` via
//!      `pub use hashtrinity::codec::{bson, wire::mongodb};`.
//!   3. Update MIGRATION.md with the diff.
//!
//! The interface here is shaped for the port — `pub fn decode(buf: &[u8]) -> Result<…>`,
//! zero-copy via `&[u8]` slices (no allocation until materialised), `to_vec()` for
//! encode — which is the same shape as `hashtrinity::codec::msgpack`.
//!
//! # What is implemented
//!
//! The subset of BSON v1.1 the MongoDB Java driver 5.0.1 actually emits when talking
//! to a Spring Data MongoDB workload:
//!
//! | tag | type          | status   |
//! |-----|---------------|----------|
//! | 0x01| Double (f64)  | ✓        |
//! | 0x02| String        | ✓        |
//! | 0x03| Document      | ✓        |
//! | 0x04| Array         | ✓        |
//! | 0x05| Binary (gen)  | ✓ (subtype 0x00/0x04) |
//! | 0x07| ObjectId      | ✓        |
//! | 0x08| Bool          | ✓        |
//! | 0x09| DateTime      | ✓ (i64 unix millis) |
//! | 0x0a| Null          | ✓        |
//! | 0x0b| Regex         | ✓        |
//! | 0x10| Int32         | ✓        |
//! | 0x12| Int64         | ✓        |
//!
//! Not implemented (return `BsonError::UnknownType` if encountered; the wire-protocol
//! layer can choose to pass through or reject):
//!
//! | tag | type          | notes |
//! |-----|---------------|-------|
//! | 0x06| Undefined     | deprecated; not seen in modern MongoDB |
//! | 0x0c| DBPointer     | deprecated |
//! | 0x0d| JavaScript    | rare; not seen in the test corpus |
//! | 0x0e| Symbol        | deprecated |
//! | 0x0f| JS w/ scope   | deprecated |
//! | 0x11| Timestamp     | internal MongoDB use; oplog `ts` field |
//! | 0x13| Decimal128    | financial precision; `NumberDecimal` |
//!
//! Adding more types is mechanical: extend the `BsonValue` enum, the encode arm in
//! `codec::encode_element`, the decode arm in `codec::decode_value`. The wire-protocol
//! layer never touches the type layout directly — it operates on `BsonDoc` /
//! `BsonValue` via `as_*` accessors and `get`/`insert`.

pub mod codec;
pub mod value;

pub use codec::{decode, encode};
pub use value::{BsonDoc, BsonError, BsonValue};
