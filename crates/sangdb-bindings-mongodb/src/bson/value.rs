//! The BSON value type — the wire surface enum.
//!
//! Modelled after MongoDB's own `BsonValue` (the canonical Java / C# / Go enum) — `BsonValue`
//! here is the Rust analog. The structural subset (Double/String/Bool/Null/Int32/Int64/Array/Document)
//! is the universal floor; the rest (ObjectId/Binary/DateTime/Regex) are the surface richness.
//!
//! # Hashtrinity-style mapping (when ported upstream)
//!
//! When this moves to `hashtrinity::codec::bson`, `BsonValue` will live alongside `Atom`
//! (the hashtrinity universal's scalar arm) and `SyntaxTerm`. The mapping:
//!
//! | `BsonValue`         | `SyntaxTerm` / `Atom` representation         |
//! |---------------------|----------------------------------------------|
//! | `Double`            | `B(Atom::Num(f.to_string()))`              |
//! | `String`            | `B(Atom::Str(s))`                          |
//! | `Bool`              | `B(Atom::Bool(b))`                         |
//! | `Null`              | `Nb`                                       |
//! | `Int32` / `Int64`   | `B(Atom::Num(n.to_string()))`              |
//! | `Array`             | `Fourth(items)`                            |
//! | `Document`          | `Bu(fields)`                               |
//! | `ObjectId`/`Binary`/`DateTime`/`Regex` | `Unknown` (msgpack's `ext` mapping) for |
//! |                     | round-trip loss — the surface adds its own  |
//! |                     | rich type when constructing outgoing       |
//! |                     | responses. Lossy on decode for these four   |
//! |                     | is acceptable for the shim's purpose; for   |
//! |                     | hashtrinity, `BsonValue` IS the surface so  |
//! |                     | there is no loss.                            |
//!
//! The local `BsonValue` here is the **surface** form (no loss) — kept because the
//! shim's reply path needs to construct rich responses (`{ok: 1, ...}`) and the
//! `SyntaxTerm`-lossy model would force it through `Atom::Num("1.0")` strings.
//!
//! # Why `BTreeMap` for `BsonDoc`
//!
//! MongoDB's wire format requires **canonical key ordering** for hashing and
//! reproducible bytes — `serde_json`, `bson` crate, and the official MongoDB drivers
//! all use `LinkedHashMap` or sort on output. We use `BTreeMap` because (a) it's
//! deterministic without a `Hash` impl on `BsonValue`, (b) it matches the project's
//! preference for `Table`-shaped stores (the hashtrinity corpus uses `BTreeMap` for
//! the same reason), and (c) it costs nothing at our document sizes (the
//! `openframe-data-mongo-sync` IT fixtures top out at a few hundred fields).

use std::collections::BTreeMap;
use thiserror::Error;

/// A BSON value — the wire surface enum (analogous to MongoDB's `BsonValue`).
#[derive(Debug, Clone, PartialEq)]
pub enum BsonValue {
    Double(f64),
    String(String),
    Bool(bool),
    Null,
    Int32(i32),
    Int64(i64),
    /// BSON datetime — i64 unix milliseconds (UTC).
    DateTime(i64),
    ObjectId([u8; 12]),
    /// BSON binary — payload + subtype. We keep `Vec<u8>` (the payload) and the
    /// subtype as separate fields; subtype 0x00 (generic) and 0x04 (UUID) are the
    /// only ones seen in the test corpus.
    Binary { subtype: u8, bytes: Vec<u8> },
    Array(Vec<BsonValue>),
    Document(BsonDoc),
    /// BSON regex — pattern + options (e.g. "i", "ims").
    Regex { pattern: String, options: String },
}

impl BsonValue {
    /// Convenience: `&str` for `String`, `i32` for `Int32`, `i64` for `Int64`,
    /// `String` (decimal form) for `Double`. Used by `query.rs` for collection names,
    /// sort directions, and similar fields where the driver puts a JSON-like value
    /// in the BSON doc.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            BsonValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Convenience: `i32` for `Int32`, `i64` for `Int64`, `String` (decimal) for `Double`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            BsonValue::Int32(i) => Some(*i as i64),
            BsonValue::Int64(i) => Some(*i),
            BsonValue::Double(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Convenience: bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            BsonValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Convenience: `BsonDoc` for `Document`, `Vec<BsonValue>` for `Array`.
    pub fn as_doc(&self) -> Option<&BsonDoc> {
        match self {
            BsonValue::Document(d) => Some(d),
            _ => None,
        }
    }

    /// Convenience: mutable document view.
    pub fn as_doc_mut(&mut self) -> Option<&mut BsonDoc> {
        match self {
            BsonValue::Document(d) => Some(d),
            _ => None,
        }
    }

    /// Convenience: array view.
    pub fn as_array(&self) -> Option<&Vec<BsonValue>> {
        match self {
            BsonValue::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Convenience: array view, mutable.
    pub fn as_array_mut(&mut self) -> Option<&mut Vec<BsonValue>> {
        match self {
            BsonValue::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Convenience: ObjectId as 12 bytes.
    pub fn as_object_id(&self) -> Option<&[u8; 12]> {
        match self {
            BsonValue::ObjectId(id) => Some(id),
            _ => None,
        }
    }

    /// Convenience: i64 DateTime.
    pub fn as_datetime(&self) -> Option<i64> {
        match self {
            BsonValue::DateTime(t) => Some(*t),
            _ => None,
        }
    }
}

/// Order-preserving map for BSON documents. BSON documents are
/// order-significant on the wire (index `key` docs, sort specs, projection
/// order), so the document face keeps insertion order; `From<BTreeMap>`
/// preserves the historical alphabetical behaviour for literal-built docs.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BsonMap(Vec<(String, BsonValue)>);

impl BsonMap {
    pub fn new() -> Self { Self(Vec::new()) }

    pub fn insert(&mut self, k: impl Into<String>, v: BsonValue) -> Option<BsonValue> {
        let k = k.into();
        match self.0.iter_mut().find(|(key, _)| *key == k) {
            Some(slot) => Some(std::mem::replace(&mut slot.1, v)),
            None => {
                self.0.push((k, v));
                None
            }
        }
    }

    pub fn get(&self, k: &str) -> Option<&BsonValue> {
        self.0.iter().find(|(key, _)| key == k).map(|(_, v)| v)
    }

    pub fn remove(&mut self, k: &str) -> Option<BsonValue> {
        let index = self.0.iter().position(|(key, _)| key == k)?;
        Some(self.0.remove(index).1)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &BsonValue)> {
        self.0.iter().map(|(k, v)| (k, v))
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.iter().map(|(k, _)| k)
    }

    pub fn contains_key(&self, k: &str) -> bool { self.get(k).is_some() }

    pub fn len(&self) -> usize { self.0.len() }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }
}

impl From<BTreeMap<String, BsonValue>> for BsonMap {
    /// BTreeMap has no insertion order — iterate sorted, matching the
    /// historical alphabetical wire order for literal-built documents.
    fn from(m: BTreeMap<String, BsonValue>) -> Self {
        Self(m.into_iter().collect())
    }
}

impl FromIterator<(String, BsonValue)> for BsonMap {
    fn from_iter<T: IntoIterator<Item = (String, BsonValue)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// A BSON document — insertion-ordered (see [`BsonMap`]).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BsonDoc(pub BsonMap);

impl BsonDoc {
    pub fn new() -> Self { Self(BsonMap::new()) }

    pub fn insert(&mut self, k: impl Into<String>, v: BsonValue) -> Option<BsonValue> {
        self.0.insert(k, v)
    }

    pub fn get(&self, k: &str) -> Option<&BsonValue> { self.0.get(k) }

    pub fn remove(&mut self, k: &str) -> Option<BsonValue> { self.0.remove(k) }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &BsonValue)> { self.0.iter() }

    pub fn len(&self) -> usize { self.0.len() }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }
}

impl From<BTreeMap<String, BsonValue>> for BsonDoc {
    fn from(m: BTreeMap<String, BsonValue>) -> Self { Self(m.into()) }
}

/// BSON decode/encode errors.
#[derive(Debug, Error)]
pub enum BsonError {
    #[error("unexpected EOF at offset {0}")]
    UnexpectedEof(usize),
    #[error("invalid cstring at offset {0}")]
    InvalidCString(usize),
    #[error("unknown element type 0x{0:02x}")]
    UnknownType(u8),
    #[error("invalid utf-8 at offset {0}")]
    InvalidUtf8(usize),
    #[error("invalid length {0}")]
    InvalidLength(i32),
}
