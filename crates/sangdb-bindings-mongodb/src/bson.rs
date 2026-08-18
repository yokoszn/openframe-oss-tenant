//! Minimal BSON codec. MongoDB's binary JSON. We implement enough to parse
//! the documents our wire-protocol shim receives — full BSON is huge, so we
//! cover the subset OpenFrame uses (the `@Document`-annotated POJOs in
//! `openframe-data-mongo-common`): String, Int32, Int64, Double, Boolean,
//! NullDate, Array, Document, ObjectId, Binary (UUID), DateTime.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// A BSON value. The recursive type matches MongoDB's `BsonValue` enum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Bson {
    Double(f64),
    String(String),
    Bool(bool),
    Null,
    Int32(i32),
    Int64(i64),
    DateTime(i64), // unix millis
    ObjectId([u8; 12]),
    Binary(Vec<u8>),
    Array(Vec<Bson>),
    Document(BsonDoc),
}

/// A BSON document — `BTreeMap<String, Bson>` so serialisation is
/// deterministic and matches MongoDB's canonical ordering on the wire.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct BsonDoc(pub BTreeMap<String, Bson>);

impl BsonDoc {
    pub fn new() -> Self { Self(BTreeMap::new()) }
    pub fn insert(&mut self, k: impl Into<String>, v: Bson) -> Option<Bson> {
        self.0.insert(k.into(), v)
    }
    pub fn get(&self, k: &str) -> Option<&Bson> { self.0.get(k) }
    pub fn remove(&mut self, k: &str) -> Option<Bson> { self.0.remove(k) }
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Bson)> { self.0.iter() }
    pub fn len(&self) -> usize { self.0.len() }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
}

impl From<BTreeMap<String, Bson>> for BsonDoc {
    fn from(m: BTreeMap<String, Bson>) -> Self { Self(m) }
}

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

/// Encode a `BsonDoc` to canonical BSON bytes.
pub fn encode(doc: &BsonDoc) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    encode_doc(&mut buf, doc);
    buf
}

fn encode_doc(buf: &mut Vec<u8>, doc: &BsonDoc) {
    // Reserve 4 bytes for the document length, fill in at the end.
    let len_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);
    for (k, v) in doc.iter() {
        encode_element(buf, k, v);
    }
    buf.push(0x00); // null terminator
    let len = (buf.len() - len_pos) as i32;
    buf[len_pos..len_pos + 4].copy_from_slice(&len.to_le_bytes());
}

fn encode_element(buf: &mut Vec<u8>, key: &str, value: &Bson) {
    let (tag, payload) = match value {
        Bson::Double(d) => (0x01, d.to_le_bytes().to_vec()),
        Bson::String(s) => (0x02, {
            let mut p = Vec::with_capacity(s.len() + 5);
            p.extend_from_slice(&((s.len() as i32) + 1).to_le_bytes());
            p.extend_from_slice(s.as_bytes());
            p.push(0x00);
            p
        }),
        Bson::Bool(b) => (0x08, vec![u8::from(*b)]),
        Bson::Null => (0x0a, vec![]),
        Bson::Int32(i) => (0x10, i.to_le_bytes().to_vec()),
        Bson::Int64(i) => (0x12, i.to_le_bytes().to_vec()),
        Bson::DateTime(t) => (0x09, t.to_le_bytes().to_vec()),
        Bson::ObjectId(id) => (0x07, id.to_vec()),
        Bson::Binary(b) => (0x05, {
            let mut p = Vec::with_capacity(b.len() + 5);
            p.push(0x00); // generic binary subtype
            p.extend_from_slice(&(b.len() as i32).to_le_bytes());
            p.extend_from_slice(b);
            p
        }),
        Bson::Array(arr) => {
            buf.push(0x04); // array tag
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            // Reuse doc encoding with numeric keys.
            let mut arr_doc = BsonDoc::new();
            for (i, v) in arr.iter().enumerate() {
                arr_doc.insert(i.to_string(), v.clone());
            }
            encode_doc(buf, &arr_doc);
            return;
        }
        Bson::Document(d) => {
            buf.push(0x03); // document tag
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            encode_doc(buf, d);
            return;
        }
    };
    buf.push(tag);
    buf.extend_from_slice(key.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(&payload);
}

/// Decode BSON bytes into a `BsonDoc`.
pub fn decode(buf: &[u8]) -> Result<BsonDoc, BsonError> {
    let (doc, consumed) = decode_doc(buf, 0)?;
    if consumed != buf.len() {
        return Err(BsonError::InvalidLength(consumed as i32));
    }
    Ok(doc)
}

fn decode_doc(buf: &[u8], off: usize) -> Result<(BsonDoc, usize), BsonError> {
    if off + 4 > buf.len() {
        return Err(BsonError::UnexpectedEof(off));
    }
    let len = i32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
    if off + len > buf.len() || len < 5 {
        return Err(BsonError::InvalidLength(len as i32));
    }
    let end = off + len;
    let mut pos = off + 4;
    let mut doc = BsonDoc::new();
    while pos < end {
        if buf[pos] == 0x00 {
            pos += 1;
            break;
        }
        let tag = buf[pos];
        pos += 1;
        let (key, after) = read_cstring(buf, pos)?;
        pos = after;
        let (val, after) = decode_value(buf, pos, tag)?;
        pos = after;
        doc.insert(key, val);
    }
    Ok((doc, pos))
}

fn read_cstring(buf: &[u8], off: usize) -> Result<(String, usize), BsonError> {
    let mut end = off;
    while end < buf.len() && buf[end] != 0x00 {
        end += 1;
    }
    if end >= buf.len() {
        return Err(BsonError::InvalidCString(off));
    }
    let s = std::str::from_utf8(&buf[off..end])
        .map_err(|_| BsonError::InvalidUtf8(off))?
        .to_string();
    Ok((s, end + 1))
}

fn decode_value(buf: &[u8], off: usize, tag: u8) -> Result<(Bson, usize), BsonError> {
    match tag {
        0x01 => {
            if off + 8 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let bytes: [u8; 8] = buf[off..off + 8].try_into().unwrap();
            Ok((Bson::Double(f64::from_le_bytes(bytes)), off + 8))
        }
        0x02 => {
            if off + 4 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let len = i32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let str_off = off + 4;
            if str_off + len > buf.len() { return Err(BsonError::UnexpectedEof(str_off)); }
            let s = std::str::from_utf8(&buf[str_off..str_off + len - 1])
                .map_err(|_| BsonError::InvalidUtf8(off))?
                .to_string();
            Ok((Bson::String(s), str_off + len))
        }
        0x03 => {
            let (doc, after) = decode_doc(buf, off)?;
            Ok((Bson::Document(doc), after))
        }
        0x04 => {
            // Array — same encoding as a document with stringified-int keys.
            let (doc, after) = decode_doc(buf, off)?;
            let mut arr = Vec::new();
            let mut i = 0;
            loop {
                let key = i.to_string();
                match doc.get(&key) {
                    Some(v) => arr.push(v.clone()),
                    None => break,
                }
                i += 1;
            }
            Ok((Bson::Array(arr), after))
        }
        0x05 => {
            if off + 5 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let len = i32::from_le_bytes(buf[off + 1..off + 5].try_into().unwrap()) as usize;
            let payload_off = off + 5;
            if payload_off + len > buf.len() { return Err(BsonError::UnexpectedEof(payload_off)); }
            Ok((Bson::Binary(buf[payload_off..payload_off + len].to_vec()), payload_off + len))
        }
        0x07 => {
            if off + 12 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let id: [u8; 12] = buf[off..off + 12].try_into().unwrap();
            Ok((Bson::ObjectId(id), off + 12))
        }
        0x08 => {
            if off + 1 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            Ok((Bson::Bool(buf[off] != 0), off + 1))
        }
        0x09 => {
            if off + 8 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let bytes: [u8; 8] = buf[off..off + 8].try_into().unwrap();
            Ok((Bson::DateTime(i64::from_le_bytes(bytes)), off + 8))
        }
        0x0a => Ok((Bson::Null, off)),
        0x10 => {
            if off + 4 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let bytes: [u8; 4] = buf[off..off + 4].try_into().unwrap();
            Ok((Bson::Int32(i32::from_le_bytes(bytes)), off + 4))
        }
        0x12 => {
            if off + 8 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let bytes: [u8; 8] = buf[off..off + 8].try_into().unwrap();
            Ok((Bson::Int64(i64::from_le_bytes(bytes)), off + 8))
        }
        other => Err(BsonError::UnknownType(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_string() {
        let mut d = BsonDoc::new();
        d.insert("hello", Bson::String("world".into()));
        d.insert("n", Bson::Int64(42));
        d.insert("f", Bson::Double(3.14));
        d.insert("b", Bson::Bool(true));
        d.insert("z", Bson::Null);
        let bytes = encode(&d);
        let back = decode(&bytes).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn roundtrip_nested_doc() {
        let mut inner = BsonDoc::new();
        inner.insert("k", Bson::Int32(1));
        let mut outer = BsonDoc::new();
        outer.insert("nested", Bson::Document(inner));
        let back = decode(&encode(&outer)).unwrap();
        assert_eq!(outer, back);
    }

    #[test]
    fn roundtrip_array() {
        let mut d = BsonDoc::new();
        d.insert("arr", Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]));
        let back = decode(&encode(&d)).unwrap();
        assert_eq!(d, back);
    }
}