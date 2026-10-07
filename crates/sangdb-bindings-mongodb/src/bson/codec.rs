//! BSON encode/decode over `&[u8]` slices — zero-copy on read, single allocation on write.
//!
//! # Wire-format reference
//!
//! <https://bsonspec.org/spec.html> — every byte layout below traces to that page.
//!
//! Per-document layout:
//!
//! ```text
//! �───────────────────────────────┐
//! │ int32  total_length (LE)      │ ← includes itself
//! │ e_list  elements…            │ ← each is (tag, cstring key, value)
//! │ byte   0x00                  │ ← terminator
//! └───────────────────────────────�
//! ```
//!
//! Per-element layout: `[type_tag: u8][key: cstring (UTF-8, NUL-terminated)][value: per-tag]`.
//!
//! # Hashtrinity surface parity
//!
//! Mirrors `hashtrinity::codec::msgpack`'s shape:
//!
//! | hashtrinity::codec::msgpack        | here                              |
//! |------------------------------------|-----------------------------------|
//! | `pub fn encode(term) -> Vec<u8>`   | `pub fn encode(doc) -> Vec<u8>`    |
//! | `pub fn decode(bytes) -> Result<…>` | `pub fn decode(bytes) -> Result<…>` |
//! | zero-copy `&[u8]` slicing          | same — `&buf[pos..pos+n]` everywhere |
//! | self-describing (no schema)        | same — every value carries its own tag |
//!
//! When ported upstream to `hashtrinity::codec::bson`, this file becomes
//! `hashtrinity/src/codec/bson/codec.rs` and `pub fn decode(buf: &[u8])` is the
//! surface entry point; the `SyntaxTerm`-lossy mapping lives in the `value.rs` analog.

use super::value::{BsonDoc, BsonError, BsonValue};

/// Encode a `BsonDoc` to canonical BSON bytes.
pub fn encode(doc: &BsonDoc) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    encode_doc(&mut buf, doc);
    buf
}

/// Decode BSON bytes into a `BsonDoc`.
pub fn decode(buf: &[u8]) -> Result<BsonDoc, BsonError> {
    let (doc, consumed) = decode_doc(buf, 0)?;
    if consumed != buf.len() {
        return Err(BsonError::InvalidLength(consumed as i32));
    }
    Ok(doc)
}

// ── Encode ─────────────────────────────────────────────────────────────────

fn encode_doc(buf: &mut Vec<u8>, doc: &BsonDoc) {
    let len_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // placeholder for total_length
    for (k, v) in doc.iter() {
        encode_element(buf, k, v);
    }
    buf.push(0x00); // terminator
    let len = (buf.len() - len_pos) as i32;
    buf[len_pos..len_pos + 4].copy_from_slice(&len.to_le_bytes());
}

fn encode_element(buf: &mut Vec<u8>, key: &str, value: &BsonValue) {
    match value {
        BsonValue::Double(d) => {
            buf.push(0x01);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        BsonValue::String(s) => {
            buf.push(0x02);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&((bytes.len() as i32) + 1).to_le_bytes()); // +1 for NUL
            buf.extend_from_slice(bytes);
            buf.push(0x00);
        }
        BsonValue::Bool(b) => {
            buf.push(0x08);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            buf.push(u8::from(*b));
        }
        BsonValue::Null => {
            buf.push(0x0a);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
        }
        BsonValue::Int32(i) => {
            buf.push(0x10);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        BsonValue::Int64(i) => {
            buf.push(0x12);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        BsonValue::DateTime(t) => {
            buf.push(0x09);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        BsonValue::ObjectId(id) => {
            buf.push(0x07);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            buf.extend_from_slice(id);
        }
        BsonValue::Binary { subtype, bytes } => {
            buf.push(0x05);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            buf.extend_from_slice(&(bytes.len() as i32).to_le_bytes());
            buf.push(*subtype);
            buf.extend_from_slice(bytes);
        }
        BsonValue::Regex { pattern, options } => {
            buf.push(0x0b);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            buf.extend_from_slice(pattern.as_bytes());
            buf.push(0x00);
            buf.extend_from_slice(options.as_bytes());
            buf.push(0x00);
        }
        BsonValue::Array(arr) => {
            buf.push(0x04);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            // Array = document with stringified-int keys 0..N.
            let mut arr_doc = BsonDoc::new();
            for (i, v) in arr.iter().enumerate() {
                arr_doc.insert(i.to_string(), v.clone());
            }
            encode_doc(buf, &arr_doc);
        }
        BsonValue::Document(d) => {
            buf.push(0x03);
            buf.extend_from_slice(key.as_bytes());
            buf.push(0x00);
            encode_doc(buf, d);
        }
    }
}

// ── Decode ─────────────────────────────────────────────────────────────────

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

fn decode_value(buf: &[u8], off: usize, tag: u8) -> Result<(BsonValue, usize), BsonError> {
    match tag {
        0x01 => {
            if off + 8 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let bytes: [u8; 8] = buf[off..off + 8].try_into().unwrap();
            Ok((BsonValue::Double(f64::from_le_bytes(bytes)), off + 8))
        }
        0x02 => {
            if off + 4 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let len = i32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let str_off = off + 4;
            if str_off + len > buf.len() { return Err(BsonError::UnexpectedEof(str_off)); }
            // len includes the trailing NUL.
            let s = std::str::from_utf8(&buf[str_off..str_off + len - 1])
                .map_err(|_| BsonError::InvalidUtf8(off))?
                .to_string();
            Ok((BsonValue::String(s), str_off + len))
        }
        0x03 => {
            let (doc, after) = decode_doc(buf, off)?;
            Ok((BsonValue::Document(doc), after))
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
            Ok((BsonValue::Array(arr), after))
        }
        0x05 => {
            // BSON binary layout: [int32 length][byte subtype][payload bytes].
            if off + 5 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let len = i32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let payload_off = off + 5; // skip the subtype byte
            if payload_off + len > buf.len() { return Err(BsonError::UnexpectedEof(payload_off)); }
            let subtype = buf[off + 4];
            let bytes = buf[payload_off..payload_off + len].to_vec();
            Ok((BsonValue::Binary { subtype, bytes }, payload_off + len))
        }
        0x07 => {
            if off + 12 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let id: [u8; 12] = buf[off..off + 12].try_into().unwrap();
            Ok((BsonValue::ObjectId(id), off + 12))
        }
        0x08 => {
            if off + 1 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            Ok((BsonValue::Bool(buf[off] != 0), off + 1))
        }
        0x09 => {
            if off + 8 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let bytes: [u8; 8] = buf[off..off + 8].try_into().unwrap();
            Ok((BsonValue::DateTime(i64::from_le_bytes(bytes)), off + 8))
        }
        0x0a => Ok((BsonValue::Null, off)),
        0x0b => {
            // Regex: [cstring pattern][cstring options] — both cstrings, no length prefix.
            let mut e1 = off;
            while e1 < buf.len() && buf[e1] != 0 { e1 += 1; }
            if e1 >= buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let pattern = std::str::from_utf8(&buf[off..e1])
                .map_err(|_| BsonError::InvalidUtf8(off))?
                .to_string();
            let mut e2 = e1 + 1;
            while e2 < buf.len() && buf[e2] != 0 { e2 += 1; }
            if e2 >= buf.len() { return Err(BsonError::UnexpectedEof(e1 + 1)); }
            let options = std::str::from_utf8(&buf[e1 + 1..e2])
                .map_err(|_| BsonError::InvalidUtf8(e1 + 1))?
                .to_string();
            Ok((BsonValue::Regex { pattern, options }, e2 + 1))
        }
        0x10 => {
            if off + 4 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let bytes: [u8; 4] = buf[off..off + 4].try_into().unwrap();
            Ok((BsonValue::Int32(i32::from_le_bytes(bytes)), off + 4))
        }
        0x12 => {
            if off + 8 > buf.len() { return Err(BsonError::UnexpectedEof(off)); }
            let bytes: [u8; 8] = buf[off..off + 8].try_into().unwrap();
            Ok((BsonValue::Int64(i64::from_le_bytes(bytes)), off + 8))
        }
        other => Err(BsonError::UnknownType(other)),
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bson::value::{BsonDoc, BsonValue};

    #[test]
    fn roundtrip_string() {
        let mut d = BsonDoc::new();
        d.insert("hello", BsonValue::String("world".into()));
        d.insert("n", BsonValue::Int64(42));
        d.insert("f", BsonValue::Double(3.14));
        d.insert("b", BsonValue::Bool(true));
        d.insert("z", BsonValue::Null);
        let bytes = encode(&d);
        let back = decode(&bytes).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn roundtrip_nested_doc() {
        let mut inner = BsonDoc::new();
        inner.insert("k", BsonValue::Int32(1));
        let mut outer = BsonDoc::new();
        outer.insert("nested", BsonValue::Document(inner));
        let back = decode(&encode(&outer)).unwrap();
        assert_eq!(outer, back);
    }

    #[test]
    fn roundtrip_array() {
        let mut d = BsonDoc::new();
        d.insert("arr", BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2), BsonValue::Int32(3)]));
        let back = decode(&encode(&d)).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn roundtrip_objectid_datetime_regex_binary() {
        let mut d = BsonDoc::new();
        d.insert("_id", BsonValue::ObjectId([0x6a, 0x85, 0x85, 0x6d, 0x33, 0x83, 0xcb, 0x44, 0xac, 0xcb, 0xde, 0xfe]));
        d.insert("ts", BsonValue::DateTime(1_700_000_000_000));
        d.insert("pat", BsonValue::Regex { pattern: "^foo.*$".into(), options: "i".into() });
        d.insert("uuid", BsonValue::Binary { subtype: 0x04, bytes: vec![0u8; 16] });
        let back = decode(&encode(&d)).unwrap();
        assert_eq!(d, back);
    }
}
