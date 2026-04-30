//! Generic on-chain primitives reused across SORA v2 and Iroha 3.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Fixed-size 32-byte array used for hashes, public keys, commitments.
///
/// Stored as `BYTEA(32)` in PostgreSQL. Hex-encoded in JSON / API responses
/// (lowercase, no `0x` prefix to match upstream Iroha conventions).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Bytes32(pub [u8; 32]);

impl Bytes32 {
    /// Creates from raw bytes.
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the inner byte array.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Hex string (lowercase, no prefix).
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parses from hex string. Accepts optional `0x` prefix.
    pub fn from_hex(s: &str) -> Result<Self, Bytes32Error> {
        let s = s.strip_prefix("0x").unwrap_or(s);
        let bytes = hex::decode(s).map_err(|_| Bytes32Error::InvalidHex)?;
        if bytes.len() != 32 {
            return Err(Bytes32Error::WrongLength(bytes.len()));
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(&bytes);
        Ok(Self(out))
    }
}

/// Errors when parsing a `Bytes32`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Bytes32Error {
    /// Hex string is malformed.
    #[error("invalid hex encoding")]
    InvalidHex,
    /// Decoded length is not 32 bytes.
    #[error("expected 32 bytes, got {0}")]
    WrongLength(usize),
}

impl fmt::Debug for Bytes32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bytes32({})", self.to_hex())
    }
}

impl fmt::Display for Bytes32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Bytes32 {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Bytes32 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

/// Block hash (32 bytes). Same wire format on SORA v2 and Iroha 3.
pub type BlockHash = Bytes32;

/// Block height. Common monotonic counter.
///
/// Wrapped to prevent accidental arithmetic with unrelated `u64`s.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlockHeight(pub u64);

impl BlockHeight {
    /// Genesis block height.
    pub const ZERO: Self = Self(0);

    /// Increments and returns the new value.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for BlockHeight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Account address.
///
/// SORA v2 uses SS58-encoded strings. Iroha 3 uses `name@domain` strings.
/// We keep them as `String` since validation depends on which chain they
/// belong to — that context lives at the parser layer, not here.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Address(pub String);

impl Address {
    /// Creates from any string-like value.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Returns the inner string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Asset identifier.
///
/// SORA v2 assets are `0x` + 64 hex chars. Iroha 3 assets are
/// `asset_name#domain.dataspace`. Same `String` rationale as [`Address`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AssetId(pub String);

impl AssetId {
    /// Creates from any string-like value.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Returns the inner string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AssetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes32_roundtrip_hex() {
        let original = Bytes32::new([0xAB; 32]);
        let hex = original.to_hex();
        let parsed = Bytes32::from_hex(&hex).expect("roundtrip");
        assert_eq!(original, parsed);
    }

    #[test]
    fn bytes32_accepts_0x_prefix() {
        let with_prefix = Bytes32::from_hex(&format!("0x{}", "ab".repeat(32))).unwrap();
        let without_prefix = Bytes32::from_hex(&"ab".repeat(32)).unwrap();
        assert_eq!(with_prefix, without_prefix);
    }

    #[test]
    fn bytes32_rejects_wrong_length() {
        let err = Bytes32::from_hex("ab").unwrap_err();
        assert_eq!(err, Bytes32Error::WrongLength(1));
    }

    #[test]
    fn bytes32_rejects_invalid_hex() {
        let err = Bytes32::from_hex("zzzz").unwrap_err();
        assert_eq!(err, Bytes32Error::InvalidHex);
    }

    #[test]
    fn bytes32_serde_json() {
        let original = Bytes32::new([0x01; 32]);
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(json, format!("\"{}\"", "01".repeat(32)));
        let parsed: Bytes32 = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn block_height_next() {
        assert_eq!(BlockHeight(5).next(), BlockHeight(6));
        assert_eq!(BlockHeight::ZERO.next(), BlockHeight(1));
    }

    #[test]
    fn block_height_orderable() {
        assert!(BlockHeight(1) < BlockHeight(2));
    }

    #[test]
    fn address_display_passthrough() {
        let a = Address::new("cnXX...");
        assert_eq!(a.to_string(), "cnXX...");
    }
}
