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

// =============================================================
// SS58 address codec
// =============================================================
//
// SORA v2 addresses are SS58 with network prefix 69 ("cn…" strings).
// The legacy DB stores SS58 and the frontend expects SS58, so the
// ingest path encodes at decode time (decision locked 2026-04-30).
//
// Format (simple account, 32-byte body):
//   payload  = prefix_bytes ‖ pubkey[32] ‖ checksum[0..2]
//   checksum = blake2b-512(b"SS58PRE" ‖ prefix_bytes ‖ pubkey)[0..2]
//   address  = base58(payload)
// Prefixes 0..=63 take one byte; 64..=16383 take the two-byte form of
// the SS58 spec. Larger idents are reserved and rejected at
// construction ([`Ss58Prefix::new`]), which keeps encoding infallible.

/// First 16 bytes of every SORA *technical* account (pool reserves,
/// XST/TBC, order-book, bridge and other pallet-owned accounts).
/// `technical::tech_account_id_encoded_to_account_id_32` builds the
/// AccountId as `MAGIC_PREFIX ‖ xxhash64(seed 0) ‖ xxhash64(seed 1)`
/// (`common::TECH_ACCOUNT_MAGIC_PREFIX`, sora2 `common/src/lib.rs`).
/// In SS58 these all start with `cnTQ1kbv7PBNNQrEb1tZpmK7`.
pub const TECH_ACCOUNT_MAGIC_PREFIX: [u8; 16] = [
    84, 115, 79, 144, 249, 113, 160, 44, 96, 155, 45, 104, 78, 97, 181, 87,
];

/// `true` when `account` is a pallet-owned technical account
/// (`common::IsRepresentation::is_representation` in sora2).
pub fn is_technical_account(account: &[u8; 32]) -> bool {
    account[..16] == TECH_ACCOUNT_MAGIC_PREFIX
}

/// A validated SS58 network prefix (0..=16383).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ss58Prefix(u16);

impl Ss58Prefix {
    /// SORA mainnet prefix (69, addresses start with "cn").
    pub const SORA: Self = Self(69);

    /// Validates a raw prefix. `None` for reserved idents (> 16383).
    pub const fn new(raw: u16) -> Option<Self> {
        if raw <= 16383 {
            Some(Self(raw))
        } else {
            None
        }
    }

    /// The raw numeric prefix.
    pub const fn raw(self) -> u16 {
        self.0
    }

    /// SS58 wire bytes for this prefix (1 or 2 bytes).
    fn wire_bytes(self) -> ([u8; 2], usize) {
        let ident = self.0;
        if ident <= 63 {
            ([ident as u8, 0], 1)
        } else {
            // Two-byte form, per the SS58 spec (and substrate's
            // `Ss58Codec::to_ss58check_with_version`).
            let first = (((ident & 0b0000_0000_1111_1100) >> 2) as u8) | 0b0100_0000;
            let second = ((ident >> 8) as u8) | (((ident & 0b11) as u8) << 6);
            ([first, second], 2)
        }
    }
}

/// Errors when decoding an SS58 address string.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Ss58Error {
    /// Not valid base58.
    #[error("invalid base58")]
    InvalidBase58,
    /// Decoded payload has an impossible length.
    #[error("invalid payload length {0}")]
    InvalidLength(usize),
    /// Prefix bytes are not a supported simple-account form.
    #[error("unsupported prefix encoding")]
    UnsupportedPrefix,
    /// Checksum mismatch (corrupted or truncated address).
    #[error("checksum mismatch")]
    BadChecksum,
}

/// Blake2b-512 SS58 checksum over `prefix_bytes ‖ body`, first 2 bytes.
fn ss58_checksum(prefixed_body: &[u8]) -> [u8; 2] {
    use blake2::{Blake2b512, Digest};
    let mut hasher = Blake2b512::new();
    hasher.update(b"SS58PRE");
    hasher.update(prefixed_body);
    let digest = hasher.finalize();
    [digest[0], digest[1]]
}

/// Encodes a 32-byte account id as an SS58 address string.
pub fn ss58_encode(account: &[u8; 32], prefix: Ss58Prefix) -> String {
    let (prefix_bytes, prefix_len) = prefix.wire_bytes();
    let mut payload = Vec::with_capacity(prefix_len + 32 + 2);
    payload.extend_from_slice(&prefix_bytes[..prefix_len]);
    payload.extend_from_slice(account);
    let checksum = ss58_checksum(&payload);
    payload.extend_from_slice(&checksum);
    bs58::encode(payload).into_string()
}

/// Convenience: encode with the SORA mainnet prefix (69).
pub fn ss58_encode_sora(account: &[u8; 32]) -> String {
    ss58_encode(account, Ss58Prefix::SORA)
}

/// Decodes an SS58 address string into `(account, prefix)`.
///
/// Only the simple-account format with a 32-byte body is supported —
/// that covers every SORA v2 address. Checksum is always verified.
pub fn ss58_decode(address: &str) -> Result<([u8; 32], Ss58Prefix), Ss58Error> {
    let payload = bs58::decode(address)
        .into_vec()
        .map_err(|_| Ss58Error::InvalidBase58)?;

    // 1-byte prefix: 1 + 32 + 2 = 35. 2-byte prefix: 2 + 32 + 2 = 36.
    let prefix_len = match payload.len() {
        35 => 1,
        36 => 2,
        n => return Err(Ss58Error::InvalidLength(n)),
    };

    let prefix = match prefix_len {
        1 if payload[0] <= 63 => Ss58Prefix(payload[0] as u16),
        2 if (0b0100_0000..=0b0111_1111).contains(&payload[0]) => {
            let lower = (payload[0] << 2) | (payload[1] >> 6);
            let upper = payload[1] & 0b0011_1111;
            Ss58Prefix((lower as u16) | ((upper as u16) << 8))
        }
        _ => return Err(Ss58Error::UnsupportedPrefix),
    };

    let body_end = payload.len() - 2;
    let expected = ss58_checksum(&payload[..body_end]);
    if payload[body_end..] != expected {
        return Err(Ss58Error::BadChecksum);
    }

    let mut account = [0u8; 32];
    account.copy_from_slice(&payload[prefix_len..body_end]);
    Ok((account, prefix))
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

    // SS58 ground-truth vectors, triple-sourced 2026-08-10:
    // production sorametrics.org row (block 27245127) + independent
    // @polkadot/util-crypto `encodeAddress(hex, 69)` runs.
    const REAL_PUBKEY_HEX: &str =
        "b6952251ddff222bb7e97bc725439bd1ca33105e02114680be1a3712cde62a0f";
    const REAL_SS58: &str = "cnVcgVYJqhyuQohhYrZraVs85dujMDCBsBhMj5z8QPHq91C84";

    fn real_pubkey() -> [u8; 32] {
        let mut out = [0u8; 32];
        out.copy_from_slice(&hex::decode(REAL_PUBKEY_HEX).unwrap());
        out
    }

    #[test]
    fn ss58_encode_matches_production_address() {
        assert_eq!(ss58_encode_sora(&real_pubkey()), REAL_SS58);
    }

    #[test]
    fn ss58_encode_known_vectors_prefix_69() {
        // @polkadot/util-crypto encodeAddress(_, 69) outputs.
        assert_eq!(
            ss58_encode_sora(&[0x00; 32]),
            "cnRVHUYq6zah5dwdxD7B3CehfyfREb21GXBwyqqwbneyJoTRq"
        );
        assert_eq!(
            ss58_encode_sora(&[0x01; 32]),
            "cnRWbpd9ehcMGGNSfwPfu23emzGL346bXejn6hLkq3mmAGaDv"
        );
        assert_eq!(
            ss58_encode_sora(&[0xff; 32]),
            "cnXGwjYNWNsZoo13R7e44aYy6d9ZXeCdni8W7TXauFWZo5cWD"
        );
    }

    #[test]
    fn ss58_decode_roundtrip() {
        let (account, prefix) = ss58_decode(REAL_SS58).unwrap();
        assert_eq!(account, real_pubkey());
        assert_eq!(prefix, Ss58Prefix::SORA);
        assert_eq!(ss58_encode(&account, prefix), REAL_SS58);
    }

    #[test]
    fn ss58_decode_single_byte_prefix() {
        // Polkadot mainnet (prefix 0) treasury-ish vector: encode then
        // decode our own output for a 1-byte-prefix roundtrip.
        let prefix0 = Ss58Prefix::new(0).unwrap();
        let addr = ss58_encode(&[0x2a; 32], prefix0);
        let (account, prefix) = ss58_decode(&addr).unwrap();
        assert_eq!(account, [0x2a; 32]);
        assert_eq!(prefix.raw(), 0);
    }

    #[test]
    fn ss58_decode_rejects_corruption() {
        // Flip the last character: checksum must fail.
        let mut s = REAL_SS58.to_string();
        let last = s.pop().unwrap();
        s.push(if last == '4' { '5' } else { '4' });
        assert_eq!(ss58_decode(&s), Err(Ss58Error::BadChecksum));

        assert_eq!(ss58_decode("not-base58-!!"), Err(Ss58Error::InvalidBase58));
        assert_eq!(ss58_decode("cnVcg"), Err(Ss58Error::InvalidLength(4)));
    }

    #[test]
    fn ss58_prefix_rejects_reserved() {
        assert!(Ss58Prefix::new(16383).is_some());
        assert!(Ss58Prefix::new(16384).is_none());
    }

    #[test]
    fn technical_account_prefix_matches_real_pool_account() {
        // cnTQ1kbv7PBNNQrEb1tZpmK7fuxWZxsAP6HA1UauiMxyJ4Wmp — DAI/XOR pool
        // reserves account seen in block 27542813.
        let (bytes, _) = ss58_decode("cnTQ1kbv7PBNNQrEb1tZpmK7fuxWZxsAP6HA1UauiMxyJ4Wmp").unwrap();
        assert!(is_technical_account(&bytes));
        assert_eq!(
            hex::encode(&bytes[..16]),
            "54734f90f971a02c609b2d684e61b557"
        );
    }

    #[test]
    fn user_account_is_not_technical() {
        let (bytes, _) = ss58_decode("cnWeiModLdWS4hC75QeZxEANGNUmekog7YaaJ9PevFH1UTnhh").unwrap();
        assert!(!is_technical_account(&bytes));
    }
}
