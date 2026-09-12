//! Small helpers reused across handlers.

use crate::error::ApiError;
use sorametrics_core::chain::{ss58_decode, ss58_encode_sora, Ss58Prefix};

/// Validates and canonicalises a SORA account address path parameter.
///
/// Accepted input:
/// - SS58 with the SORA prefix (`cn…`) — checksum-verified.
/// - `0x`-prefixed (prefix optional) 64-hex-char public key — converted.
///
/// Canonical output: SS58 (SORA prefix 69). This matches what the
/// ingest path stores in `sm.live_*` since Bloque 1, so SQL equality
/// against `caller` / `from_address` / `to_address` / `payer` is direct.
pub fn validate_address(raw: &str) -> Result<String, ApiError> {
    // Hex form: strip 0x, 64 hex chars → encode to SS58.
    let body = raw.strip_prefix("0x").unwrap_or(raw);
    if body.len() == 64 && body.chars().all(|c| c.is_ascii_hexdigit()) {
        let mut account = [0u8; 32];
        // len checked above; decode cannot fail on pure hex of even length.
        hex::decode_to_slice(body.to_ascii_lowercase(), &mut account)
            .map_err(|_| ApiError::BadRequest("address contains non-hex characters".into()))?;
        return Ok(ss58_encode_sora(&account));
    }

    // SS58 form: the checksum must hold; any prefix is accepted and the
    // key is re-encoded with the SORA prefix. The Node only checked the
    // base58 shape and let polkadot-js decode whatever prefix came in,
    // so production answers `/balance/<prefix-81 address>` with the
    // account's balances; a foreign prefix must not be a 400 here.
    match ss58_decode(raw) {
        Ok((_, prefix)) if prefix == Ss58Prefix::SORA => Ok(raw.to_string()),
        Ok((bytes, _)) => Ok(ss58_encode_sora(&bytes)),
        Err(e) => Err(ApiError::BadRequest(format!(
            "address is neither 32-byte hex nor valid SORA SS58: {e}"
        ))),
    }
}

/// Validate an asset id path parameter: `0x` + 64 hex, lower-cased.
/// (Node `validateAssetId`: `/^0x[0-9a-fA-F]{64}$/`.)
pub fn validate_asset_id(raw: &str) -> Result<String, ApiError> {
    let ok =
        raw.len() == 66 && raw.starts_with("0x") && raw[2..].chars().all(|c| c.is_ascii_hexdigit());
    if ok {
        Ok(raw.to_lowercase())
    } else {
        Err(ApiError::BadRequest("Invalid asset ID format".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Same triple-sourced ground-truth pair as core::chain tests.
    const REAL_HEX: &str = "b6952251ddff222bb7e97bc725439bd1ca33105e02114680be1a3712cde62a0f";
    const REAL_SS58: &str = "cnVcgVYJqhyuQohhYrZraVs85dujMDCBsBhMj5z8QPHq91C84";

    #[test]
    fn accepts_ss58_passthrough() {
        assert_eq!(validate_address(REAL_SS58).unwrap(), REAL_SS58);
    }

    #[test]
    fn converts_hex_with_prefix_to_ss58() {
        let s = format!("0x{REAL_HEX}");
        assert_eq!(validate_address(&s).unwrap(), REAL_SS58);
    }

    #[test]
    fn converts_hex_without_prefix_to_ss58() {
        assert_eq!(validate_address(REAL_HEX).unwrap(), REAL_SS58);
    }

    #[test]
    fn converts_uppercase_hex() {
        let s = format!("0x{}", REAL_HEX.to_ascii_uppercase());
        assert_eq!(validate_address(&s).unwrap(), REAL_SS58);
    }

    #[test]
    fn rejects_corrupted_ss58() {
        let mut s = REAL_SS58.to_string();
        let last = s.pop().unwrap();
        s.push(if last == '4' { '5' } else { '4' });
        assert!(matches!(
            validate_address(&s).unwrap_err(),
            ApiError::BadRequest(_)
        ));
    }

    #[test]
    fn normalises_a_foreign_prefix_to_sora() {
        // Production serves this prefix-81 address (75k hits/day): same
        // public key, re-encoded with prefix 69.
        let foreign = "e75nAWoXBh2Rrq7pQzKnXKxDuTfzGUs7JZjfZQZAXtijHtMvE";
        let (bytes, prefix) = ss58_decode(foreign).unwrap();
        assert_ne!(prefix, Ss58Prefix::SORA);
        let got = validate_address(foreign).unwrap();
        assert_eq!(got, ss58_encode_sora(&bytes));
        assert!(got.starts_with("cn"));
    }

    #[test]
    fn rejects_short_and_garbage() {
        assert!(matches!(
            validate_address("0xabcd").unwrap_err(),
            ApiError::BadRequest(_)
        ));
        assert!(matches!(
            validate_address("not-an-address").unwrap_err(),
            ApiError::BadRequest(_)
        ));
    }
}
