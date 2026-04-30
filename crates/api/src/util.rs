//! Small helpers reused across handlers.

use crate::error::ApiError;

/// Validates and canonicalises a SORA account address path parameter.
///
/// Accepted input: `0x`-prefixed (with or without prefix) lowercase or
/// uppercase 64 hex chars (= 32 bytes). Anything else → `400`.
///
/// Canonical output: lowercase, with leading `0x`. This matches the
/// shape the indexer wrote into `sm.live_*` (see
/// `substrate::decoder::bytes32_hex`), so SQL equality comparisons
/// against `caller` / `from_address` / `to_address` / `payer` columns
/// are direct (no `LOWER()` wrapper needed).
pub fn validate_address(raw: &str) -> Result<String, ApiError> {
    let body = raw.strip_prefix("0x").unwrap_or(raw);
    if body.len() != 64 {
        return Err(ApiError::BadRequest(format!(
            "address must be 32 bytes hex (got {} chars after stripping 0x)",
            body.len()
        )));
    }
    if !body.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest(
            "address contains non-hex characters".into(),
        ));
    }
    let mut canonical = String::with_capacity(2 + 64);
    canonical.push_str("0x");
    for ch in body.chars() {
        canonical.push(ch.to_ascii_lowercase());
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_lowercase_with_prefix() {
        let s = format!("0x{}", "ab".repeat(32));
        let out = validate_address(&s).unwrap();
        assert_eq!(out, s);
    }

    #[test]
    fn accepts_uppercase_lowercased() {
        let s = format!("0x{}", "AB".repeat(32));
        let out = validate_address(&s).unwrap();
        assert_eq!(out, format!("0x{}", "ab".repeat(32)));
    }

    #[test]
    fn accepts_no_prefix() {
        let s = "ab".repeat(32);
        let out = validate_address(&s).unwrap();
        assert_eq!(out, format!("0x{}", "ab".repeat(32)));
    }

    #[test]
    fn rejects_short_address() {
        let err = validate_address("0xabcd").unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[test]
    fn rejects_long_address() {
        let s = format!("0x{}", "a".repeat(65));
        let err = validate_address(&s).unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[test]
    fn rejects_non_hex() {
        let s = format!("0x{}", "z".repeat(64));
        let err = validate_address(&s).unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
    }
}
