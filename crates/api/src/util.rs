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

/// Most wallets a `?wallets=` listing accepts (the CSV export's bound).
pub const MAX_WALLETS: usize = 50;

/// `?wallets=a,b,c`, split into SORA accounts and everything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletSet {
    /// `(as given, canonical SS58)`, canonical addresses unique.
    pub resolved: Vec<(String, String)>,
    /// Entries that are not SORA accounts.
    pub invalid: Vec<String>,
}

impl WalletSet {
    /// Canonical addresses, in request order.
    pub fn addresses(&self) -> Vec<String> {
        self.resolved.iter().map(|(_, a)| a.clone()).collect()
    }
}

/// Order kept, duplicates dropped; no entry at all or more than
/// [`MAX_WALLETS`] → 400.
pub fn parse_wallets(raw: &str) -> Result<WalletSet, ApiError> {
    let mut set = WalletSet {
        resolved: Vec::new(),
        invalid: Vec::new(),
    };
    for part in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        match validate_address(part) {
            Ok(addr) if set.resolved.iter().any(|(_, a)| *a == addr) => {}
            Ok(addr) => set.resolved.push((part.to_string(), addr)),
            Err(_) if set.invalid.iter().any(|i| i == part) => {}
            Err(_) => set.invalid.push(part.to_string()),
        }
    }
    let n = set.resolved.len() + set.invalid.len();
    if n == 0 {
        return Err(ApiError::BadRequest("wallets: no address given".into()));
    }
    if n > MAX_WALLETS {
        return Err(ApiError::BadRequest(format!(
            "wallets: at most {MAX_WALLETS} addresses"
        )));
    }
    Ok(set)
}

/// The Node's `Math.min(parseInt(limit) || default, max)`: a missing, zero or
/// negative limit falls back to the default, a larger one is capped. The
/// Node never rejects a limit, and the frontend relies on it
/// (`/tokens?limit=500`).
pub fn clamp_limit(raw: Option<i64>, default: i64, max: i64) -> i64 {
    raw.filter(|l| *l > 0).unwrap_or(default).min(max)
}

/// The Node's `parseInt(page) || 1`, normalised like its history routes: a
/// missing, zero or negative page is page 1. Never a 400.
pub fn clamp_page(raw: Option<i64>) -> i64 {
    raw.filter(|p| *p > 0).unwrap_or(1)
}

/// `parseInt` for query numbers: leading sign and digits are read, the rest
/// is ignored, and anything that does not start with a number is `None`
/// (the Node then falls back to its default). Never a 400.
pub fn lenient_i64<'de, D>(de: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: Option<String> = serde::Deserialize::deserialize(de)?;
    Ok(raw.as_deref().and_then(parse_int_prefix))
}

fn parse_int_prefix(raw: &str) -> Option<i64> {
    let t = raw.trim_start();
    let (sign, digits) = match t.as_bytes().first() {
        Some(b'-') => (-1, &t[1..]),
        Some(b'+') => (1, &t[1..]),
        _ => (1, t),
    };
    let end = digits.bytes().take_while(u8::is_ascii_digit).count();
    digits[..end].parse::<i64>().ok().map(|n| sign * n)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Same triple-sourced ground-truth pair as core::chain tests.
    const REAL_HEX: &str = "b6952251ddff222bb7e97bc725439bd1ca33105e02114680be1a3712cde62a0f";
    const REAL_SS58: &str = "cnVcgVYJqhyuQohhYrZraVs85dujMDCBsBhMj5z8QPHq91C84";

    #[test]
    fn wallets_are_validated_deduplicated_and_bounded() {
        let bot = "cnVcgVYJqhyuQohhYrZraVs85dujMDCBsBhMj5z8QPHq91C84";
        let got = parse_wallets(&format!(" {bot} ,{bot},")).unwrap();
        assert_eq!(got.addresses(), vec![bot.to_string()]);
        assert!(got.invalid.is_empty());
        assert!(parse_wallets(" , ").is_err());
        let typo = &bot[..bot.len() - 1];
        let mixed = parse_wallets(&format!("notanaddress,{bot},{typo},notanaddress")).unwrap();
        assert_eq!(mixed.addresses(), vec![bot.to_string()]);
        assert_eq!(
            mixed.invalid,
            vec!["notanaddress".to_string(), typo.to_string()]
        );
        let many = std::iter::repeat_n(bot, MAX_WALLETS + 1)
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(parse_wallets(&many).unwrap().resolved.len(), 1);
        let too_many = (0..=MAX_WALLETS)
            .map(|i| format!("x{i}"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(parse_wallets(&too_many).is_err());
    }

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

    #[test]
    fn limit_is_clamped_like_the_node() {
        assert_eq!(clamp_limit(None, 20, 100), 20);
        assert_eq!(clamp_limit(Some(0), 20, 100), 20);
        assert_eq!(clamp_limit(Some(-5), 20, 100), 20);
        assert_eq!(clamp_limit(Some(37), 20, 100), 37);
        assert_eq!(clamp_limit(Some(500), 20, 100), 100);
        assert_eq!(clamp_page(None), 1);
        assert_eq!(clamp_page(Some(0)), 1);
        assert_eq!(clamp_page(Some(-1)), 1);
        assert_eq!(clamp_page(Some(7)), 7);
    }

    #[test]
    fn query_numbers_parse_like_parse_int() {
        assert_eq!(parse_int_prefix("25"), Some(25));
        assert_eq!(parse_int_prefix("12abc"), Some(12));
        assert_eq!(parse_int_prefix(" 7"), Some(7));
        assert_eq!(parse_int_prefix("-3"), Some(-3));
        assert_eq!(parse_int_prefix("abc"), None);
        assert_eq!(parse_int_prefix(""), None);
    }
}
