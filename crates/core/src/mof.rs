//! MOF (SORA Ministry of Finance) circulating supply — the Node's
//! `fetchMofSupply`: `https://mof.sora.org/qty/<symbol>` with the
//! `mof2` / `mof3` mirrors as fallback. A mirror's answer counts when it
//! parses as a positive number (for XOR also below 1e9, the
//! denomination guard); otherwise the next mirror is tried.

/// MOF mirrors, in the Node's order.
pub const MOF_URLS: [&str; 3] = [
    "https://mof.sora.org",
    "https://mof2.sora.org",
    "https://mof3.sora.org",
];

/// `(symbol, MOF symbol, asset id)` of the burn tracker tokens
/// (`BURN_TOKENS` ∪ `MOF_SYMBOL_MAP`; KUSD is `xstusd` on the MOF).
pub const SUPPLY_TOKENS: [(&str, &str, &str); 5] = [
    (
        "XOR",
        "xor",
        "0x0200000000000000000000000000000000000000000000000000000000000000",
    ),
    (
        "VAL",
        "val",
        "0x0200040000000000000000000000000000000000000000000000000000000000",
    ),
    (
        "PSWAP",
        "pswap",
        "0x0200050000000000000000000000000000000000000000000000000000000000",
    ),
    (
        "TBCD",
        "tbcd",
        "0x02000a0000000000000000000000000000000000000000000000000000000000",
    ),
    (
        "KUSD",
        "xstusd",
        "0x0200080000000000000000000000000000000000000000000000000000000000",
    ),
];

/// `(asset id, MOF symbol)` of a burn tracker symbol.
pub fn supply_token(symbol: &str) -> Option<(&'static str, &'static str)> {
    SUPPLY_TOKENS
        .iter()
        .find(|(s, _, _)| *s == symbol)
        .map(|(_, mof, id)| (*id, *mof))
}

/// Node `fetchMofSupply` acceptance: positive, and below 1e9 for XOR.
pub fn accept_mof_value(symbol: &str, text: &str) -> Option<f64> {
    let v: f64 = text.trim().parse().ok()?;
    if !v.is_finite() || v <= 0.0 || (symbol == "XOR" && v >= 1e9) {
        return None;
    }
    Some(v)
}

/// Circulating supply of `symbol` from the first MOF mirror that answers.
pub async fn fetch_mof_supply(
    http: &reqwest::Client,
    symbol: &str,
    mof_symbol: &str,
) -> Option<f64> {
    for base in MOF_URLS {
        let url = format!("{base}/qty/{mof_symbol}");
        match http.get(&url).send().await {
            Ok(r) if r.status().is_success() => {
                if let Ok(text) = r.text().await {
                    if let Some(v) = accept_mof_value(symbol, &text) {
                        return Some(v);
                    }
                }
            }
            Ok(r) => tracing::warn!(url, status = %r.status(), "mof supply mirror rejected"),
            Err(e) => tracing::warn!(url, error = %e, "mof supply mirror failed"),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mof_values_follow_the_node_acceptance_rule() {
        assert_eq!(
            accept_mof_value("VAL", "55789857.44304373\n"),
            Some(55789857.44304373)
        );
        assert_eq!(accept_mof_value("XOR", "989988209.5"), Some(989988209.5));
        assert_eq!(accept_mof_value("XOR", "98899900000002100"), None);
        assert_eq!(accept_mof_value("VAL", "0"), None);
        assert_eq!(accept_mof_value("VAL", "nope"), None);
        assert_eq!(supply_token("KUSD").map(|(_, m)| m), Some("xstusd"));
        assert!(supply_token("DAI").is_none());
    }
}
