//! Per-extrinsic rows — the Node's `live_extrinsics` mechanism
//! (`index.js`, the `signedBlock.block.extrinsics` loop): for every
//! extrinsic, its `section.method`, signer, success flag (from
//! `System::ExtrinsicSuccess` / `ExtrinsicFailed` in its phase), a
//! resolved error message (`pallet.Error: docs`), the decoded call args
//! and the phase's events as `[{s, m, d}]` — all in the polkadot-js
//! `toHuman()` style the frontend renders: pallet/call names in
//! camelCase, numbers as grouped digit strings, 32-byte account fields
//! as SS58, other byte arrays as `0x` hex, enums as `{Variant: inner}`.

use crate::decoder::EventCoords;
use crate::runtime::sora;
use serde_json::{json, Map, Value as Json};
use sorametrics_core::chain::ss58_encode_sora;
use sorametrics_core::sora_v2::V2Extrinsic;
use subxt::blocks::{ExtrinsicDetails, Extrinsics};
use subxt::events::EventDetails;
use subxt::ext::scale_value::{Composite, Primitive, Value, ValueDef};
use subxt::{Metadata, OnlineClient, SubstrateConfig};

/// Type registry view used to recognise `AccountId32` by type id.
pub type Types = scale_info::PortableRegistry;

/// `true` when the type id resolves to `sp_core::crypto::AccountId32`.
fn is_account_type(types: &Types, id: u32) -> bool {
    types
        .resolve(id)
        .map(|t| t.path.segments.last().map(String::as_str) == Some("AccountId32"))
        .unwrap_or(false)
}

/// polkadot-js `stringCamelCase` for pallet names: the first word is
/// lower-cased, the rest keep their case (`LiquidityProxy` →
/// `liquidityProxy`, `PoolXYK` → `poolXYK`, `XYKPool` → `xykPool`).
pub fn pallet_camel(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    // Length of the leading word: an uppercase run ends where the run is
    // followed by an uppercase letter + lowercase (start of next word),
    // or a Capitalised word ends at the next uppercase letter.
    let mut end = 0;
    if chars.len() > 1 && chars[1].is_ascii_uppercase() {
        // Acronym run.
        while end < chars.len() && chars[end].is_ascii_uppercase() {
            end += 1;
        }
        // If the run is followed by lowercase, the last upper belongs to
        // the next word (`XYKPool` → `XYK` + `Pool`).
        if end < chars.len() && chars[end].is_ascii_lowercase() && end > 1 {
            end -= 1;
        }
    } else {
        end = 1;
        while end < chars.len() && !chars[end].is_ascii_uppercase() {
            end += 1;
        }
    }
    let (head, tail) = chars.split_at(end);
    let mut out: String = head.iter().collect::<String>().to_lowercase();
    out.extend(tail);
    out
}

/// snake_case → lowerCamelCase (`transfer_to_sidechain` → `transferToSidechain`).
pub fn snake_camel(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper_next = false;
    for c in name.chars() {
        if c == '_' {
            upper_next = true;
        } else if upper_next {
            out.extend(c.to_uppercase());
            upper_next = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// Field names polkadot-js renders as SS58 accounts.
fn is_account_field(name: &str) -> bool {
    matches!(
        name,
        "who"
            | "from"
            | "to"
            | "account"
            | "account_id"
            | "accountId"
            | "sender"
            | "recipient"
            | "caller"
            | "owner"
            | "payer"
            | "referrer"
            | "referral"
            | "stash"
            | "controller"
            | "target"
            | "dest"
            | "delegate"
            | "beneficiary"
            | "validator"
            | "nominator"
            | "authority"
            | "author"
            | "signer"
    )
}

/// Digits with thousands separators (polkadot-js `toHuman` for numbers).
pub fn group_digits(n: u128) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// A composite that is really a byte array (all elements small ints).
fn as_bytes(c: &Composite<u32>) -> Option<Vec<u8>> {
    let vals = match c {
        Composite::Unnamed(v) => v,
        Composite::Named(_) => return None,
    };
    if vals.is_empty() {
        return None;
    }
    vals.iter()
        .map(|v| match &v.value {
            ValueDef::Primitive(Primitive::U128(n)) if *n <= 255 => Some(*n as u8),
            _ => None,
        })
        .collect()
}

/// Render a decoded SCALE value the way polkadot-js `toHuman` would.
/// `types` lets positional `AccountId32` values (tuple events) render
/// as SS58 like named ones.
pub fn to_human(v: &Value<u32>, field: Option<&str>, types: &Types) -> Json {
    let account = is_account_type(types, v.context);
    match &v.value {
        ValueDef::Primitive(p) => match p {
            Primitive::Bool(b) => Json::Bool(*b),
            Primitive::Char(c) => Json::String(c.to_string()),
            Primitive::String(s) => Json::String(s.clone()),
            Primitive::U128(n) => Json::String(group_digits(*n)),
            Primitive::I128(n) => Json::String(n.to_string()),
            Primitive::U256(b) | Primitive::I256(b) => {
                Json::String(format!("0x{}", hex::encode(b)))
            }
        },
        ValueDef::BitSequence(bits) => Json::String(format!("{bits:?}")),
        ValueDef::Variant(var) => {
            let inner = composite_to_human(&var.values, field, types, false);
            if matches!(&var.values, Composite::Unnamed(v) if v.is_empty()) {
                Json::String(var.name.clone())
            } else {
                json!({ var.name.clone(): inner })
            }
        }
        ValueDef::Composite(c) => composite_to_human(c, field, types, account),
    }
}

fn composite_to_human(
    c: &Composite<u32>,
    field: Option<&str>,
    types: &Types,
    account: bool,
) -> Json {
    if let Some(bytes) = as_bytes(c) {
        if bytes.len() == 32 && (account || field.is_some_and(is_account_field)) {
            let arr: [u8; 32] = bytes.as_slice().try_into().unwrap_or([0; 32]);
            return Json::String(ss58_encode_sora(&arr));
        }
        return Json::String(format!("0x{}", hex::encode(bytes)));
    }
    match c {
        Composite::Named(fields) => {
            let mut m = Map::with_capacity(fields.len());
            for (name, val) in fields {
                m.insert(snake_camel(name), to_human(val, Some(name), types));
            }
            Json::Object(m)
        }
        Composite::Unnamed(vals) => {
            if vals.len() == 1 {
                // Newtype wrapper (AccountId32(bytes), AssetId32 { code }…):
                // keep the field / type context so account newtypes become SS58.
                let inner = &vals[0];
                if account {
                    if let ValueDef::Composite(ic) = &inner.value {
                        return composite_to_human(ic, field, types, true);
                    }
                }
                return to_human(inner, field, types);
            }
            Json::Array(vals.iter().map(|v| to_human(v, None, types)).collect())
        }
    }
}

/// `pallet.ErrorName: docs` for a module error, else the variant label.
pub fn dispatch_error_text(
    metadata: &Metadata,
    err: &sora::runtime_types::sp_runtime::DispatchError,
) -> String {
    use sora::runtime_types::sp_runtime::DispatchError as E;
    match err {
        E::Module(m) => {
            let resolved = metadata.pallet_by_index(m.index).and_then(|p| {
                let v = p.error_variant_by_index(m.error[0])?;
                Some(format!(
                    "{}.{}: {}",
                    pallet_camel(p.name()),
                    v.name,
                    v.docs.join(" ")
                ))
            });
            resolved.unwrap_or_else(|| format!("Module({}, {})", m.index, m.error[0]))
        }
        E::Other => "Other".into(),
        E::CannotLookup => "CannotLookup".into(),
        E::BadOrigin => "BadOrigin".into(),
        E::ConsumerRemaining => "ConsumerRemaining".into(),
        E::NoProviders => "NoProviders".into(),
        E::TooManyConsumers => "TooManyConsumers".into(),
        E::Token(t) => format!("Token({t:?})"),
        E::Arithmetic(a) => format!("Arithmetic({a:?})"),
        E::Transactional(t) => format!("Transactional({t:?})"),
        E::Exhausted => "Exhausted".into(),
        E::Corruption => "Corruption".into(),
        E::Unavailable => "Unavailable".into(),
        E::RootNotAllowed => "RootNotAllowed".into(),
        E::Trie(t) => format!("Trie({t:?})"),
    }
}

/// Per-extrinsic facts accumulated from the events of its phase.
#[derive(Debug, Default, Clone)]
pub struct ExtrinsicFacts {
    success: bool,
    error_msg: String,
    events: Vec<Json>,
}

impl ExtrinsicFacts {
    /// Record one event of the extrinsic's phase.
    pub fn observe(&mut self, ev: &EventDetails<SubstrateConfig>, metadata: &Metadata) {
        match (ev.pallet_name(), ev.variant_name()) {
            ("System", "ExtrinsicSuccess") => self.success = true,
            ("System", "ExtrinsicFailed") => {
                self.success = false;
                self.error_msg = match ev.as_event::<sora::system::events::ExtrinsicFailed>() {
                    Ok(Some(f)) => dispatch_error_text(metadata, &f.dispatch_error),
                    _ => "Unknown error".into(),
                };
            }
            (pallet, variant) => {
                let d = match ev.field_values() {
                    Ok(c) => composite_to_human(&c, None, metadata.types(), false),
                    Err(_) => Json::Null,
                };
                self.events.push(json!({
                    "s": pallet_camel(pallet),
                    "m": variant,
                    "d": d,
                }));
            }
        }
    }
}

fn signer_from_address_bytes(bytes: &[u8]) -> Option<[u8; 32]> {
    let raw = match bytes.len() {
        32 => bytes,
        33 if bytes[0] == 0 => &bytes[1..],
        _ => return None,
    };
    raw.try_into().ok()
}

/// Node: args JSON capped at 2048 chars (`… ` appended when cut).
const ARGS_MAX: usize = 2048;

fn capped_args(args: Json) -> Json {
    let text = args.to_string();
    if text.len() <= ARGS_MAX {
        args
    } else {
        let mut cut = text;
        cut.truncate(ARGS_MAX);
        Json::String(format!("{cut}..."))
    }
}

/// Build the row for one extrinsic of the block.
pub fn extrinsic_row(
    ext: &ExtrinsicDetails<SubstrateConfig, OnlineClient<SubstrateConfig>>,
    facts: ExtrinsicFacts,
    coords: EventCoords,
    metadata: &Metadata,
) -> V2Extrinsic {
    let section = ext.pallet_name().map(pallet_camel).unwrap_or_default();
    let method = ext.variant_name().map(snake_camel).unwrap_or_default();
    let signer = ext
        .address_bytes()
        .and_then(signer_from_address_bytes)
        .map(|a| ss58_encode_sora(&a))
        .unwrap_or_else(|| "System".to_string());
    let args = match ext.field_values() {
        Ok(c) => capped_args(composite_to_human(&c, None, metadata.types(), false)),
        Err(_) => json!({}),
    };
    V2Extrinsic {
        block_height: coords.block_height,
        extrinsic_index: ext.index(),
        hash: format!("0x{}", hex::encode(ext.hash().0)),
        section,
        method,
        signer,
        success: facts.success,
        error_msg: if facts.success {
            String::new()
        } else {
            facts.error_msg
        },
        args,
        events: Json::Array(facts.events),
        timestamp: coords.block_timestamp,
    }
}

/// Convenience: iterate a block's extrinsics with their index.
pub fn iter_extrinsics(
    extrinsics: &Extrinsics<SubstrateConfig, OnlineClient<SubstrateConfig>>,
) -> Vec<ExtrinsicDetails<SubstrateConfig, OnlineClient<SubstrateConfig>>> {
    extrinsics.iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_types() -> Types {
        Types::from(scale_info::Registry::new())
    }

    fn ctx(v: Value<()>) -> Value<u32> {
        v.map_context(|_| u32::MAX)
    }

    #[test]
    fn pallet_names_follow_polkadot_js() {
        assert_eq!(pallet_camel("LiquidityProxy"), "liquidityProxy");
        assert_eq!(pallet_camel("PoolXYK"), "poolXYK");
        assert_eq!(pallet_camel("XYKPool"), "xykPool");
        assert_eq!(pallet_camel("EthBridge"), "ethBridge");
        assert_eq!(pallet_camel("System"), "system");
        assert_eq!(pallet_camel("XSTPool"), "xstPool");
    }

    #[test]
    fn calls_are_camel_cased() {
        assert_eq!(snake_camel("transfer_to_sidechain"), "transferToSidechain");
        assert_eq!(snake_camel("swap"), "swap");
        assert_eq!(snake_camel("currency_id"), "currencyId");
    }

    #[test]
    fn numbers_are_grouped() {
        assert_eq!(
            group_digits(23_633_769_750_512_797_420),
            "23,633,769,750,512,797,420"
        );
        assert_eq!(group_digits(5), "5");
    }

    #[test]
    fn to_human_renders_accounts_assets_and_numbers() {
        let acc = Value::unnamed_composite(
            (0..32u8)
                .map(|i| Value::u128(i as u128))
                .collect::<Vec<_>>(),
        );
        let acc_wrapped = Value::unnamed_composite(vec![acc.clone()]);
        let asset = Value::named_composite(vec![("code", acc.clone())]);
        let ev = ctx(Value::named_composite(vec![
            ("currency_id", asset),
            ("from", acc_wrapped),
            ("amount", Value::u128(1_500)),
            ("flag", Value::bool(true)),
        ]));
        let j = to_human(&ev, None, &no_types());
        assert_eq!(j["amount"], "1,500");
        assert_eq!(j["flag"], true);
        assert!(j["from"].as_str().unwrap().starts_with("cn"));
        assert!(j["currencyId"]["code"]
            .as_str()
            .unwrap()
            .starts_with("0x000102"));
    }

    #[test]
    fn unit_variant_is_a_string_and_data_variant_an_object() {
        let unit = ctx(Value::variant("Disabled", Composite::Unnamed(vec![])));
        assert_eq!(to_human(&unit, None, &no_types()), "Disabled");
        let with = ctx(Value::variant(
            "WithDesiredInput",
            Composite::named(vec![("desired_amount_in", Value::u128(7))]),
        ));
        assert_eq!(
            to_human(&with, None, &no_types())["WithDesiredInput"]["desiredAmountIn"],
            "7"
        );
    }

    #[test]
    fn args_are_capped_like_the_node() {
        let big = json!({ "x": "y".repeat(5000) });
        let capped = capped_args(big);
        let s = capped.as_str().unwrap();
        assert!(s.ends_with("..."));
        assert_eq!(s.len(), ARGS_MAX + 3);
    }
}
