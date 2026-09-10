//! `/block/:n` — the Node's Subscan-style block view: header, every
//! extrinsic with its `toJSON()` args and the events of its phase, the
//! inherent events, the digest logs and the runtime version at that
//! block. Each event carries a `decoded` field list (`{ name, type,
//! typeName, value, human }`): balances are formatted with the asset
//! paired positionally from sibling AssetId / CurrencyId fields (XOR for
//! the native sections), asset ids become symbols, weights
//! `<ms>ms · <proof>b`, and dispatch errors resolve to
//! `section.Name — docs`; an error found anywhere in the event data is
//! surfaced as `decodedError`.

use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use serde_json::{json, Value as Jv};
use sorametrics_core::chain::ss58_encode_sora;
use sorametrics_substrate::extrinsics::{pallet_camel, snake_camel};
use sorametrics_substrate::governance::to_json;
use sorametrics_substrate::runtime::sora;
use std::collections::HashMap;
use subxt::config::substrate::DigestItem;
use subxt::ext::scale_value::{Composite, Value, ValueDef};
use subxt::Metadata;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new().route("/block/:n", get(block))
}

const XOR_NATIVE_SECTIONS: [&str; 5] = [
    "balances",
    "xorFee",
    "transactionPayment",
    "staking",
    "session",
];

#[derive(Serialize, Clone)]
struct DecodedField {
    name: Option<String>,
    #[serde(rename = "type")]
    type_id: Option<String>,
    #[serde(rename = "typeName")]
    type_name: Option<String>,
    value: Jv,
    human: Option<String>,
}

#[derive(Serialize, Clone)]
struct DecodedError {
    section: Option<String>,
    name: String,
    docs: String,
}

#[derive(Serialize, Clone)]
struct EventOut {
    index: usize,
    section: String,
    method: String,
    data: Jv,
    decoded: Vec<DecodedField>,
    #[serde(rename = "decodedError", skip_serializing_if = "Option::is_none")]
    decoded_error: Option<DecodedError>,
}

#[derive(Serialize)]
struct ExtrinsicOut {
    index: usize,
    hash: String,
    signer: Option<String>,
    section: String,
    method: String,
    args: Option<Jv>,
    success: Option<bool>,
    events: Vec<EventOut>,
}

#[derive(Serialize)]
struct BlockOut {
    number: u32,
    hash: String,
    #[serde(rename = "parentHash")]
    parent_hash: String,
    #[serde(rename = "stateRoot")]
    state_root: String,
    #[serde(rename = "extrinsicsRoot")]
    extrinsics_root: String,
    timestamp: Option<i64>,
    #[serde(rename = "specVersion")]
    spec_version: Option<u32>,
    #[serde(rename = "specName")]
    spec_name: Option<String>,
    extrinsics: Vec<ExtrinsicOut>,
    #[serde(rename = "inherentEvents")]
    inherent_events: Vec<EventOut>,
    logs: Vec<Jv>,
    #[serde(rename = "totalExtrinsics")]
    total_extrinsics: usize,
    #[serde(rename = "totalEvents")]
    total_events: usize,
    source: String,
}

/// Node `formatTokenAmount`: grouped integer part, up to 6 trimmed decimals.
pub fn format_token_amount(raw: u128, decimals: u32) -> String {
    if raw == 0 {
        return "0".into();
    }
    let div = 10u128.pow(decimals);
    let int = (raw / div).to_string();
    let frac = format!("{:0width$}", raw % div, width = decimals as usize);
    let trimmed: String = frac.trim_end_matches('0').chars().take(6).collect();
    let grouped = sorametrics_substrate::extrinsics::group_digits(int.parse().unwrap_or(0));
    if trimmed.is_empty() {
        grouped
    } else {
        format!("{grouped}.{trimmed}")
    }
}

/// A `toJSON()` number: JS number or `0x` hex string.
fn json_u128(v: &Jv) -> Option<u128> {
    match v {
        Jv::Number(n) => n.as_u64().map(u128::from),
        Jv::String(s) => u128::from_str_radix(s.strip_prefix("0x")?, 16).ok(),
        _ => None,
    }
}

/// Node `normalizeAssetIdJson`: `{ code }`, hex string or `{ Token: 'XOR' }`.
fn asset_key(v: &Jv) -> Option<String> {
    match v {
        Jv::String(s) => Some(s.to_lowercase()),
        Jv::Object(o) => {
            if let Some(Jv::String(c)) = o.get("code") {
                return Some(c.to_lowercase());
            }
            o.values()
                .next()
                .and_then(|x| x.as_str())
                .map(str::to_string)
        }
        _ => None,
    }
}

struct AssetInfo {
    symbol: String,
    decimals: u32,
}

fn resolve_asset(registry: &crate::state::Registry, raw: &Jv) -> Option<AssetInfo> {
    let key = asset_key(raw)?;
    if let Some(a) = registry.get(&key) {
        return Some(AssetInfo {
            symbol: a.symbol.clone(),
            decimals: a.decimals.max(0) as u32,
        });
    }
    let id = registry.asset_id_for_symbol(&key.to_uppercase())?;
    registry.get(id).map(|a| AssetInfo {
        symbol: a.symbol.clone(),
        decimals: a.decimals.max(0) as u32,
    })
}

fn is_asset_type(type_name: &str) -> bool {
    let t = type_name.to_lowercase().replace('_', "");
    t.contains("assetid") || t.contains("technicalassetid") || t.contains("currencyid")
}

/// Node `decodeDispatchError` over a `toJSON()` error value.
fn decode_dispatch_error(metadata: &Metadata, err: &Jv) -> Option<DecodedError> {
    match err {
        Jv::String(s) => Some(DecodedError {
            section: None,
            name: s.clone(),
            docs: String::new(),
        }),
        Jv::Object(o) => {
            if let Some(m) = o.get("module").or_else(|| o.get("Module")) {
                let index = m.get("index").and_then(|i| i.as_u64())? as u8;
                let error_hex = m.get("error").and_then(|e| e.as_str())?;
                let first = hex::decode(error_hex.trim_start_matches("0x"))
                    .ok()?
                    .first()
                    .copied()?;
                let pallet = metadata.pallet_by_index(index)?;
                let v = pallet.error_variant_by_index(first)?;
                return Some(DecodedError {
                    section: Some(pallet_camel(pallet.name())),
                    name: v.name.clone(),
                    docs: v
                        .docs
                        .iter()
                        .map(|d| d.trim())
                        .filter(|d| !d.is_empty())
                        .collect::<Vec<_>>()
                        .join(" "),
                });
            }
            let (k, sub) = o.iter().find(|(k, _)| *k != "module" && *k != "Module")?;
            Some(DecodedError {
                section: None,
                name: match sub {
                    Jv::String(s) => format!("{k}.{s}"),
                    _ => k.clone(),
                },
                docs: String::new(),
            })
        }
        _ => None,
    }
}

/// Node `findEmbeddedError`: first DispatchError-like value in the tree.
fn find_embedded_error(metadata: &Metadata, v: &Jv) -> Option<DecodedError> {
    match v {
        Jv::Object(o) => {
            if let Some(e) = o.get("Err").or_else(|| o.get("err")) {
                if let Some(d) = decode_dispatch_error(metadata, e) {
                    return Some(d);
                }
            }
            if o.contains_key("module") || o.contains_key("Module") {
                if let Some(d) = decode_dispatch_error(metadata, v) {
                    return Some(d);
                }
            }
            o.values().find_map(|x| find_embedded_error(metadata, x))
        }
        Jv::Array(a) => a.iter().find_map(|x| find_embedded_error(metadata, x)),
        _ => None,
    }
}

/// Node `enrichEventFields`.
fn enrich_fields(
    metadata: &Metadata,
    registry: &crate::state::Registry,
    section: &str,
    fields: &[(Option<String>, u32, Option<String>)],
    values: &[Jv],
) -> Vec<DecodedField> {
    let assets: Vec<Option<AssetInfo>> = fields
        .iter()
        .zip(values)
        .filter(|((_, _, tn), _)| tn.as_deref().is_some_and(is_asset_type))
        .map(|(_, v)| resolve_asset(registry, v))
        .collect();
    let default_asset = assets.iter().find_map(|a| a.as_ref());
    let native = XOR_NATIVE_SECTIONS.contains(&section);
    let mut slot = 0usize;
    fields
        .iter()
        .zip(values)
        .map(|((name, ty, type_name), raw)| {
            let tn = type_name.as_deref().unwrap_or("").to_lowercase();
            let mut human = None;
            if (tn.contains("balance") || tn == "u128") && !raw.is_null() {
                let (ticker, decimals) = if native {
                    (Some("XOR".to_string()), 18)
                } else {
                    let paired = assets.get(slot).and_then(|a| a.as_ref()).or(default_asset);
                    slot += 1;
                    (
                        paired.map(|a| a.symbol.clone()),
                        paired.map(|a| a.decimals).unwrap_or(18),
                    )
                };
                if let Some(n) = json_u128(raw) {
                    let f = format_token_amount(n, decimals);
                    human = Some(match ticker {
                        Some(t) => format!("{f} {t}"),
                        None => f,
                    });
                }
            } else if is_asset_type(&tn) && !raw.is_null() {
                human = resolve_asset(registry, raw).map(|a| a.symbol);
            } else if tn.contains("weight") {
                if let (Some(rt), Some(ps)) = (
                    raw.get("refTime").and_then(json_u128),
                    raw.get("proofSize").and_then(json_u128),
                ) {
                    human = Some(format!("{:.2}ms · {ps}b", rt as f64 / 1e9));
                }
            } else if tn.contains("dispatcherror") || tn.contains("dispatchresult") {
                if let Some(d) = find_embedded_error(metadata, raw) {
                    let sec = d.section.map(|s| format!("{s}.")).unwrap_or_default();
                    let docs = if d.docs.is_empty() {
                        String::new()
                    } else {
                        format!(" — {}", d.docs)
                    };
                    human = Some(format!("{sec}{}{docs}", d.name));
                }
            }
            DecodedField {
                name: name.clone(),
                type_id: Some(ty.to_string()),
                type_name: type_name.clone(),
                value: raw.clone(),
                human,
            }
        })
        .collect()
}

fn digest_json(item: &DigestItem) -> Jv {
    let hex = |b: &[u8]| format!("0x{}", hex::encode(b));
    match item {
        DigestItem::PreRuntime(id, data) => json!({ "preRuntime": [hex(id), hex(data)] }),
        DigestItem::Consensus(id, data) => json!({ "consensus": [hex(id), hex(data)] }),
        DigestItem::Seal(id, data) => json!({ "seal": [hex(id), hex(data)] }),
        DigestItem::Other(data) => json!({ "other": hex(data) }),
        DigestItem::RuntimeEnvironmentUpdated => json!("RuntimeEnvironmentUpdated"),
    }
}

async fn block(
    State(state): State<AppState>,
    Path(raw): Path<String>,
) -> Result<Json<BlockOut>, ApiError> {
    let n: u32 = raw
        .parse()
        .map_err(|_| ApiError::BadRequest("Invalid block number".into()))?;
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let client = chain.client().await?;
    let legacy = chain.legacy_rpc().await?;
    let hash = legacy
        .chain_get_block_hash(Some(n.into()))
        .await
        .map_err(|e| ApiError::Chain(e.into()))?
        .ok_or_else(|| ApiError::NotFound("block not found".into()))?;
    let block = client
        .blocks()
        .at(hash)
        .await
        .map_err(|e| ApiError::Chain(e.into()))?;
    let header = block.header().clone();
    let exts = block
        .extrinsics()
        .await
        .map_err(|e| ApiError::Chain(e.into()))?;
    let events = block
        .events()
        .await
        .map_err(|e| ApiError::Chain(e.into()))?;
    let rv = legacy.state_get_runtime_version(Some(hash)).await.ok();
    let metadata = client.metadata();
    let types = metadata.types();
    let registry = state.registry.read().await;

    let mut timestamp = None;
    let mut ext_events: Vec<Vec<EventOut>> = vec![Vec::new(); exts.len()];
    let mut inherent = Vec::new();
    let mut total_events = 0usize;
    for (idx, ev) in events.iter().enumerate() {
        let ev = ev.map_err(|e| ApiError::Chain(subxt::Error::from(e).into()))?;
        total_events += 1;
        let section = pallet_camel(ev.pallet_name());
        let fields_meta: Vec<(Option<String>, u32, Option<String>)> = ev
            .event_metadata()
            .variant
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.ty.id, f.type_name.clone()))
            .collect();
        let values: Vec<Jv> = match ev.field_values() {
            Ok(Composite::Named(f)) => f.iter().map(|(_, v)| to_json(v, types)).collect(),
            Ok(Composite::Unnamed(v)) => v.iter().map(|x| to_json(x, types)).collect(),
            Err(_) => Vec::new(),
        };
        let data = Jv::Array(values.clone());
        let out = EventOut {
            index: idx,
            section: section.clone(),
            method: ev.variant_name().to_string(),
            decoded: enrich_fields(&metadata, &registry, &section, &fields_meta, &values),
            decoded_error: find_embedded_error(&metadata, &data),
            data,
        };
        match ev.phase() {
            subxt::events::Phase::ApplyExtrinsic(i) if (i as usize) < ext_events.len() => {
                ext_events[i as usize].push(out)
            }
            _ => inherent.push(out),
        }
    }
    let mut ext_list = Vec::with_capacity(exts.len());
    for (i, ext) in exts.iter().enumerate() {
        if let Ok(Some(set)) = ext.as_extrinsic::<sora::timestamp::calls::types::Set>() {
            timestamp = Some(set.now as i64);
        }
        let evs = std::mem::take(&mut ext_events[i]);
        let success = if evs
            .iter()
            .any(|e| e.section == "system" && e.method == "ExtrinsicSuccess")
        {
            Some(true)
        } else if evs
            .iter()
            .any(|e| e.section == "system" && e.method == "ExtrinsicFailed")
        {
            Some(false)
        } else {
            None
        };
        let args = ext.field_values().ok().map(|c| {
            Jv::Array(match c {
                Composite::Named(f) => f.iter().map(|(_, v)| to_json(v, types)).collect(),
                Composite::Unnamed(v) => v.iter().map(|x| to_json(x, types)).collect(),
            })
        });
        let signer = ext.address_bytes().and_then(|b| {
            let raw = if b.len() == 33 && b[0] == 0 {
                &b[1..]
            } else {
                b
            };
            <[u8; 32]>::try_from(raw).ok().map(|a| ss58_encode_sora(&a))
        });
        ext_list.push(ExtrinsicOut {
            index: i,
            hash: format!("0x{}", hex::encode(ext.hash().0)),
            signer,
            section: ext.pallet_name().map(pallet_camel).unwrap_or_default(),
            method: ext.variant_name().map(snake_camel).unwrap_or_default(),
            args,
            success,
            events: evs,
        });
    }
    let source = chain
        .active_endpoint()
        .await
        .map(|u| u.to_string().trim_end_matches('/').to_string())
        .unwrap_or_default();
    Ok(Json(BlockOut {
        number: n,
        hash: format!("0x{}", hex::encode(hash.0)),
        parent_hash: format!("0x{}", hex::encode(header.parent_hash.0)),
        state_root: format!("0x{}", hex::encode(header.state_root.0)),
        extrinsics_root: format!("0x{}", hex::encode(header.extrinsics_root.0)),
        timestamp,
        spec_version: rv.as_ref().map(|r| r.spec_version),
        spec_name: rv
            .as_ref()
            .and_then(|r| r.other.get("specName"))
            .and_then(|v| v.as_str())
            .map(str::to_string),
        extrinsics: ext_list,
        inherent_events: inherent,
        logs: header.digest.logs.iter().map(digest_json).collect(),
        total_extrinsics: exts.len(),
        total_events,
        source,
    }))
}

// Keep the composite/value imports used by the decoders above.
#[allow(dead_code)]
fn _value_marker(_: &Value<u32>, _: &ValueDef<u32>, _: &HashMap<String, Jv>) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_amounts_match_the_node_formatter() {
        assert_eq!(format_token_amount(0, 18), "0");
        assert_eq!(format_token_amount(10_014_281_258_970_732, 18), "0.010014");
        assert_eq!(format_token_amount(4_123_527_577_223_243, 18), "0.004123");
        assert_eq!(
            format_token_amount(1_234_500_000_000_000_000_000, 18),
            "1,234.5"
        );
        assert_eq!(
            json_u128(&json!("0x0000000000000000002393ef8d1f2e6c")),
            Some(10_014_281_258_970_732)
        );
        assert!(is_asset_type("T::CurrencyId"));
        assert!(!is_asset_type("T::Balance"));
    }
}
