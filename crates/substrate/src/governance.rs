//! Governance helpers — the Node's `decodeProposal` and the polkadot-js
//! `toJSON()` rendering used by `/governance/*`, plus the preimage event
//! indexer (`preimage_indexer.js`).
//!
//! - [`decode_call`] decodes raw `RuntimeCall` bytes with the pinned
//!   metadata; [`proposal`] renders it as `{ section, method, args,
//!   description, remark, innerCalls }`: `utility.batch*` calls are
//!   expanded, a `system.remark` inside becomes the description.
//! - [`to_json`] is `toJSON()`: numbers when they fit in a JS number,
//!   else `0x` hex padded to the type width; enums `{ camelVariant:
//!   inner }` or the variant name; `Option` → `null` / inner; bytes as
//!   hex; accounts as SS58; the democracy `Vote` byte as `0x80`-style.
//! - [`PreimageBlockFacts`] gathers what the indexer needs to explain a
//!   `Cleared` / `Unnoted`: a runtime upgrade in the block
//!   (`system.CodeUpdated`), a scheduler dispatch, or a manual
//!   `preimage.unnote_preimage`.

use crate::extrinsics::{
    is_newtype, is_u8_collection, pallet_camel, snake_camel, to_human_keys, Types,
};
use serde_json::{json, Map, Value as Json};
use sorametrics_core::chain::{ss58_encode_sora, BlockHeight};
use sorametrics_core::sora_v2::V2PreimageEvent;
use subxt::blocks::Extrinsics;
use subxt::events::EventDetails;
use subxt::ext::scale_value::{Composite, Primitive, Value, ValueDef};
use subxt::{Metadata, OnlineClient, SubstrateConfig};

/// Decode SCALE `RuntimeCall` bytes against the pinned metadata.
pub fn decode_call(bytes: &[u8], metadata: &Metadata) -> Result<Value<u32>, String> {
    let ty = metadata.outer_enums().call_enum_ty();
    let mut cursor = bytes;
    subxt::ext::scale_value::scale::decode_as_type(&mut cursor, ty, metadata.types())
        .map(|v| v.map_context(|c| c))
        .map_err(|e| e.to_string())
}

/// Node `decodeProposal` output.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct Proposal {
    /// Pallet in camelCase.
    pub section: String,
    /// Call in camelCase.
    pub method: String,
    /// Call args in `toHuman` style, metadata (snake_case) keys.
    pub args: Json,
    /// `section.method`, the remark of a batch, or `Batch of N calls`.
    pub description: String,
    /// UTF-8 of a `system.remark` inside a batch.
    pub remark: Option<String>,
    /// Decoded inner calls of a batch (remarks excluded).
    #[serde(rename = "innerCalls")]
    pub inner_calls: Vec<Proposal>,
}

fn unknown(description: String) -> Proposal {
    Proposal {
        section: "?".into(),
        method: "?".into(),
        args: json!({}),
        description,
        remark: None,
        inner_calls: Vec::new(),
    }
}

/// Bytes of a byte-array value, looking through newtype wrappers
/// (`H256([u8; 32])`, `BoundedVec<u8>`).
fn bytes_of(v: &Value<u32>) -> Option<Vec<u8>> {
    match &v.value {
        ValueDef::Composite(Composite::Unnamed(vals)) if vals.len() == 1 => {
            if let ValueDef::Composite(_) = &vals[0].value {
                return bytes_of(&vals[0]);
            }
            bytes_list(vals)
        }
        ValueDef::Composite(Composite::Unnamed(vals)) => bytes_list(vals),
        ValueDef::Composite(Composite::Named(fields)) if fields.len() == 1 => {
            bytes_of(&fields[0].1)
        }
        _ => None,
    }
}

fn bytes_list(vals: &[Value<u32>]) -> Option<Vec<u8>> {
    vals.iter()
        .map(|x| match &x.value {
            ValueDef::Primitive(Primitive::U128(n)) if *n <= 255 => Some(*n as u8),
            _ => None,
        })
        .collect()
}

/// Node `decodeProposal` over a decoded `RuntimeCall` value.
pub fn proposal(call: &Value<u32>, types: &Types) -> Proposal {
    let ValueDef::Variant(pallet) = &call.value else {
        return unknown("Unknown format".into());
    };
    let inner = match &pallet.values {
        Composite::Unnamed(v) if v.len() == 1 => &v[0],
        _ => return unknown("Unknown format".into()),
    };
    let ValueDef::Variant(callv) = &inner.value else {
        return unknown("Unknown format".into());
    };
    let section = pallet_camel(&pallet.name);
    let method = snake_camel(&callv.name);
    let args = match &callv.values {
        Composite::Named(fields) => {
            let mut m = Map::with_capacity(fields.len());
            for (name, val) in fields {
                m.insert(name.clone(), to_human_keys(val, Some(name), types, false));
            }
            Json::Object(m)
        }
        Composite::Unnamed(vals) if vals.is_empty() => json!({}),
        c => to_human_keys(
            &Value {
                value: ValueDef::Composite(c.clone()),
                context: inner.context,
            },
            None,
            types,
            false,
        ),
    };
    let mut description = format!("{section}.{method}");
    let mut remark = None;
    let mut inner_calls = Vec::new();
    if section == "utility" && matches!(method.as_str(), "batchAll" | "batch" | "forceBatch") {
        let calls = match &callv.values {
            Composite::Named(fields) => fields.first().map(|(_, v)| v),
            Composite::Unnamed(vals) => vals.first(),
        };
        if let Some(Value {
            value: ValueDef::Composite(Composite::Unnamed(list)),
            ..
        }) = calls
        {
            if !list.is_empty() {
                for c in list {
                    let d = proposal(c, types);
                    if d.section == "system" && d.method == "remark" {
                        remark = remark_text(c).or_else(|| {
                            d.args
                                .get("remark")
                                .and_then(|r| r.as_str())
                                .map(str::to_string)
                        });
                    } else {
                        inner_calls.push(d);
                    }
                }
                description = remark
                    .clone()
                    .unwrap_or_else(|| format!("Batch of {} calls", list.len()));
            }
        }
    }
    Proposal {
        section,
        method,
        args,
        description,
        remark,
        inner_calls,
    }
}

/// UTF-8 of the `remark` bytes of a `system.remark` call value.
fn remark_text(call: &Value<u32>) -> Option<String> {
    let ValueDef::Variant(pallet) = &call.value else {
        return None;
    };
    let inner = match &pallet.values {
        Composite::Unnamed(v) if v.len() == 1 => &v[0],
        _ => return None,
    };
    let ValueDef::Variant(callv) = &inner.value else {
        return None;
    };
    let first = match &callv.values {
        Composite::Named(fields) => fields.first().map(|(_, v)| v),
        Composite::Unnamed(vals) => vals.first(),
    }?;
    bytes_of(first).map(|b| String::from_utf8_lossy(&b).to_string())
}

/// polkadot-js `stringCamelCase` of an enum variant (`SimpleMajority` →
/// `simpleMajority`, `Lookup` → `lookup`).
fn lower_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

fn type_path_last(types: &Types, id: u32) -> Option<&str> {
    types
        .resolve(id)
        .and_then(|t| t.path.segments.last().map(String::as_str))
}

/// Bit width of a primitive type id, if it is one.
fn primitive_bits(types: &Types, id: u32) -> Option<u32> {
    use scale_info::{TypeDef, TypeDefPrimitive};
    match types.resolve(id).map(|t| &t.type_def) {
        Some(TypeDef::Primitive(p)) => Some(match p {
            TypeDefPrimitive::U8 | TypeDefPrimitive::I8 => 8,
            TypeDefPrimitive::U16 | TypeDefPrimitive::I16 => 16,
            TypeDefPrimitive::U32 | TypeDefPrimitive::I32 => 32,
            TypeDefPrimitive::U64 | TypeDefPrimitive::I64 => 64,
            TypeDefPrimitive::U128 | TypeDefPrimitive::I128 => 128,
            TypeDefPrimitive::U256 | TypeDefPrimitive::I256 => 256,
            _ => return None,
        }),
        Some(TypeDef::Compact(c)) => primitive_bits(types, c.type_param.id),
        _ => None,
    }
}

/// polkadot-js switches to hex above 52 bits (`4712602945397992` is hex
/// in prod, `4123527577223243` a number).
const JS_SAFE: u128 = (1u128 << 52) - 1;

/// polkadot-js number `toJSON`: a JS number up to 52 bits, else `0x`
/// hex padded to the type width (`0x000000000000000001e6fe8087a8b33c`).
pub fn json_number(n: u128, bits: u32) -> Json {
    if n <= JS_SAFE {
        Json::Number(serde_json::Number::from(n as u64))
    } else {
        let width = (bits.max(8) / 8) as usize * 2;
        Json::String(format!("0x{:0width$x}", n, width = width))
    }
}

/// polkadot-js `toJSON()` rendering of a decoded value.
pub fn to_json(v: &Value<u32>, types: &Types) -> Json {
    let last = type_path_last(types, v.context);
    match &v.value {
        ValueDef::Primitive(p) => match p {
            Primitive::Bool(b) => Json::Bool(*b),
            Primitive::Char(c) => Json::String(c.to_string()),
            Primitive::String(s) => Json::String(s.clone()),
            Primitive::U128(n) => json_number(*n, primitive_bits(types, v.context).unwrap_or(128)),
            Primitive::I128(n) => Json::Number(serde_json::Number::from(*n as i64)),
            Primitive::U256(b) | Primitive::I256(b) => {
                Json::String(format!("0x{}", hex::encode(b)))
            }
        },
        ValueDef::BitSequence(bits) => Json::String(format!("{bits:?}")),
        ValueDef::Variant(var) => {
            if last == Some("Option") {
                return match &var.values {
                    Composite::Unnamed(vals) if var.name == "Some" && vals.len() == 1 => {
                        to_json(&vals[0], types)
                    }
                    _ => Json::Null,
                };
            }
            if matches!(&var.values, Composite::Unnamed(v) if v.is_empty()) {
                Json::String(var.name.clone())
            } else {
                json!({ lower_first(&var.name): composite_to_json(&var.values, types, false, true) })
            }
        }
        ValueDef::Composite(c) => {
            if last == Some("AccountId32") {
                if let Some(b) = bytes_of(v).filter(|b| b.len() == 32) {
                    let arr: [u8; 32] = b.as_slice().try_into().unwrap_or([0; 32]);
                    return Json::String(ss58_encode_sora(&arr));
                }
            }
            if last == Some("Vote") {
                if let ValueDef::Composite(Composite::Unnamed(vals)) = &v.value {
                    if let Some(Value {
                        value: ValueDef::Primitive(Primitive::U128(n)),
                        ..
                    }) = vals.first()
                    {
                        return Json::String(format!("0x{n:02x}"));
                    }
                }
            }
            composite_to_json(
                c,
                types,
                is_u8_collection(types, v.context).unwrap_or(true),
                is_newtype(types, v.context).unwrap_or(true),
            )
        }
    }
}

fn composite_to_json(c: &Composite<u32>, types: &Types, bytes_ok: bool, collapse: bool) -> Json {
    match c {
        Composite::Named(fields) => {
            let mut m = Map::with_capacity(fields.len());
            for (name, val) in fields {
                m.insert(snake_camel(name), to_json(val, types));
            }
            Json::Object(m)
        }
        Composite::Unnamed(vals) => {
            if let Some(bytes) = vals
                .iter()
                .map(|x| match &x.value {
                    ValueDef::Primitive(Primitive::U128(n)) if *n <= 255 => Some(*n as u8),
                    _ => None,
                })
                .collect::<Option<Vec<u8>>>()
                .filter(|b| bytes_ok && !b.is_empty() && vals.len() > 1)
            {
                return Json::String(format!("0x{}", hex::encode(bytes)));
            }
            if vals.len() == 1 && collapse {
                return to_json(&vals[0], types);
            }
            Json::Array(vals.iter().map(|x| to_json(x, types)).collect())
        }
    }
}

/// Block-level facts for the preimage indexer's `reason` inference.
#[derive(Debug, Default, Clone)]
pub struct PreimageBlockFacts {
    code_updated: bool,
    scheduler_dispatched: bool,
    unnote_manual: bool,
}

impl PreimageBlockFacts {
    /// Note one event of the block.
    pub fn observe(&mut self, ev: &EventDetails<SubstrateConfig>) {
        match (ev.pallet_name(), ev.variant_name()) {
            ("System", "CodeUpdated") => self.code_updated = true,
            ("Scheduler", "Dispatched") | ("Scheduler", "Called") => {
                self.scheduler_dispatched = true
            }
            _ => {}
        }
    }

    /// Note the block's extrinsics (a manual `preimage.unnote_preimage`).
    pub fn observe_extrinsics(
        &mut self,
        extrinsics: &Extrinsics<SubstrateConfig, OnlineClient<SubstrateConfig>>,
    ) {
        for ext in extrinsics.iter() {
            if ext.pallet_name().ok() == Some("Preimage")
                && ext
                    .variant_name()
                    .ok()
                    .map(|v| v.to_lowercase().replace('_', "").contains("unnotepreimage"))
                    .unwrap_or(false)
            {
                self.unnote_manual = true;
            }
        }
    }

    /// Node `inferClearedReason` (`(reason, detail)`), in its priority order.
    pub fn reason(&self) -> Option<(&'static str, &'static str)> {
        if self.code_updated {
            Some((
                "runtime_upgrade",
                "Scheduler ran system.setCode in this block",
            ))
        } else if self.scheduler_dispatched {
            Some((
                "scheduler_dispatched",
                "Consumed by scheduler running a scheduled call",
            ))
        } else if self.unnote_manual {
            Some((
                "unnote_manual",
                "Depositor called preimage.unnotePreimage to reclaim the deposit",
            ))
        } else {
            None
        }
    }
}

/// A `preimage.*` event's hash, when the event belongs to that pallet.
pub fn preimage_event(
    ev: &EventDetails<SubstrateConfig>,
    height: BlockHeight,
    ts_millis: i64,
) -> Option<V2PreimageEvent> {
    if ev.pallet_name() != "Preimage" {
        return None;
    }
    let fields = ev.field_values().ok()?;
    let first = match &fields {
        Composite::Named(f) => f.first().map(|(_, v)| v),
        Composite::Unnamed(v) => v.first(),
    }?;
    let hash = bytes_of(first).filter(|b| b.len() == 32)?;
    let hash_hex = format!("0x{}", hex::encode(hash));
    Some(V2PreimageEvent {
        block_height: height,
        event_index: ev.index(),
        ts_millis,
        method: ev.variant_name().to_string(),
        hash: hash_hex.clone(),
        data: json!([hash_hex]),
        reason: None,
        reason_detail: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn js_numbers_switch_to_padded_hex_above_2_53() {
        assert_eq!(json_number(0, 128), json!(0));
        assert_eq!(json_number(947, 32), json!(947));
        assert_eq!(
            json_number(4_123_527_577_223_243, 128),
            json!(4_123_527_577_223_243u64)
        );
        assert_eq!(
            json_number(4_712_602_945_397_992, 128),
            json!("0x00000000000000000010be16608724e8")
        );
        assert_eq!(
            json_number(0x1e6fe8087a8b33c, 128),
            json!("0x000000000000000001e6fe8087a8b33c")
        );
        assert_eq!(lower_first("SimpleMajority"), "simpleMajority");
    }

    #[test]
    fn cleared_reason_priority_matches_the_indexer() {
        let mut f = PreimageBlockFacts::default();
        assert_eq!(f.reason(), None);
        f.unnote_manual = true;
        assert_eq!(f.reason().map(|r| r.0), Some("unnote_manual"));
        f.scheduler_dispatched = true;
        assert_eq!(f.reason().map(|r| r.0), Some("scheduler_dispatched"));
        f.code_updated = true;
        assert_eq!(f.reason().map(|r| r.0), Some("runtime_upgrade"));
    }
}
