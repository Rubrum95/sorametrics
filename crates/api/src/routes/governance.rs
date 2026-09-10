//! Governance on the legacy contract (`index.js`): the preimage tools,
//! the scheduler agenda, council / technical committee / elections /
//! democracy reads and per-address votes.
//!
//! Proposals are decoded with the pinned metadata (`decodeProposal`:
//! `{ section, method, args, description, remark, innerCalls }`), chain
//! values that the Node returned as `toJSON()` (referendum details,
//! votes, scheduler entries) go through [`to_json`], amounts through
//! `formatChainAmount` (4 dp) and block distances through `blocksToTime`.
//!
//! Preimage history comes from `sm.preimage_events` (the Node's
//! SQLite indexer): `/governance/preimage/:hash/events-fast` and the
//! `firstSeen*` enrichment of `/governance/preimages`; the archive scan
//! `/governance/preimage/:hash/events` walks up to 800 blocks with 30
//! workers like the Node. `/governance/preimages/indexed` and
//! `/governance/preimage/recover/:hash` re-read the `notePreimage`
//! extrinsics indexed in `sm.extrinsics`.

use crate::legacy::fmt_time_es;
use crate::routes::identity::display_names;
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use bigdecimal::{BigDecimal, RoundingMode};
use blake2::{digest::consts::U32, Blake2b, Digest};
use futures::{stream, StreamExt};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as Jv};
use sorametrics_core::chain::ss58_encode_sora;
use sorametrics_substrate::governance::{decode_call, proposal, to_json, Proposal};
use sorametrics_substrate::runtime::sora;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;
use subxt::dynamic::Value as DynValue;
use subxt::ext::scale_value::Value;
use subxt::utils::{AccountId32, H256};
use subxt::{OnlineClient, SubstrateConfig};

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/governance/preimages", get(preimages))
        .route("/governance/scheduler/agenda", get(scheduler_agenda))
        .route(
            "/governance/preimage/:hash/events-fast",
            get(preimage_events_fast),
        )
        .route(
            "/governance/preimage/:hash/referendums",
            get(preimage_referendums),
        )
        .route(
            "/governance/preimage/:hash/decode-pretty",
            get(decode_pretty),
        )
        .route("/governance/preimage/:hash", get(preimage_detail))
        .route("/governance/council", get(council))
        .route("/governance/elections", get(elections))
        .route("/governance/motions", get(motions))
        .route("/governance/democracy", get(democracy))
        .route("/governance/technical-committee", get(technical_committee))
        .route("/governance/votes/:address", get(votes))
}

/// The long scans, on the 120 s router.
pub fn scan_router() -> Router<AppState> {
    Router::new()
        .route(
            "/governance/preimage/:hash/events",
            get(preimage_events_scan),
        )
        .route("/governance/preimage/recover/:hash", get(preimage_recover))
        .route("/governance/preimages/indexed", get(preimages_indexed))
}

const PRETTY_TTL: Duration = Duration::from_secs(3600);
const EVENTS_SCAN_MAX_RANGE: u32 = 800;
const EVENTS_SCAN_CONCURRENCY: usize = 30;
const SUBSTRATE_ZSTD_MAGIC: [u8; 8] = [0x52, 0xbc, 0x53, 0x76, 0x46, 0xdb, 0x8e, 0x05];
const WASM_MAGIC: [u8; 8] = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
/// The Node's dedicated preimage indexer backfilled this many blocks.
const PREIMAGE_BACKFILL_SPAN: i64 = 864_000;

fn chain_err(e: subxt::Error) -> ApiError {
    ApiError::Chain(e.into())
}

/// Node `blocksToTime`.
pub fn blocks_to_time(blocks: i64) -> String {
    let seconds = blocks * 6;
    if seconds < 3600 {
        format!("{}m", ((seconds as f64) / 60.0).round() as i64)
    } else if seconds < 86_400 {
        format!("{:.1}h", seconds as f64 / 3600.0)
    } else {
        format!("{:.1}d", seconds as f64 / 86_400.0)
    }
}

/// Node `formatChainAmount`: `/1e18` with 4 decimals.
pub fn format_chain_amount(raw: u128) -> String {
    let v = (BigDecimal::from(BigInt::from(raw)) / BigDecimal::new(BigInt::from(1), -18))
        .with_scale_round(4, RoundingMode::HalfUp);
    format!("{v:.4}")
}

fn valid_hash(raw: &str) -> Option<String> {
    let h = raw.to_lowercase();
    (h.len() == 66 && h.starts_with("0x") && h[2..].chars().all(|c| c.is_ascii_hexdigit()))
        .then_some(h)
}

fn h256(hash: &str) -> Option<H256> {
    let bytes = hex::decode(hash.strip_prefix("0x")?).ok()?;
    let arr: [u8; 32] = bytes.try_into().ok()?;
    Some(H256(arr))
}

fn blake2_256(bytes: &[u8]) -> String {
    let mut h = Blake2b::<U32>::new();
    h.update(bytes);
    format!("0x{}", hex::encode(h.finalize()))
}

async fn client(state: &AppState) -> Result<OnlineClient<SubstrateConfig>, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    Ok(chain.client().await?)
}

fn decode_proposal(bytes: &[u8], client: &OnlineClient<SubstrateConfig>) -> Option<Proposal> {
    let metadata = client.metadata();
    match decode_call(bytes, &metadata) {
        Ok(v) => Some(proposal(&v, metadata.types())),
        Err(e) => Some(Proposal {
            section: "?".into(),
            method: "?".into(),
            args: json!({}),
            description: format!("Error decoding: {e}"),
            remark: None,
            inner_calls: Vec::new(),
        }),
    }
}

/// Node `resolvePreimage`: `preimage.preimageFor((hash, len))` decoded.
async fn resolve_preimage(
    client: &OnlineClient<SubstrateConfig>,
    hash: &str,
    len: u32,
) -> Option<Proposal> {
    let h = h256(hash)?;
    let bytes = client
        .storage()
        .at_latest()
        .await
        .ok()?
        .fetch(&sora::storage().preimage().preimage_for(h, len))
        .await
        .ok()??;
    decode_proposal(&bytes.0, client)
}

/// One preimage status as the Node reads it (`readPreimageState` body).
#[derive(Serialize, Clone, Default)]
struct PreimageState {
    status: Option<String>,
    len: Option<u32>,
    count: Option<u32>,
    depositor: Option<String>,
    deposit: Option<Jv>,
    #[serde(rename = "bytesAvailable")]
    bytes_available: bool,
}

/// `(kind, body)` of a request status value rendered as `toJSON()`.
fn status_kind_body(v: &Jv) -> (Option<String>, Map<String, Jv>) {
    match v {
        Jv::Object(m) => match m.iter().next() {
            Some((k, Jv::Object(body))) => (Some(k.clone()), body.clone()),
            Some((k, _)) => (Some(k.clone()), Map::new()),
            None => (None, Map::new()),
        },
        Jv::String(s) => (Some(s.clone()), Map::new()),
        _ => (None, Map::new()),
    }
}

fn ticket_of(body: &Map<String, Jv>) -> (Option<String>, Option<Jv>) {
    let ticket = body
        .get("deposit")
        .or_else(|| body.get("ticket"))
        .or_else(|| body.get("maybeTicket"));
    match ticket {
        Some(Jv::Array(t)) => (
            t.first().and_then(|d| d.as_str()).map(str::to_string),
            t.get(1).cloned(),
        ),
        _ => (None, None),
    }
}

fn len_of(body: &Map<String, Jv>) -> Option<u32> {
    body.get("len")
        .or_else(|| body.get("maybeLen"))
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
}

async fn fetch_dynamic(
    client: &OnlineClient<SubstrateConfig>,
    pallet: &str,
    entry: &str,
    keys: Vec<DynValue>,
) -> Result<Option<Jv>, subxt::Error> {
    let at = client.storage().at_latest().await?;
    let addr = subxt::dynamic::storage(pallet, entry, keys);
    match at.fetch(&addr).await? {
        Some(thunk) => {
            let v: Value<u32> = thunk.to_value()?;
            Ok(Some(to_json(&v, client.metadata().types())))
        }
        None => Ok(None),
    }
}

/// Node `readPreimageState`: v2 `requestStatusFor`, then v1
/// `statusFor`, then `preimageFor` for `bytesAvailable`.
async fn read_preimage_state(
    client: &OnlineClient<SubstrateConfig>,
    hash: &str,
    known_len: Option<u32>,
) -> Option<PreimageState> {
    let h = h256(hash)?;
    let key = DynValue::from_bytes(h.0);
    let mut kind = None;
    let mut body = Map::new();
    if let Ok(Some(v)) =
        fetch_dynamic(client, "Preimage", "RequestStatusFor", vec![key.clone()]).await
    {
        let (k, b) = status_kind_body(&v);
        kind = k;
        body = b;
    }
    if kind.is_none() {
        if let Ok(Some(v)) = fetch_dynamic(client, "Preimage", "StatusFor", vec![key]).await {
            let (k, b) = status_kind_body(&v);
            kind = k;
            body = b;
        }
    }
    let len = len_of(&body).or(known_len);
    let mut bytes_available = false;
    if let Some(l) = len {
        if let Ok(at) = client.storage().at_latest().await {
            bytes_available = at
                .fetch(&sora::storage().preimage().preimage_for(h, l))
                .await
                .ok()
                .flatten()
                .is_some();
        }
    }
    let (depositor, deposit) = ticket_of(&body);
    Some(PreimageState {
        status: kind,
        len,
        count: body.get("count").and_then(|c| c.as_u64()).map(|c| c as u32),
        depositor,
        deposit,
        bytes_available,
    })
}

fn upper_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

// ---------------------------------------------------------------------
// /governance/preimages
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct PreimageRow {
    hash: String,
    status: String,
    len: Option<u32>,
    count: Option<u32>,
    depositor: Option<String>,
    deposit: Option<String>,
    #[serde(rename = "firstSeenBlock")]
    first_seen_block: Option<i64>,
    #[serde(rename = "firstSeenTimestamp")]
    first_seen_timestamp: Option<i64>,
}

#[derive(Serialize)]
struct PreimagesResponse {
    preimages: Vec<PreimageRow>,
    identities: BTreeMap<String, String>,
}

/// `String(deposit)` of the `toJSON()` number.
fn deposit_text(v: Option<&Jv>) -> Option<String> {
    match v {
        Some(Jv::Number(n)) => Some(n.to_string()),
        Some(Jv::String(s)) => Some(s.clone()),
        _ => None,
    }
}

async fn enumerate_status(
    client: &OnlineClient<SubstrateConfig>,
    entry: &str,
    seen: &mut BTreeMap<String, (Option<String>, Map<String, Jv>)>,
) -> Result<(), subxt::Error> {
    let at = client.storage().at_latest().await?;
    let addr = subxt::dynamic::storage("Preimage", entry, Vec::<DynValue>::new());
    let mut stream = at.iter(addr).await?;
    while let Some(kv) = stream.next().await {
        let kv = kv?;
        let n = kv.key_bytes.len();
        if n < 32 {
            continue;
        }
        let hash = format!("0x{}", hex::encode(&kv.key_bytes[n - 32..]));
        if seen.contains_key(&hash) {
            continue;
        }
        let v: Value<u32> = kv.value.to_value()?;
        let (kind, body) = status_kind_body(&to_json(&v, client.metadata().types()));
        seen.insert(hash, (kind, body));
    }
    Ok(())
}

async fn preimages(State(state): State<AppState>) -> Result<Json<PreimagesResponse>, ApiError> {
    let client = client(&state).await?;
    let mut seen = BTreeMap::new();
    enumerate_status(&client, "RequestStatusFor", &mut seen)
        .await
        .map_err(chain_err)?;
    enumerate_status(&client, "StatusFor", &mut seen)
        .await
        .map_err(chain_err)?;
    let hashes: Vec<String> = seen.keys().cloned().collect();
    let first_seen = sqlx::query!(
        r#"SELECT DISTINCT ON (hash) hash, block_height, ts FROM sm.preimage_events
           WHERE method = 'Noted' AND hash = ANY($1) ORDER BY hash, block_height ASC"#,
        &hashes
    )
    .fetch_all(&state.db)
    .await?;
    let first: HashMap<String, (i64, Option<i64>)> = first_seen
        .into_iter()
        .map(|r| (r.hash, (r.block_height, r.ts)))
        .collect();
    let mut addresses = HashSet::new();
    let mut rows: Vec<PreimageRow> = seen
        .into_iter()
        .map(|(hash, (kind, body))| {
            let (depositor, deposit) = ticket_of(&body);
            if let Some(d) = &depositor {
                addresses.insert(d.clone());
            }
            let fs = first.get(&hash);
            PreimageRow {
                status: kind
                    .as_deref()
                    .map(upper_first)
                    .unwrap_or_else(|| "Unknown".into()),
                len: len_of(&body),
                count: body.get("count").and_then(|c| c.as_u64()).map(|c| c as u32),
                depositor,
                deposit: deposit_text(deposit.as_ref()),
                first_seen_block: fs.map(|f| f.0),
                first_seen_timestamp: fs.and_then(|f| f.1),
                hash,
            }
        })
        .collect();
    let rank = |s: &str| match s {
        "Requested" => 0,
        "Unrequested" => 1,
        _ => 2,
    };
    rows.sort_by(|a, b| {
        let (at, bt) = (
            a.first_seen_timestamp.unwrap_or(0),
            b.first_seen_timestamp.unwrap_or(0),
        );
        bt.cmp(&at).then(rank(&a.status).cmp(&rank(&b.status)))
    });
    let addrs: Vec<String> = addresses.into_iter().collect();
    let identities = display_names(&state, &addrs).await;
    Ok(Json(PreimagesResponse {
        preimages: rows,
        identities,
    }))
}

// ---------------------------------------------------------------------
// /governance/scheduler/agenda
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct AgendaEntry {
    block: u32,
    #[serde(rename = "blocksRemaining")]
    blocks_remaining: u32,
    #[serde(rename = "secondsRemaining")]
    seconds_remaining: u32,
    slot: usize,
    #[serde(rename = "maybeId")]
    maybe_id: Jv,
    priority: Jv,
    origin: Jv,
    #[serde(rename = "lookupHash")]
    lookup_hash: Option<String>,
    #[serde(rename = "lookupLen")]
    lookup_len: Option<u32>,
    #[serde(rename = "inlineDecoded")]
    inline_decoded: Option<Proposal>,
    preimage: Option<PreimageState>,
    alert: bool,
}

#[derive(Serialize)]
struct AgendaResponse {
    tip: u32,
    entries: Vec<AgendaEntry>,
}

async fn head_number(state: &AppState) -> Result<u32, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    Ok(chain
        .legacy_rpc()
        .await?
        .chain_get_header(None)
        .await
        .map_err(chain_err)?
        .map(|h| h.number)
        .unwrap_or(0))
}

async fn scheduler_agenda(State(state): State<AppState>) -> Result<Json<AgendaResponse>, ApiError> {
    let client = client(&state).await?;
    let tip = head_number(&state).await?;
    let at = client.storage().at_latest().await.map_err(chain_err)?;
    let addr = subxt::dynamic::storage("Scheduler", "Agenda", Vec::<DynValue>::new());
    let mut stream = at.iter(addr).await.map_err(chain_err)?;
    struct Raw {
        block: u32,
        slot: usize,
        maybe_id: Jv,
        priority: Jv,
        origin: Jv,
        lookup_hash: Option<String>,
        lookup_len: Option<u32>,
        inline: Option<Vec<u8>>,
    }
    let mut raw_list = Vec::new();
    let mut len_by_hash: HashMap<String, u32> = HashMap::new();
    while let Some(kv) = stream.next().await {
        let kv = kv.map_err(chain_err)?;
        let n = kv.key_bytes.len();
        if n < 4 {
            continue;
        }
        let block = u32::from_le_bytes(kv.key_bytes[n - 4..].try_into().unwrap_or([0; 4]));
        if block < tip {
            continue;
        }
        let v: Value<u32> = kv.value.to_value().map_err(|e| chain_err(e.into()))?;
        let list = to_json(&v, client.metadata().types());
        let Jv::Array(items) = list else { continue };
        for (slot, s) in items.iter().enumerate() {
            let Jv::Object(s) = s else { continue };
            let call = s.get("call");
            let mut lookup_hash = None;
            let mut lookup_len = None;
            let mut inline = None;
            if let Some(Jv::Object(c)) = call {
                if let Some(Jv::Object(l)) = c.get("lookup").or_else(|| c.get("Lookup")) {
                    lookup_hash = l
                        .get("hash")
                        .and_then(|h| h.as_str())
                        .map(|h| h.to_lowercase());
                    lookup_len = l.get("len").and_then(|l| l.as_u64()).map(|l| l as u32);
                }
                if let Some(Jv::String(hexs)) = c.get("inline").or_else(|| c.get("Inline")) {
                    inline = hex::decode(hexs.trim_start_matches("0x")).ok();
                }
            }
            if let (Some(h), Some(l)) = (&lookup_hash, lookup_len) {
                len_by_hash.insert(h.clone(), l);
            }
            raw_list.push(Raw {
                block,
                slot,
                maybe_id: s.get("maybeId").cloned().unwrap_or(Jv::Null),
                priority: s.get("priority").cloned().unwrap_or(Jv::Null),
                origin: s.get("origin").cloned().unwrap_or(Jv::Null),
                lookup_hash,
                lookup_len,
                inline,
            });
        }
    }
    let mut hash_status: HashMap<String, PreimageState> = HashMap::new();
    for h in raw_list
        .iter()
        .filter_map(|r| r.lookup_hash.clone())
        .collect::<HashSet<_>>()
    {
        let st = read_preimage_state(&client, &h, len_by_hash.get(&h).copied())
            .await
            .unwrap_or_default();
        hash_status.insert(h, st);
    }
    let mut entries: Vec<AgendaEntry> = raw_list
        .into_iter()
        .map(|r| {
            let blocks_remaining = r.block.saturating_sub(tip);
            let preimage = r
                .lookup_hash
                .as_ref()
                .and_then(|h| hash_status.get(h).cloned());
            let alert =
                r.lookup_hash.is_some() && preimage.as_ref().is_some_and(|p| !p.bytes_available);
            AgendaEntry {
                block: r.block,
                blocks_remaining,
                seconds_remaining: blocks_remaining * 6,
                slot: r.slot,
                maybe_id: r.maybe_id,
                priority: r.priority,
                origin: r.origin,
                lookup_hash: r.lookup_hash,
                lookup_len: r.lookup_len,
                inline_decoded: r
                    .inline
                    .as_deref()
                    .and_then(|b| decode_proposal(b, &client)),
                preimage,
                alert,
            }
        })
        .collect();
    entries.sort_by_key(|e| e.block);
    Ok(Json(AgendaResponse { tip, entries }))
}

// ---------------------------------------------------------------------
// /governance/preimage/:hash (detail), /referendums, /events-fast, /decode-pretty
// ---------------------------------------------------------------------

#[derive(Deserialize)]
struct LenQuery {
    len: Option<u32>,
}

#[derive(Serialize)]
struct PreimageDetail {
    hash: String,
    len: u32,
    decoded: Option<Proposal>,
}

async fn preimage_detail(
    State(state): State<AppState>,
    Path(raw): Path<String>,
    Query(q): Query<LenQuery>,
) -> Result<Json<PreimageDetail>, ApiError> {
    let hash = valid_hash(&raw).ok_or_else(|| ApiError::BadRequest("Invalid hash".into()))?;
    let len = q.len.unwrap_or(0);
    let client = client(&state).await?;
    let decoded = resolve_preimage(&client, &hash, len).await;
    Ok(Json(PreimageDetail {
        hash: raw,
        len,
        decoded,
    }))
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<u32>,
}

#[derive(Serialize)]
struct RefMatch {
    id: u32,
    status: String,
    detail: Jv,
}

#[derive(Serialize)]
struct ReferendumsResponse {
    hash: String,
    scanned: u32,
    #[serde(rename = "fromId")]
    from_id: u32,
    #[serde(rename = "toId")]
    to_id: i64,
    matches: Vec<RefMatch>,
}

/// `(status, detail)` of a `ReferendumInfo` rendered as `toJSON()`.
fn referendum_status(info: &Jv) -> (String, Jv) {
    match info {
        Jv::Object(m) => m
            .iter()
            .next()
            .map(|(k, v)| (k.clone(), v.clone()))
            .unwrap_or(("unknown".into(), json!({}))),
        _ => ("unknown".into(), json!({})),
    }
}

fn proposal_lookup_hash(detail: &Jv) -> Option<String> {
    let prop = detail.get("proposal")?;
    match prop {
        Jv::Object(p) => p
            .get("lookup")
            .or_else(|| p.get("Lookup"))
            .and_then(|l| l.get("hash"))
            .and_then(|h| h.as_str())
            .map(|h| h.to_lowercase()),
        Jv::String(s) => Some(s.to_lowercase()),
        _ => None,
    }
}

async fn referendum_info(client: &OnlineClient<SubstrateConfig>, id: u32) -> Option<Jv> {
    fetch_dynamic(
        client,
        "Democracy",
        "ReferendumInfoOf",
        vec![DynValue::u128(u128::from(id))],
    )
    .await
    .ok()
    .flatten()
}

async fn preimage_referendums(
    State(state): State<AppState>,
    Path(raw): Path<String>,
    Query(q): Query<LimitQuery>,
) -> Result<Json<ReferendumsResponse>, ApiError> {
    let hash = valid_hash(&raw).ok_or_else(|| ApiError::BadRequest("Invalid hash".into()))?;
    let limit = q.limit.filter(|l| *l > 0).unwrap_or(50).min(200);
    let client = client(&state).await?;
    let count = client
        .storage()
        .at_latest()
        .await
        .map_err(chain_err)?
        .fetch(&sora::storage().democracy().referendum_count())
        .await
        .map_err(chain_err)?
        .unwrap_or(0);
    let from = count.saturating_sub(limit);
    let mut matches = Vec::new();
    for i in (from..count).rev() {
        let Some(info) = referendum_info(&client, i).await else {
            continue;
        };
        let (status, detail) = referendum_status(&info);
        if proposal_lookup_hash(&detail).as_deref() == Some(hash.as_str()) {
            matches.push(RefMatch {
                id: i,
                status,
                detail,
            });
        }
    }
    Ok(Json(ReferendumsResponse {
        hash,
        scanned: count - from,
        from_id: from,
        to_id: i64::from(count) - 1,
        matches,
    }))
}

#[derive(Deserialize)]
struct RangeQuery {
    from: Option<i64>,
    to: Option<i64>,
}

#[derive(Serialize)]
struct FastEvent {
    block: i64,
    event: String,
    hash: String,
    timestamp: Option<i64>,
    data: Jv,
    reason: Option<String>,
    reason_detail: Option<String>,
}

#[derive(Serialize)]
struct IndexerState {
    #[serde(rename = "liveLastBlock")]
    live_last_block: Option<i64>,
    #[serde(rename = "backfillCursor")]
    backfill_cursor: Option<i64>,
    #[serde(rename = "backfillComplete")]
    backfill_complete: bool,
}

#[derive(Serialize)]
struct FastResponse {
    hash: String,
    count: usize,
    events: Vec<FastEvent>,
    indexer: IndexerState,
}

async fn preimage_events_fast(
    State(state): State<AppState>,
    Path(raw): Path<String>,
    Query(q): Query<RangeQuery>,
) -> Result<Json<FastResponse>, ApiError> {
    let hash = valid_hash(&raw).ok_or_else(|| ApiError::BadRequest("Invalid hash".into()))?;
    let rows = sqlx::query!(
        r#"SELECT block_height, event_index, ts, method, hash, data, reason, reason_detail
           FROM sm.preimage_events
           WHERE hash = $1 AND ($2::bigint IS NULL OR block_height >= $2) AND ($3::bigint IS NULL OR block_height <= $3)
           ORDER BY block_height ASC, event_index ASC"#,
        hash,
        q.from,
        q.to
    )
    .fetch_all(&state.db)
    .await?;
    let bounds = sqlx::query!(
        r#"SELECT MIN(block_height) AS "min", MAX(block_height) AS "max" FROM sm.preimage_events"#
    )
    .fetch_one(&state.db)
    .await?;
    let events: Vec<FastEvent> = rows
        .into_iter()
        .map(|r| FastEvent {
            block: r.block_height,
            event: format!("preimage.{}", r.method),
            hash: r.hash,
            timestamp: r.ts,
            data: r.data.unwrap_or(Jv::Null),
            reason: r.reason,
            reason_detail: r.reason_detail,
        })
        .collect();
    Ok(Json(FastResponse {
        hash,
        count: events.len(),
        events,
        indexer: IndexerState {
            live_last_block: bounds.max,
            backfill_cursor: bounds.min,
            backfill_complete: matches!((bounds.min, bounds.max), (Some(lo), Some(hi)) if lo <= hi - PREIMAGE_BACKFILL_SPAN),
        },
    }))
}

/// `runtime_version` custom section of a WASM runtime (Node `extractRuntimeVersion`).
pub fn extract_runtime_version(wasm: &[u8]) -> Option<Jv> {
    const NAME: &[u8] = b"runtime_version";
    fn read_compact(buf: &[u8], off: usize) -> Option<(usize, usize)> {
        let first = *buf.get(off)? as usize;
        match first & 0x03 {
            0 => Some((first >> 2, 1)),
            1 => Some(((first | ((*buf.get(off + 1)? as usize) << 8)) >> 2, 2)),
            2 => Some((
                (first
                    | ((*buf.get(off + 1)? as usize) << 8)
                    | ((*buf.get(off + 2)? as usize) << 16)
                    | ((*buf.get(off + 3)? as usize) << 24))
                    >> 2,
                4,
            )),
            _ => None,
        }
    }
    let mut start = 0usize;
    while let Some(pos) = wasm[start..].windows(NAME.len()).position(|w| w == NAME) {
        let off = start + pos;
        if off >= 1 && wasm[off - 1] as usize == NAME.len() {
            let mut cursor = off + NAME.len();
            if let Some((spec_len, n1)) = read_compact(wasm, cursor) {
                cursor += n1;
                let spec_name =
                    String::from_utf8_lossy(wasm.get(cursor..cursor + spec_len)?).to_string();
                cursor += spec_len;
                if let Some((impl_len, n2)) = read_compact(wasm, cursor) {
                    cursor += n2;
                    let impl_name =
                        String::from_utf8_lossy(wasm.get(cursor..cursor + impl_len)?).to_string();
                    cursor += impl_len;
                    let u32_at = |c: usize| {
                        wasm.get(c..c + 4)
                            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
                    };
                    if let (Some(authoring), Some(spec), Some(impl_v)) =
                        (u32_at(cursor), u32_at(cursor + 4), u32_at(cursor + 8))
                    {
                        if spec_name == "sora-substrate" && spec > 0 && spec < 100_000 {
                            return Some(json!({
                                "specName": spec_name, "implName": impl_name,
                                "authoringVersion": authoring, "specVersion": spec, "implVersion": impl_v
                            }));
                        }
                    }
                }
            }
        }
        start = off + 1;
    }
    None
}

async fn preimage_bytes(
    client: &OnlineClient<SubstrateConfig>,
    hash: &str,
    len: u32,
) -> Option<Vec<u8>> {
    let h = h256(hash)?;
    client
        .storage()
        .at_latest()
        .await
        .ok()?
        .fetch(&sora::storage().preimage().preimage_for(h, len))
        .await
        .ok()?
        .map(|b| b.0)
}

async fn decode_pretty(
    State(state): State<AppState>,
    Path(raw): Path<String>,
    Query(q): Query<LenQuery>,
) -> Result<Json<Jv>, ApiError> {
    let hash = valid_hash(&raw).ok_or_else(|| ApiError::BadRequest("bad_hash".into()))?;
    let key = format!("gov:pretty:{hash}");
    if let Some(v) = state.cached_scan(&key, PRETTY_TTL).await {
        return Ok(Json(v));
    }
    let client = client(&state).await?;
    let bytes = preimage_bytes(&client, &hash, q.len.unwrap_or(0))
        .await
        .ok_or_else(|| ApiError::NotFound(format!("preimage_not_found {hash}")))?;
    let integrity = if blake2_256(&bytes) == hash {
        "match"
    } else {
        "mismatch"
    };
    let metadata = client.metadata();
    let call = decode_call(&bytes, &metadata)
        .map_err(|e| ApiError::Internal(format!("decode_failed: {e}")))?;
    let p = proposal(&call, metadata.types());
    let is_upgrade =
        p.section == "system" && (p.method == "setCode" || p.method == "setCodeWithoutChecks");
    let payload = if !is_upgrade {
        json!({ "hash": hash, "kind": "generic", "section": p.section, "method": p.method, "args": p.args, "integrity": integrity })
    } else {
        let code_hex = p.args.get("code").and_then(|c| c.as_str()).unwrap_or("0x");
        let code = hex::decode(code_hex.trim_start_matches("0x")).unwrap_or_default();
        if code.get(..8) != Some(&SUBSTRATE_ZSTD_MAGIC) {
            json!({ "hash": hash, "kind": "runtime_upgrade", "section": p.section, "method": p.method,
                    "compressedBytes": code.len(), "wasmMagicOk": code.get(..8) == Some(&WASM_MAGIC),
                    "integrity": integrity, "note": "not_zstd_wrapped" })
        } else {
            let wasm = zstd::decode_all(&code[8..])
                .map_err(|e| ApiError::Internal(format!("decode_failed: zstd {e}")))?;
            let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
            let rv = chain
                .legacy_rpc()
                .await?
                .state_get_runtime_version(None)
                .await
                .map_err(chain_err)?;
            let other = |k: &str| rv.other.get(k).cloned().unwrap_or(Jv::Null);
            json!({
                "hash": hash, "kind": "runtime_upgrade", "section": p.section, "method": p.method,
                "current": { "specName": other("specName"), "specVersion": rv.spec_version,
                             "implVersion": other("implVersion"), "authoringVersion": other("authoringVersion") },
                "target": extract_runtime_version(&wasm),
                "compressedBytes": code.len(), "decompressedBytes": wasm.len(),
                "wasmMagicOk": wasm.get(..8) == Some(&WASM_MAGIC), "integrity": integrity
            })
        }
    };
    state.store_scan(&key, payload.clone()).await;
    Ok(Json(payload))
}

// ---------------------------------------------------------------------
// /governance/preimage/:hash/events (archive scan)
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct ScanHit {
    block: u32,
    event: String,
    data: Jv,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct ScanResponse {
    hash: String,
    from: u32,
    to: u32,
    tip: u32,
    scanned: u32,
    count: usize,
    events: Vec<ScanHit>,
    archive: String,
}

async fn scan_block(
    client: &OnlineClient<SubstrateConfig>,
    chain: &crate::chain::ChainClient,
    n: u32,
    target: &str,
) -> Vec<ScanHit> {
    let mut hits = Vec::new();
    let Ok(legacy) = chain.legacy_rpc().await else {
        return hits;
    };
    let Ok(Some(hash)) = legacy.chain_get_block_hash(Some(n.into())).await else {
        return hits;
    };
    let Ok(block) = client.blocks().at(hash).await else {
        return hits;
    };
    let Ok(events) = block.events().await else {
        return hits;
    };
    for ev in events.iter().flatten() {
        if ev.pallet_name() != "Preimage" {
            continue;
        }
        let Ok(fields) = ev.field_values() else {
            continue;
        };
        let v = Value {
            value: subxt::ext::scale_value::ValueDef::Composite(fields),
            context: 0,
        };
        let data = to_json(&v, client.metadata().types());
        let first = match &data {
            Jv::Array(a) => a.first().and_then(|x| x.as_str()).map(|s| s.to_lowercase()),
            Jv::Object(o) => o
                .values()
                .next()
                .and_then(|x| x.as_str())
                .map(|s| s.to_lowercase()),
            _ => None,
        };
        if first.as_deref() != Some(target) {
            continue;
        }
        let data = match data {
            Jv::Object(o) => Jv::Array(o.into_iter().map(|(_, v)| v).collect()),
            d => d,
        };
        hits.push(ScanHit {
            block: n,
            event: format!("preimage.{}", ev.variant_name()),
            data,
            timestamp: None,
        });
    }
    if !hits.is_empty() {
        if let Ok(exts) = block.extrinsics().await {
            for ext in exts.iter() {
                if let Ok(Some(set)) = ext.as_extrinsic::<sora::timestamp::calls::types::Set>() {
                    for h in hits.iter_mut() {
                        h.timestamp = Some(set.now as i64);
                    }
                    break;
                }
            }
        }
    }
    hits
}

async fn preimage_events_scan(
    State(state): State<AppState>,
    Path(raw): Path<String>,
    Query(q): Query<RangeQuery>,
) -> Result<Json<ScanResponse>, ApiError> {
    let hash = valid_hash(&raw).ok_or_else(|| ApiError::BadRequest("Invalid hash".into()))?;
    let tip = head_number(&state).await?;
    let to = q.to.map(|t| (t.max(0) as u32).min(tip)).unwrap_or(tip);
    let from = q
        .from
        .map(|f| f.max(0) as u32)
        .unwrap_or_else(|| to.saturating_sub(EVENTS_SCAN_MAX_RANGE - 1));
    if to < from {
        return Err(ApiError::BadRequest("to < from".into()));
    }
    if to - from + 1 > EVENTS_SCAN_MAX_RANGE {
        return Err(ApiError::BadRequest(format!(
            "range too large ({}), max={EVENTS_SCAN_MAX_RANGE}. Narrow to/from.",
            to - from + 1
        )));
    }
    let key = format!("gov:scan:{hash}:{from}:{to}");
    if let Some(v) = state.cached_scan(&key, Duration::from_secs(3600)).await {
        return serde_json::from_value(v)
            .map(Json)
            .map_err(|e| ApiError::Internal(e.to_string()));
    }
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let client = chain.client().await?;
    let archive = chain
        .active_endpoint()
        .await
        .map(|u| u.to_string())
        .unwrap_or_default();
    let target = hash.clone();
    let mut hits: Vec<ScanHit> = stream::iter((from..=to).rev())
        .map(|n| {
            let client = client.clone();
            let chain = chain.clone();
            let target = target.clone();
            async move { scan_block(&client, &chain, n, &target).await }
        })
        .buffer_unordered(EVENTS_SCAN_CONCURRENCY)
        .collect::<Vec<Vec<ScanHit>>>()
        .await
        .into_iter()
        .flatten()
        .collect();
    hits.sort_by_key(|h| h.block);
    let payload = ScanResponse {
        hash,
        from,
        to,
        tip,
        scanned: to - from + 1,
        count: hits.len(),
        events: hits,
        archive,
    };
    if let Ok(v) = serde_json::to_value(&payload) {
        state.store_scan(&key, v).await;
    }
    Ok(Json(payload))
}

// ---------------------------------------------------------------------
// /governance/preimages/indexed, /governance/preimage/recover/:hash
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct Note {
    block: i64,
    timestamp: String,
    #[serde(rename = "extrinsicMethod")]
    extrinsic_method: String,
    signer: String,
    hash: String,
    bytes: String,
    #[serde(rename = "bytesLen")]
    bytes_len: usize,
    decoded: Option<Proposal>,
    #[serde(rename = "stillOnChain")]
    still_on_chain: bool,
}

/// `notePreimage` extrinsics of a block: `(method, signer, bytes)`.
async fn note_extrinsics(
    client: &OnlineClient<SubstrateConfig>,
    chain: &crate::chain::ChainClient,
    n: i64,
) -> Result<Vec<(String, String, Vec<u8>)>, ApiError> {
    let legacy = chain.legacy_rpc().await?;
    let Some(hash) = legacy
        .chain_get_block_hash(Some((n as u32).into()))
        .await
        .map_err(chain_err)?
    else {
        return Ok(Vec::new());
    };
    let block = client.blocks().at(hash).await.map_err(chain_err)?;
    let exts = block.extrinsics().await.map_err(chain_err)?;
    let mut out = Vec::new();
    for ext in exts.iter() {
        if ext.pallet_name().ok() != Some("Preimage") {
            continue;
        }
        let method = ext.variant_name().unwrap_or("").to_string();
        if !method
            .to_lowercase()
            .replace('_', "")
            .starts_with("notepreimage")
        {
            continue;
        }
        let bytes =
            if let Ok(Some(c)) = ext.as_extrinsic::<sora::preimage::calls::types::NotePreimage>() {
                c.bytes
            } else {
                continue;
            };
        let signer = ext
            .address_bytes()
            .and_then(|b| {
                let raw = if b.len() == 33 && b[0] == 0 {
                    &b[1..]
                } else {
                    b
                };
                <[u8; 32]>::try_from(raw).ok()
            })
            .map(|a| ss58_encode_sora(&a))
            .unwrap_or_default();
        out.push((
            format!(
                "preimage.{}",
                sorametrics_substrate::extrinsics::snake_camel(&method)
            ),
            signer,
            bytes,
        ));
    }
    Ok(out)
}

async fn note_blocks(
    state: &AppState,
) -> Result<Vec<(i64, chrono::DateTime<chrono::Utc>)>, ApiError> {
    Ok(sqlx::query!(
        r#"SELECT DISTINCT block_height, block_timestamp FROM sm.extrinsics
           WHERE section = 'preimage' AND method IN ('notePreimage', 'notePreimageOperational')
           ORDER BY block_height DESC"#
    )
    .fetch_all(&state.db)
    .await?
    .into_iter()
    .map(|r| (r.block_height, r.block_timestamp))
    .collect())
}

#[derive(Serialize)]
struct IndexedResponse {
    count: usize,
    notes: Vec<Jv>,
}

async fn preimages_indexed(
    State(state): State<AppState>,
) -> Result<Json<IndexedResponse>, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let client = chain.client().await?;
    let mut notes = Vec::new();
    for (block, ts) in note_blocks(&state).await? {
        match note_extrinsics(&client, chain, block).await {
            Ok(list) => {
                for (method, signer, bytes) in list {
                    let hash = blake2_256(&bytes);
                    let still = read_preimage_state(&client, &hash, Some(bytes.len() as u32))
                        .await
                        .is_some_and(|s| s.status.is_some());
                    let n = Note {
                        block,
                        timestamp: fmt_time_es(ts, state.time_zone),
                        extrinsic_method: method,
                        signer,
                        hash,
                        bytes: format!("0x{}", hex::encode(&bytes)),
                        bytes_len: bytes.len(),
                        decoded: decode_proposal(&bytes, &client),
                        still_on_chain: still,
                    };
                    notes.push(serde_json::to_value(n).unwrap_or(Jv::Null));
                }
            }
            Err(e) => notes.push(json!({ "block": block, "error": e.to_string() })),
        }
    }
    Ok(Json(IndexedResponse {
        count: notes.len(),
        notes,
    }))
}

async fn preimage_recover(
    State(state): State<AppState>,
    Path(raw): Path<String>,
) -> Result<Json<Jv>, ApiError> {
    let hash = valid_hash(&raw).ok_or_else(|| ApiError::BadRequest("Invalid hash".into()))?;
    let key = format!("gov:recover:{hash}");
    if let Some(v) = state.cached_scan(&key, Duration::from_secs(3600)).await {
        return Ok(Json(v));
    }
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let client = chain.client().await?;
    let archive = chain
        .active_endpoint()
        .await
        .map(|u| u.to_string())
        .unwrap_or_default();
    let blocks = note_blocks(&state).await?;
    for (block, ts) in &blocks {
        let Ok(list) = note_extrinsics(&client, chain, *block).await else {
            continue;
        };
        for (method, signer, bytes) in list {
            if blake2_256(&bytes) != hash {
                continue;
            }
            let payload = json!({
                "found": true, "hash": hash, "bytes": format!("0x{}", hex::encode(&bytes)),
                "bytesLen": bytes.len(), "decoded": decode_proposal(&bytes, &client),
                "block": block, "timestamp": fmt_time_es(*ts, state.time_zone), "signer": signer,
                "extrinsicMethod": method, "source": format!("indexed extrinsics + archive RPC ({})", archive.trim_end_matches('/'))
            });
            state.store_scan(&key, payload.clone()).await;
            return Ok(Json(payload));
        }
    }
    Ok(Json(json!({
        "found": false, "hash": hash, "scannedBlocks": blocks.len(),
        "message": "Not found in indexed notePreimage extrinsics (rolling ~30 days). Older preimages require an archive-node scan."
    })))
}

// ---------------------------------------------------------------------
// Council / technical committee / elections / motions / democracy / votes
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct Member {
    address: String,
    identity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stake: Option<String>,
    #[serde(rename = "isPrime")]
    is_prime: bool,
}

#[derive(Serialize)]
struct MembersResponse {
    members: Vec<Member>,
    prime: Option<String>,
    identities: BTreeMap<String, String>,
}

async fn council(State(state): State<AppState>) -> Result<Json<MembersResponse>, ApiError> {
    let client = client(&state).await?;
    let at = client.storage().at_latest().await.map_err(chain_err)?;
    let members = at
        .fetch(&sora::storage().council().members())
        .await
        .map_err(chain_err)?
        .unwrap_or_default();
    let prime = at
        .fetch(&sora::storage().council().prime())
        .await
        .map_err(chain_err)?
        .map(|p| ss58_encode_sora(&p.0));
    let elected = at
        .fetch(&sora::storage().elections_phragmen().members())
        .await
        .map_err(chain_err)?
        .unwrap_or_default();
    let stake: HashMap<String, String> = elected
        .iter()
        .map(|s| (ss58_encode_sora(&s.who.0), format_chain_amount(s.stake)))
        .collect();
    let addresses: Vec<String> = members.iter().map(|m| ss58_encode_sora(&m.0)).collect();
    let identities = display_names(&state, &addresses).await;
    let rows = addresses
        .iter()
        .map(|a| Member {
            address: a.clone(),
            identity: identities.get(a).cloned(),
            stake: Some(stake.get(a).cloned().unwrap_or_else(|| "0".into())),
            is_prime: prime.as_deref() == Some(a),
        })
        .collect();
    Ok(Json(MembersResponse {
        members: rows,
        prime,
        identities,
    }))
}

async fn technical_committee(
    State(state): State<AppState>,
) -> Result<Json<MembersResponse>, ApiError> {
    let client = client(&state).await?;
    let at = client.storage().at_latest().await.map_err(chain_err)?;
    let members = at
        .fetch(&sora::storage().technical_committee().members())
        .await
        .map_err(chain_err)?
        .unwrap_or_default();
    let prime = at
        .fetch(&sora::storage().technical_committee().prime())
        .await
        .map_err(chain_err)?
        .map(|p| ss58_encode_sora(&p.0));
    let addresses: Vec<String> = members.iter().map(|m| ss58_encode_sora(&m.0)).collect();
    let identities = display_names(&state, &addresses).await;
    let rows = addresses
        .iter()
        .map(|a| Member {
            address: a.clone(),
            identity: identities.get(a).cloned(),
            stake: None,
            is_prime: prime.as_deref() == Some(a),
        })
        .collect();
    Ok(Json(MembersResponse {
        members: rows,
        prime,
        identities,
    }))
}

#[derive(Serialize)]
struct Seat {
    address: String,
    stake: String,
}

#[derive(Serialize)]
struct Candidate {
    address: String,
    deposit: String,
}

#[derive(Serialize)]
struct ElectionsResponse {
    elected: Vec<Seat>,
    candidates: Vec<Candidate>,
    #[serde(rename = "runnersUp")]
    runners_up: Vec<Seat>,
    #[serde(rename = "electionRounds")]
    election_rounds: u32,
    #[serde(rename = "currentBlock")]
    current_block: u32,
    #[serde(rename = "termDuration")]
    term_duration: u32,
    #[serde(rename = "desiredMembers")]
    desired_members: u32,
    #[serde(rename = "candidacyBond")]
    candidacy_bond: String,
    #[serde(rename = "blocksUntilElection")]
    blocks_until_election: u32,
    #[serde(rename = "timeUntilElection")]
    time_until_election: String,
    identities: BTreeMap<String, String>,
}

async fn elections(State(state): State<AppState>) -> Result<Json<ElectionsResponse>, ApiError> {
    let client = client(&state).await?;
    let at = client.storage().at_latest().await.map_err(chain_err)?;
    let s = sora::storage().elections_phragmen();
    let members = at
        .fetch(&s.members())
        .await
        .map_err(chain_err)?
        .unwrap_or_default();
    let candidates = at
        .fetch(&s.candidates())
        .await
        .map_err(chain_err)?
        .unwrap_or_default();
    let runners = at
        .fetch(&s.runners_up())
        .await
        .map_err(chain_err)?
        .unwrap_or_default();
    let rounds = at
        .fetch(&s.election_rounds())
        .await
        .map_err(chain_err)?
        .unwrap_or(0);
    let current_block = head_number(&state).await?;
    let c = client.constants();
    let k = sora::constants().elections_phragmen();
    let term_duration: u32 = c.at(&k.term_duration()).unwrap_or(0);
    let desired_members: u32 = c.at(&k.desired_members()).unwrap_or(0);
    let candidacy_bond = c
        .at(&k.candidacy_bond())
        .map(format_chain_amount)
        .unwrap_or_else(|_| "0".into());
    let blocks_until = if term_duration > 0 {
        term_duration - (current_block % term_duration)
    } else {
        0
    };
    let elected: Vec<Seat> = members
        .iter()
        .map(|m| Seat {
            address: ss58_encode_sora(&m.who.0),
            stake: format_chain_amount(m.stake),
        })
        .collect();
    let cands: Vec<Candidate> = candidates
        .iter()
        .map(|(a, d)| Candidate {
            address: ss58_encode_sora(&a.0),
            deposit: format_chain_amount(*d),
        })
        .collect();
    let runners_up: Vec<Seat> = runners
        .iter()
        .map(|m| Seat {
            address: ss58_encode_sora(&m.who.0),
            stake: format_chain_amount(m.stake),
        })
        .collect();
    let all: Vec<String> = elected
        .iter()
        .map(|e| e.address.clone())
        .chain(cands.iter().map(|c| c.address.clone()))
        .chain(runners_up.iter().map(|r| r.address.clone()))
        .collect();
    let identities = display_names(&state, &all).await;
    Ok(Json(ElectionsResponse {
        elected,
        candidates: cands,
        runners_up,
        election_rounds: rounds,
        current_block,
        term_duration,
        desired_members,
        candidacy_bond,
        blocks_until_election: blocks_until,
        time_until_election: blocks_to_time(i64::from(blocks_until)),
        identities,
    }))
}

#[derive(Serialize)]
struct Motion {
    hash: String,
    index: Option<u32>,
    decoded: Option<Proposal>,
    #[serde(rename = "resolvedProposal")]
    resolved_proposal: Option<Proposal>,
    voting: Option<Jv>,
    #[serde(rename = "blocksRemaining")]
    blocks_remaining: u32,
    #[serde(rename = "timeRemaining")]
    time_remaining: String,
}

async fn collective_motions(
    state: &AppState,
    client: &OnlineClient<SubstrateConfig>,
    pallet: &str,
    current_block: u32,
) -> Result<(Vec<Motion>, BTreeMap<String, String>), ApiError> {
    let at = client.storage().at_latest().await.map_err(chain_err)?;
    let hashes = match fetch_dynamic(client, pallet, "Proposals", Vec::new())
        .await
        .map_err(chain_err)?
    {
        Some(Jv::Array(a)) => a
            .into_iter()
            .filter_map(|h| h.as_str().map(|s| s.to_lowercase()))
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    let mut motions = Vec::new();
    let mut addresses = HashSet::new();
    for hash in hashes {
        let Some(h) = h256(&hash) else { continue };
        let raw_addr =
            subxt::dynamic::storage(pallet, "ProposalOf", vec![DynValue::from_bytes(h.0)]);
        let key = client
            .storage()
            .address_bytes(&raw_addr)
            .map_err(chain_err)?;
        let bytes = at.fetch_raw(key).await.map_err(chain_err)?;
        let decoded = bytes.as_deref().and_then(|b| decode_proposal(b, client));
        let voting = fetch_dynamic(client, pallet, "Voting", vec![DynValue::from_bytes(h.0)])
            .await
            .map_err(chain_err)?;
        let mut resolved = None;
        if let Some(d) = &decoded {
            let arg = d
                .args
                .get("proposal")
                .or_else(|| d.args.get("proposal_hash"));
            if let Some(Jv::Object(p)) = arg {
                if let Some(Jv::Object(l)) = p.get("Lookup").or_else(|| p.get("lookup")) {
                    let lh = l
                        .get("hash_")
                        .or_else(|| l.get("hash"))
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_lowercase();
                    let ll = l
                        .get("len")
                        .and_then(|x| x.as_str())
                        .map(|s| s.replace(',', ""))
                        .and_then(|s| s.parse::<u32>().ok())
                        .or_else(|| l.get("len").and_then(|x| x.as_u64()).map(|x| x as u32))
                        .unwrap_or(0);
                    resolved = resolve_preimage(client, &lh, ll).await;
                }
            }
        }
        let mut blocks_remaining = 0;
        let mut index = None;
        if let Some(Jv::Object(v)) = &voting {
            for k in ["ayes", "nays"] {
                if let Some(Jv::Array(a)) = v.get(k) {
                    for x in a {
                        if let Some(s) = x.as_str() {
                            addresses.insert(s.to_string());
                        }
                    }
                }
            }
            if let Some(end) = v.get("end").and_then(|e| e.as_u64()) {
                blocks_remaining = (end as u32).saturating_sub(current_block);
            }
            index = v.get("index").and_then(|i| i.as_u64()).map(|i| i as u32);
        }
        motions.push(Motion {
            hash,
            index,
            decoded,
            resolved_proposal: resolved,
            voting,
            blocks_remaining,
            time_remaining: blocks_to_time(i64::from(blocks_remaining)),
        });
    }
    let addrs: Vec<String> = addresses.into_iter().collect();
    let identities = display_names(state, &addrs).await;
    Ok((motions, identities))
}

#[derive(Serialize)]
struct MotionsResponse {
    council: Vec<Motion>,
    #[serde(rename = "technicalCommittee")]
    technical_committee: Vec<Motion>,
    identities: BTreeMap<String, String>,
    #[serde(rename = "currentBlock")]
    current_block: u32,
}

async fn motions(State(state): State<AppState>) -> Result<Json<MotionsResponse>, ApiError> {
    let client = client(&state).await?;
    let current_block = head_number(&state).await?;
    let (council, mut identities) =
        collective_motions(&state, &client, "Council", current_block).await?;
    let (tech, tech_ids) =
        collective_motions(&state, &client, "TechnicalCommittee", current_block).await?;
    identities.extend(tech_ids);
    Ok(Json(MotionsResponse {
        council,
        technical_committee: tech,
        identities,
        current_block,
    }))
}

#[derive(Serialize)]
struct Referendum {
    id: u32,
    status: String,
    detail: Jv,
    decoded: Option<Proposal>,
    #[serde(rename = "blocksRemaining")]
    blocks_remaining: u32,
    #[serde(rename = "timeRemaining")]
    time_remaining: String,
}

#[derive(Serialize)]
struct PublicProp {
    index: Jv,
    hash: Jv,
    proposer: Jv,
}

#[derive(Serialize)]
struct DemocracyResponse {
    referendums: Vec<Referendum>,
    proposals: Vec<PublicProp>,
    #[serde(rename = "currentBlock")]
    current_block: u32,
    #[serde(rename = "totalReferendums")]
    total_referendums: u32,
    #[serde(rename = "votingPeriod")]
    voting_period: u32,
    #[serde(rename = "enactmentPeriod")]
    enactment_period: u32,
    #[serde(rename = "launchPeriod")]
    launch_period: u32,
}

async fn democracy(State(state): State<AppState>) -> Result<Json<DemocracyResponse>, ApiError> {
    let client = client(&state).await?;
    let at = client.storage().at_latest().await.map_err(chain_err)?;
    let s = sora::storage().democracy();
    let count = at
        .fetch(&s.referendum_count())
        .await
        .map_err(chain_err)?
        .unwrap_or(0);
    let lowest = at
        .fetch(&s.lowest_unbaked())
        .await
        .map_err(chain_err)?
        .unwrap_or(0);
    let current_block = head_number(&state).await?;
    let c = client.constants();
    let k = sora::constants().democracy();
    let voting_period: u32 = c.at(&k.voting_period()).unwrap_or(0);
    let enactment_period: u32 = c.at(&k.enactment_period()).unwrap_or(0);
    let launch_period: u32 = c.at(&k.launch_period()).unwrap_or(0);
    let mut referendums = Vec::new();
    for i in lowest..count {
        let Some(info) = referendum_info(&client, i).await else {
            continue;
        };
        let (status, detail) = referendum_status(&info);
        let mut decoded = None;
        if status == "ongoing" {
            if let Some(h) = proposal_lookup_hash(&detail) {
                let len = detail
                    .get("proposal")
                    .and_then(|p| p.get("lookup").or_else(|| p.get("Lookup")))
                    .and_then(|l| l.get("len"))
                    .and_then(|l| l.as_u64())
                    .map(|l| l as u32)
                    .unwrap_or(0);
                decoded = resolve_preimage(&client, &h, len).await;
            }
        }
        let mut blocks_remaining = 0;
        if status == "ongoing" {
            if let Some(end) = detail.get("end").and_then(|e| e.as_u64()) {
                blocks_remaining = (end as u32).saturating_sub(current_block);
            }
        }
        referendums.push(Referendum {
            id: i,
            status,
            detail,
            decoded,
            blocks_remaining,
            time_remaining: blocks_to_time(i64::from(blocks_remaining)),
        });
    }
    let proposals = match fetch_dynamic(&client, "Democracy", "PublicProps", Vec::new())
        .await
        .map_err(chain_err)?
    {
        Some(Jv::Array(a)) => a
            .into_iter()
            .filter_map(|p| match p {
                Jv::Array(t) if t.len() >= 3 => Some(PublicProp {
                    index: t[0].clone(),
                    hash: t[1].clone(),
                    proposer: t[2].clone(),
                }),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    Ok(Json(DemocracyResponse {
        referendums,
        proposals,
        current_block,
        total_referendums: count,
        voting_period,
        enactment_period,
        launch_period,
    }))
}

#[derive(Serialize)]
struct VotesResponse {
    address: String,
    voting: Option<Jv>,
}

async fn votes(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<VotesResponse>, ApiError> {
    let address = crate::util::validate_address(&address)?;
    let client = client(&state).await?;
    let (bytes, _) = sorametrics_core::chain::ss58_decode(&address)
        .map_err(|_| ApiError::BadRequest("Invalid address format".into()))?;
    let voting = fetch_dynamic(
        &client,
        "Democracy",
        "VotingOf",
        vec![DynValue::from_bytes(AccountId32(bytes).0)],
    )
    .await
    .map_err(chain_err)?;
    Ok(Json(VotesResponse { address, voting }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers_match_the_node() {
        assert_eq!(blocks_to_time(0), "0m");
        assert_eq!(blocks_to_time(16589), "1.2d");
        assert_eq!(blocks_to_time(700), "1.2h");
        assert_eq!(
            format_chain_amount(1_187_500_100_000_000_000_000),
            "1187.5001"
        );
        assert_eq!(format_chain_amount(1_000_000_000_000_000_000), "1.0000");
        assert!(
            valid_hash("0xAD10b3d7a9e6becaf3f3c70f3c23757c3af282e0a26b6a3bd4bcf01dad15aea3")
                .is_some()
        );
        assert!(valid_hash("0x12").is_none());
        assert_eq!(
            blake2_256(&[]),
            "0x0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8"
        );
    }

    #[test]
    fn runtime_version_section_is_parsed() {
        let mut wasm = Vec::new();
        wasm.extend_from_slice(&WASM_MAGIC);
        wasm.push(15);
        wasm.extend_from_slice(b"runtime_version");
        let spec = b"sora-substrate";
        wasm.push((spec.len() as u8) << 2);
        wasm.extend_from_slice(spec);
        let imp = b"sora-substrate";
        wasm.push((imp.len() as u8) << 2);
        wasm.extend_from_slice(imp);
        wasm.extend_from_slice(&1u32.to_le_bytes());
        wasm.extend_from_slice(&131u32.to_le_bytes());
        wasm.extend_from_slice(&1u32.to_le_bytes());
        let v = extract_runtime_version(&wasm).unwrap();
        assert_eq!(v["specVersion"], 131);
        assert_eq!(v["specName"], "sora-substrate");
    }
}
