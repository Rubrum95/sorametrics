//! On-chain identities (`pallet_identity`): `GET /identity/:address`
//! and `POST /api/identities` (`index.js::resolveIdentitiesBatch`,
//! `parseIdentityFull`).
//!
//! - `/identity/:address` → `{ display, email, web, twitter, discord }`
//!   (all `null` when the account has no identity; the SORA pallet has
//!   no `discord` field, so it is always `null` — as in the Node).
//! - `POST /api/identities { addresses: [...] }` → `{ <addr>: { display } }`
//!   only for accounts with a display name; addresses are filtered to
//!   strings longer than 40 chars not starting with `0x`, capped at 200.
//!
//! Each `Data` field is decoded from its SCALE encoding: variant index
//! `1 + n` is `Raw<n>` (UTF-8 bytes), anything else (`None`, hashes) is
//! `null`. Resolution order as the Node's `resolveIdentitiesBatch`:
//! memory (1 h, `IDENTITY_MEM_TTL`) → `sm.identity_cache` (24 h,
//! `IDENTITY_DB_TTL`) → chain, in chunks of 50; every chain answer is
//! written back to the table. At boot the rows with a display name are
//! loaded into memory (`getAllCachedIdentities`).

use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sorametrics_core::chain::ss58_decode;
use sorametrics_substrate::runtime::sora;
use sorametrics_substrate::runtime::sora::runtime_types::pallet_identity::types::Data;
use std::collections::BTreeMap;
use std::time::Duration;
use subxt::ext::codec::Encode;
use subxt::utils::AccountId32;
use tracing::{info, warn};

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/identity/:address", get(identity))
        .route("/api/identities", post(identities))
}

const TTL: Duration = Duration::from_secs(3600);
/// Node `IDENTITY_DB_TTL`: a table row younger than this skips the chain.
const DB_TTL_MS: i64 = 86_400_000;
/// Node: `identityOf.multi` in chunks of 50.
const CHUNK: usize = 50;

#[derive(Clone, Default, Serialize, Deserialize)]
struct Identity {
    display: Option<String>,
    email: Option<String>,
    web: Option<String>,
    twitter: Option<String>,
    discord: Option<String>,
}

/// `Data::Raw<n>` → UTF-8 string; everything else → `None`.
pub fn data_text(d: &Data) -> Option<String> {
    let enc = d.encode();
    match enc.first() {
        Some(idx) if (1..=33).contains(idx) => {
            let bytes = &enc[1..];
            if bytes.is_empty() {
                None
            } else {
                Some(String::from_utf8_lossy(bytes).into_owned())
            }
        }
        _ => None,
    }
}

fn mem_key(address: &str) -> String {
    format!("identity:{address}")
}

async fn remember(state: &AppState, address: &str, ident: &Identity) {
    if let Ok(v) = serde_json::to_value(ident) {
        state.store_scan(&mem_key(address), v).await;
    }
}

/// `sm.identity_cache` rows for `addresses`, with their age.
async fn db_rows(
    state: &AppState,
    addresses: &[String],
) -> Result<Vec<(String, Identity, i64)>, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT address, display, email, web, twitter, discord, updated_at
           FROM sm.identity_cache WHERE address = ANY($1::text[])"#,
        addresses
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.address,
                Identity {
                    display: r.display,
                    email: r.email,
                    web: r.web,
                    twitter: r.twitter,
                    discord: r.discord,
                },
                r.updated_at,
            )
        })
        .collect())
}

/// Node `upsertIdentityBatch`: one row per resolved account, `updated_at` = now (ms).
async fn db_upsert(state: &AppState, batch: &[(String, Identity)]) -> Result<(), ApiError> {
    let now = Utc::now().timestamp_millis();
    for (address, id) in batch {
        sqlx::query!(
            r#"INSERT INTO sm.identity_cache (address, display, email, web, twitter, discord, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (address) DO UPDATE SET
                   display = EXCLUDED.display, email = EXCLUDED.email, web = EXCLUDED.web,
                   twitter = EXCLUDED.twitter, discord = EXCLUDED.discord, updated_at = EXCLUDED.updated_at"#,
            address,
            id.display,
            id.email,
            id.web,
            id.twitter,
            id.discord,
            now
        )
        .execute(&state.db)
        .await?;
    }
    Ok(())
}

async fn chain_lookup(state: &AppState, address: &str) -> Result<Identity, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let (bytes, _) =
        ss58_decode(address).map_err(|_| ApiError::BadRequest("Invalid address format".into()))?;
    let account = AccountId32(bytes);
    Ok(chain
        .with_client(|client| async move {
            let reg = client
                .storage()
                .at_latest()
                .await?
                .fetch(&sora::storage().identity().identity_of(&account))
                .await?;
            Ok(reg.map(|r| Identity {
                display: data_text(&r.info.display),
                email: data_text(&r.info.email),
                web: data_text(&r.info.web),
                twitter: data_text(&r.info.twitter),
                discord: None,
            }))
        })
        .await?
        .unwrap_or_default())
}

/// Node `resolveIdentitiesBatch`: memory → table (24 h) → chain in
/// chunks of 50, writing the chain answers back. Returns what is known
/// for each address (lookup failures are left out).
async fn resolve_many(state: &AppState, addresses: &[String]) -> BTreeMap<String, Identity> {
    let mut out = BTreeMap::new();
    let mut to_resolve = Vec::new();
    for a in addresses {
        match state.cached_scan(&mem_key(a), TTL).await {
            Some(v) => {
                if let Ok(id) = serde_json::from_value::<Identity>(v) {
                    out.insert(a.clone(), id);
                }
            }
            None => to_resolve.push(a.clone()),
        }
    }
    if to_resolve.is_empty() {
        return out;
    }
    let now = Utc::now().timestamp_millis();
    let mut need_chain = to_resolve.clone();
    match db_rows(state, &to_resolve).await {
        Ok(rows) => {
            for (address, id, updated_at) in rows {
                if now - updated_at < DB_TTL_MS {
                    remember(state, &address, &id).await;
                    need_chain.retain(|a| a != &address);
                    out.insert(address, id);
                }
            }
        }
        Err(e) => warn!(error = %e, "identity cache read failed; asking the chain"),
    }
    for chunk in need_chain.chunks(CHUNK) {
        let looked_up =
            futures::future::join_all(chunk.iter().map(|a| chain_lookup(state, a))).await;
        let mut batch = Vec::new();
        for (a, res) in chunk.iter().zip(looked_up) {
            match res {
                Ok(id) => {
                    remember(state, a, &id).await;
                    out.insert(a.clone(), id.clone());
                    batch.push((a.clone(), id));
                }
                Err(e) => warn!(address = %a, error = %e, "identity chain lookup failed"),
            }
        }
        if let Err(e) = db_upsert(state, &batch).await {
            warn!(error = %e, "identity cache write failed");
        }
    }
    out
}

async fn resolve(state: &AppState, address: &str) -> Result<Identity, ApiError> {
    let mut m = resolve_many(state, std::slice::from_ref(&address.to_string())).await;
    m.remove(address)
        .ok_or_else(|| ApiError::Internal("identity lookup failed".into()))
}

/// Node boot: `getAllCachedIdentities` → memory (display only, as the Node).
pub async fn warm_from_db(state: &AppState) {
    match sqlx::query!(
        r#"SELECT address, display AS "display!" FROM sm.identity_cache WHERE display IS NOT NULL"#
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(rows) => {
            let n = rows.len();
            for r in rows {
                remember(
                    state,
                    &r.address,
                    &Identity {
                        display: Some(r.display),
                        ..Default::default()
                    },
                )
                .await;
            }
            info!(loaded = n, "identity cache warmed from sm.identity_cache");
        }
        Err(e) => warn!(error = %e, "identity cache warm failed"),
    }
}

/// Display names for `addresses` (Node `attachIdentities`): only the
/// accounts with a display are present. Lookup failures leave the
/// address out, as the Node's cache miss did.
pub async fn display_names(state: &AppState, addresses: &[String]) -> BTreeMap<String, String> {
    resolve_many(state, addresses)
        .await
        .into_iter()
        .filter_map(|(a, id)| id.display.map(|d| (a, d)))
        .collect()
}

async fn identity(State(state): State<AppState>, Path(address): Path<String>) -> Json<Identity> {
    // Node: any failure → `{ display: null }`.
    match resolve(&state, &address).await {
        Ok(i) if i.display.is_some() => Json(i),
        _ => Json(Identity::default()),
    }
}

#[derive(Deserialize)]
struct IdentitiesBody {
    addresses: Option<Vec<String>>,
}

#[derive(Serialize)]
struct DisplayOnly {
    display: String,
}

async fn identities(
    State(state): State<AppState>,
    Json(body): Json<IdentitiesBody>,
) -> Result<Json<BTreeMap<String, DisplayOnly>>, ApiError> {
    let addresses = match body.addresses {
        Some(a) if !a.is_empty() => a,
        _ => return Err(ApiError::BadRequest("addresses array required".into())),
    };
    let capped: Vec<String> = addresses
        .into_iter()
        .filter(|a| a.len() > 40 && !a.starts_with("0x"))
        .take(200)
        .collect();
    let out = resolve_many(&state, &capped)
        .await
        .into_iter()
        .filter_map(|(a, id)| id.display.map(|display| (a, DisplayOnly { display })))
        .collect();
    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_data_decodes_to_text_and_hashes_to_none() {
        assert_eq!(data_text(&Data::Raw5(*b"hello")), Some("hello".into()));
        assert_eq!(data_text(&Data::Raw0([])), None);
        assert_eq!(data_text(&Data::None), None);
        assert_eq!(data_text(&Data::Sha256([0u8; 32])), None);
    }
}
