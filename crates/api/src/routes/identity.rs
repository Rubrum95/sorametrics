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
//! `null`. Results are cached 1 h in-process (Node `IDENTITY_MEM_TTL`).

use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sorametrics_core::chain::ss58_decode;
use sorametrics_substrate::runtime::sora;
use sorametrics_substrate::runtime::sora::runtime_types::pallet_identity::types::Data;
use std::collections::BTreeMap;
use std::time::Duration;
use subxt::ext::codec::Encode;
use subxt::utils::AccountId32;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/identity/:address", get(identity))
        .route("/api/identities", post(identities))
}

const TTL: Duration = Duration::from_secs(3600);

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

async fn resolve(state: &AppState, address: &str) -> Result<Identity, ApiError> {
    let key = format!("identity:{address}");
    if let Some(v) = state.cached_scan(&key, TTL).await {
        return serde_json::from_value(v).map_err(|e| ApiError::Internal(e.to_string()));
    }
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let (bytes, _) =
        ss58_decode(address).map_err(|_| ApiError::BadRequest("Invalid address format".into()))?;
    let account = AccountId32(bytes);
    let ident = chain
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
        .unwrap_or_default();
    let v = serde_json::to_value(&ident).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan(&key, v).await;
    Ok(ident)
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
    let mut out = BTreeMap::new();
    for addr in capped {
        if let Ok(Identity {
            display: Some(display),
            ..
        }) = resolve(&state, &addr).await
        {
            out.insert(addr, DisplayOnly { display });
        }
    }
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
