//! Torii response shapes.
//!
//! Field names and nesting follow `crates/iroha_torii/src/explorer.rs`
//! and `crates/iroha_torii_shared/src/status/*.rs` of
//! `hyperledger-iroha/iroha` `optimizations@cfa5e8ce77`. Routes the
//! Node proxied verbatim (`/status`, block / transaction detail, account
//! sub-resources) stay `serde_json::Value`; only what the indexer
//! persists or the API derives from is typed.

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Seek-pagination metadata of the world-backed explorer collections
/// (`/v1/explorer/{accounts,domains,assets,nfts,rwas}`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorMeta {
    /// Page size that was applied.
    pub limit: u32,
    /// Resume token for the next page; `None` when exhausted.
    pub next_cursor: Option<String>,
    /// Whether more candidates remain after this page.
    pub has_more: bool,
}

/// Seek-pagination metadata of the chain-history collections
/// (`/v1/explorer/{blocks,transactions,instructions}`), pinned to a
/// committed snapshot so pages never overlap while the chain advances.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryMeta {
    /// Page size that was applied.
    pub limit: u32,
    /// Height of the committed snapshot the cursor walks.
    pub snapshot_height: u64,
    /// Hash of the block at `snapshot_height`; `None` on an empty chain.
    pub snapshot_hash: Option<String>,
    /// Resume token for the next page; `None` when exhausted.
    pub next_cursor: Option<String>,
    /// Whether more candidates remain after this page.
    pub has_more: bool,
}

/// One page of a cursor-paginated collection.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct CursorPage<T> {
    /// Pagination metadata.
    pub pagination: CursorMeta,
    /// Page items, newest first.
    pub items: Vec<T>,
}

/// One page of a history collection.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct HistoryPage<T> {
    /// Pagination metadata.
    pub pagination: HistoryMeta,
    /// Page items, newest first.
    pub items: Vec<T>,
}

/// One page of an offset-paginated application list
/// (`/v1/assets/definitions`, `/v1/accounts/{id}/assets`).
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct AppPage<T> {
    /// Page items.
    pub items: Vec<T>,
    /// Exact total when `count_mode` is `exact`, else absent.
    #[serde(default)]
    pub total: Option<u64>,
    /// Whether more rows exist after `offset + items.len()`.
    #[serde(default)]
    pub has_more: bool,
}

/// `ExplorerBlockDto`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockDto {
    /// Block hash, hex (no `0x`).
    pub hash: String,
    /// Height.
    pub height: u64,
    /// RFC 3339 creation time.
    pub created_at: String,
    /// Previous block hash, hex; `None` for the genesis block.
    pub prev_block_hash: Option<String>,
    /// Merkle root of the block's transactions, hex.
    pub transactions_hash: Option<String>,
    /// Rejected transactions in the block.
    pub transactions_rejected: u32,
    /// Total transactions in the block.
    pub transactions_total: u32,
}

/// `ExplorerTransactionDto` (list item).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionDto {
    /// Signing account (I105).
    pub authority: String,
    /// Transaction hash, hex.
    pub hash: String,
    /// Block height.
    pub block: u64,
    /// RFC 3339 creation time.
    pub created_at: String,
    /// Executable kind label (`Instructions` | `Wasm` | …).
    pub executable: String,
    /// `Committed` | `Rejected`.
    pub status: String,
}

/// `ExplorerInstructionBoxDto`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstructionBoxDto {
    /// Norito-encoded instruction, hex.
    pub encoded: String,
    /// SHA-256 of the framed encoding, hex.
    pub framed_sha256: String,
    /// Structured rendering: `{kind, payload: {variant, value}, wire_id, encoded}`.
    pub json: serde_json::Value,
}

/// `ExplorerInstructionDto`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstructionDto {
    /// Signing account of the parent transaction.
    pub authority: String,
    /// RFC 3339 creation time of the parent transaction.
    pub created_at: String,
    /// Instruction kind (`Register`, `Mint`, `Transfer`, `Grant`, `Custom`, …).
    pub kind: String,
    /// Encoded + decoded forms.
    #[serde(rename = "box")]
    pub r#box: InstructionBoxDto,
    /// Parent transaction hash, hex.
    pub transaction_hash: String,
    /// Parent transaction status.
    pub transaction_status: String,
    /// Block height.
    pub block: u64,
    /// Index inside the transaction.
    pub index: u32,
}

impl InstructionDto {
    /// The structured payload the Node persisted: `box.json.payload`.
    pub fn payload(&self) -> Option<&serde_json::Value> {
        self.r#box.json.get("payload")
    }
}

/// `ExplorerAccountDto`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountDto {
    /// Account id (I105 literal).
    pub id: String,
    /// Network prefix.
    pub network_prefix: u16,
    /// Account metadata.
    pub metadata: serde_json::Value,
    /// Domains owned.
    pub owned_domains: u32,
    /// Asset definitions owned.
    pub owned_assets: u32,
    /// NFTs owned.
    pub owned_nfts: u32,
}

/// `ExplorerDomainDto`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DomainDto {
    /// Domain id.
    pub id: String,
    /// Logo URI.
    pub logo: Option<String>,
    /// Domain metadata.
    pub metadata: serde_json::Value,
    /// Owner account.
    pub owned_by: String,
    /// Accounts in the domain.
    pub accounts: u32,
    /// Asset definitions in the domain.
    pub assets: u32,
    /// NFTs in the domain.
    pub nfts: u32,
}

/// `ExplorerAssetDto`. `value` is typed `Quantity` upstream; its JSON
/// form is not verified (the build Minamoto indexed sent a decimal
/// string), so both a string and a number are accepted and kept as the
/// exact decimal text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetDto {
    /// `definition#account` asset id.
    pub id: String,
    /// Asset definition id.
    pub definition_id: String,
    /// Holder account.
    pub account_id: String,
    /// Balance as decimal text.
    #[serde(deserialize_with = "decimal_text")]
    pub value: String,
}

fn decimal_text<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    let v = serde_json::Value::deserialize(d)?;
    match v {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        other => Err(de::Error::custom(format!(
            "asset value must be a decimal string or number, got {other}"
        ))),
    }
}

/// `PeerInfoDto` (`/v1/telemetry/peers-info` array item).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PeerInfoDto {
    /// Torii URL of the peer.
    pub url: String,
    /// Whether this node currently reaches it.
    pub connected: bool,
    /// The peer does not expose telemetry.
    #[serde(default)]
    pub telemetry_unsupported: bool,
    /// Configuration disclosed by the peer, when any.
    #[serde(default)]
    pub config: Option<PeerConfigDto>,
    /// Public keys of the peers it reports as connected.
    #[serde(default)]
    pub connected_peers: Option<Vec<String>>,
}

/// `PeerConfigDto`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PeerConfigDto {
    /// Peer public key, when disclosed.
    #[serde(default)]
    pub public_key: Option<String>,
}

/// Typed subset of `/status` the indexer persists.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct StatusSnapshot {
    /// Connected peers excluding this node.
    pub peers: u64,
    /// Committed blocks (chain height).
    pub blocks: u64,
    /// Committed non-empty blocks.
    pub blocks_non_empty: u64,
    /// Commit latency of the latest block on this peer, ms.
    pub commit_time_ms: u64,
    /// Approved transactions.
    pub txs_approved: u64,
    /// Rejected transactions.
    pub txs_rejected: u64,
    /// Unix ms of the last committed block seen by this peer.
    pub last_block_committed_at_ms: u64,
    /// Build metadata.
    pub build: BuildStatus,
}

/// `BuildStatus`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildStatus {
    /// Semantic version baked into the binary.
    pub version: String,
    /// Git commit SHA baked into the binary.
    pub git_commit_sha: String,
}

/// Asset definition as `/v1/assets/definitions` renders it
/// (`norito::json::to_value(AssetDefinition)` plus `alias`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssetDefinitionDto {
    /// Definition id (hash form).
    pub id: String,
    /// Human name.
    #[serde(default)]
    pub name: Option<String>,
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
    /// Alias binding (`xor#universal`), when bound.
    #[serde(default, deserialize_with = "alias_text")]
    pub alias: Option<String>,
    /// Owner account.
    pub owned_by: String,
    /// `Infinitely` | `Once` | `Not`.
    #[serde(default, deserialize_with = "enum_label")]
    pub mintable: Option<String>,
    /// Confidential policy; `mode` is what the Node stored.
    #[serde(default)]
    pub confidential_policy: Option<serde_json::Value>,
    /// Balance scope policy label.
    #[serde(default, deserialize_with = "enum_label")]
    pub balance_scope_policy: Option<String>,
    /// Total minted quantity, decimal text.
    #[serde(default)]
    pub total_quantity: Option<serde_json::Value>,
    /// Metadata map.
    #[serde(default)]
    pub metadata: serde_json::Value,
    /// Every other field, kept for callers that need the raw object.
    #[serde(flatten)]
    pub rest: BTreeMap<String, serde_json::Value>,
}

impl AssetDefinitionDto {
    /// `confidential_policy.mode` when present as text.
    pub fn confidential_mode(&self) -> Option<String> {
        self.confidential_policy
            .as_ref()
            .and_then(|p| p.get("mode"))
            .and_then(label_of)
    }

    /// `total_quantity` as decimal text (string or number).
    pub fn total_quantity_text(&self) -> Option<String> {
        match &self.total_quantity {
            Some(serde_json::Value::String(s)) => Some(s.clone()),
            Some(serde_json::Value::Number(n)) => Some(n.to_string()),
            _ => None,
        }
    }
}

/// Alias may be a bare string or an object carrying the alias text.
fn alias_text<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(v.as_ref().and_then(|v| match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Object(o) => o.get("alias").and_then(|a| a.as_str()).map(String::from),
        _ => None,
    }))
}

/// Norito renders unit enums as a string and data enums as
/// `{Variant: …}`; the label is what the Node stored.
fn enum_label<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(v.as_ref().and_then(label_of))
}

fn label_of(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Object(o) => o.keys().next().cloned(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_page_of_blocks() {
        let raw = r#"{"pagination":{"limit":25,"snapshot_height":438,"snapshot_hash":"0f","next_cursor":"abc","has_more":true},
            "items":[{"hash":"0febe2","height":438,"created_at":"2026-06-12T22:29:35.805Z","prev_block_hash":"c7f6","transactions_hash":null,"transactions_rejected":0,"transactions_total":0}]}"#;
        let page: HistoryPage<BlockDto> = serde_json::from_str(raw).unwrap();
        assert_eq!(page.pagination.snapshot_height, 438);
        assert_eq!(page.items[0].prev_block_hash.as_deref(), Some("c7f6"));
        assert!(page.items[0].transactions_hash.is_none());
    }

    #[test]
    fn instruction_payload_is_box_json_payload() {
        let raw = r#"{"authority":"sora","created_at":"t","kind":"Mint","box":{"encoded":"00","framed_sha256":"ff","json":{"kind":"Mint","payload":{"variant":"Asset","value":{"object":"5","destination":"d#a"}},"wire_id":"iroha.mint","encoded":"00"}},"transaction_hash":"ab","transaction_status":"Committed","block":3,"index":0}"#;
        let i: InstructionDto = serde_json::from_str(raw).unwrap();
        assert_eq!(i.payload().unwrap()["variant"], "Asset");
        assert_eq!(i.payload().unwrap()["value"]["object"], "5");
    }

    #[test]
    fn asset_value_accepts_string_and_number() {
        let s: AssetDto = serde_json::from_str(
            r#"{"id":"x","definition_id":"d","account_id":"a","value":"4.37620"}"#,
        )
        .unwrap();
        assert_eq!(s.value, "4.37620");
        let n: AssetDto =
            serde_json::from_str(r#"{"id":"x","definition_id":"d","account_id":"a","value":12}"#)
                .unwrap();
        assert_eq!(n.value, "12");
        let bad = serde_json::from_str::<AssetDto>(
            r#"{"id":"x","definition_id":"d","account_id":"a","value":{"n":1}}"#,
        );
        assert!(bad.is_err());
    }

    #[test]
    fn asset_definition_labels() {
        let raw = r#"{"id":"6TEA","name":"XOR","owned_by":"sora1","mintable":"Infinitely","confidential_policy":{"mode":"Convertible","allow_shield":true},"balance_scope_policy":{"Scoped":{"x":1}},"total_quantity":"3462.445388","metadata":{},"spec":{"scale":6}}"#;
        let d: AssetDefinitionDto = serde_json::from_str(raw).unwrap();
        assert_eq!(d.mintable.as_deref(), Some("Infinitely"));
        assert_eq!(d.confidential_mode().as_deref(), Some("Convertible"));
        assert_eq!(d.balance_scope_policy.as_deref(), Some("Scoped"));
        assert_eq!(d.total_quantity_text().as_deref(), Some("3462.445388"));
        assert!(d.rest.contains_key("spec"));
        assert!(d.alias.is_none());
    }

    #[test]
    fn status_subset() {
        let raw = r#"{"build":{"version":"2.0.0-rc.2.0","git_commit_sha":"cfa5e8ce77","cargo_features":"","target_triple":"x"},"observed_at_ms":1,"peers":4,"blocks":440,"blocks_non_empty":100,"commit_time_ms":0,"txs_approved":422,"txs_rejected":18,"uptime":{"secs":1,"nanos":0},"view_changes":0,"queue_size":0,"last_block_committed_at_ms":5,"sumeragi":null}"#;
        let s: StatusSnapshot = serde_json::from_str(raw).unwrap();
        assert_eq!(s.blocks, 440);
        assert_eq!(s.build.git_commit_sha, "cfa5e8ce77");
    }

    #[test]
    fn peers_info_minimal() {
        let raw = r#"[{"url":"https://a","connected":true,"telemetry_unsupported":false,"config":{"public_key":"ea01"},"location":null,"connected_peers":["ea02"]},{"url":"https://b","connected":false}]"#;
        let p: Vec<PeerInfoDto> = serde_json::from_str(raw).unwrap();
        assert_eq!(
            p[0].config.as_ref().unwrap().public_key.as_deref(),
            Some("ea01")
        );
        assert!(p[1].connected_peers.is_none());
    }
}
