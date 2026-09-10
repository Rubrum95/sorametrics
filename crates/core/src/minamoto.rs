//! Minamoto / Iroha 3 (SORA Nexus) row types of the `mn.*` schema.
//!
//! Flat projections of what Torii exposes, exactly as the Node indexer
//! persisted them (`minamoto/db.js` upserts). Hashes are raw 32-byte
//! values (`BYTEA` in PostgreSQL, hex on the wire); account ids are the
//! I105 literal (katakana included, never normalised).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// `mn.blocks` row (`ExplorerBlockDto` projection).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MnBlock {
    /// Height.
    pub height: i64,
    /// Block hash (32 bytes).
    pub hash: Vec<u8>,
    /// Previous block hash (32 bytes), `None` for genesis.
    pub prev_hash: Option<Vec<u8>>,
    /// Transactions merkle root (32 bytes).
    pub transactions_hash: Option<Vec<u8>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Transactions in the block (`transactions_total`).
    pub transactions_committed: i32,
    /// Rejected transactions in the block.
    pub transactions_rejected: i32,
}

/// `mn.transactions` row (list DTO projection; claim columns are filled
/// later from the transaction detail).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MnTransaction {
    /// Transaction hash (32 bytes).
    pub hash: Vec<u8>,
    /// Block height.
    pub block_height: i64,
    /// Signing account.
    pub authority: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Executable kind label.
    pub executable_kind: String,
    /// `Committed` | `Rejected`.
    pub status: String,
}

/// Cross-chain claim metadata lifted from a transaction detail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MnClaimMetadata {
    /// SORA v2 burn extrinsic hash (`0x…`).
    pub sora_v2_claim_tx_hash: String,
    /// Recipient account on Minamoto.
    pub sora_nexus_claim_recipient: Option<String>,
    /// Sponsoring account, when the fee was sponsored.
    pub fee_sponsor: Option<String>,
}

/// `mn.accounts` row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MnAccount {
    /// Account id (I105 literal).
    pub id: String,
    /// Network prefix.
    pub network_prefix: i32,
    /// Primary alias, when set.
    pub primary_alias: Option<String>,
    /// Alias dataspace.
    pub primary_alias_dataspace: Option<String>,
    /// Alias domain.
    pub primary_alias_domain: Option<String>,
    /// Alias name.
    pub primary_alias_name: Option<String>,
    /// Multisig quorum from `metadata["multisig/spec"]`.
    pub multisig_quorum: Option<i32>,
    /// Multisig signatories count.
    pub multisig_signatories_count: Option<i32>,
    /// Raw metadata.
    pub metadata: serde_json::Value,
}

impl MnAccount {
    /// A bare stub row (owner referenced by a domain / asset before the
    /// accounts job saw it), as the Node's `upsertAccount({ id })`.
    pub fn stub(id: &str) -> Self {
        Self {
            id: id.to_string(),
            network_prefix: 753,
            primary_alias: None,
            primary_alias_dataspace: None,
            primary_alias_domain: None,
            primary_alias_name: None,
            multisig_quorum: None,
            multisig_signatories_count: None,
            metadata: serde_json::Value::Object(Default::default()),
        }
    }
}

/// `mn.domains` row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MnDomain {
    /// Domain id.
    pub id: String,
    /// Owner account.
    pub owned_by: String,
    /// Accounts in the domain.
    pub accounts_count: i32,
    /// Asset definitions in the domain.
    pub assets_count: i32,
    /// NFTs in the domain.
    pub nfts_count: i32,
    /// Raw metadata.
    pub metadata: serde_json::Value,
}

/// `mn.assets` row (one balance).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MnAsset {
    /// Asset definition id.
    pub definition_id: String,
    /// Holder account.
    pub account_id: String,
    /// Balance as decimal text (arbitrary scale).
    pub value: String,
}

/// `mn.asset_definitions` row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MnAssetDefinition {
    /// Definition id (hash form).
    pub id: String,
    /// Alias (`xor#universal`).
    pub alias: Option<String>,
    /// Human name.
    pub name: Option<String>,
    /// Description.
    pub description: Option<String>,
    /// Owner account.
    pub owned_by: String,
    /// `Infinitely` | `Once` | `Not`.
    pub mintable: Option<String>,
    /// `confidential_policy.mode`.
    pub confidential_mode: Option<String>,
    /// Balance scope policy label.
    pub balance_scope_policy: Option<String>,
    /// Total minted quantity, decimal text.
    pub total_quantity: Option<String>,
    /// Raw metadata.
    pub metadata: serde_json::Value,
}

/// `mn.peers` row. Since Iroha rc2 the multiaddr IS the public key
/// (peers-info exposes no ip:port).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MnPeer {
    /// Canonical key.
    pub multiaddr: String,
    /// Public key.
    pub public_key: Option<String>,
    /// IP, when known.
    pub ip_address: Option<String>,
    /// Port, when known.
    pub port: Option<i32>,
}

/// `mn.instructions` row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MnInstruction {
    /// Parent transaction hash (32 bytes).
    pub transaction_hash: Vec<u8>,
    /// Index inside the transaction.
    pub instruction_index: i32,
    /// Block height.
    pub block_height: i64,
    /// Signing account.
    pub authority: String,
    /// Instruction kind.
    pub kind: String,
    /// Structured payload (`{variant, value}`) or `{encoded}`.
    pub payload: serde_json::Value,
    /// Parent transaction status.
    pub transaction_status: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

/// `mn.network_state` single row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MnNetworkState {
    /// Connected peers.
    pub peers: i32,
    /// Domains indexed.
    pub domains: i32,
    /// Accounts indexed.
    pub accounts: i32,
    /// Asset balances indexed.
    pub assets: i32,
    /// Approved transactions.
    pub transactions_accepted: i64,
    /// Rejected transactions.
    pub transactions_rejected: i64,
    /// Chain height.
    pub block_height: i64,
    /// Finalized height (Iroha commits are final: equals `block_height`).
    pub finalized_block: i64,
    /// Latest commit latency, ms.
    pub avg_commit_time_ms: i32,
    /// Mean spacing of the last 100 blocks, ms.
    pub avg_block_time_ms: i64,
    /// Creation time of the latest block.
    pub last_block_at: Option<DateTime<Utc>>,
    /// Node version string.
    pub iroha_version: Option<String>,
}

/// One Prometheus sample to persist.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricSample {
    /// Metric name.
    pub metric_name: String,
    /// Label set as a JSON object.
    pub labels: serde_json::Value,
    /// Value.
    pub value: f64,
}

/// Decodes a 32-byte hash from hex (with or without `0x`).
pub fn hash32_from_hex(hex_str: &str) -> Result<Vec<u8>, String> {
    let clean = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes = hex::decode(clean).map_err(|e| format!("invalid hex '{hex_str}': {e}"))?;
    if bytes.len() != 32 {
        return Err(format!(
            "hash must be 32 bytes, got {} in '{hex_str}'",
            bytes.len()
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash32_accepts_prefixed_and_bare() {
        let h = "0f".repeat(32);
        assert_eq!(hash32_from_hex(&h).unwrap().len(), 32);
        assert_eq!(hash32_from_hex(&format!("0x{h}")).unwrap().len(), 32);
        assert!(hash32_from_hex("0f0f").is_err());
        assert!(hash32_from_hex("zz").is_err());
    }

    #[test]
    fn stub_account_defaults() {
        let a = MnAccount::stub("sora1");
        assert_eq!(a.network_prefix, 753);
        assert!(a.metadata.as_object().unwrap().is_empty());
    }
}
