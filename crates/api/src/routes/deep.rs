//! Deep pages without deep OFFSETs: the block holding position `pos` of a
//! listing is found with an index-only scan over `block_height`, the rows
//! before that block are counted the same way, and the page query then
//! starts at that block and skips only the rest of it.
//!
//! Block and count come from ONE statement: two statements would read two
//! snapshots, and a block committed in between would push the count past
//! `pos` (negative skip).

use crate::{error::ApiError, AppState};

/// Offsets below this are cheap enough for a plain OFFSET.
const DEEP_OFFSET: i64 = 5_000;

/// A listing whose filter has an index led by `block_height` (or by the
/// wallet, then `block_height`).
#[derive(Debug, Clone, Copy)]
pub(super) enum Stream<'a> {
    /// `sm.swaps`, global (pkey) or one caller (`swaps_caller_block_idx`).
    Swaps { wallet: Option<&'a str> },
    /// `sm.swaps` of several callers.
    SwapsOf { wallets: &'a [String] },
    /// `sm.transfers`, global (pkey).
    Transfers,
    /// `sm.transfers` sent by any of `wallets`, or received by one of them
    /// from outside the set (disjoint, so each transfer counts once).
    TransfersOf { wallets: &'a [String] },
    /// `sm.extrinsics` signed by one wallet (`extrinsics_signer_idx`).
    Extrinsics { signer: &'a str },
    /// `sm.extrinsics` signed by any of `signers`.
    ExtrinsicsOf { signers: &'a [String] },
}

/// Where a deep page starts: rows at `block` (inclusive) onwards, in the
/// listing's direction, after skipping `skip` of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Deep {
    pub block: i64,
    pub skip: i64,
}

/// How a page of `stream` (if any) at `offset` should be read.
pub(super) enum DeepStart {
    /// Shallow page, or no index-backed stream: plain OFFSET.
    Plain,
    /// Start at the located block.
    At(Deep),
    /// The listing holds fewer rows than `offset`.
    PastEnd,
}

/// `cap` is a `(block, 0)` upper bound (the `?timestamp=` cap).
pub(super) async fn deep_start(
    state: &AppState,
    stream: Option<Stream<'_>>,
    cap: Option<(i64, i32)>,
    offset: i64,
    newest_first: bool,
) -> Result<DeepStart, ApiError> {
    let Some(stream) = stream.filter(|_| offset >= DEEP_OFFSET) else {
        return Ok(DeepStart::Plain);
    };
    let below = cap.map_or(i64::MAX, |c| c.0);
    Ok(
        match block_and_before(state, stream, below, offset, newest_first).await? {
            Some((block, before)) => DeepStart::At(Deep {
                block,
                skip: offset - before,
            }),
            None => DeepStart::PastEnd,
        },
    )
}

/// Block at position `pos` among rows with `block_height < below`, counted
/// from the newest end (`newest_first`) or from the oldest, and the number
/// of rows the listing puts before that block. `None` = fewer rows.
async fn block_and_before(
    state: &AppState,
    stream: Stream<'_>,
    below: i64,
    pos: i64,
    newest_first: bool,
) -> Result<Option<(i64, i64)>, ApiError> {
    let db = &state.listing_db;
    macro_rules! run {
        ($query:expr) => {
            $query
                .fetch_optional(db)
                .await?
                .map(|r| (r.block, r.before))
        };
    }
    Ok(match (stream, newest_first) {
        (Stream::Swaps { wallet }, true) => run!(sqlx::query!(
            r#"WITH b AS (
                   SELECT block_height FROM sm.swaps
                   WHERE ($1::text IS NULL OR caller = $1) AND block_height < $2
                   ORDER BY block_height DESC OFFSET $3 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM sm.swaps s
                       WHERE ($1::text IS NULL OR s.caller = $1)
                         AND s.block_height > b.block_height AND s.block_height < $2) AS "before!"
               FROM b"#,
            wallet,
            below,
            pos
        )),
        (Stream::Swaps { wallet }, false) => run!(sqlx::query!(
            r#"WITH b AS (
                   SELECT block_height FROM sm.swaps
                   WHERE ($1::text IS NULL OR caller = $1) AND block_height < $2
                   ORDER BY block_height ASC OFFSET $3 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM sm.swaps s
                       WHERE ($1::text IS NULL OR s.caller = $1)
                         AND s.block_height < b.block_height) AS "before!"
               FROM b"#,
            wallet,
            below,
            pos
        )),
        (Stream::SwapsOf { wallets }, true) => run!(sqlx::query!(
            r#"WITH b AS (
                   SELECT block_height FROM sm.swaps
                   WHERE caller = ANY($1) AND block_height < $2
                   ORDER BY block_height DESC OFFSET $3 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM sm.swaps s
                       WHERE s.caller = ANY($1)
                         AND s.block_height > b.block_height AND s.block_height < $2) AS "before!"
               FROM b"#,
            wallets,
            below,
            pos
        )),
        (Stream::SwapsOf { wallets }, false) => run!(sqlx::query!(
            r#"WITH b AS (
                   SELECT block_height FROM sm.swaps
                   WHERE caller = ANY($1) AND block_height < $2
                   ORDER BY block_height ASC OFFSET $3 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM sm.swaps s
                       WHERE s.caller = ANY($1) AND s.block_height < b.block_height) AS "before!"
               FROM b"#,
            wallets,
            below,
            pos
        )),
        (Stream::Transfers, true) => run!(sqlx::query!(
            r#"WITH b AS (
                   SELECT block_height FROM sm.transfers WHERE block_height < $1
                   ORDER BY block_height DESC OFFSET $2 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM sm.transfers s
                       WHERE s.block_height > b.block_height AND s.block_height < $1) AS "before!"
               FROM b"#,
            below,
            pos
        )),
        (Stream::Transfers, false) => run!(sqlx::query!(
            r#"WITH b AS (
                   SELECT block_height FROM sm.transfers WHERE block_height < $1
                   ORDER BY block_height ASC OFFSET $2 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM sm.transfers s
                       WHERE s.block_height < b.block_height) AS "before!"
               FROM b"#,
            below,
            pos
        )),
        (Stream::TransfersOf { wallets }, true) => run!(sqlx::query!(
            r#"WITH s AS (
                   SELECT block_height FROM sm.transfers
                   WHERE from_address = ANY($1) AND block_height < $2
                   UNION ALL
                   SELECT block_height FROM sm.transfers
                   WHERE to_address = ANY($1) AND NOT (from_address = ANY($1))
                     AND block_height < $2),
               b AS (SELECT block_height FROM s ORDER BY block_height DESC OFFSET $3 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM s WHERE s.block_height > b.block_height) AS "before!"
               FROM b"#,
            wallets,
            below,
            pos
        )),
        (Stream::TransfersOf { wallets }, false) => run!(sqlx::query!(
            r#"WITH s AS (
                   SELECT block_height FROM sm.transfers
                   WHERE from_address = ANY($1) AND block_height < $2
                   UNION ALL
                   SELECT block_height FROM sm.transfers
                   WHERE to_address = ANY($1) AND NOT (from_address = ANY($1))
                     AND block_height < $2),
               b AS (SELECT block_height FROM s ORDER BY block_height ASC OFFSET $3 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM s WHERE s.block_height < b.block_height) AS "before!"
               FROM b"#,
            wallets,
            below,
            pos
        )),
        (Stream::Extrinsics { signer }, true) => run!(sqlx::query!(
            r#"WITH b AS (
                   SELECT block_height FROM sm.extrinsics
                   WHERE signer = $1 AND block_height < $2
                   ORDER BY block_height DESC OFFSET $3 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM sm.extrinsics s
                       WHERE s.signer = $1
                         AND s.block_height > b.block_height AND s.block_height < $2) AS "before!"
               FROM b"#,
            signer,
            below,
            pos
        )),
        (Stream::Extrinsics { signer }, false) => run!(sqlx::query!(
            r#"WITH b AS (
                   SELECT block_height FROM sm.extrinsics
                   WHERE signer = $1 AND block_height < $2
                   ORDER BY block_height ASC OFFSET $3 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM sm.extrinsics s
                       WHERE s.signer = $1 AND s.block_height < b.block_height) AS "before!"
               FROM b"#,
            signer,
            below,
            pos
        )),
        (Stream::ExtrinsicsOf { signers }, true) => run!(sqlx::query!(
            r#"WITH b AS (
                   SELECT block_height FROM sm.extrinsics
                   WHERE signer = ANY($1) AND block_height < $2
                   ORDER BY block_height DESC OFFSET $3 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM sm.extrinsics s
                       WHERE s.signer = ANY($1)
                         AND s.block_height > b.block_height AND s.block_height < $2) AS "before!"
               FROM b"#,
            signers,
            below,
            pos
        )),
        (Stream::ExtrinsicsOf { signers }, false) => run!(sqlx::query!(
            r#"WITH b AS (
                   SELECT block_height FROM sm.extrinsics
                   WHERE signer = ANY($1) AND block_height < $2
                   ORDER BY block_height ASC OFFSET $3 LIMIT 1)
               SELECT b.block_height AS "block!",
                      (SELECT COUNT(*) FROM sm.extrinsics s
                       WHERE s.signer = ANY($1) AND s.block_height < b.block_height) AS "before!"
               FROM b"#,
            signers,
            below,
            pos
        )),
    })
}
