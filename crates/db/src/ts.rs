//! TimescaleDB (`ts.*` schema) typed query helpers.
//!
//! `ts.price_history` keeps hourly USD price buckets per asset. Its
//! grain and update rule are inherited verbatim from the legacy Node
//! (`db_pg.js::updatePriceHistory`): `hour_bucket` is unix SECONDS
//! floored to the hour, and each new sample folds into the bucket as a
//! running mean weighted by `sample_count`.

use crate::DbError;
use sqlx::PgPool;

/// Fold one price sample into its hourly bucket (insert-or-average).
///
/// Running mean: `(price * n + sample) / (n + 1)`, `n += 1` — the exact
/// legacy expression, so v33 buckets are directly comparable with the
/// ones migrated from the Node.
pub async fn upsert_price_sample(
    pool: &PgPool,
    asset_id: &str,
    hour_bucket: i64,
    price_usd: f64,
) -> Result<(), DbError> {
    sqlx::query!(
        r#"
        INSERT INTO ts.price_history (asset_id, hour_bucket, price_usd, sample_count)
        VALUES ($1, $2, $3, 1)
        ON CONFLICT (asset_id, hour_bucket) DO UPDATE SET
            price_usd = (ts.price_history.price_usd * ts.price_history.sample_count + $3)
                        / (ts.price_history.sample_count + 1),
            sample_count = ts.price_history.sample_count + 1
        "#,
        asset_id,
        hour_bucket,
        price_usd,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Mean USD price of an asset in one hourly bucket, if any sample landed.
pub async fn price_at_bucket(
    pool: &PgPool,
    asset_id: &str,
    hour_bucket: i64,
) -> Result<Option<f64>, DbError> {
    let row = sqlx::query!(
        r#"
        SELECT price_usd
        FROM ts.price_history
        WHERE asset_id = $1 AND hour_bucket = $2
        "#,
        asset_id,
        hour_bucket,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.price_usd))
}

/// Publish the most recent quote of an asset (`ts.price_latest`).
pub async fn upsert_price_latest(
    pool: &PgPool,
    asset_id: &str,
    price_usd: f64,
    sampled_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), DbError> {
    sqlx::query!(
        r#"
        INSERT INTO ts.price_latest (asset_id, price_usd, sampled_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (asset_id) DO UPDATE SET
            price_usd  = EXCLUDED.price_usd,
            sampled_at = EXCLUDED.sampled_at
        "#,
        asset_id,
        price_usd,
        sampled_at,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// One `(asset_id, price_usd)` pair.
#[derive(Clone, Debug, PartialEq)]
pub struct AssetPrice {
    /// `0x`-hex asset id.
    pub asset_id: String,
    /// Latest USD price.
    pub price_usd: f64,
}

/// Latest quotes for a set of assets (missing ids are simply absent).
pub async fn latest_prices(
    pool: &PgPool,
    asset_ids: &[String],
) -> Result<Vec<AssetPrice>, DbError> {
    let rows = sqlx::query_as!(
        AssetPrice,
        r#"
        SELECT asset_id, price_usd
        FROM ts.price_latest
        WHERE asset_id = ANY($1)
        "#,
        asset_ids,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// One hourly point.
#[derive(Clone, Debug, PartialEq)]
pub struct PricePoint {
    /// Unix seconds, hour-aligned.
    pub hour_bucket: i64,
    /// Mean USD price in that hour.
    pub price_usd: f64,
}

/// Hourly buckets of an asset from `from_bucket` (inclusive) onwards,
/// ascending (Node: `getSparkline` / `getCandles` source query).
pub async fn price_buckets_since(
    pool: &PgPool,
    asset_id: &str,
    from_bucket: i64,
) -> Result<Vec<PricePoint>, DbError> {
    let rows = sqlx::query_as!(
        PricePoint,
        r#"
        SELECT hour_bucket, price_usd
        FROM ts.price_history
        WHERE asset_id = $1 AND hour_bucket >= $2
        ORDER BY hour_bucket ASC
        "#,
        asset_id,
        from_bucket,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// The last bucket at or before `bucket` (Node: `getPriceChange`).
pub async fn price_at_or_before(
    pool: &PgPool,
    asset_id: &str,
    bucket: i64,
) -> Result<Option<f64>, DbError> {
    let row = sqlx::query!(
        r#"
        SELECT price_usd
        FROM ts.price_history
        WHERE asset_id = $1 AND hour_bucket <= $2
        ORDER BY hour_bucket DESC
        LIMIT 1
        "#,
        asset_id,
        bucket,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.price_usd))
}

/// One resampled series point (Node: `getPriceSeries`).
#[derive(Clone, Debug, PartialEq)]
pub struct SeriesPoint {
    /// `0x`-hex asset id.
    pub asset_id: String,
    /// Start of the resample bucket, unix seconds.
    pub t: i64,
    /// Mean USD price over the bucket.
    pub p: f64,
}

/// Mean price per `bucket_secs` window since `from_secs`, for up to a
/// few assets, ascending by time. Zero/negative prices are excluded.
pub async fn price_series(
    pool: &PgPool,
    asset_ids: &[String],
    bucket_secs: i64,
    from_secs: i64,
) -> Result<Vec<SeriesPoint>, DbError> {
    let rows = sqlx::query_as!(
        SeriesPoint,
        r#"
        SELECT asset_id,
               (hour_bucket / $2) * $2 AS "t!",
               AVG(price_usd)          AS "p!"
        FROM ts.price_history
        WHERE asset_id = ANY($1) AND price_usd > 0 AND hour_bucket >= $3
        GROUP BY asset_id, (hour_bucket / $2) * $2
        ORDER BY "t!" ASC
        "#,
        asset_ids,
        bucket_secs,
        from_secs,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
