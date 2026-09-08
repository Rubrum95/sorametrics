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

/// Min / max of the VAL/XOR USD price ratio over a window.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RateWindow {
    /// Lowest ratio in the window, `None` without data.
    pub min: Option<f64>,
    /// Highest ratio in the window.
    pub max: Option<f64>,
}

/// The Node's `getValXorRateWindows`: 24 h / 7 d / 30 d.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RateWindows {
    /// Last 24 hours.
    pub h24: RateWindow,
    /// Last 7 days.
    pub d7: RateWindow,
    /// Last 30 days.
    pub d30: RateWindow,
}

/// `price(b) / price(a)` per shared hourly bucket, min/max over the
/// three windows (Node: VAL per XOR from `sm.price_history`).
pub async fn val_xor_rate_windows(
    pool: &PgPool,
    xor_asset_id: &str,
    val_asset_id: &str,
) -> Result<RateWindows, DbError> {
    let r = sqlx::query!(
        r#"
        WITH x AS (SELECT hour_bucket h, price_usd xp FROM ts.price_history WHERE asset_id = $1 AND price_usd > 0),
             v AS (SELECT hour_bucket h, price_usd vp FROM ts.price_history WHERE asset_id = $2 AND price_usd > 0),
             r AS (SELECT x.h, v.vp / x.xp AS ratio FROM x JOIN v ON v.h = x.h),
             n AS (SELECT EXTRACT(EPOCH FROM NOW())::bigint nh)
        SELECT
            MIN(ratio) FILTER (WHERE h >= nh - 86400)   AS "min_h24: f64",
            MAX(ratio) FILTER (WHERE h >= nh - 86400)   AS "max_h24: f64",
            MIN(ratio) FILTER (WHERE h >= nh - 604800)  AS "min_d7: f64",
            MAX(ratio) FILTER (WHERE h >= nh - 604800)  AS "max_d7: f64",
            MIN(ratio) FILTER (WHERE h >= nh - 2592000) AS "min_d30: f64",
            MAX(ratio) FILTER (WHERE h >= nh - 2592000) AS "max_d30: f64"
        FROM r, n
        "#,
        xor_asset_id,
        val_asset_id,
    )
    .fetch_one(pool)
    .await?;
    Ok(RateWindows {
        h24: RateWindow {
            min: r.min_h24,
            max: r.max_h24,
        },
        d7: RateWindow {
            min: r.min_d7,
            max: r.max_d7,
        },
        d30: RateWindow {
            min: r.min_d30,
            max: r.max_d30,
        },
    })
}
