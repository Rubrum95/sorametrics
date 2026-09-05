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
