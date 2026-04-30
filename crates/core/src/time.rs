//! Time wrappers and freshness helpers.

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

/// UTC timestamp wrapper.
///
/// Always serialized as RFC3339 string in JSON (matches the format the JS
/// frontend already parses with `new Date(...)`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub DateTime<Utc>);

impl Timestamp {
    /// Current wall-clock time. Use sparingly — prefer injected clocks for testability.
    pub fn now() -> Self {
        Self(Utc::now())
    }

    /// Creates from raw `DateTime<Utc>`.
    pub const fn new(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }

    /// Returns the inner `DateTime<Utc>`.
    pub const fn inner(&self) -> &DateTime<Utc> {
        &self.0
    }

    /// Time elapsed since this timestamp until `now`.
    pub fn elapsed_until(self, now: Self) -> TimeDelta {
        now.0 - self.0
    }
}

/// Freshness classification used by `/health/freshness` and per-table SLOs.
///
/// Thresholds are deliberately conservative: anything less than 30 seconds
/// behind head is healthy, up to 5 minutes is degraded, beyond is stale.
/// Tables with intrinsically slow update cadence (e.g. domains polled every
/// 5 minutes) override these thresholds at call site.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FreshnessStatus {
    /// Lag is within expected nominal bounds.
    Healthy,
    /// Lag is elevated but data is still usable.
    Degraded,
    /// Lag exceeds usable threshold.
    Stale,
}

impl FreshnessStatus {
    /// Default thresholds: healthy < 30s, degraded < 5min, stale ≥ 5min.
    pub fn classify_default(lag: TimeDelta) -> Self {
        Self::classify(lag, TimeDelta::seconds(30), TimeDelta::minutes(5))
    }

    /// Classify with explicit thresholds. `healthy_below` and
    /// `degraded_below` are exclusive upper bounds.
    pub fn classify(lag: TimeDelta, healthy_below: TimeDelta, degraded_below: TimeDelta) -> Self {
        if lag < healthy_below {
            Self::Healthy
        } else if lag < degraded_below {
            Self::Degraded
        } else {
            Self::Stale
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn freshness_default_thresholds() {
        assert_eq!(
            FreshnessStatus::classify_default(TimeDelta::seconds(0)),
            FreshnessStatus::Healthy
        );
        assert_eq!(
            FreshnessStatus::classify_default(TimeDelta::seconds(29)),
            FreshnessStatus::Healthy
        );
        assert_eq!(
            FreshnessStatus::classify_default(TimeDelta::seconds(30)),
            FreshnessStatus::Degraded
        );
        assert_eq!(
            FreshnessStatus::classify_default(TimeDelta::minutes(4)),
            FreshnessStatus::Degraded
        );
        assert_eq!(
            FreshnessStatus::classify_default(TimeDelta::minutes(5)),
            FreshnessStatus::Stale
        );
        assert_eq!(
            FreshnessStatus::classify_default(TimeDelta::hours(1)),
            FreshnessStatus::Stale
        );
    }

    #[test]
    fn freshness_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&FreshnessStatus::Healthy).unwrap(),
            "\"healthy\""
        );
        assert_eq!(
            serde_json::to_string(&FreshnessStatus::Degraded).unwrap(),
            "\"degraded\""
        );
        assert_eq!(
            serde_json::to_string(&FreshnessStatus::Stale).unwrap(),
            "\"stale\""
        );
    }

    #[test]
    fn timestamp_elapsed_positive() {
        let earlier = Timestamp::new(DateTime::from_timestamp(1_700_000_000, 0).unwrap());
        let later = Timestamp::new(DateTime::from_timestamp(1_700_000_060, 0).unwrap());
        assert_eq!(earlier.elapsed_until(later), TimeDelta::seconds(60));
    }
}
