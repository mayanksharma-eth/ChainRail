//! Backoff policy shared by the RPC gateway, Kafka consumers and DB retries.
//!
//! Pure computation, no sleeping and no I/O, so the schedule is unit-testable
//! and identical everywhere. Callers do the actual waiting.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    pub base: Duration,
    pub max: Duration,
    pub max_attempts: u32,
    /// Jitter as a percentage of the computed delay, applied symmetrically.
    /// Full jitter is the default because synchronized retries across many
    /// workers are the usual cause of a thundering-herd outage.
    pub jitter_pct: u32,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff {
            base: Duration::from_millis(200),
            max: Duration::from_secs(30),
            max_attempts: 5,
            jitter_pct: 50,
        }
    }
}

impl Backoff {
    pub fn new(base_ms: u64, max_ms: u64, max_attempts: u32) -> Self {
        Backoff {
            base: Duration::from_millis(base_ms),
            max: Duration::from_millis(max_ms),
            max_attempts,
            ..Default::default()
        }
    }

    /// Delay before attempt `attempt` (1-based: attempt 1 is the first retry).
    /// Exponential with saturation, then jitter.
    pub fn delay(&self, attempt: u32) -> Duration {
        self.delay_with_jitter(attempt, rand_unit())
    }

    /// Deterministic variant: `jitter_unit` in `[0, 1)`.
    pub fn delay_with_jitter(&self, attempt: u32, jitter_unit: f64) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }
        let shift = (attempt - 1).min(31);
        let scaled = self
            .base
            .as_millis()
            .saturating_mul(1u128 << shift)
            .min(self.max.as_millis());
        let scaled = scaled as u64;
        if self.jitter_pct == 0 {
            return Duration::from_millis(scaled);
        }
        let span = scaled.saturating_mul(u64::from(self.jitter_pct.min(100))) / 100;
        // Center the jitter window on `scaled`, clamped to [0, max].
        let low = scaled.saturating_sub(span / 2);
        let jittered = low + ((span as f64) * jitter_unit.clamp(0.0, 1.0)) as u64;
        Duration::from_millis(jittered.min(self.max.as_millis() as u64))
    }

    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }

    /// Total worst-case wall time if every attempt fails.
    pub fn worst_case_total(&self) -> Duration {
        (1..=self.max_attempts)
            .map(|a| self.delay_with_jitter(a, 1.0))
            .sum()
    }
}

fn rand_unit() -> f64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    // Cheap, dependency-free source of per-call entropy. Jitter does not need
    // to be cryptographically random -- it only needs to be uncorrelated across
    // processes, which `RandomState`'s per-instance keys provide.
    let mut h = RandomState::new().build_hasher();
    h.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0),
    );
    (h.finish() % 10_000) as f64 / 10_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_exponentially_and_saturates() {
        let b = Backoff {
            base: Duration::from_millis(100),
            max: Duration::from_millis(1_000),
            max_attempts: 10,
            jitter_pct: 0,
        };
        assert_eq!(b.delay(1), Duration::from_millis(100));
        assert_eq!(b.delay(2), Duration::from_millis(200));
        assert_eq!(b.delay(3), Duration::from_millis(400));
        assert_eq!(b.delay(4), Duration::from_millis(800));
        assert_eq!(b.delay(5), Duration::from_millis(1_000)); // capped
        assert_eq!(b.delay(50), Duration::from_millis(1_000)); // no overflow
    }

    #[test]
    fn attempt_zero_is_immediate() {
        assert_eq!(Backoff::default().delay(0), Duration::ZERO);
    }

    #[test]
    fn jitter_stays_within_the_window() {
        let b = Backoff {
            base: Duration::from_millis(1_000),
            max: Duration::from_secs(60),
            max_attempts: 5,
            jitter_pct: 50,
        };
        // window is 1000ms +/- 25% => [750, 1250]
        assert_eq!(b.delay_with_jitter(1, 0.0), Duration::from_millis(750));
        assert_eq!(b.delay_with_jitter(1, 1.0), Duration::from_millis(1_250));
        for i in 0..100 {
            let d = b.delay_with_jitter(1, f64::from(i) / 100.0);
            assert!(d >= Duration::from_millis(750) && d <= Duration::from_millis(1_250));
        }
    }

    #[test]
    fn jitter_never_exceeds_the_ceiling() {
        let b = Backoff {
            base: Duration::from_millis(1_000),
            max: Duration::from_millis(1_000),
            max_attempts: 5,
            jitter_pct: 100,
        };
        assert!(b.delay_with_jitter(9, 1.0) <= Duration::from_millis(1_000));
    }

    #[test]
    fn attempt_budget_is_respected() {
        let b = Backoff::new(10, 100, 3);
        assert!(b.should_retry(0));
        assert!(b.should_retry(2));
        assert!(!b.should_retry(3));
        assert!(b.worst_case_total() < Duration::from_secs(1));
    }

    #[test]
    fn random_jitter_is_in_unit_range() {
        for _ in 0..1_000 {
            let u = rand_unit();
            assert!((0.0..1.0).contains(&u));
        }
    }
}
