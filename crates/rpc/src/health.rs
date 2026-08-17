//! Endpoint health scoring and circuit breaking.
//!
//! Deliberately pure: no I/O, no clock reads except what the caller passes in.
//! That makes every state transition unit-testable without a network or a
//! sleeping test, and it means the selection policy can be reasoned about
//! independently of the transport.

use std::time::{Duration, Instant};

/// Circuit breaker state for one endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Normal operation.
    Closed,
    /// Tripped: requests are rejected without being attempted until `until`.
    Open { until: Instant },
    /// One probe request is allowed through to test recovery.
    HalfOpen,
}

impl BreakerState {
    pub fn label(&self) -> &'static str {
        match self {
            BreakerState::Closed => "closed",
            BreakerState::Open { .. } => "open",
            BreakerState::HalfOpen => "half_open",
        }
    }
}

#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Consecutive failures that trip the breaker.
    pub failure_threshold: u32,
    /// How long the breaker stays open before allowing a probe.
    pub reset_timeout: Duration,
    /// Smoothing factor for the latency EWMA, in percent (higher = faster to
    /// react to recent latency).
    pub latency_alpha_pct: u32,
    /// Successes required in half-open before fully closing.
    pub success_threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        HealthConfig {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(10),
            latency_alpha_pct: 20,
            success_threshold: 2,
        }
    }
}

/// Rolling health of a single endpoint.
#[derive(Debug, Clone)]
pub struct EndpointHealth {
    cfg: HealthConfig,
    state: BreakerState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    total_requests: u64,
    total_failures: u64,
    /// Exponentially weighted moving average latency, in microseconds.
    ewma_latency_us: Option<u64>,
    last_success: Option<Instant>,
    last_failure: Option<Instant>,
}

impl EndpointHealth {
    pub fn new(cfg: HealthConfig) -> Self {
        EndpointHealth {
            cfg,
            state: BreakerState::Closed,
            consecutive_failures: 0,
            consecutive_successes: 0,
            total_requests: 0,
            total_failures: 0,
            ewma_latency_us: None,
            last_success: None,
            last_failure: None,
        }
    }

    pub fn state(&self) -> BreakerState {
        self.state
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    pub fn total_requests(&self) -> u64 {
        self.total_requests
    }

    pub fn last_success(&self) -> Option<Instant> {
        self.last_success
    }

    pub fn last_failure(&self) -> Option<Instant> {
        self.last_failure
    }

    pub fn ewma_latency(&self) -> Option<Duration> {
        self.ewma_latency_us.map(Duration::from_micros)
    }

    /// Observed failure rate in `[0, 1]`.
    pub fn failure_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.total_failures as f64 / self.total_requests as f64
        }
    }

    /// Whether a request may be attempted right now.
    ///
    /// Transitions Open -> HalfOpen when the reset timeout has elapsed, which is
    /// why this takes `&mut self` and a clock reading.
    pub fn allows_request(&mut self, now: Instant) -> bool {
        match self.state {
            BreakerState::Closed | BreakerState::HalfOpen => true,
            BreakerState::Open { until } => {
                if now >= until {
                    self.state = BreakerState::HalfOpen;
                    self.consecutive_successes = 0;
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn record_success(&mut self, latency: Duration, now: Instant) {
        self.total_requests += 1;
        self.consecutive_failures = 0;
        self.consecutive_successes += 1;
        self.last_success = Some(now);

        let sample = latency.as_micros().min(u128::from(u64::MAX)) as u64;
        self.ewma_latency_us = Some(match self.ewma_latency_us {
            None => sample,
            Some(prev) => {
                let a = u128::from(self.cfg.latency_alpha_pct.min(100));
                let blended = (u128::from(sample) * a + u128::from(prev) * (100 - a)) / 100;
                blended.min(u128::from(u64::MAX)) as u64
            }
        });

        if self.state == BreakerState::HalfOpen
            && self.consecutive_successes >= self.cfg.success_threshold
        {
            self.state = BreakerState::Closed;
        }
    }

    pub fn record_failure(&mut self, now: Instant) {
        self.total_requests += 1;
        self.total_failures += 1;
        self.consecutive_failures += 1;
        self.consecutive_successes = 0;
        self.last_failure = Some(now);

        // A failure during a probe re-opens immediately: recovery is not real yet.
        let should_open = self.state == BreakerState::HalfOpen
            || self.consecutive_failures >= self.cfg.failure_threshold;
        if should_open {
            self.state = BreakerState::Open {
                until: now + self.cfg.reset_timeout,
            };
        }
    }

    /// Selection score: higher is better. `None` means "not selectable".
    ///
    /// Combines static operator preference (`weight`) with observed latency and
    /// failure rate. A never-used endpoint is scored optimistically so it gets
    /// a chance rather than being starved by an incumbent.
    pub fn score(&self, weight: u32, now: Instant) -> Option<f64> {
        match self.state {
            BreakerState::Open { until } if now < until => return None,
            _ => {}
        }
        let base = f64::from(weight.max(1));
        // Latency penalty: 1.0 at 0ms, halving roughly every 250ms.
        let latency_factor = match self.ewma_latency_us {
            None => 1.0,
            Some(us) => 1.0 / (1.0 + (us as f64 / 250_000.0)),
        };
        // Reliability penalty, weighted more heavily than latency: a slow
        // endpoint is an annoyance, a flaky one is a correctness hazard.
        let reliability_factor = (1.0 - self.failure_rate()).powi(2);
        // A half-open endpoint is deprioritised but still selectable, so probes
        // happen on real traffic without preferring a suspect endpoint.
        let probe_penalty = if self.state == BreakerState::HalfOpen {
            0.1
        } else {
            1.0
        };
        Some(base * latency_factor * reliability_factor.max(0.01) * probe_penalty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> HealthConfig {
        HealthConfig {
            failure_threshold: 3,
            reset_timeout: Duration::from_secs(10),
            latency_alpha_pct: 50,
            success_threshold: 2,
        }
    }

    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn starts_closed_and_allows_requests() {
        let mut h = EndpointHealth::new(cfg());
        assert_eq!(h.state(), BreakerState::Closed);
        assert!(h.allows_request(t0()));
        assert_eq!(h.failure_rate(), 0.0);
        assert!(h.score(100, t0()).is_some());
    }

    #[test]
    fn breaker_trips_after_consecutive_failures() {
        let now = t0();
        let mut h = EndpointHealth::new(cfg());
        h.record_failure(now);
        h.record_failure(now);
        assert_eq!(h.state(), BreakerState::Closed, "must not trip early");
        h.record_failure(now);
        assert!(matches!(h.state(), BreakerState::Open { .. }));
        assert!(!h.allows_request(now), "open breaker rejects requests");
        assert_eq!(h.score(100, now), None, "open breaker is not selectable");
    }

    #[test]
    fn a_success_resets_the_consecutive_counter() {
        let now = t0();
        let mut h = EndpointHealth::new(cfg());
        h.record_failure(now);
        h.record_failure(now);
        h.record_success(Duration::from_millis(10), now);
        assert_eq!(h.consecutive_failures(), 0);
        h.record_failure(now);
        h.record_failure(now);
        assert_eq!(h.state(), BreakerState::Closed, "counter did not reset");
    }

    #[test]
    fn open_breaker_half_opens_after_the_reset_timeout() {
        let now = t0();
        let mut h = EndpointHealth::new(cfg());
        for _ in 0..3 {
            h.record_failure(now);
        }
        assert!(!h.allows_request(now + Duration::from_secs(9)));
        assert!(h.allows_request(now + Duration::from_secs(10)));
        assert_eq!(h.state(), BreakerState::HalfOpen);
    }

    #[test]
    fn half_open_closes_only_after_enough_successes() {
        let now = t0();
        let mut h = EndpointHealth::new(cfg());
        for _ in 0..3 {
            h.record_failure(now);
        }
        let later = now + Duration::from_secs(11);
        assert!(h.allows_request(later));
        h.record_success(Duration::from_millis(5), later);
        assert_eq!(
            h.state(),
            BreakerState::HalfOpen,
            "one success is not enough"
        );
        h.record_success(Duration::from_millis(5), later);
        assert_eq!(h.state(), BreakerState::Closed);
    }

    #[test]
    fn a_failed_probe_reopens_immediately() {
        let now = t0();
        let mut h = EndpointHealth::new(cfg());
        for _ in 0..3 {
            h.record_failure(now);
        }
        let later = now + Duration::from_secs(11);
        assert!(h.allows_request(later));
        assert_eq!(h.state(), BreakerState::HalfOpen);
        // A single failure in half-open re-opens, without waiting for the
        // threshold again.
        h.record_failure(later);
        match h.state() {
            BreakerState::Open { until } => {
                assert_eq!(until, later + Duration::from_secs(10))
            }
            other => panic!("expected reopen, got {other:?}"),
        }
    }

    #[test]
    fn latency_ewma_tracks_recent_samples() {
        let now = t0();
        let mut h = EndpointHealth::new(cfg()); // alpha = 50%
        h.record_success(Duration::from_millis(100), now);
        assert_eq!(h.ewma_latency(), Some(Duration::from_millis(100)));
        h.record_success(Duration::from_millis(200), now);
        assert_eq!(h.ewma_latency(), Some(Duration::from_millis(150)));
        h.record_success(Duration::from_millis(200), now);
        assert_eq!(h.ewma_latency(), Some(Duration::from_millis(175)));
    }

    #[test]
    fn faster_endpoint_scores_higher_at_equal_weight() {
        let now = t0();
        let mut fast = EndpointHealth::new(cfg());
        let mut slow = EndpointHealth::new(cfg());
        fast.record_success(Duration::from_millis(20), now);
        slow.record_success(Duration::from_millis(800), now);
        assert!(fast.score(100, now).unwrap() > slow.score(100, now).unwrap());
    }

    #[test]
    fn reliable_endpoint_beats_a_faster_flaky_one() {
        let now = t0();
        let mut flaky = EndpointHealth::new(cfg());
        let mut steady = EndpointHealth::new(cfg());
        // flaky: fast but fails half the time
        for _ in 0..5 {
            flaky.record_success(Duration::from_millis(10), now);
            flaky.record_failure(now);
        }
        // steady: slower but never fails
        for _ in 0..10 {
            steady.record_success(Duration::from_millis(120), now);
        }
        assert!(
            steady.score(100, now).unwrap() > flaky.score(100, now).unwrap(),
            "reliability must outweigh latency"
        );
    }

    #[test]
    fn operator_weight_breaks_ties() {
        let now = t0();
        let mut a = EndpointHealth::new(cfg());
        let mut b = EndpointHealth::new(cfg());
        a.record_success(Duration::from_millis(50), now);
        b.record_success(Duration::from_millis(50), now);
        assert!(a.score(200, now).unwrap() > b.score(100, now).unwrap());
    }

    #[test]
    fn unused_endpoint_is_scored_optimistically() {
        let now = t0();
        let fresh = EndpointHealth::new(cfg());
        let mut degraded = EndpointHealth::new(cfg());
        for _ in 0..10 {
            degraded.record_success(Duration::from_millis(900), now);
        }
        assert!(
            fresh.score(100, now).unwrap() > degraded.score(100, now).unwrap(),
            "a fresh endpoint must get a chance"
        );
    }

    #[test]
    fn half_open_endpoint_is_deprioritised_but_selectable() {
        let now = t0();
        let mut h = EndpointHealth::new(cfg());
        for _ in 0..3 {
            h.record_failure(now);
        }
        let later = now + Duration::from_secs(11);
        h.allows_request(later);
        assert_eq!(h.state(), BreakerState::HalfOpen);
        let healthy = EndpointHealth::new(cfg());
        let probe_score = h.score(100, later).unwrap();
        assert!(probe_score > 0.0, "must remain selectable for probing");
        assert!(probe_score < healthy.score(100, later).unwrap());
    }

    #[test]
    fn failure_rate_is_bounded_and_accurate() {
        let now = t0();
        let mut h = EndpointHealth::new(HealthConfig {
            failure_threshold: 100,
            ..cfg()
        });
        for _ in 0..3 {
            h.record_success(Duration::from_millis(1), now);
        }
        h.record_failure(now);
        assert_eq!(h.total_requests(), 4);
        assert!((h.failure_rate() - 0.25).abs() < 1e-9);
    }
}
