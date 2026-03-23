use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::config::CircuitBreakerConfig;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum State {
    Closed,
    Open,
    HalfOpen,
}

struct CircuitBreakerState {
    state: State,
    consecutive_failures: u32,
    consecutive_successes: u32,
    opened_at: Option<Instant>,
    recent_calls: VecDeque<(Instant, bool)>, // (timestamp, is_success)
}

#[derive(Clone)]
pub struct CircuitBreaker {
    name: String,
    config: CircuitBreakerConfig,
    state: Arc<Mutex<CircuitBreakerState>>,
}

impl CircuitBreaker {
    pub fn new(name: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        Self {
            name: name.into(),
            config,
            state: Arc::new(Mutex::new(CircuitBreakerState {
                state: State::Closed,
                consecutive_failures: 0,
                consecutive_successes: 0,
                opened_at: None,
                recent_calls: VecDeque::new(),
            })),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, CircuitBreakerState> {
        self.state.lock().unwrap_or_else(|poisoned| {
            tracing::error!(service = %self.name, "Circuit breaker state lock poisoned");
            poisoned.into_inner()
        })
    }

    /// Checks if a call is permitted based on the current state and timing of the circuit breaker.
    pub fn is_call_permitted(&self) -> bool {
        let mut lock = self.lock_state();

        match lock.state {
            State::Closed => true,
            State::HalfOpen => true,
            State::Open => {
                if let Some(opened_at) = lock.opened_at {
                    if opened_at.elapsed() >= Duration::from_secs(self.config.recovery_timeout_secs)
                    {
                        tracing::warn!(
                            service = %self.name,
                            "Circuit breaker transitioning: OPEN -> HALF-OPEN"
                        );
                        lock.state = State::HalfOpen;
                        lock.consecutive_successes = 0;
                        return true;
                    }
                }
                false
            }
        }
    }

    /// Updates the circuit breaker state on a successful call, potentially transitioning from HALF-OPEN to CLOSED.
    pub fn on_success(&self) {
        let mut lock = self.lock_state();
        let now = Instant::now();

        lock.recent_calls.push_back((now, true));
        self.cleanup_window(&mut lock, now);

        match lock.state {
            State::Closed => {
                // Reset consecutive failures on success
                lock.consecutive_failures = 0;
            }
            State::HalfOpen => {
                lock.consecutive_successes += 1;
                if lock.consecutive_successes >= self.config.success_threshold {
                    tracing::warn!(
                        service = %self.name,
                        "Circuit breaker transitioning: HALF-OPEN -> CLOSED"
                    );
                    lock.state = State::Closed;
                    lock.consecutive_failures = 0;
                    lock.consecutive_successes = 0;
                    lock.recent_calls.clear();
                }
            }
            State::Open => {} // Should not happen, but safe to ignore
        }
    }

    /// Updates the circuit breaker state on a failed call, potentially transitioning to OPEN based on consecutive failures or failure rate.
    pub fn on_error(&self) {
        let mut lock = self.lock_state();
        let now = Instant::now();

        lock.recent_calls.push_back((now, false));
        self.cleanup_window(&mut lock, now);

        match lock.state {
            State::Closed => {
                lock.consecutive_failures += 1;

                let total_calls = lock.recent_calls.len() as u32;
                let failed_calls =
                    lock.recent_calls.iter().filter(|(_, success)| !*success).count() as u32;

                // Calculate if failure rate > 50% with a minimum sample size to avoid tripping on small numbers of calls
                let min_sample_size = self.config.failure_threshold;
                let high_failure_rate = total_calls >= min_sample_size
                    && (failed_calls as f32 / total_calls as f32) > 0.5;

                if lock.consecutive_failures >= self.config.failure_threshold || high_failure_rate {
                    tracing::warn!(
                        service = %self.name,
                        reason = if high_failure_rate { ">50% failure rate" } else { "consecutive failures" },
                        "Circuit breaker transitioning: CLOSED -> OPEN"
                    );
                    lock.state = State::Open;
                    lock.opened_at = Some(now);
                }
            }
            State::HalfOpen => {
                // ANY failure in Half-Open instantly trips it back to Open
                tracing::warn!(
                    service = %self.name,
                    "Circuit breaker transitioning: HALF-OPEN -> OPEN"
                );
                lock.state = State::Open;
                lock.opened_at = Some(now);
            }
            State::Open => {
                // Already open, reset the recovery timer
                lock.opened_at = Some(now);
            }
        }
    }

    /// Removes calls older than the configured recovery timeout from the sliding window
    fn cleanup_window(&self, state: &mut CircuitBreakerState, now: Instant) {
        let window = Duration::from_secs(self.config.recovery_timeout_secs);
        while let Some(&(timestamp, _)) = state.recent_calls.front() {
            if now.duration_since(timestamp) > window {
                state.recent_calls.pop_front();
            } else {
                // Since they are pushed in chronological order, once we hit a recent one, we can stop
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold: 5,
            recovery_timeout_secs: 1, // Short timeout for testing
            success_threshold: 3,
        }
    }

    #[test]
    fn test_consecutive_failures_trip_circuit() {
        let cb = CircuitBreaker::new("Test", test_config());

        for _ in 0..4 {
            cb.on_error();
            assert!(cb.is_call_permitted());
        }

        // 5th failure trips it
        cb.on_error();
        assert!(!cb.is_call_permitted());
    }

    #[test]
    fn test_failure_rate_trips_circuit() {
        let cb = CircuitBreaker::new("Test", test_config()); // failure_threshold is 5

        // 5 calls: Error, Error, Success, Success, Error.

        cb.on_error();
        assert!(cb.is_call_permitted()); // total: 1, fails: 1 (sample size not met)

        cb.on_error();
        assert!(cb.is_call_permitted()); // total: 2, fails: 2 (sample size not met)

        cb.on_success();
        assert!(cb.is_call_permitted()); // total: 3, fails: 2 (sample size not met)

        cb.on_success();
        assert!(cb.is_call_permitted()); // total: 4, fails: 2 (sample size not met)

        // 5th call meets the sample size (5).
        // Total fails = 3. 3/5 = 60%. >50% rule triggers
        cb.on_error();

        // Should be tripped now due to failure rate > 50%
        assert!(!cb.is_call_permitted());
    }

    #[tokio::test]
    async fn test_recovery_flow() {
        let cb = CircuitBreaker::new("Test", test_config());

        // 1. Trip the circuit
        for _ in 0..5 {
            cb.on_error();
        }
        assert!(!cb.is_call_permitted());

        // 2. Wait for recovery timeout (1 sec)
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // 3. Should now be Half-Open
        assert!(cb.is_call_permitted());

        // 4. Requires 3 consecutive successes to Close
        cb.on_success();
        cb.on_success();
        // If we fail now, it should instantly snap back to Open!
        cb.on_error();
        assert!(!cb.is_call_permitted());

        // Wait again...
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(cb.is_call_permitted());

        // 3 successes fully closes it
        cb.on_success();
        cb.on_success();
        cb.on_success();

        // Should be closed now, so a single failure doesn't trip it
        cb.on_error();
        assert!(cb.is_call_permitted());
    }
}
