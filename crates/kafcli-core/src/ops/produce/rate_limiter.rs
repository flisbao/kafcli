use std::time::{Duration, Instant};

/// Spaces message slots evenly at a fixed rate.
///
/// The limiter never sleeps. It tells the caller how long to wait before the next slot, and the
/// caller spends that time polling the producer.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    interval: Duration,
    next_slot: Option<Instant>,
}

impl RateLimiter {
    /// Creates a limiter for `per_second` slots. A rate of zero is treated as one slot per second.
    pub fn new(per_second: u32) -> Self {
        Self {
            interval: Duration::from_secs(1) / per_second.max(1),
            next_slot: None,
        }
    }

    /// Reserves the next slot and returns how long to wait from `now` until it opens.
    ///
    /// The first call returns zero. Calls made while a slot is still pending queue up behind it,
    /// one interval apart.
    pub fn wait_for_next_slot(&mut self, now: Instant) -> Duration {
        let slot = match self.next_slot {
            Some(next) if next > now => next,
            _ => now,
        };
        self.next_slot = Some(slot + self.interval);
        slot.saturating_duration_since(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_spaces_slots_evenly() {
        let mut limiter = RateLimiter::new(10);
        let start = Instant::now();

        let first = limiter.wait_for_next_slot(start);
        let second = limiter.wait_for_next_slot(start);
        let third = limiter.wait_for_next_slot(start);

        assert_eq!(first, Duration::ZERO);
        assert_eq!(second, Duration::from_millis(100));
        assert_eq!(third, Duration::from_millis(200));
    }

    #[test]
    fn idle_limiter_does_not_wait_for_past_slots() {
        let mut limiter = RateLimiter::new(10);
        let start = Instant::now();
        limiter.wait_for_next_slot(start);

        let later = start + Duration::from_secs(5);

        assert_eq!(limiter.wait_for_next_slot(later), Duration::ZERO);
    }
}
