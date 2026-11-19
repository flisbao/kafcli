use super::access_token::AccessToken;
use super::token_source::TokenSource;
use crate::error::CoreResult;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

/// A token is reused only while more than this much time is left before it expires.
pub const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// Source of the current time. Replaced by a fake clock in tests.
pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Keeps the last token in a single slot and asks the inner source again once it is close to
/// expiry. Failed fetches are never stored.
pub struct CachedTokenSource {
    inner: Box<dyn TokenSource>,
    clock: Box<dyn Clock>,
    cached: Mutex<Option<AccessToken>>,
}

impl CachedTokenSource {
    pub fn new(inner: Box<dyn TokenSource>) -> Self {
        Self::with_clock(inner, Box::new(SystemClock))
    }

    pub(crate) fn with_clock(inner: Box<dyn TokenSource>, clock: Box<dyn Clock>) -> Self {
        Self {
            inner,
            clock,
            cached: Mutex::new(None),
        }
    }
}

impl TokenSource for CachedTokenSource {
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the lock is held while fetching, so concurrent callers share one fetch"
    )]
    fn fetch_access_token(&self) -> CoreResult<AccessToken> {
        let mut cached = lock_cache(&self.cached);
        let now = self.clock.now();
        if let Some(token) = cached.as_ref()
            && is_reusable(token.expires_at, now)
        {
            return Ok(token.clone());
        }

        *cached = None;
        let token = self.inner.fetch_access_token()?;
        *cached = Some(token.clone());
        Ok(token)
    }

    fn describe_source(&self) -> &'static str {
        self.inner.describe_source()
    }
}

fn is_reusable(expires_at: SystemTime, now: SystemTime) -> bool {
    expires_at
        .duration_since(now)
        .is_ok_and(|remaining| remaining > TOKEN_REFRESH_MARGIN)
}

fn lock_cache(cache: &Mutex<Option<AccessToken>>) -> MutexGuard<'_, Option<AccessToken>> {
    cache.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CoreError;
    use secrecy::SecretString;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, UNIX_EPOCH};

    const T0_SECONDS: u64 = 2_000_000_000;

    fn at(offset_seconds: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(T0_SECONDS + offset_seconds)
    }

    #[derive(Clone)]
    struct FakeClock {
        now: Arc<Mutex<SystemTime>>,
    }

    impl FakeClock {
        fn set(&self, time: SystemTime) {
            *self.now.lock().unwrap() = time;
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> SystemTime {
            *self.now.lock().unwrap()
        }
    }

    struct FakeTokenSource {
        fetches: Arc<AtomicUsize>,
        expires_at: SystemTime,
        fail_first_call: bool,
    }

    impl TokenSource for FakeTokenSource {
        fn fetch_access_token(&self) -> CoreResult<AccessToken> {
            let previous = self.fetches.fetch_add(1, Ordering::SeqCst);
            if self.fail_first_call && previous == 0 {
                return Err(CoreError::Authentication("first call fails".to_owned()));
            }
            Ok(AccessToken::new(
                SecretString::from("fake-token"),
                self.expires_at,
            ))
        }

        fn describe_source(&self) -> &'static str {
            "fake"
        }
    }

    fn cache_with(
        expires_at: SystemTime,
        fail_first_call: bool,
    ) -> (CachedTokenSource, Arc<AtomicUsize>, FakeClock) {
        let fetches = Arc::new(AtomicUsize::new(0));
        let clock = FakeClock {
            now: Arc::new(Mutex::new(at(0))),
        };
        let source = FakeTokenSource {
            fetches: Arc::clone(&fetches),
            expires_at,
            fail_first_call,
        };
        let cache = CachedTokenSource::with_clock(Box::new(source), Box::new(clock.clone()));
        (cache, fetches, clock)
    }

    #[test]
    fn reuses_token_until_refresh_margin() {
        let (cache, fetches, clock) = cache_with(at(3600), false);

        cache.fetch_access_token().unwrap();
        clock.set(at(3299));
        cache.fetch_access_token().unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 1);

        clock.set(at(3301));
        cache.fetch_access_token().unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn failure_is_not_cached() {
        let (cache, fetches, _clock) = cache_with(at(3600), true);

        let first = cache.fetch_access_token();
        assert!(matches!(first, Err(CoreError::Authentication(_))));

        let second = cache.fetch_access_token();
        assert!(second.is_ok());
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn token_already_inside_margin_is_fetched_every_time() {
        let (cache, fetches, _clock) = cache_with(at(60), false);

        cache.fetch_access_token().unwrap();
        cache.fetch_access_token().unwrap();

        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }
}
