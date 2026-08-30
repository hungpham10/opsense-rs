//! Cancellation token for long-running session operations.
//!
//! Std-only (no async runtime): the REPL and MCP tool handlers run on plain
//! threads, so cancellation is a mutex-guarded flag plus a condvar for waiters.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Token for cancelling in-flight analysis work. Cloning shares the flag;
/// [`CancellationToken::cancel] is idempotent and wakes all waiters.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    state: Arc<(Mutex<bool>, Condvar)>,
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    /// Mark the operation as cancelled and wake every waiter.
    pub fn cancel(&self) {
        let (flag, signal) = &*self.state;
        *flag.lock().unwrap() = true;
        signal.notify_all();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.state.0.lock().unwrap()
    }

    /// Error helper for execution loops: `token.check()?` bails out when
    /// the user interrupted or the deadline passed.
    ///
    /// # Errors
    /// Returns an error when the token has been cancelled.
    pub fn check(&self) -> anyhow::Result<()> {
        if self.is_cancelled() {
            Err(anyhow::anyhow!("operation cancelled"))
        } else {
            Ok(())
        }
    }

    /// Block until cancelled (or `timeout` elapses; `None` waits forever).
    /// Returns whether the token is cancelled at return time.
    pub fn wait_cancelled(&self, timeout: Option<Duration>) -> bool {
        let (flag, signal) = &*self.state;
        let guard = flag.lock().unwrap();
        if *guard {
            return true;
        }
        match timeout {
            Some(d) => {
                let (g, _res) = signal.wait_timeout_while(guard, d, |c| !*c).unwrap();
                *g
            }
            None => {
                let g = signal.wait_while(guard, |c| !*c).unwrap();
                *g
            }
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn cancel_wakes_waiter_and_check_fails() {
        let token = CancellationToken::new();
        assert!(!token.wait_cancelled(Some(Duration::from_millis(10))));

        let waiter = token.clone();
        let handle = std::thread::spawn(move || waiter.wait_cancelled(None));
        std::thread::sleep(Duration::from_millis(20));
        token.cancel();
        assert!(handle.join().unwrap());

        assert!(token.is_cancelled());
        assert!(token.check().is_err());
    }

    #[test]
    fn timeout_returns_without_cancel() {
        let token = CancellationToken::new();
        let start = Instant::now();
        assert!(!token.wait_cancelled(Some(Duration::from_millis(30))));
        assert!(start.elapsed() >= Duration::from_millis(25));
        assert!(token.check().is_ok());
    }

    #[test]
    fn cancel_is_idempotent() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn check_returns_error_after_cancel() {
        let token = CancellationToken::new();
        assert!(token.check().is_ok());
        token.cancel();
        let err = token.check().unwrap_err();
        assert!(err.to_string().contains("cancelled"));
    }

    #[test]
    fn default_is_same_as_new() {
        let token: CancellationToken = Default::default();
        assert!(!token.is_cancelled());
        assert!(token.check().is_ok());
    }

    #[test]
    fn cloned_token_shares_state() {
        let token = CancellationToken::new();
        let clone = token.clone();
        assert!(!clone.is_cancelled());
        clone.cancel();
        // original sees the cancel
        assert!(token.is_cancelled());
    }

    #[test]
    fn wait_cancelled_returns_immediately_if_already_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        // no timeout supplied — must still return true immediately.
        let start = Instant::now();
        assert!(token.wait_cancelled(None));
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn wait_cancelled_returns_true_when_timeout_with_zero_duration() {
        let token = CancellationToken::new();
        // Zero timeout: should return false (not cancelled) immediately.
        let start = Instant::now();
        assert!(!token.wait_cancelled(Some(Duration::from_millis(0))));
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn multiple_waiters_all_woken() {
        let token = CancellationToken::new();
        let mut handles = Vec::new();
        for _ in 0..5 {
            let t = token.clone();
            handles.push(std::thread::spawn(move || t.wait_cancelled(None)));
        }
        std::thread::sleep(Duration::from_millis(20));
        token.cancel();
        for h in handles {
            assert!(h.join().unwrap());
        }
    }

    #[test]
    fn cancel_message_includes_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        let err = token.check().unwrap_err();
        // the error message must indicate cancellation to callers
        assert!(err.to_string().contains("cancelled"));
    }
}
