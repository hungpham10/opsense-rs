//! Lightweight OAuth metrics — atomic counters + JSON `/metrics/oauth` endpoint.
//!
//! 6 counters theo plan:
//! - `device_code_issued_total`
//! - `device_code_approved_total`
//! - `device_code_denied_total`
//! - `access_token_issued_total`
//! - `access_token_refreshed_total`
//! - `long_session_issued_total`
//!
//! Tránh kéo thêm dep `prometheus`; dùng `AtomicU64` + expose JSON.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default, Debug)]
pub struct OAuthMetrics {
    pub device_code_issued:    AtomicU64,
    pub device_code_approved:  AtomicU64,
    pub device_code_denied:    AtomicU64,
    pub access_token_issued:   AtomicU64,
    pub access_token_refreshed: AtomicU64,
    pub long_session_issued:   AtomicU64,
}

impl OAuthMetrics {
    pub const fn new() -> Self {
        Self {
            device_code_issued:    AtomicU64::new(0),
            device_code_approved:  AtomicU64::new(0),
            device_code_denied:    AtomicU64::new(0),
            access_token_issued:   AtomicU64::new(0),
            access_token_refreshed: AtomicU64::new(0),
            long_session_issued:   AtomicU64::new(0),
        }
    }

    pub fn inc_device_code_issued(&self) {
        self.device_code_issued.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_device_code_approved(&self) {
        self.device_code_approved.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_device_code_denied(&self) {
        self.device_code_denied.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_access_token_issued(&self) {
        self.access_token_issued.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_access_token_refreshed(&self) {
        self.access_token_refreshed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_long_session_issued(&self) {
        self.long_session_issued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> OAuthMetricsSnapshot {
        OAuthMetricsSnapshot {
            device_code_issued:    self.device_code_issued.load(Ordering::Relaxed),
            device_code_approved:  self.device_code_approved.load(Ordering::Relaxed),
            device_code_denied:    self.device_code_denied.load(Ordering::Relaxed),
            access_token_issued:   self.access_token_issued.load(Ordering::Relaxed),
            access_token_refreshed: self.access_token_refreshed.load(Ordering::Relaxed),
            long_session_issued:   self.long_session_issued.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OAuthMetricsSnapshot {
    pub device_code_issued:    u64,
    pub device_code_approved:  u64,
    pub device_code_denied:    u64,
    pub access_token_issued:   u64,
    pub access_token_refreshed: u64,
    pub long_session_issued:   u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increments() {
        let m = OAuthMetrics::new();
        m.inc_device_code_issued();
        m.inc_device_code_issued();
        m.inc_device_code_approved();
        m.inc_long_session_issued();

        let s = m.snapshot();
        assert_eq!(s.device_code_issued, 2);
        assert_eq!(s.device_code_approved, 1);
        assert_eq!(s.device_code_denied, 0);
        assert_eq!(s.long_session_issued, 1);
        assert_eq!(s.access_token_issued, 0);
    }

    #[test]
    fn test_atomic_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let m = Arc::new(OAuthMetrics::new());
        let mut handles = vec![];
        for _ in 0..10 {
            let m = m.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    m.inc_device_code_issued();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.snapshot().device_code_issued, 10_000);
    }
}
