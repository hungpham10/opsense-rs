//! Resource limits for sessions.

use chrono::Duration as ChronoDuration;
use opsense_core::config::Config;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Resource limits for a session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum memory usage in MB
    pub max_memory_mb: u64,
    /// Maximum CPU time in seconds
    pub max_cpu_time_secs: u64,
    /// Maximum result rows
    pub max_result_rows: u64,
    /// Maximum execution time for a single command in seconds
    pub max_execution_time_secs: u64,
    /// Maximum history entries
    pub max_history: usize,
    /// Allow filesystem access from Python
    pub allow_fs: bool,
    /// Allow network access from Python
    pub allow_net: bool,
    /// Idle timeout in seconds
    pub idle_timeout_secs: u64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_mb: 2048,
            max_cpu_time_secs: 300,
            max_result_rows: 1_000_000,
            max_execution_time_secs: 60,
            max_history: 10000,
            allow_fs: false,
            allow_net: false,
            idle_timeout_secs: 1800,
        }
    }
}

impl ResourceLimits {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_memory_mb: config.session.max_memory_mb,
            max_cpu_time_secs: config.session.max_cpu_time_secs,
            max_result_rows: config.session.max_result_rows,
            max_execution_time_secs: config.session.max_execution_time_secs,
            max_history: config.repl.max_history,
            allow_fs: config.session.allow_fs,
            allow_net: config.session.allow_net,
            idle_timeout_secs: config.session.idle_timeout_secs,
        }
    }

    pub fn memory_limit_bytes(&self) -> u64 {
        self.max_memory_mb * 1024 * 1024
    }

    pub fn cpu_time_limit(&self) -> Duration {
        Duration::from_secs(self.max_cpu_time_secs)
    }

    pub fn execution_time_limit(&self) -> Duration {
        Duration::from_secs(self.max_execution_time_secs)
    }

    pub fn idle_timeout(&self) -> ChronoDuration {
        ChronoDuration::seconds(self.idle_timeout_secs as i64)
    }
}

/// Resource usage tracking
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub memory_bytes: u64,
    pub cpu_time_ms: u64,
    pub result_rows: u64,
    pub execution_time_ms: u64,
}

impl ResourceUsage {
    pub fn add_memory(&mut self, bytes: u64) {
        self.memory_bytes += bytes;
    }

    pub fn add_cpu_time(&mut self, ms: u64) {
        self.cpu_time_ms += ms;
    }

    pub fn add_rows(&mut self, rows: u64) {
        self.result_rows += rows;
    }

    pub fn check_limits(&self, limits: &ResourceLimits) -> Result<(), String> {
        if self.memory_bytes > limits.memory_limit_bytes() {
            return Err(format!(
                "Memory limit exceeded: {} > {} MB",
                self.memory_bytes / 1024 / 1024,
                limits.max_memory_mb
            ));
        }
        if self.result_rows > limits.max_result_rows {
            return Err(format!(
                "Result row limit exceeded: {} > {}",
                self.result_rows, limits.max_result_rows
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_resource_limits_have_expected_values() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_memory_mb, 2048);
        assert_eq!(limits.max_cpu_time_secs, 300);
        assert_eq!(limits.max_result_rows, 1_000_000);
        assert_eq!(limits.max_execution_time_secs, 60);
        assert_eq!(limits.max_history, 10000);
        assert!(!limits.allow_fs);
        assert!(!limits.allow_net);
        assert_eq!(limits.idle_timeout_secs, 1800);
    }

    #[test]
    fn memory_limit_bytes_converts_mb_to_bytes() {
        let mut limits = ResourceLimits::default();
        limits.max_memory_mb = 512;
        assert_eq!(limits.memory_limit_bytes(), 512 * 1024 * 1024);

        limits.max_memory_mb = 1;
        assert_eq!(limits.memory_limit_bytes(), 1024 * 1024);

        limits.max_memory_mb = 0;
        assert_eq!(limits.memory_limit_bytes(), 0);
    }

    #[test]
    fn cpu_time_limit_returns_duration() {
        let mut limits = ResourceLimits::default();
        limits.max_cpu_time_secs = 42;
        assert_eq!(limits.cpu_time_limit(), Duration::from_secs(42));
    }

    #[test]
    fn execution_time_limit_returns_duration() {
        let mut limits = ResourceLimits::default();
        limits.max_execution_time_secs = 90;
        assert_eq!(limits.execution_time_limit(), Duration::from_secs(90));
    }

    #[test]
    fn idle_timeout_returns_chrono_duration() {
        let mut limits = ResourceLimits::default();
        limits.idle_timeout_secs = 600;
        assert_eq!(limits.idle_timeout(), ChronoDuration::seconds(600));
    }

    #[test]
    fn from_config_maps_fields() {
        use opsense_core::config::{
            Config, EngineConfig, ReplConfig, SessionConfig, SourcesConfig, StorageConfig,
        };
        use std::collections::HashMap;

        let cfg = Config {
            engine: EngineConfig::default(),
            capacity: HashMap::new(),
            sources: SourcesConfig::default(),
            attributes: HashMap::new(),
            storage: StorageConfig::default(),
            pipeline: None,
            session: SessionConfig {
                max_memory_mb: 1024,
                max_cpu_time_secs: 120,
                max_result_rows: 50_000,
                max_execution_time_secs: 30,
                idle_timeout_secs: 900,
                allow_fs: true,
                allow_net: false,
            },
            repl: ReplConfig {
                max_history: 500,
                ..ReplConfig::default()
            },
        };
        let limits = ResourceLimits::from_config(&cfg);
        assert_eq!(limits.max_memory_mb, 1024);
        assert_eq!(limits.max_cpu_time_secs, 120);
        assert_eq!(limits.max_result_rows, 50_000);
        assert_eq!(limits.max_execution_time_secs, 30);
        assert_eq!(limits.max_history, 500);
        assert!(limits.allow_fs);
        assert!(!limits.allow_net);
        assert_eq!(limits.idle_timeout_secs, 900);
    }

    #[test]
    fn default_resource_usage_starts_zero() {
        let usage = ResourceUsage::default();
        assert_eq!(usage.memory_bytes, 0);
        assert_eq!(usage.cpu_time_ms, 0);
        assert_eq!(usage.result_rows, 0);
        assert_eq!(usage.execution_time_ms, 0);
    }

    #[test]
    fn add_memory_accumulates() {
        let mut usage = ResourceUsage::default();
        usage.add_memory(100);
        usage.add_memory(50);
        usage.add_memory(0);
        assert_eq!(usage.memory_bytes, 150);
    }

    #[test]
    fn add_cpu_time_accumulates() {
        let mut usage = ResourceUsage::default();
        usage.add_cpu_time(10);
        usage.add_cpu_time(20);
        assert_eq!(usage.cpu_time_ms, 30);
    }

    #[test]
    fn add_rows_accumulates() {
        let mut usage = ResourceUsage::default();
        usage.add_rows(1000);
        usage.add_rows(500);
        assert_eq!(usage.result_rows, 1500);
    }

    #[test]
    fn check_limits_passes_when_within_bounds() {
        let usage = ResourceUsage {
            memory_bytes: 1024 * 1024 * 1024, // 1 GB
            cpu_time_ms: 0,
            result_rows: 100,
            execution_time_ms: 0,
        };
        let limits = ResourceLimits::default();
        assert!(usage.check_limits(&limits).is_ok());
    }

    #[test]
    fn check_limits_fails_when_memory_exceeded() {
        let usage = ResourceUsage {
            memory_bytes: 4096 * 1024 * 1024, // 4 GB
            cpu_time_ms: 0,
            result_rows: 0,
            execution_time_ms: 0,
        };
        let mut limits = ResourceLimits::default();
        limits.max_memory_mb = 2048; // 2 GB
        let err = usage.check_limits(&limits).unwrap_err();
        assert!(err.contains("Memory limit exceeded"));
    }

    #[test]
    fn check_limits_passes_at_exact_memory_boundary() {
        // The check uses `>`, so equal is OK.
        let mut limits = ResourceLimits::default();
        limits.max_memory_mb = 1;
        let usage = ResourceUsage {
            memory_bytes: limits.memory_limit_bytes(),
            ..ResourceUsage::default()
        };
        assert!(usage.check_limits(&limits).is_ok());
    }

    #[test]
    fn check_limits_fails_when_one_byte_over_memory() {
        let mut limits = ResourceLimits::default();
        limits.max_memory_mb = 1;
        let usage = ResourceUsage {
            memory_bytes: limits.memory_limit_bytes() + 1,
            ..ResourceUsage::default()
        };
        assert!(usage.check_limits(&limits).is_err());
    }

    #[test]
    fn check_limits_fails_when_rows_exceeded() {
        let usage = ResourceUsage {
            memory_bytes: 0,
            cpu_time_ms: 0,
            result_rows: 2_000_000,
            execution_time_ms: 0,
        };
        let mut limits = ResourceLimits::default();
        limits.max_result_rows = 1_000_000;
        let err = usage.check_limits(&limits).unwrap_err();
        assert!(err.contains("Result row limit exceeded"));
    }

    #[test]
    fn check_limits_passes_at_exact_row_boundary() {
        let mut limits = ResourceLimits::default();
        limits.max_result_rows = 100;
        let usage = ResourceUsage {
            result_rows: 100,
            ..ResourceUsage::default()
        };
        assert!(usage.check_limits(&limits).is_ok());
    }

    #[test]
    fn check_limits_memory_takes_precedence_over_rows() {
        let usage = ResourceUsage {
            memory_bytes: 9999 * 1024 * 1024 * 1024, // huge
            cpu_time_ms: 0,
            result_rows: 9999_999_999, // huge
            execution_time_ms: 0,
        };
        let mut limits = ResourceLimits::default();
        limits.max_memory_mb = 1;
        limits.max_result_rows = 1;
        let err = usage.check_limits(&limits).unwrap_err();
        // Memory is checked first.
        assert!(err.contains("Memory"));
    }
}
