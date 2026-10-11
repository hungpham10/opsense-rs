//! Module configuration — defines which modules are enabled and how to load them.

use std::path::PathBuf;
use serde::{Deserialize, Serialize};

/// Top-level module configuration from `modules.toml`.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ModulesConfig {
    #[serde(default)]
    pub module: Vec<ModuleConfig>,
}

/// Individual module configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModuleConfig {
    /// Module identifier (e.g., "repl", "xt-pnl").
    pub id: String,
    /// Whether this module is built-in (compiled into the binary).
    #[serde(default)]
    pub builtin: bool,
    /// Whether this module is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Path to the .so file (for dynamic modules). Relative to `module_dir`.
    #[serde(default)]
    pub path: Option<String>,
    /// Module-specific config (passed to plugin init).
    #[serde(default)]
    pub config: Option<serde_json::Value>,
}

fn default_true() -> bool { true }

impl ModulesConfig {
    /// Load module config from `modules.toml` in the given directory.
    pub fn load(dir: &std::path::Path) -> Result<Self, std::io::Error> {
        let path = dir.join("modules.toml");
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(&path)?;
        toml::from_str(&content).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Get enabled modules, applying environment allowlist if set.
    pub fn enabled_modules(&self) -> Vec<&ModuleConfig> {
        let allowlist: Option<Vec<String>> = std::env::var("OPSENSE_MODULES")
            .ok()
            .map(|s| s.split(',').map(str::trim).map(String::from).collect());

        self.module
            .iter()
            .filter(|m| m.enabled)
            .filter(|m| allowlist.as_ref().is_none_or(|al| al.contains(&m.id)))
            .collect()
    }

    /// Get the .so path for a dynamic module.
    pub fn dynamic_path(&self, module_dir: &std::path::Path, m: &ModuleConfig) -> Option<PathBuf> {
        if m.builtin { return None; }
        m.path.as_ref().map(|p| module_dir.join(p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_modules_toml() {
        let toml_str = r#"
[[module]]
id = "repl"
builtin = true
enabled = true

[[module]]
id = "xt-pnl"
builtin = false
enabled = true
path = "xt-pnl.so"
"#;
        let cfg: ModulesConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.module.len(), 2);
        assert!(cfg.module[0].builtin);
        assert!(!cfg.module[1].builtin);
        assert_eq!(cfg.module[1].path.as_deref(), Some("xt-pnl.so"));
    }

    #[test]
    fn allowlist_env() {
        let toml_str = r#"
[[module]]
id = "a"
enabled = true
[[module]]
id = "b"
enabled = true
"#;
        let cfg: ModulesConfig = toml::from_str(toml_str).unwrap();
        unsafe { std::env::set_var("OPSENSE_MODULES", "a"); }
        let enabled = cfg.enabled_modules();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].id, "a");
        unsafe { std::env::remove_var("OPSENSE_MODULES"); }
    }
}