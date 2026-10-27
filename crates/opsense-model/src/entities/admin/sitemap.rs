//! Sitemap + file-map entities for admin module.

use serde::{Deserialize, Serialize};

/// Một dòng trong sitemap của tenant.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Site {
    pub loc: String,
    pub freq: String,
    pub priority: f64,

    #[serde(with = "chrono::serde::ts_seconds")]
    pub last_mod: chrono::DateTime<chrono::Utc>,
}

/// Một ánh xạ file (src -> dest) của tenant.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileMap {
    pub src: String,
    pub dest: String,
}