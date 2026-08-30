//! [`CatalogTool`] — key/value catalog backed by Radix substring search
//! (`Search<u8>` from opsense-libs). Mỗi node có một index riêng; transform
//! nhận observations, insert key/value vào đây; REPL/MCP search trả matching
//! entries.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use opsense_libs::search::Search;

pub struct CatalogTool {
    pub id: String,
    inner: Mutex<CatalogInner>,
}

struct CatalogInner {
    search: Search<u8>,
    /// record_idx → (key, value).
    entries: BTreeMap<usize, (String, String)>,
    next_idx: u64,
}

fn tools() -> &'static Mutex<BTreeMap<String, Arc<CatalogTool>>> {
    use std::sync::OnceLock;
    static TOOLS: OnceLock<Mutex<BTreeMap<String, Arc<CatalogTool>>>> = OnceLock::new();
    TOOLS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Get or create the catalog tool for a node id.
#[must_use]
pub fn ensure_catalog_tool(id: &str) -> Arc<CatalogTool> {
    tools()
        .lock()
        .unwrap()
        .entry(id.to_string())
        .or_insert_with(|| {
            Arc::new(CatalogTool {
                id: id.to_string(),
                inner: Mutex::new(CatalogInner {
                    search: Search::in_memory(4),
                    entries: BTreeMap::new(),
                    next_idx: 1,
                }),
            })
        })
        .clone()
}

/// Look up a registered catalog tool.
#[must_use]
pub fn catalog_tool(id: &str) -> Option<Arc<CatalogTool>> {
    tools().lock().unwrap().get(id).cloned()
}

/// List all registered catalog tool ids.
#[must_use]
pub fn catalog_tool_ids() -> Vec<String> {
    tools().lock().unwrap().keys().cloned().collect()
}

impl CatalogTool {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Index one key/value pair. Duplicate keys are idempotent.
    pub fn insert(&self, key: &[u8], value: &str) {
        let mut inner = self.inner.lock().unwrap();
        let idx = inner.next_idx as usize;
        inner.next_idx += 1;
        if futures::executor::block_on(async {
            inner
                .search
                .insert_chain(idx, key, &vec![None; key.len()])
                .await
        })
        .is_ok()
        {
            inner.entries.insert(
                idx,
                (String::from_utf8_lossy(key).to_string(), value.to_string()),
            );
        }
    }

    /// Substring search — returns `(key, value)` pairs whose keys contain
    /// `pattern` anywhere. Empty vec when no match.
    #[must_use]
    pub fn search(&self, pattern: &[u8]) -> Vec<(String, String)> {
        let inner = self.inner.lock().unwrap();
        futures::executor::block_on(async {
            match inner.search.search(pattern, None).await {
                Ok(hits) => hits
                    .iter()
                    .filter_map(|(rid, _)| {
                        inner.entries.get(rid).map(|(k, v)| (k.clone(), v.clone()))
                    })
                    .collect(),
                Err(_) => Vec::new(),
            }
        })
    }

    /// Total indexed entries.
    #[must_use]
    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    /// All entries sorted by record idx.
    #[must_use]
    pub fn entries(&self) -> Vec<(String, String)> {
        self.inner
            .lock()
            .unwrap()
            .entries
            .values()
            .cloned()
            .collect()
    }
}
