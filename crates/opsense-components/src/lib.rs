//! Opsense components: the vector dataflow [`Component`]s that drive collection.
//!
//! This crate holds every Opsense-specific component registered into the
//! `opsense_libs::vector` [`Runtime`]. It is split from `opsense-core` so the
//! growing pile of components cannot bloat the pure domain crate.
//!
//! Nodes register their own stations into the process-wide
//! [`opsense_core::Context`] via [`Context::registry`]. The runtime injects
//! `Context` into every component through `Outbound.ctx`, so each `run()` can
//! both write to its own station and read from upstream stations.

use std::collections::BTreeMap;

pub mod http;
pub mod processor;
pub mod signal;
pub mod station;

pub use station::{
    CategoryStationTransform, PatternStationTransform, TimeseriesStationSink,
    TimeseriesStationTransform,
};

/// Render `{{name}}` placeholders in a template using the provided vars.
///
/// 1-pass scan: any `{{ ... }}` segment is trimmed and looked up in `vars`.
/// Missing variable → `Err`. No nesting, no escaping, no `{{` inside `{{`.
/// Used by `http.rs` to interpolate URL, headers, params, and body fields.
pub fn render(template: &str, vars: &BTreeMap<String, String>) -> Result<String, String> {
    let bytes = template.as_bytes();
    let mut out = String::with_capacity(template.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            // Find the closing `}}`.
            let mut j = i + 2;
            while j + 1 < bytes.len() && !(bytes[j] == b'}' && bytes[j + 1] == b'}') {
                j += 1;
            }
            if j + 1 >= bytes.len() {
                return Err(format!("unterminated placeholder starting at byte {i}"));
            }
            let raw = &template[i + 2..j];
            let key = raw.trim();
            if key.is_empty() {
                return Err(format!("empty placeholder at byte {i}"));
            }
            let value = vars
                .get(key)
                .ok_or_else(|| format!("missing variable `{key}`"))?;
            out.push_str(value);
            i = j + 2;
        } else {
            // Push one char safely (template is UTF-8).
            let ch = template[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    Ok(out)
}

/// Re-export of the `vector` runtime under `crate::vector::runtime`.
///
/// `opsense-macros`' `#[source]`/`#[sink]`/`#[transform]`/`#[input]`/`#[output]`
/// attributes expand to code that refers to `crate::vector::runtime::*`; this
/// mirror lets those macros be used from this crate exactly as they are from
/// `opsense-libs`.
pub mod vector {
    pub use opsense_libs::vector::runtime;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn render_basic() {
        let v = vars(&[("a", "1")]);
        assert_eq!(render("http://x/?a={{a}}", &v).unwrap(), "http://x/?a=1");
    }

    #[test]
    fn render_multiple() {
        let v = vars(&[("a", "1"), ("b", "2")]);
        assert_eq!(render("{{a}}-{{b}}", &v).unwrap(), "1-2");
    }

    #[test]
    fn render_whitespace() {
        let v = vars(&[("a", "1")]);
        assert_eq!(render("{{ a }}", &v).unwrap(), "1");
    }

    #[test]
    fn render_missing() {
        let v = vars(&[("a", "1")]);
        let err = render("{{b}}", &v).unwrap_err();
        assert!(err.contains("missing variable `b`"));
    }

    #[test]
    fn render_no_placeholder() {
        let v = vars(&[("a", "1")]);
        assert_eq!(render("plain text", &v).unwrap(), "plain text");
    }

    #[test]
    fn render_empty_placeholder() {
        let v = vars(&[]);
        assert!(render("{{}}", &v).is_err());
        assert!(render("{{   }}", &v).is_err());
    }

    #[test]
    fn render_unterminated() {
        let v = vars(&[("a", "1")]);
        assert!(render("{{a", &v).is_err());
    }

    #[test]
    fn render_literal_braces() {
        // Single `{` or `}` is passed through; only `{{` opens a placeholder.
        let v = vars(&[("a", "1")]);
        assert_eq!(render("curly {a} end", &v).unwrap(), "curly {a} end");
    }
}
