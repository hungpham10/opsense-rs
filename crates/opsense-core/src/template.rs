//! Minimal `{{variable}}` template renderer for pipeline components.
//!
//! Nodes like [`crate::config`] HTTP fetchers declare their URL, query
//! parameters, headers and body as templates. Three namespaces resolve:
//!
//! - **built-ins**: `{{from_ts}}`, `{{to_ts}}`, and `{{ts}}` (alias of
//!   `to_ts`) — the watermark window the node is currently processing;
//! - **attributes**: `{{name}}` — a key of the resolved `[attributes]` map
//!   (`opsense_core::config::Config::resolved_attributes`, i.e. TOML values
//!   overridable by `OPSENSE_ATTR_<NAME>` environment variables);
//! - **environment**: `{{env.NAME}}` — a raw environment variable lookup,
//!   typically secrets that should never land in a config file.
//!
//! Unknown names fail loudly listing what *is* available, so typos surface at
//! the first tick instead of silently fetching the wrong thing.

use std::collections::BTreeMap;
use std::fmt::Write as _;

/// The built-in time window handed to every render call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemplateVars {
    /// Start of the current window (the node's cursor before this cycle).
    pub from_ts: i64,
    /// End of the current window (the timestamp carried by the signal).
    pub to_ts: i64,
}

/// Render every `{{...}}` occurrence in `text`. An unterminated `{{` is an
/// error; braces are not otherwise escapable in v1.
pub fn render(
    text: &str,
    vars: &TemplateVars,
    attributes: &BTreeMap<String, String>,
) -> Result<String, String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;

    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + "{{".len()..];
        let Some(end) = after.find("}}") else {
            return Err(format!("template has an unclosed `{{{{`: {text:?}"));
        };

        let name = after[..end].trim();
        out.push_str(&resolve(name, vars, attributes)?);
        rest = &after[end + "}}".len()..];
    }
    out.push_str(rest);
    Ok(out)
}

fn resolve(
    name: &str,
    vars: &TemplateVars,
    attributes: &BTreeMap<String, String>,
) -> Result<String, String> {
    match name {
        "from_ts" => return Ok(vars.from_ts.to_string()),
        "to_ts" | "ts" => return Ok(vars.to_ts.to_string()),
        _ => {}
    }

    if let Some(env_name) = name.strip_prefix("env.") {
        return std::env::var(env_name).map_err(|_| missing(name, attributes, true));
    }

    if let Some(value) = attributes.get(name) {
        return Ok(value.clone());
    }

    Err(missing(name, attributes, false))
}

fn missing(name: &str, attributes: &BTreeMap<String, String>, env_only: bool) -> String {
    let mut hint = String::from("available variables: from_ts, to_ts, ts");
    for key in attributes.keys() {
        let _ = write!(hint, ", {key}");
    }
    hint.push_str(" (+ {{env.NAME}} for environment variables)");
    if env_only {
        format!("environment variable `{name}` is not set; {hint}")
    } else {
        format!("unknown template variable `{name}`; {hint}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars() -> TemplateVars {
        TemplateVars {
            from_ts: 1_000,
            to_ts: 2_000,
        }
    }

    fn attrs() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("dc".to_string(), "hcm".to_string()),
            ("prom_url".to_string(), "http://prom:9090".to_string()),
        ])
    }

    #[test]
    fn builtins_and_text_pass_through() {
        assert_eq!(render("plain", &vars(), &attrs()).unwrap(), "plain");
        assert_eq!(
            render(
                "http://x/api?start={{from_ts}}&end={{to_ts}}",
                &vars(),
                &attrs()
            )
            .unwrap(),
            "http://x/api?start=1000&end=2000"
        );
        // `ts` is an alias of `to_ts`.
        assert_eq!(render("{{ts}}", &vars(), &attrs()).unwrap(), "2000");
    }

    #[test]
    fn attributes_resolve() {
        assert_eq!(
            render("{{prom_url}}/api/v1/query_range", &vars(), &attrs()).unwrap(),
            "http://prom:9090/api/v1/query_range"
        );
    }

    #[test]
    fn whitespace_inside_braces_is_trimmed() {
        assert_eq!(render("{{ dc }}!", &vars(), &attrs()).unwrap(), "hcm!");
    }

    #[test]
    fn env_namespace_reads_directly() {
        std::env::set_var("OPSENSE_TEST_TOKEN", "abc123");
        assert_eq!(
            render("Bearer {{env.OPSENSE_TEST_TOKEN}}", &vars(), &attrs()).unwrap(),
            "Bearer abc123"
        );
        std::env::remove_var("OPSENSE_TEST_TOKEN");
    }

    #[test]
    fn unknown_variable_lists_available_names() {
        let err = render("{{dcx}}", &vars(), &attrs()).unwrap_err();
        assert!(err.contains("`dcx`"), "{err}");
        assert!(err.contains("dc"), "{err}");
        assert!(err.contains("env.NAME"), "{err}");
    }

    #[test]
    fn missing_env_var_errors_cleanly() {
        let err = render("{{env.DEFINITELY_UNSET_VAR}}", &vars(), &attrs()).unwrap_err();
        assert!(err.contains("not set"), "{err}");
    }

    #[test]
    fn unclosed_brace_is_an_error() {
        let err = render("start={{from_ts until end", &vars(), &attrs()).unwrap_err();
        assert!(err.contains("unclosed"), "{err}");
    }
}
