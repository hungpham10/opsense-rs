//! `opsense init`: scaffold a ready-to-edit config file.
//!
//! Writes the embedded sample ([`template.toml`]) to the given path —
//! `.opsense/config.toml` by default, which is exactly what MCP
//! `opsense_init` loads. An existing file is never overwritten silently:
//! the command refuses unless `--force` is passed.

use std::io::{Error, ErrorKind};
use std::path::Path;

const TEMPLATE: &str = include_str!("template.toml");

/// Write the sample config to `path` (default `.opsense/config.toml`) and
/// print the next steps.
pub fn run(path: Option<&Path>, force: bool) -> std::io::Result<()> {
    let target = path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| Path::new(".opsense/config.toml").to_path_buf());

    if target.exists() && !force {
        return Err(Error::new(
            ErrorKind::AlreadyExists,
            format!(
                "{} already exists — edit it, or re-run with --force to overwrite",
                target.display()
            ),
        ));
    }

    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    std::fs::write(&target, TEMPLATE)?;

    println!("wrote {}", target.display());
    println!();
    println!("Next steps:");
    println!(
        "  1. Edit {} — adjust [attributes], then uncomment one [pipeline] block.",
        target.display()
    );
    println!("  2. Drive it interactively:");
    println!("       opsense mcp");
    println!("     (MCP tools: opsense_init / opsense_run / opsense_query / opsense_status)");
    println!("     or run the gateway against it:");
    println!("       OPSENSE_CONFIG={} opsense serve", target.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaffolds_a_valid_config_and_respects_force() {
        let dir = std::env::temp_dir().join(format!("opsense-init-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("nested").join("config.toml");

        run(Some(&path), false).expect("init creates parent dirs and writes config");
        // An existing file is protected…
        let err = run(Some(&path), false).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AlreadyExists);
        // …unless --force.
        run(Some(&path), true).expect("force rewrites");

        // The generated file must load cleanly and expose the attribute.
        let cfg = opsense_core::config::Config::load(&path).expect("template parses as config");
        assert!(cfg.attributes.contains_key("prom_url"));
        // Everything under [pipeline] is commented out by default.
        assert!(cfg.pipeline.is_none());

        std::fs::remove_dir_all(&dir).ok();
    }
}
