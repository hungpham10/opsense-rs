//! Launcher for the Python analysis kernel. Embeds every kernel asset
//! (main.py, the bundled `opsense_*` helper modules and the vendored
//! protobuf code), materialises them in a temp dir, then spawns the
//! interpreter with **inherited stdio** — the Python process speaks the
//! framed kernel protocol directly over our stdin/stdout.

use std::io::Write as _;
use std::path::PathBuf;
use std::process::Command;

/// `(relative path, embedded contents)` of every kernel asset.
const ASSETS: &[(&str, &str)] = &[
    ("kernel/main.py", include_str!("../kernel/main.py")),
    (
        "kernel/opsense_store.py",
        include_str!("../kernel/opsense_store.py"),
    ),
    (
        "kernel/opsense_station.py",
        include_str!("../kernel/opsense_station.py"),
    ),
    (
        "kernel/opsense_time.py",
        include_str!("../kernel/opsense_time.py"),
    ),
    (
        "kernel/opsense_stats.py",
        include_str!("../kernel/opsense_stats.py"),
    ),
    (
        "kernel/opsense_probability.py",
        include_str!("../kernel/opsense_probability.py"),
    ),
    (
        "kernel/opsense_capacity.py",
        include_str!("../kernel/opsense_capacity.py"),
    ),
    (
        "kernel/opsense_ml.py",
        include_str!("../kernel/opsense_ml.py"),
    ),
    (
        "kernel/opsense_plots.py",
        include_str!("../kernel/opsense_plots.py"),
    ),
    (
        "kernel/gen/__init__.py",
        include_str!("../kernel/gen/__init__.py"),
    ),
    (
        "kernel/gen/opsense/__init__.py",
        include_str!("../kernel/gen/opsense/__init__.py"),
    ),
    (
        "kernel/gen/opsense/kernel/__init__.py",
        include_str!("../kernel/gen/opsense/kernel/__init__.py"),
    ),
    (
        "kernel/gen/opsense/kernel/v1/__init__.py",
        include_str!("../kernel/gen/opsense/kernel/v1/__init__.py"),
    ),
    (
        "kernel/gen/opsense/kernel/v1/opsense_pb2.py",
        include_str!("../kernel/gen/opsense/kernel/v1/opsense_pb2.py"),
    ),
];

fn materialise_assets() -> std::io::Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "opsense-kernel-python-{}-{}",
        std::process::id(),
        env!("CARGO_PKG_VERSION")
    ));
    // A stale dir from a crashed previous run would carry old assets.
    let _ = std::fs::remove_dir_all(&root);
    for (rel, contents) in ASSETS {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::File::create(&path)?;
        file.write_all(contents.as_bytes())?;
    }
    Ok(root)
}

fn main() {
    let exit_code = match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("opsense-kernel-python launcher failed: {err}");
            1
        }
    };
    std::process::exit(exit_code);
}

fn run() -> std::io::Result<i32> {
    let interpreter = std::env::var("OPSENSE_PYTHON").unwrap_or_else(|_| "python3".into());
    let asset_dir = materialise_assets()?;
    let status = Command::new(&interpreter)
        .arg(asset_dir.join("kernel/main.py"))
        .env("OPSENSE_KERNEL_VERSION", env!("CARGO_PKG_VERSION"))
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .map_err(|e| {
            std::io::Error::other(format!(
                "cannot launch python interpreter `{interpreter}` \
                 (set OPSENSE_PYTHON): {e}"
            ))
        })?
        .wait()?;
    let _ = std::fs::remove_dir_all(&asset_dir);
    Ok(status.code().unwrap_or(1))
}
