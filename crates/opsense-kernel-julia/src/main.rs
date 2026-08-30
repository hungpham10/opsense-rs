//! Launcher cho Julia analysis kernel. Nhúng `kernel/main.jl`, ghi ra thư
//! mục tạm rồi spawn interpreter với **inherited stdio** — process Julia nói
//! framed kernel protocol trực tiếp trên stdin/stdout của chúng ta.
//!
//! Julia deps sống theo depot mặc định từng user (`~/.julia`) nên không cần
//! venv riêng: chỉ cần `Arrow.jl` trong depot nếu muốn truyền dataset/DataFrame.

use std::io::Write as _;
use std::path::PathBuf;
use std::process::Command;

const MAIN_JL: &str = include_str!("../kernel/main.jl");

fn materialise_assets() -> std::io::Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "opsense-kernel-julia-{}-{}",
        std::process::id(),
        env!("CARGO_PKG_VERSION")
    ));
    // Stale dir từ lần chạy crash trước sẽ mang asset cũ.
    let _ = std::fs::remove_dir_all(&root);
    let kernel_dir = root.join("kernel");
    std::fs::create_dir_all(&kernel_dir)?;
    let mut file = std::fs::File::create(kernel_dir.join("main.jl"))?;
    file.write_all(MAIN_JL.as_bytes())?;
    Ok(root)
}

fn run() -> std::io::Result<i32> {
    let interpreter = std::env::var("OPSENSE_JULIA").unwrap_or_else(|_| "julia".into());
    let asset_dir = materialise_assets()?;
    // -t 2: reader thread cần một worker thread thật; --startup-file=no để
    // khởi động nhanh và deterministic.
    let status = Command::new(&interpreter)
        .args(["-t", "2", "--startup-file=no"])
        .arg(asset_dir.join("kernel/main.jl"))
        .env("OPSENSE_KERNEL_VERSION", env!("CARGO_PKG_VERSION"))
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .map_err(|e| {
            std::io::Error::other(format!(
                "cannot launch julia interpreter `{interpreter}` \
                 (set OPSENSE_JULIA): {e}"
            ))
        })?
        .wait()?;
    let _ = std::fs::remove_dir_all(&asset_dir);
    Ok(status.code().unwrap_or(1))
}

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("opsense-kernel-julia launcher failed: {err}");
            std::process::exit(1);
        }
    }
}
