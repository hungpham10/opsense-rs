//! Single-session guard: one Opsense session per `.opsense` directory.
//!
//! `SessionLock::acquire` atomically creates `<config-dir>/session.lock`
//! (containing the owning PID). A second `opsense mcp` process (or a second
//! `opsense_init`) in the same directory fails with a clear error. Locks left
//! behind by crashed processes are detected via PID liveness and reclaimed.
//! The lock is released when the `Session` (and thus the guard) is dropped.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct SessionLock {
    path: PathBuf,
}

impl SessionLock {
    /// Acquire the session lock for `dir`. Fails if another live process holds
    /// it; reclaims a stale lock whose owner has exited.
    pub fn acquire(dir: &Path) -> Result<Self, String> {
        let path = dir.join("session.lock");

        for attempt in 0..2 {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    let _ = writeln!(file, "{}", std::process::id());
                    return Ok(Self { path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let pid = read_pid(&path);
                    if attempt == 0 && !pid_alive(pid) {
                        // Owner is gone (crashed or killed) — reclaim.
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                    return Err(format!(
                        "another opsense session is already running in {} (pid {pid}); \
                         call opsense_deinit in that session, or remove {} if stale",
                        dir.display(),
                        path.display(),
                    ));
                }
                Err(e) => {
                    return Err(format!(
                        "cannot create session lock {}: {e}",
                        path.display()
                    ));
                }
            }
        }

        Err(format!(
            "race on session lock {}; retry opsense_init",
            path.display()
        ))
    }
}

impl Drop for SessionLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn read_pid(path: &Path) -> i32 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

#[cfg(unix)]
fn pid_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    // SAFETY: kill(2) with signal 0 performs only an existence/permission
    // check — no signal is delivered.
    unsafe { libc::kill(pid, 0) == 0 }
}

#[cfg(not(unix))]
fn pid_alive(pid: i32) -> bool {
    // No portable liveness probe: never reclaim, require manual removal.
    pid > 0
}
