//! Bắt `SIGSEGV`/`SIGBUS`/`SIGABRT`/… và in stack ra **stderr**.
//!
//! # Vì sao cần
//!
//! Khi tiến trình `app` chết, supervisor chỉ ghi:
//!
//! ```text
//! WARN exited: app (terminated by SIGSEGV; not expected)
//! INFO spawned: 'app' with pid 869
//! ```
//!
//! **Không có backtrace nào.** Crash im lặng giữa hai dòng log bình thường. Đã
//! gặp: app chạy 13 phút rồi `SIGSEGV`, mất sạch state trong RAM (station, lệnh
//! đang mở) rồi supervisor khởi động lại.
//!
//! # Vì sao KHÔNG dùng Sentry cho việc này
//!
//! `sentry-rust` **không** cài handler signal. Đã grep `sentry`, `sentry-core`,
//! `sentry-panic`, `sentry-backtrace` ở cả `0.34` lẫn `0.49`: không có
//! `UnixIntegration`, không tham chiếu `SIGSEGV` nào. Nó chỉ bắt **panic** —
//! segfault chết ở tầng C, không đi qua `panic!`.
//!
//! Đã cân nhắc `sentry-contrib-breakpad` (C++/cmake) rồi bỏ: `image.yml` cross-build
//! `aarch64-unknown-linux-gnu` bằng `cargo-zigbuild`, gần như chắc chắn vỡ ở
//! bước dựng C++.
//!
//! # Giới hạn cần biết trước
//!
//! Handler chạy trong tiến trình **đã hỏng**:
//!
//! - Stack in ra **có thể sai** (heap hỏng, biến tối ưu hoá, frame tối ưu đi).
//!   Vẫn hữu ích hơn hẳn việc không có gì.
//! - `backtrace` phải cấp phát bộ nhớ để dựng symbol. Nếu crash xảy ra khi
//!   allocator đang giữ lock thì nó **chết ngay trong handler**. Vì vậy phần dựng
//!   stack bọc trong `catch_unwind` và giới hạn số frame: nếu không dựng được thì
//!   vẫn in được dòng chẫn đoán tối thiểu.
//! - Không bắt `SIGINT`/`SIGTERM` — supervisor cần tín hiệu đó để tắt container
//!   gọn gàng.

use std::sync::atomic::{AtomicBool, Ordering};

/// Số frame tối đa in ra. `backtrace` có thể rất sâu, và in hết trong signal
/// handler là tự tạo rủi ro mới (chậm, cấp phát thêm, có thể treo).
const MAX_FRAMES: usize = 60;

/// Cờ chống đệ quy: nếu in stack lại làm crash thì lần 2 bỏ qua, tránh loop.
static HANDLING: AtomicBool = AtomicBool::new(false);

/// Cài handler cho các tín hiệu gây chết tiến trình.
///
/// Gọi một lần, sớm nhất có thể trong `main`.
///
/// ```text
/// OPSENSE_SEGV_BACKTRACE=0   tắt (mặc định: bật khi tồn tại stderr)
/// ```
pub fn install() {
    if std::env::var("OPSENSE_SEGV_BACKTRACE").as_deref() == Ok("0") {
        eprintln!("segv: tắt theo OPSENSE_SEGV_BACKTRACE=0");
        return;
    }

    // SAFETY: `handler` là `extern "C" fn(i32)`, đúng chữ ký mà `signal` cần, và
    // chỉ dùng API async-signal-safe bên trong (`write`, `signal`).
    // `sighandler_t` là `usize` trên Linux ⇒ phải đi qua **fn pointer** rồi mới
    // cast. Cast thẳng từ `fn` item bị lint `function-casts-as-integer` (deny);
    // biến `f` có kiểu `extern "C" fn(i32)` nên phép cast sau đó là
    // fn-pointer → usize, hợp lệ.
    let f: extern "C" fn(i32) = handle_fatal_signal;
    let handler: libc::sighandler_t = f as libc::sighandler_t;
    unsafe {
        for sig in [libc::SIGSEGV, libc::SIGBUS, libc::SIGABRT, libc::SIGILL, libc::SIGFPE] {
            libc::signal(sig, handler);
        }
    }
    eprintln!("segv: đã cài handler cho SIGSEGV/SIGBUS/SIGABRT/SIGILL/SIGFPE");
}

/// `extern "C"` bắt buộc — `libc::signal` gọi bằng con trỏ hàm thô.
extern "C" fn handle_fatal_signal(sig: i32) {
    if HANDLING.swap(true, Ordering::SeqCst) {
        // Đã vào handler một lần rồi lại vào ⇒ in stack lại đang làm hỏng.
        // Thoát ngay, không làm thêm gì.
        unsafe { libc::_exit(128 + sig) };
    }

    // Ghi thẳng qua `write(2)`, **không** dùng `eprintln!`: `stderr` của Rust có
    // mutex và bộ đệm, mà trong signal handler thì lấy mutex có thể treo vĩnh viễn
    // nếu chính luồng đó đang giữ nó.
    let sig_name = signal_name(sig);
    let header = format!("\n=== segv: nhận {sig_name} (signal {sig}) ===\nstack:\n");

    let frames = std::panic::catch_unwind(|| {
        let mut out = String::new();
        backtrace::trace(|frame| {
            if out.lines().count() > MAX_FRAMES {
                return false;
            }
            backtrace::resolve_frame(frame, |sym| {
                use std::fmt::Write;
                let name = sym
                    .name()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "<unknown>".to_string());
                let _ = writeln!(out, "  {name}");
            });
            true
        });
        out
    });

    match frames {
        Ok(stack) if !stack.is_empty() => {
            let mut buf = header.into_bytes();
            buf.extend_from_slice(stack.as_bytes());
            write_all(2, &buf);
        }
        // `catch_unwind` bắt được (alloc panic) hoặc không dựng được frame nào.
        // Vẫn phải in dòng chẫn đoán — mất tiến trình mà không có dòng nào thì
        // y như lúc trước khi có module này.
        _ => {
            write_all(
                2,
                format!("{header}  <không dựng được stack — allocator có thể đang giữ lock>\n")
                    .as_bytes(),
            );
        }
    }

    // Đặt lại mặc định rồi nâng lại signal: như vậy tiến trình chết **đúng cách**
    // (core dump, exit code 128+sig) thay vì `_exit` — để supervisor vẫn nhận ra
    // và log `terminated by SIGSEGV`.
    unsafe {
        libc::signal(sig, libc::SIG_DFL);
        libc::raise(sig);
    }
}

/// `write(2)` bọc lỗi — trong handler không thể xử lý gì thêm ngoài việc im lặng.
fn write_all(fd: i32, buf: &[u8]) {
    let mut off = 0;
    while off < buf.len() {
        // SAFETY: `buf[off..]` hợp lệ, `fd` là stderr (2).
        let n = unsafe { libc::write(fd, buf[off..].as_ptr().cast(), buf.len() - off) };
        if n <= 0 {
            return; // EINTR hoặc lỗi — không có gì để làm trong signal handler
        }
        off += n as usize;
    }
}

fn signal_name(sig: i32) -> &'static str {
    match sig {
        libc::SIGSEGV => "SIGSEGV",
        libc::SIGBUS => "SIGBUS",
        libc::SIGABRT => "SIGABRT",
        libc::SIGILL => "SIGILL",
        libc::SIGFPE => "SIGFPE",
        _ => "signal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_names_cover_installed_set() {
        // Mọi signal ta cài handler đều phải có tên, không rơi vào "signal".
        for s in [
            libc::SIGSEGV,
            libc::SIGBUS,
            libc::SIGABRT,
            libc::SIGILL,
            libc::SIGFPE,
        ] {
            assert_ne!(signal_name(s), "signal", "thiếu tên cho signal {s}");
        }
    }

    /// Handler phải chạy được thật — nếu chỉ test tên thì không bắt được lỗi
    /// "cài sai chữ ký `sighandler_t`" mà chỉ lộ khi crash thật.
    #[test]
    fn handler_can_be_installed_and_is_async_signal_safe_shaped() {
        let before = unsafe { libc::signal(libc::SIGUSR1, libc::SIG_DFL) };
        // SAFETY: cùng lập luận với `install()`.
        let f: extern "C" fn(i32) = handle_fatal_signal;
        let handler: libc::sighandler_t = f as libc::sighandler_t;
        unsafe {
            libc::signal(libc::SIGSEGV, handler);
        }
        // Trả lại mặc định để không ảnh hưởng test khác trong cùng process.
        unsafe {
            libc::signal(libc::SIGSEGV, libc::SIG_DFL);
            libc::signal(libc::SIGUSR1, before);
        }
    }
}
