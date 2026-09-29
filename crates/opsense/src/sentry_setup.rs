//! Sentry — bắt panic và `SIGSEGV`.
//!
//! # Vì sao cần
//!
//! Trước đây khi tiến trình `app` chết, supervisor chỉ ghi:
//!
//! ```text
//! WARN exited: app (terminated by SIGSEGV; not expected)
//! INFO spawned: 'app' with pid 869
//! ```
//!
//! **Không có backtrace nào.** Không biết crash ở đâu, và crash thì im lặng —
//! chỉ xuất hiện giữa hai dòng log bình thường. Đã gặp đúng một lần: app
//! chạy 13 phút rồi `SIGSEGV`, mất sạch state trong RAM (station, lệnh đang mở)
//! rồi supervisor khởi động lại, và `mode` rơi về giá trị trong file.
//!
//! Module này dựng handler **trước khi** làm bất cứ việc gì, để lỗi lúc khởi
//! động cũng được bắt.
//!
//! # Bật/tắt
//!
//! Chỉ bật khi có `SENTRY_DSN`. Thiếu biến này thì **no-op hoàn toàn** — không
//! phát mạng, không ghi gì. Nên build và chạy local không cần cấu hình gì.
//!
//! ```bash
//! SENTRY_DSN='https://<key>@o<id>.ingest.sentry.io/<project>' opsense serve
//! ```
//!
//! # KHÔNG bắt được `SIGSEGV` — đã kiểm tra, không phải suy đoán
//!
//! `sentry-rust` **không** cài handler cho `SIGSEGV`/`SIGBUS`. Đã grep toàn bộ
//! `sentry-core` và `sentry` ở cả `0.34` lẫn `0.49`: không có `UnixIntegration`,
//! không có mảy nào tham chiếu `SIGSEGV`. `sentry-panic` chỉ bắt **panic**
//! (`unwind` qua `panic!`), còn segfault làm chết tiến trình ở tầng C — không
//! đi qua Rust.
//!
//! Nên module này **không** giải quyết được segfault đã gặp. Cái nó làm được:
//! bắt panic, gửi kèm stack, gắn `release` để lọc theo bản.
//!
//! Muốn có stack cho segfault thì phải làm bằng tay: handler `SIGSEGV` tự cài
//! qua `libc` + dựng stack bằng crate `backtrace` (xem `segv_handler` ở
//! `opsense::serve`), hoặc bật core dump rồi phân tích hậu kỳ bằng gdb.

/// Handle giữ client. Drop nó sẽ shutdown client.
pub type Guard = Option<sentry::ClientInitGuard>;

/// Dựng Sentry nếu có `SENTRY_DSN`.
///
/// Gọi **một lần**, sớm nhất có thể trong `main` — trước cả `dotenvy` và trước
/// khi parse CLI, để lỗi ở phần khởi động cũng được báo.
///
/// Trả `None` khi không có DSN (chế độ mặc định, no-op).
pub fn init() -> Guard {
    let Ok(dsn) = std::env::var("SENTRY_DSN") else {
        return None;
    };
    let dsn = dsn.trim();
    if dsn.is_empty() {
        return None;
    }

    let env = std::env::var("APP_ENV").unwrap_or_else(|_| "dev".to_string());
    let release = format!("opsense@{}", env!("CARGO_PKG_VERSION"));

    // DSN sai cú pháp ⇒ không bật, nhưng **phải nói ra** chứ im lặng, vì im
    // lặng thì tưởng đã bật mà không có.
    let Ok(odsn) = dsn.parse() else {
        eprintln!("sentry: SENTRY_DSN không hợp lệ — tắt báo cáo lỗi");
        return None;
    };

    let _guard = sentry::init(sentry::ClientOptions {
        dsn: Some(odsn),
        environment: Some(env.into()),
        // Gắn version để lọc theo bản — không có nó thì không biết lỗi xảy ra
        // ở `v1.0.21` hay bản sau.
        release: Some(release.into()),
        // Lỗi hiếm, lấy hết. Giảm sample rate chỉ khi đã xử lý xong.
        sample_rate: 1.0,
        // Panic không có `RUST_BACKTRACE` mặc định. Bắt buộc, không bật thì
        // event chỉ có thông điệp lỗi chứ không có stack.
        attach_stacktrace: true,
        ..Default::default()
    });

    eprintln!(
        "sentry: đã bật (env={}, release=opsense@{})",
        std::env::var("APP_ENV").unwrap_or_default(),
        env!("CARGO_PKG_VERSION"),
    );
    Some(_guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Không có `SENTRY_DSN` thì phải **no-op**, không phát mạng. Đây là chế độ
    /// mặc định của build local và của mọi lần chạy CI.
    #[test]
    fn no_dsn_is_noop() {
        // Test chạy song song với test khác nên không sửa env toàn cục; chỉ kiểm
        // đường "thiếu biến" bằng cách gọi với env đã bảo đảm không có.
        if std::env::var_os("SENTRY_DSN").is_some() {
            return;
        }
        assert!(init().is_none());
    }
}
