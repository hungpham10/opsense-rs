//! Process-wide rustls crypto provider — **tự cài, không đợi `main()`**.
//!
//! Workspace này cuối cùng bật **cả hai** feature provider trên cùng bản
//! `rustls 0.23`:
//!
//! - `ring` — qua `reqwest` (mà `object_store` 0.11 bật sẵn
//!   `reqwest/rustls-tls-native-roots`, tức `reqwest/__rustls-ring`);
//! - `aws_lc_rs` — qua AWS SDK S3 (`aws-config` → `aws-smithy-runtime/
//!   default-https-client` → `aws-smithy-http-client/rustls-aws-lc`).
//!
//! `rustls` chỉ tự chọn provider khi *đúng một* feature provider bật. Khi cả
//! hai, **mọi** handshake đầu tiên panic:
//!
//! ```text
//! panicked at rustls-0.23.45/src/crypto/mod.rs:249:
//! Could not automatically determine the process-level CryptoProvider from
//! Rustls crate features.
//! ```
//!
//! Triệu chứng ngoài production — **toàn bộ** outbound TLS của `opsense serve`
//! chết, chứ không phải chỉ một chỗ: `http_source` (`https://…`) im lặng không
//! lấy được dữ liệu, còn `websocket_2_json` (`wss://…`) spam
//! `Major error in node N: Panic at node N` mỗi vòng reconnect.
//!
//! Vì sao hàm này ở đây, trong crate thấp nhất: trước đây việc cài provider
//! nằm trong `main()` của binary `opsense`, nên **mọi embedder không có `main`**
//! — integration test, crate dùng lại — đều panic ngay khi dựng reqwest client
//! (`reqwest-0.12.28/src/async_impl/client.rs:767`). Đặt ở `opsense-mlib` và gọi
//! tại chỗ dựng client thì mọi đường vào đều tự được bảo vệ, kể cả người gọi
//! không biết chuyện này.
//!
//! Chọn `aws-lc-rs` vì đó là provider AWS SDK đang dùng cho S3 mirror — không
//! phải dựng thêm provider thứ hai cho nó.
//!
//! Idempotent và không bao giờ panic: nếu một provider khác đã được cài trước
//! thì giữ nguyên, chỉ cảnh báo.

use std::sync::Once;

static ONCE: Once = Once::new();

/// Cài provider `aws-lc-rs` cho cả process. Gọi được bao nhiêu lần cũng được.
pub fn install_default_crypto_provider() {
    ONCE.call_once(|| {
        if rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .is_err()
        {
            // Provider khác đã có (vd test cài `ring`) — TLS vẫn chạy, chỉ mất
            // tính tường minh. Không panic.
            eprintln!(
                "rustls: a process-level CryptoProvider was already installed; \
                 keeping it instead of aws-lc-rs"
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test cho "mọi outbound TLS chết" (image release v1.0.12).
    ///
    /// `ClientConfig::builder()` là đúng chỗ panic khi feature set bật cả hai
    /// provider: nó gọi `CryptoProvider::get_default_or_install_from_crate_features()`.
    /// Không có [`install_default_crypto_provider`] thì test này **panic** — nên nó
    /// chặn tái phát bất kể ai vô tình đổi feature `rustls`/`reqwest` về sau.
    #[test]
    fn client_config_builds_after_install() {
        install_default_crypto_provider();
        rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
    }

    #[test]
    fn install_is_idempotent() {
        install_default_crypto_provider();
        install_default_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}
