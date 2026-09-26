//! Process-wide rustls crypto provider.
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
//! hai, mọi handshake đầu tiên panic:
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
//! [`install_default_provider`] là cách rustls khuyến nghị: biến việc chọn
//! provider thành quyết định tường minh của app thay vì để feature-unification
//! quyết định hộ. Chọn `aws-lc-rs` vì đó là provider AWS SDK đang dùng cho S3
//! mirror — không phải dựng thêm provider thứ hai cho nó.
//!
//! Phải gọi **trước mọi** thao tác TLS; binary `opsense` gọi ở `main()`.

/// Pin provider `aws-lc-rs` cho cả process. Idempotent và không bao giờ panic.
///
/// Nếu một provider khác đã được cài trước (vd test cài `ring`) thì giữ
/// nguyên provider đó và chỉ cảnh báo — TLS vẫn chạy được, chỉ mất tính tường
/// minh.
pub fn install_default_provider() {
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_ok()
    {
        return;
    }
    eprintln!(
        "rustls: a process-level CryptoProvider was already installed; \
         keeping it instead of aws-lc-rs"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test cho bug "mọi outbound TLS chết" (image release v1.0.12).
    ///
    /// `ClientConfig::builder()` là đúng chỗ panic khi feature set bật cả hai
    /// provider: nó gọi `CryptoProvider::get_default_or_install_from_crate_features()`.
    /// Không có [`install_default_provider`] thì test này **panic** — nên nó chặn
    /// tái phát bất kể ai vô tình đổi feature `rustls`/`reqwest` về sau.
    #[test]
    fn client_config_builds_after_install() {
        install_default_provider();
        rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
    }

    /// Provider đã cài phải là `aws_lc_rs` — khẳng định lựa chọn ở docstring
    /// không bị lệch nếu ai đó sửa implementation.
    #[test]
    fn installed_provider_is_aws_lc_rs() {
        install_default_provider();
        let p = rustls::crypto::CryptoProvider::get_default().expect("provider phải được cài");
        assert_eq!(
            p.cipher_suites.len(),
            rustls::crypto::aws_lc_rs::default_provider()
                .cipher_suites
                .len()
        );
    }
}
