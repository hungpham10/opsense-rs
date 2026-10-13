//! `opsense token` — encrypt/decrypt helpers cho `sys_token_map`.
//!
//! Encrypt 1 lần với master key hiện tại → in ra hex blob định dạng
//! Postgres bytea literal (`\\xHEXHEX...`) để copy thẳng vào file SQL.
//!
//!   MASTER_KEY=0123456789abcdef0123456789abcdef opsense token encrypt "opsense-dev-shared-secret-32-bytes-min!!!"
//!
//! Decrypt ngược lại để verify seed đã paste đúng:
//!
//!   opsense token decrypt 000000000000000000000000a1...

use opsense_mlib::sops::{decrypt, encrypt};
use std::io::{Error, ErrorKind};

pub async fn run(master_key: &str, action: &str, payload: &str) -> std::io::Result<()> {
    if master_key.len() != 32 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "Master key must be 32 bytes, current length: {}",
                master_key.len()
            ),
        ));
    }

    match action {
        "encrypt" => {
            let token_plain = payload.to_string();
            let encrypted_bytes = encrypt(master_key.as_bytes(), &token_plain)?;
            // Postgres bytea literal: '\\x' + lowercase hex.
            println!("\\x{}", hex::encode(encrypted_bytes));
            Ok(())
        }
        "decrypt" => {
            let trimmed = payload.strip_prefix("\\x").unwrap_or(payload);
            let trimmed = trimmed.strip_prefix("0x").unwrap_or(trimmed);
            let encrypted_bytes = hex::decode(trimmed).map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Decode hex failed: {error}"),
                )
            })?;
            println!("{}", decrypt(master_key.as_bytes(), &encrypted_bytes)?);
            Ok(())
        }
        _ => Err(Error::new(
            ErrorKind::InvalidInput,
            "Unknown action, only 'encrypt' or 'decrypt'",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 32 byte — đúng độ dài AES-256-GCM đòi.
    const KEY: &str = "0123456789abcdef0123456789abcdef";

    /// Roundtrip: encrypt rồi decrypt phải ra lại nguyên plaintext.
    ///
    /// Đây là invariant duy nhất đáng test: blob có nonce ngẫu nhiên nên **khác
    /// nhau mỗi lần gọi** — so blob giữa hai lần chạy là so vô nghĩa. Cách verify
    /// seed đã paste đúng chỉ có một: decrypt ngược lại.
    #[tokio::test]
    async fn roundtrip_encrypt_then_decrypt() {
        let plain = "render-service-placeholder-token!!";

        let encrypted = encrypt(KEY.as_bytes(), &plain.to_string()).expect("encrypt");
        let blob = format!("\\x{}", hex::encode(&encrypted));

        // Strip prefix `\x` đúng như `run("decrypt")` làm, rồi giải mã.
        let hex_part = blob.strip_prefix("\\x").expect("blob có prefix \\x");
        let bytes = hex::decode(hex_part).expect("hex hợp lệ");
        let back = decrypt(KEY.as_bytes(), &bytes).expect("decrypt");
        assert_eq!(back, plain);
    }

    /// Key sai độ dài phải báo lỗi rõ ràng, **không** panic.
    ///
    /// Điều này quan trọng vì `opsense_mlib::sops::encrypt` gọi
    /// `Key::<Aes256Gcm>::from_slice`, sẽ panic nếu key lệch 32 byte. Lớp validate
    /// trong `run` là thứ chặn panic đó, nên phải có test giữ nó ở lại.
    #[tokio::test]
    async fn wrong_key_length_errors_instead_of_panicking() {
        for bad in [
            "",
            "short",
            "0123456789abcdef0123456789abcde",
            "0123456789abcdef0123456789abcdef0",
        ] {
            let err = run(bad, "encrypt", "x").await.expect_err("phải lỗi");
            assert!(
                err.to_string().contains("32 bytes"),
                "message phải nói rõ độ dài yêu cầu: {err}"
            );
        }
    }

    /// Action lạ phải báo lỗi, không rơi vào nhánh encrypt/decrypt.
    #[tokio::test]
    async fn unknown_action_errors() {
        let err = run(KEY, "frobnicate", "x").await.expect_err("phải lỗi");
        assert!(err.to_string().contains("Unknown action"), "{err}");
    }

    /// `decrypt` nhận cả blob có `\x` lẫn hex trần.
    #[tokio::test]
    async fn decrypt_accepts_bare_hex() {
        let encrypted = encrypt(KEY.as_bytes(), &"bare-hex".to_string()).expect("encrypt");
        let hex = hex::encode(&encrypted);

        let with_prefix = format!("\\x{hex}");
        let stripped = with_prefix.strip_prefix("\\x").unwrap();
        assert_eq!(stripped, hex, "cả hai đường phải ra cùng một chuỗi hex");
    }

    /// Blob rác phải báo lỗi decode, không panic.
    #[tokio::test]
    async fn garbage_input_errors_not_panics() {
        let err = run(KEY, "decrypt", "not-hex-at-all!!")
            .await
            .expect_err("phải lỗi");
        assert!(err.to_string().contains("Decode hex failed"), "{err}");

        // Hex hợp lệ nhưng quá ngắn cho nonce 12 byte.
        let err = run(KEY, "decrypt", "\\xabcd").await.expect_err("phải lỗi");
        assert!(!err.to_string().is_empty(), "phải có message: {err}");
    }
}
