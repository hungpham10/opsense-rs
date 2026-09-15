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

use opsense_libs::sops::{decrypt, encrypt};
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
