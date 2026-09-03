use std::io::{Error, ErrorKind};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use rand::{RngCore, thread_rng};

pub fn decrypt(master_key: &[u8], encrypted_bytes: &[u8]) -> Result<String, Error> {
    let key = Key::<Aes256Gcm>::from_slice(master_key);
    let cipher = Aes256Gcm::new(key);

    if encrypted_bytes.len() < 12 {
        return Err(Error::new(
            ErrorKind::BrokenPipe,
            "Data too short for decoding",
        ));
    }

    let (nonce_part, ciphertext_part) = encrypted_bytes.split_at(12);
    let nonce = Nonce::from_slice(nonce_part);

    let decrypted_bytes = cipher
        .decrypt(nonce, ciphertext_part)
        .map_err(|error| Error::new(ErrorKind::BrokenPipe, format!("Decode failed: {error}",)))?;

    let token = String::from_utf8(decrypted_bytes)
        .map_err(|error| Error::new(ErrorKind::BrokenPipe, format!("Validate failed: {error}",)))?;

    Ok(token)
}

pub fn encrypt(master_key: &[u8], token_plain: &String) -> Result<Vec<u8>, Error> {
    let key = Key::<Aes256Gcm>::from_slice(master_key);
    let cipher = Aes256Gcm::new(key);

    let mut nonce_bytes = [0u8; 12];
    thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, token_plain.as_bytes())
        .map_err(|error| Error::new(ErrorKind::BrokenPipe, format!("Encrypt failed: {error}",)))?;

    let mut final_blob = nonce_bytes.to_vec();
    final_blob.extend_from_slice(&ciphertext);
    Ok(final_blob)
}
