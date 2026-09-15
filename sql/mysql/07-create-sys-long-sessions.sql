-- Long-lived sessions cho MCP client — Ed25519 keypair signing.
-- MySQL equivalent. TTL 8h. Cleanup: lazy (đọc thấy expired thì xóa trước khi trả).

CREATE TABLE IF NOT EXISTS sys_long_sessions (
    id              BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT NOT NULL,
    user_id         VARCHAR(255) NOT NULL,
    session_id      VARCHAR(64) NOT NULL,
    -- private_key_enc = Ed25519 private key (32 bytes), mã hóa AES-256-GCM
    -- bằng MASTER_KEY (sops::encrypt). session_id = base64(public_key).
    private_key_enc VARBINARY(512) NOT NULL,
    status          VARCHAR(16) NOT NULL DEFAULT 'active',
    expires_at      TIMESTAMP NOT NULL,
    last_used_at    TIMESTAMP DEFAULT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uk_session_id (session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_long_sessions_tenant_user ON sys_long_sessions(tenant_id, user_id);
CREATE INDEX idx_long_sessions_expires      ON sys_long_sessions(expires_at, status);
