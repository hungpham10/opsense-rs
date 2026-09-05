-- OAuth2 Device Authorization Grant (RFC 8628) — device_code table.
-- MySQL equivalent. Không dùng trigger — dùng ON UPDATE CURRENT_TIMESTAMP.

CREATE TABLE IF NOT EXISTS sys_device_code (
    id              BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT NOT NULL,
    device_code     VARCHAR(128) NOT NULL,
    user_code       VARCHAR(16) NOT NULL,
    interval_secs   INT NOT NULL DEFAULT 5,
    expires_at      TIMESTAMP NOT NULL,
    status          VARCHAR(16) NOT NULL DEFAULT 'pending',
    user_id         VARCHAR(255) DEFAULT NULL,
    client_id       VARCHAR(255) NOT NULL DEFAULT 'opsense-cli',
    approved_at     TIMESTAMP DEFAULT NULL,
    access_token    VARCHAR(512) DEFAULT NULL,
    refresh_token   VARCHAR(512) DEFAULT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uk_device_code (device_code),
    UNIQUE KEY uk_user_code (user_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_device_code_user_code ON sys_device_code(user_code);
CREATE INDEX idx_device_code_poll      ON sys_device_code(device_code, status, expires_at);
