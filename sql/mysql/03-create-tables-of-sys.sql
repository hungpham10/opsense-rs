-- Bảng sys_*: chỉ 4 bảng phục vụ admin entity (tenant + token management).
-- Mọi query của Admin đều đi qua 4 bảng này (xem docs/architecture.md §10).

CREATE TABLE IF NOT EXISTS `sys_tenant` (
  `host` VARCHAR(200) PRIMARY KEY,
  `id` BIGINT NOT NULL UNIQUE,
  `jwt_mode` VARCHAR(20) DEFAULT NULL,
  `jwt_secret` BIGINT DEFAULT NULL COMMENT 'Trỏ tới sys_token_map.id',
  `oidc_jwks_url` VARCHAR(500) DEFAULT NULL,
  `oidc_issuer` VARCHAR(255) DEFAULT NULL,
  `oidc_client_id` VARCHAR(255) DEFAULT NULL COMMENT 'Client ID công khai (chuỗi)',
  `oidc_client_secret` BIGINT DEFAULT NULL COMMENT 'Trỏ tới sys_token_map.id',
  `oidc_expected_alg` VARCHAR(10) DEFAULT NULL,
  `session_secret` BIGINT DEFAULT NULL COMMENT 'Trỏ tới sys_token_map.id',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `sys_oidc` (
  `id` BIGINT NOT NULL PRIMARY KEY,
  `tenant_id` BIGINT NOT NULL,
  `name` VARCHAR(100) NOT NULL,
  `jwt_mode` VARCHAR(20) DEFAULT NULL,
  `jwt_secret` BIGINT DEFAULT NULL,
  `oidc_jwks_url` VARCHAR(500) DEFAULT NULL,
  `oidc_issuer` VARCHAR(255) DEFAULT NULL,
  `oidc_client_id` VARCHAR(255) DEFAULT NULL,
  `oidc_client_secret` BIGINT DEFAULT NULL,
  `oidc_expected_alg` VARCHAR(10) DEFAULT NULL,
  `session_secret` BIGINT DEFAULT NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `sys_token_map` (
  `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
  `tenant_id` BIGINT,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `service` VARCHAR(200) NOT NULL,
  `token` VARBINARY(1024) NOT NULL,

  UNIQUE KEY `uk_tenant_service` (`tenant_id`, `service`)
);

CREATE TABLE IF NOT EXISTS `sys_user` (
  `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
  `tenant_id` BIGINT NOT NULL,
  `user_id` VARCHAR(255) NOT NULL,
  `token_hash` VARCHAR(64) NOT NULL COMMENT 'Chỉ lưu sha256 hex, plaintext được mã hoá trong sys_token_map',
  `token_id` BIGINT NOT NULL COMMENT 'Trỏ tới sys_token_map.id',
  `expires_at` TIMESTAMP NULL DEFAULT NULL,
  `revoked_at` TIMESTAMP NULL DEFAULT NULL,
  `last_used_at` TIMESTAMP NULL DEFAULT NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  UNIQUE KEY `uk_user_tenant_user` (`tenant_id`, `user_id`),
  UNIQUE KEY `uk_user_token_hash` (`token_hash`)
);
