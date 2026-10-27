-- sys_sitemap: bảng sitemap entries per tenant (MySQL version)
CREATE TABLE IF NOT EXISTS `sys_sitemap` (
    `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `tenant_id` BIGINT NOT NULL,
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `loc` VARCHAR(500) NOT NULL,
    `freq` VARCHAR(50) NOT NULL,
    `priority` DOUBLE NOT NULL,

    UNIQUE KEY `uk_tenant_loc` (`tenant_id`, `loc`)
);

-- sys_filemap: bảng file path mapping per tenant (MySQL version)
CREATE TABLE IF NOT EXISTS `sys_filemap` (
    `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `tenant_id` BIGINT NOT NULL,
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `src` VARCHAR(500) NOT NULL,
    `dest` VARCHAR(500) NOT NULL,

    UNIQUE KEY `uk_tenant_src` (`tenant_id`, `src`)
);