-- Indexes for sys_* tables to speed up common lookups.
-- PostgreSQL only; MySQL uses inline PRIMARY KEY / UNIQUE constraints instead.

CREATE INDEX IF NOT EXISTS idx_sys_user_tenant_id      ON sys_user(tenant_id);
CREATE INDEX IF NOT EXISTS idx_sys_token_map_tenant_id ON sys_token_map(tenant_id);
CREATE INDEX IF NOT EXISTS idx_sys_oidc_tenant_id      ON sys_oidc(tenant_id);
