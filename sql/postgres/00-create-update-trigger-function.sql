-- Trigger function used by sys_* tables to auto-update `updated_at` on row changes.
-- Referenced by: sys_tenant, sys_oidc, sys_token_map, sys_user.
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
