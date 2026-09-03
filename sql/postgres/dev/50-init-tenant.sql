-- Dev tenant seed.
-- Password hint: token below is all-zeroes (INSECURE — dev only).
-- Replace token with output from: openssl rand -hex 32
INSERT INTO sys_tenant (host, id, jwt_mode)
VALUES ('localhost', 1, NULL)
ON CONFLICT (host) DO NOTHING;

INSERT INTO sys_token_map (id, tenant_id, service, token)
VALUES (1, 1, 'dev-service', '\x0000000000000000000000000000000000000000000000000000000000000000')
ON CONFLICT (tenant_id, service) DO NOTHING;
