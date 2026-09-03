-- Prod tenant seed.
INSERT INTO sys_tenant (host, id, jwt_mode)
VALUES ('opsense.example.com', 3, NULL)
ON CONFLICT (host) DO NOTHING;

INSERT INTO sys_token_map (id, tenant_id, service, token)
VALUES (3, 3, 'prod-service', '\x0000000000000000000000000000000000000000000000000000000000000000')
ON CONFLICT (tenant_id, service) DO NOTHING;
