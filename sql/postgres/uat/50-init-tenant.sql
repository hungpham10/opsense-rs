-- UAT tenant seed.
INSERT INTO sys_tenant (host, id, jwt_mode)
VALUES ('uat.opsense.internal', 2, NULL)
ON CONFLICT (host) DO NOTHING;

INSERT INTO sys_token_map (id, tenant_id, service, token)
VALUES (2, 2, 'uat-service', '\x0000000000000000000000000000000000000000000000000000000000000000')
ON CONFLICT (tenant_id, service) DO NOTHING;
