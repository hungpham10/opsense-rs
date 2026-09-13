-- Dev tenant seed (MySQL).
-- Token is all-zeroes (INSECURE — dev only). Generate real token with: openssl rand -hex 64
INSERT IGNORE INTO sys_tenant (host, id, jwt_mode)
VALUES ('localhost', 1, NULL);

INSERT IGNORE INTO sys_token_map (id, tenant_id, service, token)
VALUES (1, 1, 'dev-service', X'0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');
