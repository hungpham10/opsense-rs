-- Dev tenant seed.
-- Password hint: token below is all-zeroes (INSECURE — dev only).
-- Replace token with output from: openssl rand -hex 32
INSERT INTO sys_tenant (host, id, jwt_mode)
VALUES ('localhost', 1, NULL)
ON CONFLICT (host) DO NOTHING;

INSERT INTO sys_token_map (id, tenant_id, service, token)
VALUES (1, 1, 'dev-service', '\x0000000000000000000000000000000000000000000000000000000000000000')
ON CONFLICT (tenant_id, service) DO NOTHING;

-- Dev OIDC config (Dex integration test).
-- Issuer: opsense-dex (compose service) on port 5556.
-- Client: opsense-test, secret matches Dex staticClients in conf/dex/config.dev.yaml.
INSERT INTO sys_oidc (id, tenant_id, name, jwt_mode, oidc_issuer, oidc_jwks_url, oidc_client_id, oidc_client_secret, oidc_expected_alg)
VALUES (1, 1, 'default', 'jwks', 'http://opsense-dex:5556/dex',
        'http://opsense-dex:5556/dex/keys',
        'opsense-test', 2, 'RS256')
ON CONFLICT (id) DO NOTHING;

INSERT INTO sys_token_map (id, tenant_id, service, token)
VALUES (2, 1, 'dev-oidc-secret', '\x6f7073656e73652d6465762d7368617265642d7365637265742d33322d62797465732d6d696e212121')
ON CONFLICT (tenant_id, service) DO NOTHING;

-- Map oidc_client_secret (id=2) → token_map row 2.
UPDATE sys_oidc SET oidc_client_secret = 2 WHERE id = 1;
