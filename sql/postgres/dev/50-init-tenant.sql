-- Dev tenant seed.
-- Master key (set via MASTER_KEY env in docker-compose.yml):
--   '0123456789abcdef0123456789abcdef' (32 ASCII bytes, dev only — INSECURE).
--
-- Encrypted blobs below were generated with the opsense CLI:
--   MASTER_KEY=0123456789abcdef0123456789abcdef \
--     opsense token encrypt "<plaintext>"
-- Each blob is `nonce(12) || ciphertext` (AES-256-GCM). Regenerate freely;
-- `opsense token decrypt <blob>` will verify against this master key.
--
-- Plaintext values:
--   id=1, service='dev-service'       → 'dev-service-placeholder-token!!'
--   id=2, service='dev-oidc-secret'   → 'opsense-dev-shared-secret-32-bytes-min!!!'

INSERT INTO sys_tenant (host, id, jwt_mode)
VALUES ('localhost', 1, NULL)
ON CONFLICT (host) DO NOTHING;

-- Token 1: dev-service (placeholder, decrypt path not exercised in tests).
INSERT INTO sys_token_map (id, tenant_id, service, token)
VALUES (1, 1, 'dev-service',
        '\x6247bda500ff5305e751beae07294cceea697ecdd04e142fe4b9f39c4cdaac89ebb07881e188b65b775e1869ecd92b56dce0871f698fda5c245501')
ON CONFLICT (tenant_id, service) DO NOTHING;

-- Dev OIDC config (Dex integration test).
-- Issuer: opsense-dex (compose service) on port 5556.
-- Client: opsense-test, secret matches Dex staticClients in conf/dex/config.dev.yaml.
INSERT INTO sys_oidc (id, tenant_id, name, jwt_mode, oidc_issuer, oidc_jwks_url, oidc_client_id, oidc_client_secret, oidc_expected_alg)
VALUES (1, 1, 'default', 'jwks', 'http://opsense-dex:5556/dex',
        'http://opsense-dex:5556/dex/keys',
        'opsense-test', 2, 'RS256')
ON CONFLICT (id) DO NOTHING;

-- Token 2: dev-oidc-secret (decrypted at runtime to validate JWT signature).
INSERT INTO sys_token_map (id, tenant_id, service, token)
VALUES (2, 1, 'dev-oidc-secret',
        '\x3422f5aa4c2612539d15080952aceeb2e92da72297f4cc57b13b5d754e986a87a32b69a14543bd7c27e39a29daedea7dff964194c64b48387a9eaa8ec13007881b072b1eff')
ON CONFLICT (tenant_id, service) DO NOTHING;

-- Map oidc_client_secret (id=2) → token_map row 2.
UPDATE sys_oidc SET oidc_client_secret = 2 WHERE id = 1;
