# env/ — SOPS-encrypted secrets

This directory holds per-environment secrets, one file per environment
(dev, uat, prod). Each file is decrypted at container boot by
`scripts/release.sh` using an age key provided via the `SOPS_AGE_KEY_CONTENT`
environment variable.

## Files

| Plaintext (gitignored)  | Encrypted (committed)              | Decrypted at boot     |
|-------------------------|------------------------------------|-----------------------|
| `secrets.dev.yaml`      | `secrets.dev.enc.yaml`             | `APP_ENV=dev`         |
| `secrets.uat.yaml`      | `secrets.uat.enc.yaml`             | `APP_ENV=uat`         |
| `secrets.prod.yaml`     | `secrets.prod.enc.yaml`            | `APP_ENV=prod`        |

## Workflow

### 1. Generate an age key (one-time per operator)

```bash
mkdir -p ~/.config/sops/age
age-keygen -o ~/.config/sops/age/keys.txt
# Note the public key (age1...) printed to stderr.
```

### 2. Add recipients to `.sops.yaml`

Create `.sops.yaml` at the repo root with the recipients (one per operator
or service account):

```yaml
creation_rules:
  - path_regex: env/secrets\.(dev|uat|prod)\.enc\.yaml
    key_groups:
      - age:
          - age1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### 3. Encrypt a plaintext file

```bash
sops -e env/secrets.dev.yaml > env/secrets.dev.enc.yaml
```

Commit `*.enc.yaml` only. Plaintext (`secrets.{dev,uat,prod}.yaml`) must
be in `.gitignore`.

### 4. Decrypt at boot

`release.sh` runs:

```bash
SOPS_AGE_KEY_FILE=<(echo "$SOPS_AGE_KEY_CONTENT") sops -d --output-type dotenv \
  /app/secrets/secrets.${APP_ENV}.enc.yaml
```

…then exports each `KEY=value` line into the shell environment. If
`SOPS_AGE_KEY_CONTENT` is not set, the script falls back to whatever
variables are already exported in the environment (useful for local dev
with `docker compose`).

## Reference

- sops docs: https://github.com/getsops/sops
- age docs: https://github.com/FiloSottile/age
