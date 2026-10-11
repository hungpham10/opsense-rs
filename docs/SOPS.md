# SOPS — mã hoá env và secrets cho deploy

> **Trạng thái:** `scripts/release.sh` đã tích hợp sẵn đường giải mã SOPS bằng
> age key. Repo **không** chứa secret dạng plaintext: chỉ có file đã mã hoá
> (commit được) + private key nằm ở máy người vận hành.

Mọi lệnh dưới đây chạy từ **gốc repo**. File mẫu kèm repo:
[`env/secrets.dev.enc.yaml`](../env/secrets.dev.enc.yaml) (xem §6 — file này
**không phải SOPS file thật**, chỉ là placeholder).

---

## 1. Tổng quan đường đi

```
máy vận hành                          repo (git)                    container
─────────────                        ───────────                    ─────────
env/render.env  ──sops -e──▶  env/render.env.enc  ──COPY──▶  /app/secrets/render.env.enc
(plaintext,                          (mã hoá,                       │
 KHÔNG commit)                       commit được)                   │ release.sh
                                                                     ▼
                                              SOPS_AGE_KEY_CONTENT ──▶ sops -d
                                              (env trên Render)         │
                                                                         ▼
                                                              export KEY=VALUE…
```

Private key **không bao giờ** vào repo và **không** vào image. Trên Render nó
nằm trong env var (Render mã hoá env at-rest).

---

## 2. Sinh age key (một lần duy nhất)

```bash
mkdir -p ~/.config/sops/age
age-keygen -o ~/.config/sops/age/keys.txt
chmod 600 ~/.config/sops/age/keys.txt
```

File sinh ra có 3 dòng — **public key ở dòng 2, là comment**:

```
# created: 2026-10-10T12:00:00+07:00      ← đừng lấy dòng này
# public key: age1ql3z7hjy54pw3hyww5ayy…   ← public key thật
AGE-SECRET-KEY-1…                          ← private key, TUYỆT ĐỐI không commit
```

Lấy public key:

```bash
sed -n 's/^# public key: //p' ~/.config/sops/age/keys.txt
```

> ⚠️ **Đừng dùng `head -1`.** File `age-keygen` bắt đầu bằng dòng
> `# created: …`; `head -1` trả về đúng dòng comment đó và SOPS sẽ fail
> `invalid recipient`. Target `make encrypt` từng có bug này.

---

## 3. `.sops.yaml` — khai recipient theo path

Tạo ở **gốc repo**, commit được (chỉ chứa public key):

```yaml
creation_rules:
  # File env cho Render
  - path_regex: \.env$
    age: age1ql3z7hjy54pw3hyww5ayy…     # public key ở bước 2

  # File secrets dạng YAML (nếu dùng)
  - path_regex: secrets\..*\.yaml$
    age: age1ql3z7hjy54pw3hyww5ayy…
```

Có `.sops.yaml` rồi thì **không cần** cờ `--age` khi encrypt — SOPS tự khớp
`path_regex`. Cờ `--age` trong `make encrypt` chỉ là fallback.

> `path_regex` khớp trên **đường dẫn tương đối** mà SOPS nhận, nên đặt
> `\.env$` là bắt mọi file `.env` bất kể nằm ở đâu.

---

## 4. Mã hoá

```bash
# 1. Viết plaintext (KHÔNG commit — .gitignore đã chặn, xem §5)
cat > env/render.env <<'EOF'
MASTER_KEY=0123456789abcdef0123456789abcdef
DB_DSN=postgres://user:pass@dpg-xxx:5432/db
GRAFANA_CLOUD_METRIC_ID=1182692
GRAFANA_CLOUD_METRIC_TOKEN=glc_xxx
EOF

# 2. Mã hoá → file .enc (file NÀY commit được)
sops -e env/render.env > env/render.env.enc

# 3. Xoá plaintext ngay
shred -u env/render.env 2>/dev/null || rm env/render.env
```

Hoặc dùng Makefile (đã khai `SOPS_AGE_KEY`, tự kiểm tra file key tồn tại):

```bash
make encrypt F=env/render.env          # → env/render.env.enc
```

---

## 5. `.gitignore`

Plaintext có thể bị commit để lộ — chặn ngay từ đầu:

```gitignore
# SOPS plaintext — chỉ commit bản .enc
env/*.env
!env/*.enc
```

---

## 6. `scripts/release.sh` đọc file ra sao

Đoạn quan trọng trong [`scripts/release.sh`](../scripts/release.sh):

```bash
decrypted_content=$(SOPS_AGE_KEY_FILE=<(echo "$SOPS_AGE_KEY_CONTENT") \
  sops -d --output-type dotenv "$ENCRYPTED_FILE")

while IFS= read -r line; do
    [[ -z "$line" || "$line" =~ ^# ]] && continue
    export "$line"
done <<< "$decrypted_content"
```

Hai hệ quả **bắt buộc** phải biết:

**a) File phải giải mã ra được dạng `KEY=VALUE`** (`--output-type dotenv`).
SOPS suy ra định dạng từ phần mở rộng/`input type`, nên file `.env` thường đúng
— nhưng **phải tự kiểm tra**, đừng đoán:

```bash
make sops-check F=env/render.env.enc
# hoặc
sops -d --output-type dotenv env/render.env.enc
```

Nếu in ra JSON/YAML thì container boot sẽ **im lặng thiếu env** — không có lỗi
rõ ràng, chỉ thấy app thiếu config.

**b) Value không được chứa newline.** `export "$line"` chỉ xử lý từng dòng;
value xuống dòng là vỡ vòng lặp. Password sinh ra phải một dòng.

> Ký tự đặc biệt trong `DB_DSN` (`@ : / #`) cũng làm lệch parse. Dùng password
> chỉ gồm chữ+số, hoặc URL-encode.

> `env/secrets.dev.enc.yaml` trong repo là **placeholder**, không phải SOPS
> file — chính comment trong file nói vậy. Nó tồn tại chỉ để Docker bind-mount
> không tự tạo thành **thư mục** (Docker tạo mount source thiếu dạng directory,
> làm cho mount target thành directory thay vì file).

---

## 7. Biến môi trường cần set trên nền tảng deploy

Trên Render (hoặc bất kỳ đâu), set **hai** biến này cho service:

| Biến | Giá trị |
|---|---|
| `SOPS_AGE_KEY_CONTENT` | **toàn bộ nội dung** file private key, kể cả dòng `# created:` |
| `ENCRYPTED_FILE` | đường dẫn file **đã mã hoá** bên trong container, vd `/app/secrets/render.env.enc` |

Set `SOPS_AGE_KEY_CONTENT` trên Render bằng cách paste nguyên block:

```
# created: 2026-10-10T12:00:00+07:00
# public key: age1ql3z7…
AGE-SECRET-KEY-1…
```

Thiếu `SOPS_AGE_KEY_CONTENT` thì `release.sh` return sớm với
`Ignore decryption` và chạy tiếp bằng env thường — **không crash**. Nên lỗi
thiếu secret biểu hiện là "app thiếu config", không phải "boot fail".

> 🔐 **Không** set `SOPS_AGE_KEY_CONTENT` trên `docker-compose.yml` local
> (sẽ lộ private key vào git). Local chỉ cần biến thường trong `.env`.

---

## 8. File mã hoá phải nằm trong image

`release.sh` đọc `$ENCRYPTED_FILE` **từ đĩa container**. Nền tảng không có
bind-mount (Render) thì phải COPY vào image — thêm một dòng vào target `serve`
của [`Earthfile`](../Earthfile):

```dockerfile
COPY env/render.env.enc /app/secrets/render.env.enc
```

Sau đó set `ENCRYPTED_FILE=/app/secrets/render.env.enc`.

---

## 9. Rotate key

**Rotate age key** (mất key cũ hoặc đổi người vận hành):

```bash
age-keygen -o ~/.config/sops/age/keys.txt.new
# thêm recipient mới vào .sops.yaml (giữ cả cái cũ nếu cần đọc lại file cũ)
sops updatekeys env/render.env.enc     # mã hoá lại recipient, giữ nguyên data key
```

`sops updatekeys` **không** cần giải mã rồi mã hoá lại — nó chỉ thay phần
data-key, nên an toàn và giữ nguyên nội dung.

**Rotate `MASTER_KEY`** là việc khác: đổi `MASTER_KEY` làm mọi blob trong
`sys_token_map` cũ **giải mã không được** ⇒ phiên đăng nhập và session hiện có
chết hết. Phải sinh lại blob bằng `opsense token encrypt` (xem §10) rồi seed
lại DB.

---

## 10. Sinh blob cho `sys_token_map`

`MASTER_KEY` **phải đúng 32 byte** — `Key::<Aes256Gcm>::from_slice` sẽ panic
nếu lệch, và lớp validate trong CLI chặn trước khi tới đó.

```bash
# Sinh key mới (32 hex chars = 32 byte)
openssl rand -hex 16

# Encrypt một secret
MASTER_KEY=<32 byte> opsense token encrypt "my-service-token"
# → \x6247bda500ff…     ← paste thẳng vào file SQL seed

# Verify seed đã paste đúng (decrypt ngược lại)
MASTER_KEY=<32 byte> opsense token decrypt "\x6247bda500ff…"
# → my-service-token
```

Blob sinh ra **khác nhau mỗi lần gọi** (nonce ngẫu nhiên) — đừng so blob với
bản cũ, so bằng `decrypt` ngược lại.

Seed idempotent (`ON CONFLICT DO NOTHING`) nên chạy lại được. Xem
[`sql/postgres/dev/50-init-tenant.sql`](../sql/postgres/dev/50-init-tenant.sql)
— header file liệt kê plaintext tương ứng từng blob để đối chiếu.

---

## 11. Checklist trước khi deploy

- [ ] `~/.config/sops/age/keys.txt` tồn tại, `chmod 600`
- [ ] Public key trong `.sops.yaml` khớp private key đang giữ
- [ ] `make sops-check F=env/render.env.enc` in ra đúng `KEY=VALUE`
- [ ] Không có value nào chứa newline
- [ ] `SOPS_AGE_KEY_CONTENT` đã set trên nền tảng (paste **nguyên block**)
- [ ] `ENCRYPTED_FILE` trỏ đúng file **đã mã hoá** trong image
- [ ] `.gitignore` chặn `env/*.env`, không chặn `env/*.enc`
- [ ] `git status` sạch — không có plaintext nào lỡ staged

---

## Xem thêm

- [`docs/KUBERNETES.md`](KUBERNETES.md) — deployment, env đầy đủ
- [`docs/GUIDE.md`](GUIDE.md) — REST/GraphQL/MCP surface
- [`Makefile`](../Makefile) — target `encrypt` / `decrypt` / `sops-check`
