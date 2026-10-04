# Đóng gói Ubuntu — `.deb` + systemd + nginx

Gói cài `opsense` trên Ubuntu/Debian như **một systemd service**, đứng sau
**nginx** làm reverse proxy. `nginx` là dependency thật của gói
(`Depends: nginx` trong `packaging/nfpm.yaml`), và site config được cài vào
`/etc/nginx/sites-available/` rồi tự bật trong `sites-enabled/`.

## Cài đặt nhanh

```bash
./scripts/build-deb.sh                 # latest release, native arch
sudo apt install ./dist/opsense_v1.0.26_amd64.deb

curl -s http://localhost/health        # qua nginx
journalctl -u opsense -f
```

## Build

```bash
./scripts/build-deb.sh                        # latest release
./scripts/build-deb.sh v1.0.26                # pin tag
OPSENSE_BIN_DIR=target/release ./scripts/build-deb.sh   # binary local, không tải
```

Cần `curl`, `tar`, `sha256sum`. **Không** cần cargo — [nfpm](https://nfpm.goreleaser.com/)
tải sẵn binary static nên chạy được trên máy không có Rust toolchain.

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `OPSENSE_VERSION` | latest release | tag/version đóng gói |
| `OPSENSE_DEB_ARCH` | `uname -m` | `amd64` \| `arm64` \| `all` |
| `OPSENSE_BIN_DIR` | — | dùng binary local, bỏ qua bước tải release |
| `OPSENSE_DEB_OUT` | `dist/` | thư mục output |
| `NFPM_VERSION` | `2.44.0` | phiên bản nfpm tải về |
| `OPSENSE_NFPM` | — | dùng nfpm đã có sẵn |

Chỉ build **native**: nfpm không cross-compile được — tạo `.deb` arm64 trên
máy x86 thì cài lên x86 sẽ bị dpkg từ chối. Muốn multi-arch thì CI build
mỗi kiến trúc trên runner tương ứng.

## Gói cài những gì

| Đường dẫn | Loại | Vai trò |
|---|---|---|
| `/usr/bin/opsense` | binary | chính |
| `/usr/bin/opsense-kernel-{echo,python,julia}` | binary | kernel mà `opsense-runner` spawn |
| `/lib/systemd/system/opsense.service` | unit | systemd service |
| `/etc/nginx/sites-available/opsense.conf` | config | reverse proxy |
| `/etc/opsense/opsense.toml` | **conffile** | config pipeline + storage |
| `/etc/opsense/opsense.env` | **conffile** | biến môi trường |
| `/var/lib/opsense/` | state | `WorkingDirectory`, `.opsense/` tương đối |

Cả hai file `/etc/opsense/*` là conffile ⇒ `apt upgrade` **không** ghi đè
chỉnh sửa của admin.

Phải cài **cả nhóm binary**: `opsense` tìm kernel theo thứ tự
`OPSENSE_KERNEL` → cạnh `opsense` → `target/debug` → `PATH`
(`crates/opsense-runner/src/config.rs`). Chỉ copy `opsense` thì lệnh trading
fail **lúc chạy**, không phải lúc cài.

## Vì sao proxy qua Unix socket

`serve` mặc định `GATEWAY_LISTENER=unix` và bind `/run/axum`
(`crates/opsense/src/serve.rs`). Nginx proxy thẳng vào socket đó:

```nginx
server unix:/run/axum fail_timeout=0;
```

Nghĩa là app **không mở port nào** trên network — không cần
`ListenAddress` trong unit, và loopback cũng không đi qua được nếu app chết.

Muốn TCP: đặt `GATEWAY_LISTENER=http` + `GATEWAY_ADDR=127.0.0.1:8080` trong
`/etc/opsense/opsense.env`, rồi sửa `upstream` trong site config thành
`server 127.0.0.1:8080;`.

## Lưu ý khi gỡ

- `/etc/opsense/` và `/var/lib/opsense/` **không** bị xoá, kể cả `--purge` —
  đó là dữ liệu của admin.
- Symlink `sites-enabled/opsense.conf` chỉ bị gỡ nếu nó **trỏ đúng** file của
  gói. Admin tự tạo symlink riêng thì giữ nguyên.
- User `opsense` cố ý **không** bị xoá (file trong `/var/lib/opsense` thuộc
  user đó). Dọn tay: `userdel opsense` sau khi tự xoá state.

## Khác với `conf/nginx/` trong repo

`conf/nginx/` là **openresty** + `lua-resty-openidc` + placeholder
`%%HTTP_SERVER%%` cho build Docker. Còn `packaging/nginx/opsense.conf` là
config cho `nginx` distro của Ubuntu: thuần reverse proxy, không Lua.

Hai thứ khác nhau đủ để đừng trộn:

| | `conf/nginx/` | `packaging/nginx/` |
|---|---|---|
| nginx | openresty | `nginx` (Ubuntu) |
| OIDC | `lua-resty-openidc` | không (app tự phục vụ `/api/oauth`) |
| logging | JSON ra stdout | file trong `/var/log/nginx/` |
| upstream | `unix:/var/run/axum` | `unix:/run/axum` |

`/run/axum` và `/var/run/axum` là cùng một chỗ (`/var/run` là symlink →
`/run`), nhưng nên viết `/run/axum` vì systemd `RuntimeDirectory` và log
hiện đại dùng `/run`.
