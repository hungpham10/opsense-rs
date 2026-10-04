#!/usr/bin/env bash
#
# Build gói .deb cho opsense từ binary đã build sẵn.
#
#   ./scripts/build-deb.sh                      # latest release, auto arch
#   ./scripts/build-deb.sh v1.0.26              # pin tag
#   OPSENSE_BIN_DIR=target/release ./scripts/build-deb.sh   # dùng binary local
#
# Cần: curl, tar, sha256sum. **Không** cần cargo — nfpm tải sẵn binary
# static của Go nên chạy được trên máy không có Rust toolchain (đúng case của
# CI build image).
set -euo pipefail

REPO="${OPSENSE_REPO:-hungpham10/opsense-rs}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${OPSENSE_DEB_OUT:-$ROOT/dist}"
NFPM_VERSION="${NFPM_VERSION:-2.44.0}"

log() { echo "==> $*" >&2; }
die() { echo "build-deb: $*" >&2; exit 1; }

# ── 1. Kiểm tra host ────────────────────────────────────────────────────
# nfpm đóng gói theo kiến trúc **đích**, không cross-compile được: tạo .deb
# arm64 trên máy x86 chạy được nhưng cài lên x86 thì dpkg từ chối. Nên build
# native, trừ khi người dùng ép `OPSENSE_DEB_ARCH`.
host_os="$(uname -s | tr '[:upper:]' '[:lower:]')"
[ "$host_os" = "linux" ] || die "phải chạy trên Linux (hiện tại: $host_os)"

host_arch="$(uname -m)"
case "$host_arch" in
  x86_64|amd64) default_deb_arch=amd64;  cargo_target=x86_64-unknown-linux-musl ;;
  aarch64|arm64) default_deb_arch=arm64; cargo_target=aarch64-unknown-linux-gnu ;;
  *) die "không hỗ trợ arch: $host_arch (đặt OPSENSE_DEB_ARCH + OPSENSE_BIN_DIR nếu tự lo)" ;;
esac
deb_arch="${OPSENSE_DEB_ARCH:-$default_deb_arch}"

# ── 2. Binary ───────────────────────────────────────────────────────────
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

if [ -n "${OPSENSE_BIN_DIR:-}" ]; then
  bin_dir="$OPSENSE_BIN_DIR"
  version="${OPSENSE_VERSION:-$(git -C "$ROOT" describe --tags --always --dirty 2>/dev/null || echo 0.0.0)}"
  log "Dùng binary local: $bin_dir (version $version)"
else
  tag="${OPSENSE_VERSION:-${1:-}}"
  if [ -z "$tag" ]; then
    tag="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
          | grep -m1 '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')" \
      || die "không dò được latest release (đặt OPSENSE_VERSION=vX.Y.Z)"
  fi
  version="$tag"

  url="https://github.com/$REPO/releases/download/$tag/opsense-$cargo_target.tar.xz"
  log "Tải $url"
  curl -fsSL "$url" -o "$tmp/opsense.tar.xz"

  # Kiểm checksum nếu release có phát (cargo-dist phát kèm `.sha256`).
  if curl -fsSL "$url.sha256" -o "$tmp/opsense.tar.xz.sha256" 2>/dev/null; then
    expected="$(awk '{print $1}' "$tmp/opsense.tar.xz.sha256")"
    actual="$(sha256sum "$tmp/opsense.tar.xz" | awk '{print $1}')"
    [ "$expected" = "$actual" ] || die "checksum mismatch ($expected != $actual)"
    log "Checksum OK"
  else
    log "CẢNH BÁO: release không có .sha256 — archive chưa được xác minh"
  fi

  tar -xJf "$tmp/opsense.tar.xz" -C "$tmp"
  # cargo-dist bọc binary trong thư mục theo target (opsense-<triple>/opsense)
  # nên dò bằng `find` thay vì đoán layout.
  found="$(find "$tmp" -type f -name opsense -perm -u+x | head -n1)"
  [ -n "$found" ] || die "archive không có binary 'opsense'"
  bin_dir="$(dirname "$found")"
fi

for b in opsense opsense-kernel-echo opsense-kernel-python opsense-kernel-julia; do
  [ -x "$bin_dir/$b" ] || die "thiếu binary '$b' trong $bin_dir"
done

# ── 3. nfpm ─────────────────────────────────────────────────────────────
# Binary static của Go từ release chính thức: nhanh hơn `cargo install nfpm`
# (compile) và không kéo thêm toolchain vào CI image.
nfpm="$tmp/nfpm"
if [ -n "${OPSENSE_NFPM:-}" ]; then
  nfpm="$OPSENSE_NFPM"
else
  log "Tải nfpm $NFPM_VERSION"
  nfpm_url="https://github.com/goreleaser/nfpm/releases/download/v$NFPM_VERSION/nfpm_${NFPM_VERSION}_linux_amd64.tar.gz"
  curl -fsSL "$nfpm_url" -o "$tmp/nfpm.tar.gz"
  tar -xzf "$tmp/nfpm.tar.gz" -C "$tmp" nfpm
  chmod +x "$nfpm"
fi

# ── 4. Validate + render template ───────────────────────────────────────
# `@VERSION@` / `@ARCH@` nằm trong **double-quoted** YAML, nên chỉ escape kiểu
# sed là chưa đủ: một giá trị chứa `"` hoặc `\` sẽ sinh ra yaml hỏng, mà
# nfpm báo lỗi ở dòng khác hẳn nên rất khó đoán là do tham số. Chặn từ đầu.
case "$version" in
  *[!A-Za-z0-9._+-]*) die "version lạ: '$version' (chỉ nhận [A-Za-z0-9._+-])" ;;
esac
case "$deb_arch" in
  amd64|arm64|all) ;;
  *) die "arch lạ: '$deb_arch' (chỉ nhận amd64|arm64|all)" ;;
esac
cfg="$tmp/nfpm.yaml"
# Escape ký tự đặc biệt của sed thay vì chỉ thay literal: `@VERSION@` có thể
# chứa `/` (VD `release/1.0`) và `&` (ký tự "toàn bộ match" của sed) — thay
# trực tiếp sẽ sinh ra yaml hỏng hoặc thay nhầm chỗ khác.
render() { sed -e "s|@VERSION@|$(printf '%s' "$1" | sed -e 's/[&|\\]/\\&/g')|g" \
                   -e "s|@ARCH@|$(printf '%s' "$2" | sed -e 's/[&|\\]/\\&/g')|g" \
                   -e "s|@BIN_DIR@|$(printf '%s' "$3" | sed -e 's/[&|\\]/\\&/g')|g"; }

# `contents.src` trong nfpm là đường dẫn tương đối **so với thư mục chạy
# nfpm**, nên cd vào repo root trước.
cd "$ROOT"
render "$version" "$deb_arch" "$bin_dir" < packaging/nfpm.yaml > "$cfg"

mkdir -p "$OUT_DIR"
pkg="$OUT_DIR/opsense_${version}_${deb_arch}.deb"
log "Đóng gói $pkg"
"$nfpm" package --config "$cfg" --packager deb --target "$pkg"

# ── 5. Kiểm tra nhanh ───────────────────────────────────────────────────
# `dpkg-deb` có sẵn trên mọi Ubuntu. Fail ở đây dễ hơn nhiều so với fail lúc
# người dùng `apt install` trên máy thật.
if command -v dpkg-deb >/dev/null 2>&1; then
  echo >&2
  log "Nội dung gói:"
  dpkg-deb -c "$pkg" | awk '{print "  " $1, $6}' >&2
  log "Depends:"
  dpkg-deb -f "$pkg" Depends >&2
  log "control scripts:"
  dpkg-deb -e "$pkg" "$tmp/control" && ls "$tmp/control" >&2
else
  log "dpkg-deb không có — bỏ qua bước kiểm tra"
fi

# `apt install` cần đường dẫn **tương đối**; `pkg` có thể là absolute nên
# `./$pkg` ra `.//tmp/...`.
pkg_name="$(basename "$pkg")"

cat >&2 <<EOF

Xong: $pkg

Cài trên Ubuntu:
  sudo dpkg -i "$pkg"
  # hoặc để apt tự kéo dependency (khuyến nghị):
  #   sudo apt install "./$pkg_name"

Sau đó:
  curl -s http://localhost/health        # qua nginx
  journalctl -u opsense -f
  vi /etc/opsense/opsense.toml          # pipeline + storage
EOF
