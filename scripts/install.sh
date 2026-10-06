#!/bin/sh
# opsense install script
# Usage: curl -fsSL https://raw.githubusercontent.com/hungpham10/opsense-rs/main/scripts/install.sh | sh

set -eu

REPO="hungpham10/opsense-rs"
BIN_NAME="opsense"
# `opsense` không tự chạy được trading: `opsense-runner` spawn kernel riêng và
# tìm nó theo thứ tự `OPSENSE_KERNEL` → **cạnh `opsense`** → `target/debug` → PATH
# (crates/opsense-runner/src/config.rs:170). Nên phải cài *cả nhóm* binary vào
# cùng một thư mục, nếu chỉ copy `opsense` thì lệnh trading sẽ fail lúc chạy.
INSTALL_DIR="${OPSENSE_INSTALL_DIR:-$HOME/.local/bin}"

uname_s="$(uname -s | tr '[:upper:]' '[:lower:]')"
uname_m="$(uname -m)"

# Danh sách target đúng với `targets` trong dist-workspace.toml. Lưu ý: bản
# release chỉ có `x86_64-unknown-linux-musl` cho linux/musl — **không có** bản
# aarch64 musl, nên linux/arm64 buộc phải dùng bản gnu.
case "$uname_s/$uname_m" in
  linux/x86_64)  target="x86_64-unknown-linux-musl" ;;
  linux/aarch64) target="aarch64-unknown-linux-gnu" ;;
  darwin/x86_64) target="x86_64-apple-darwin" ;;
  darwin/arm64)  target="aarch64-apple-darwin" ;;
  *) echo "unsupported platform: $uname_s/$uname_m" >&2; exit 1 ;;
esac
target="${OPSENSE_TARGET:-$target}"

# Version pin: OPSENSE_VERSION=v1.0.23 (hoặc truyền làm argv[1]).
want="${OPSENSE_VERSION:-${1:-}}"
if [ -n "$want" ]; then
  tag="$want"
else
  tag="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
        | grep -m1 tag_name | sed -E 's/.*"([^"]+)".*/\1/')" || true
  [ -n "${tag:-}" ] || { echo "could not detect latest tag" >&2; exit 1; }
fi

url="https://github.com/$REPO/releases/download/$tag/opsense-$target.tar.xz"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "Downloading $url"
curl -fsSL "$url" -o "$tmp/opsense.tar.xz"

# cargo-dist phát kèm `<archive>.sha256`. Kiểm tra nếu có; nếu không có thì chỉ
# cảnh báo (release cũ có thể chưa bật), trừ khi OPSENSE_SKIP_CHECKSUM=0 ép buộc.
sum_url="$url.sha256"
if [ "${OPSENSE_SKIP_CHECKSUM:-1}" = "0" ] && [ ! -f "$tmp/opsense.tar.xz.sha256" ]; then
  curl -fsSL "$sum_url" -o "$tmp/opsense.tar.xz.sha256"
fi
if [ -f "$tmp/opsense.tar.xz.sha256" ]; then
  expected="$(awk '{print $1}' "$tmp/opsense.tar.xz.sha256")"
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$tmp/opsense.tar.xz" | awk '{print $1}')"
  else
    actual="$(shasum -a 256 "$tmp/opsense.tar.xz" | awk '{print $1}')"
  fi
  [ "$expected" = "$actual" ] || { echo "checksum mismatch for opsense.tar.xz" >&2; exit 1; }
  echo "Checksum OK"
else
  echo "Warning: no .sha256 published for this release — archive is unverified." >&2
fi

tar -xJf "$tmp/opsense.tar.xz" -C "$tmp"
mkdir -p "$INSTALL_DIR"

# cargo-dist bọc binary trong thư mục theo target (vd
# opsense-aarch64-apple-darwin/opsense) nên dò bằng `find` thay vì đoán layout.
main_path="$(find "$tmp" -type f -name "$BIN_NAME" | head -n1)"
[ -n "$main_path" ] || { echo "archive does not contain '$BIN_NAME'" >&2; exit 1; }
install -m 0755 "$main_path" "$INSTALL_DIR/$BIN_NAME"

# Cài luôn các kernel nếu archive có. Thiếu kernel echo thì trading chạy được
# (codegraph-style script chỉ copy 1 file) nhưng `opsense` sẽ không tìm thấy
# kernel mặc định → lỗi lúc spawn.
installed="$BIN_NAME"
for k in opsense-kernel-echo opsense-kernel-python opsense-kernel-julia; do
  k_path="$(find "$tmp" -type f -name "$k" | head -n1)"
  [ -n "$k_path" ] || continue
  install -m 0755 "$k_path" "$INSTALL_DIR/$k"
  installed="$installed $k"
done

# Bỏ quarantine attribute trên macOS, nếu không Gatekeeper sẽ chặn binary tải
# về (repo không code-sign binary prebuilt nên file bị đánh dấu com.apple.quarantine).
if [ "$uname_s" = "darwin" ]; then
  for b in $installed; do
    xattr -cr "$INSTALL_DIR/$b" 2>/dev/null || true
  done
fi

echo "Installed $tag to $INSTALL_DIR: $installed"

if ! command -v "$BIN_NAME" >/dev/null 2>&1; then
  case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *) echo "Note: $INSTALL_DIR is not in PATH. Add it or move the binaries." ;;
  esac
fi

cat <<EOF

Next:
  $BIN_NAME init          # generate .opsense/config.toml
  $BIN_NAME validate      # parse config + build graph, no run
  OPSENSE_CONFIG=.opsense/config.toml $BIN_NAME serve
EOF
