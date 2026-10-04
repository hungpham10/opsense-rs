#!/bin/sh
# Maintainer script — chạy **sau** khi dpkg gỡ file.
#
# Dọn những thứ dpkg không tự dọn: symlink do postinstall tạo, và enable của
# systemd. Không xoá `/etc/opsense/` hay `/var/lib/opsense` — đó là dữ liệu
# của admin, và `--purge` cũng cố ý giữ (xem `dpkg -L`).
set -u

systemctl disable opsense.service >/dev/null 2>&1 || true
systemctl daemon-reload >/dev/null 2>&1 || true

# Chỉ gỡ symlink nếu nó **trỏ đúng** file của package. Nếu admin đã thay bằng
# symlink riêng thì xoá là mất cấu hình họ chủ động cài.
if [ -L /etc/nginx/sites-enabled/opsense.conf ] \
   && [ "$(readlink /etc/nginx/sites-enabled/opsense.conf)" = "../sites-available/opsense.conf" ]; then
    rm -f /etc/nginx/sites-enabled/opsense.conf
fi

# Reload nginx **chỉ khi** còn chạy và config vẫn hợp lệ — reload lúc nginx
# chưa start thì lỗi, mà lỗi ở đây chỉ in cảnh báo.
if systemctl is-active --quiet nginx && nginx -t >/dev/null 2>&1; then
    systemctl reload nginx >/dev/null 2>&1 || true
fi

# Không xoá user `opsense`: user dùng chung có thể đang giữ file trong
# /var/lib/opsense. Xoá user mà file còn thuộc nó ⇒ `apt purge` sau đó rất
# khó dọn nốt. Muốn dọn hoàn toàn: `userdel opsense` sau khi tự xoá state.
exit 0
