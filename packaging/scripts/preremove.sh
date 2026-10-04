#!/bin/sh
# Maintainer script — chạy **trước** khi dpkg gỡ file.
#
# `preremove` (không phải `postremove`) vì dpkg sẽ bỏ symlink trong
# sites-enabled và unit khỏi /lib/systemd/system ngay sau đó; còn `postremove`
# thì đã muộn để reload nginx với config đúng.
set -u

# `debian` = gỡ hẳn package; `upgrade` = thay bản mới. Khác nhau ở chỗ có nên
# dừng service không: upgrade sẽ `try-restart` ở postinstall, nên dừng ở đây
# là chuẩn — nhưng phải bỏ qua để không cắt request trong lúc dpkg chưa xong.
if [ "${1:-}" = "remove" ] || [ "${1:-}" = "deconfigure" ]; then
    systemctl stop opsense.service >/dev/null 2>&1 || true
fi

exit 0
