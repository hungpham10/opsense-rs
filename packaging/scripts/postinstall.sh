#!/bin/sh
# Maintainer script — chạy **sau** khi dpkg giải nén file vào hệ thống.
#
# Ba việc, đúng thứ tự:
#   1. tạo user `opsense` + sở hữu state dir
#   2. đăng ký + khởi động systemd service
#   3. bật site nginx rồi reload
#
# **Không** `set -e`: nếu `nginx -t` fail (admin đã sửa nginx.conf sai trước đó)
# thì cả package sẽ ở trạng thái `unconfigured` và mọi `apt install` sau đó
# đều vướng. Sai cấu hình nginx là việc của admin, không phải lý do để hỏng
# việc cài app.
set -u

log() { echo "opsense: $*" >&2; }

# ── 1. User + state dir ─────────────────────────────────────────────────
# `getent` thay vì `id -u`: `id -u opsense` in "id: 'opsense': no such user"
# ra **stderr** — mà stderr của postinst thì apt hiển thị cho user, nên lần cài
# đầu sẽ hiện một dòng cảnh báo đỏ trong khi đó là chuyện bình thường.
# `getent` không in gì khi không tìm thấy, chỉ trả exit ≠ 0.
if ! getent group opsense >/dev/null 2>&1; then
    groupadd --system opsense || log "WARN: không tạo được group opsense"
fi
if ! getent passwd opsense >/dev/null 2>&1; then
    useradd --system --gid opsense --home-dir /var/lib/opsense \
            --shell /usr/sbin/nologin --comment "opsense service" opsense \
        || log "WARN: không tạo được user opsense"
fi

# `install -d -o/-g` một lệnh làm cả mkdir + chown + chmod, và **không** đổi
# chown nếu thư mục đã tồn tại với owner khác (đây là state, không phải code).
install -d -o opsense -g opsense -m 0700 /var/lib/opsense \
    || log "WARN: không set owner cho /var/lib/opsense (user opsense có tồn tại không?)"

# ── 2. systemd ──────────────────────────────────────────────────────────
systemctl daemon-reload || log "WARN: daemon-reload lỗi"

# `enable` **không** chạy được trong container build (không có systemd) — đừng
# để nó giết cả postinst.
systemctl enable opsense.service >/dev/null 2>&1 \
    || log "WARN: không enable được opsense.service (môi trường không có systemd?)"

# `restart` chứ không phải `start`: khi **upgrade** thì service đang chạy sẽ
# nhận binary mới. `try-restart` là đúng nghĩa hơn — không start nếu nó vốn
# không chạy (trường hợp admin cố tình stop).
systemctl try-restart opsense.service >/dev/null 2>&1 \
    || systemctl start opsense.service >/dev/null 2>&1 \
    || log "WARN: không khởi động được opsense.service — xem `journalctl -u opsense`"

# ── 3. nginx ────────────────────────────────────────────────────────────
SITE_AVAIL=/etc/nginx/sites-available/opsense.conf
SITE_ENABLED=/etc/nginx/sites-enabled/opsense.conf

if [ -f "$SITE_AVAIL" ] && [ -d /etc/nginx/sites-enabled ]; then
    # Site mặc định của Ubuntu chiếm `default_server` cùng port 80 ⇒ nginx -t
    # fail với "duplicate default server". Bỏ **symlink**, không xoá file:
    # `apt remove nginx`/`--purge` còn dọn được, và admin vẫn xem lại được.
    if [ -L /etc/nginx/sites-enabled/default ]; then
        rm -f /etc/nginx/sites-enabled/default
        log "Đã tắt site mặc định của Ubuntu (sites-enabled/default)"
    fi

    # `-f` để replace: nếu symlink cũ trỏ chỗ khác thì `ln -s` không ghi đè
    # mà tạo thêm `/etc/nginx/sites-enabled/opsense.conf/opsense.conf`.
    ln -sf "../sites-available/opsense.conf" "$SITE_ENABLED"

    if nginx -t >/dev/null 2>&1; then
        # `reload` giữ connection đang mở; `restart` chỉ khi nginx chưa chạy
        # (lần cài đầu, hoặc sau reboot chưa start).
        if systemctl is-active --quiet nginx; then
            systemctl reload nginx || log "WARN: reload nginx lỗi"
        else
            systemctl enable --now nginx >/dev/null 2>&1 \
                || log "WARN: không start được nginx"
        fi
    else
        # In ra **nguyên** lý do: "nginx config sai" mà không kèm dòng nào thì
        # admin phải tự đoán.
        log "nginx -t FAIL — opsense đã cài nhưng chưa được proxy. Chi tiết:"
        nginx -t >&2 || true
        log "Sửa xong thì: systemctl reload nginx"
    fi
else
    # Thiếu chính site config thì symlink cũng nghĩa là trỏ vào hư không, còn
    # thiếu layout thì nginx không nạp file nào.
    log "WARN: không thấy $SITE_AVAIL hoặc sites-enabled — bỏ qua cấu hình nginx"
    log "Nếu nginx của bạn không dùng layout Ubuntu, proxy thủ công tới /run/axum."
fi

exit 0
