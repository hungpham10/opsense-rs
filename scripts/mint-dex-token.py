#!/usr/bin/env python3
"""Lấy Dex id_token và ghi ra `~/.config/opsense/token` — không cần trình duyệt.

Cần cho mọi thứ gọi vào gateway khi tenant bật OIDC: `opsense status`,
`opsense query`, `opsense orders`, MCP server (`opsense mcp`), REPL. Token đọc
theo thứ tự (`crates/opsense/src/client/graphql.rs:422`):

1. `OPSENSE_ACCESS_TOKEN` — thắng, kể cả khi sai (không fallback).
2. `~/.config/opsense/token` — file này tạo ra, mode 0600.

Dùng:
    ./scripts/mint-dex-token.py            # refresh nếu được, không thì login
    ./scripts/mint-dex-token.py --login    # ép đăng nhập lại, bỏ qua refresh
    ./scripts/mint-dex-token.py --print    # chỉ in ra token, không ghi file

**Refresh token**: xin scope `offline_access` nên Dex cấp kèm refresh token, lưu
ở `~/.config/opsense/refresh_token`. Mặc định script **gia hạn bằng refresh token**
khi token hiện tại sắp hết hạn (mặc định 5 phút), chỉ đăng nhập lại khi không có
refresh token hoặc nó đã hết hạn. Lý do: login lại tốn 4 request + phải qua form,
còn refresh là 1 request; và nó giữ được phiên Dex, nên restart `opsense-dex` mới
làm mất.

Khi nào cần chạy lại:
  * Token hết hạn — Dex cấp **24 giờ** (mặc định), nên khoảng một lần/ngày.
    Triệu chứng: mọi lệnh trả `401` / `Query.*` lỗi.
  * Vừa `docker compose restart` hoặc recreate `opsense-dex` (Dex lưu phiên
    trong RAM ⇒ restart là mất phiên, dù token còn hạn cũng không dùng lại
    được trên cùng flow).

Kiểm tra token còn hạn không (không cần mạng):
    python3 -c "import base64,json,time;t=open('$HOME/.config/opsense/token').read().strip();\
p=json.loads(base64.urlsafe_b64decode(t.split('.')[1]+'==='));\
print('còn', round((p['exp']-time.time())/3600,1), 'giờ')"

Yêu cầu: stack đang chạy (Dex ở `localhost:5556`, gateway ở `localhost:8080`).
Script **không** chạm S3, không đụng dữ liệu — nó chỉ đăng nhập rồi lưu token.
Override endpoint qua `OPSENSE_DEX_ISSUER` / `OPSENSE_SERVE_URL` nếu cổng khác.

Port nguyên văn flow của `crates/opsense/tests/common/dex.rs::dex_login_get_id_token`:
  1. OIDC discovery → authorization_endpoint / token_endpoint
  2. GET auth endpoint → Dex render login form (lấy action + hidden input)
  3. POST credentials → Dex approval screen (URL chứa `hmac`)
  4. POST approve → redirect về /callback?code=...
  5. Exchange code → id_token

Chỉ dùng stdlib (urllib + http.cookiejar).
"""
import base64
import http.cookiejar
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime

DEX_ISSUER = os.environ.get("OPSENSE_DEX_ISSUER", "http://localhost:5556/dex")
SERVE_URL = os.environ.get("OPSENSE_SERVE_URL", "http://localhost:8080")
CLIENT_ID = "opsense-test"
CLIENT_SECRET = "opsense-dev-shared-secret-32-bytes-min!!"
DEX_USER = "dev-user@example.com"
DEX_PASSWORD = "password"

TOKEN_PATH = os.path.expanduser("~/.config/opsense/token")
# Refresh token để riêng: `token` phải là **token thô** vì
# `load_bearer_from_env` (`crates/opsense/src/client/graphql.rs:422`) đọc thẳng
# file đó, không parse JSON.
REFRESH_PATH = os.path.expanduser("~/.config/opsense/refresh_token")
# Còn dưới ngưỡng này thì gia hạn, để không chạm lúc đang làm việc.
REFRESH_AHEAD_SECS = 300


def make_opener() -> urllib.request.OpenerDirector:
    jar = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))


def fetch(opener, url, data=None, headers=None):
    """Trả (final_url, body). HTTPError cũng trả final_url — redirect cuối
    tới `/callback?code=...` trả 500 ("no session state found") vì ta không
    đi qua `/login` của Nginx để tạo openidc session, nhưng `code` vẫn nằm
    trong URL và token endpoint vẫn đổi được (giống hệt flow Rust trong
    tests/common/dex.rs, chỉ khác ở chỗ reqwest không ném lỗi khi status 500).
    """
    body = urllib.parse.urlencode(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers or {})
    if body is not None:
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with opener.open(req, timeout=20) as resp:
            return resp.geturl(), resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.url, e.read().decode("utf-8", "replace")


def form_action(html: str):
    m = re.search(r'action="([^"]*)"', html)
    return m.group(1).replace("&amp;", "&") if m else None


def hidden_fields(html: str):
    """Hidden input của form ĐẦU TIÊN (cắt tại `</form>` đầu tiên)."""
    form = html.split("</form>", 1)[0]
    return re.findall(r'type="hidden"[^>]*?name="([^"]*)"[^>]*?value="([^"]*)"', form)


def absolute(action: str) -> str:
    if action.startswith("http"):
        return action
    parts = urllib.parse.urlsplit(DEX_ISSUER)
    return f"{parts.scheme}://{parts.netloc}{action}"


def discover(opener):
    """OIDC discovery → (auth_endpoint, token_endpoint)."""
    _, body = fetch(opener, f"{DEX_ISSUER}/.well-known/openid-configuration")
    disc = json.loads(body)
    return disc["authorization_endpoint"], disc["token_endpoint"]


class TokenError(Exception):
    """Token endpoint từ chối (HTTP lỗi, hoặc 200 mà không có `id_token`)."""


def token_call(opener, token_endpoint, data) -> dict:
    """POST token endpoint. Ném `TokenError` thay vì `SystemExit` — gọi bởi cả
    `login()` lẫn `try_refresh()`, và refresh hỏng phải rơi về login chứ không
    được giết cả script."""
    _, body = fetch(
        opener,
        token_endpoint,
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            **data,
        },
    )
    try:
        resp = json.loads(body)
    except ValueError:
        raise TokenError(f"token endpoint không trả JSON: {body[:200]}")
    if "id_token" not in resp:
        desc = resp.get("error_description") or resp.get("error") or body[:200]
        raise TokenError(str(desc))
    return resp


def login(opener, auth_endpoint, token_endpoint) -> dict:
    """Flow đầy đủ: form đăng nhập → approval → đổi code lấy token."""
    state = f"cli-state-{os.getpid()}"
    redirect_uri = f"{SERVE_URL}/callback"
    q = urllib.parse.urlencode(
        {
            "client_id": CLIENT_ID,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            # `offline_access` ⇒ Dex cấp kèm refresh token để lần sau không
            # phải qua form nữa (`scopes_supported` có nó, client cũng cho
            # `grantTypes: [refresh_token]`).
            "scope": "openid email profile offline_access",
            "state": state,
        }
    )
    _, html = fetch(opener, f"{auth_endpoint}?{q}")

    action = form_action(html)
    login_url = absolute(action) if action else f"{DEX_ISSUER}/auth/local"
    form = hidden_fields(html) + [("login", DEX_USER), ("password", DEX_PASSWORD)]
    approval_url, html = fetch(opener, login_url, data=form)
    if "code=" not in approval_url and "approval" not in html.lower():
        print(f"login failed, landing={approval_url}\n{html[:800]}", file=sys.stderr)
        return None

    action = form_action(html)
    approve_url = absolute(action) if action else approval_url
    form = hidden_fields(html)
    if not any(n == "approval" for n, _ in form):
        form.append(("approval", "approve"))
    final_url, body = fetch(opener, approve_url, data=form)

    if "code=" not in final_url:
        print(f"no code in {final_url}\n{body[:800]}", file=sys.stderr)
        return None
    code = urllib.parse.parse_qs(urllib.parse.urlsplit(final_url).query)["code"][0]
    try:
        return token_call(
            opener,
            token_endpoint,
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
            },
        )
    except TokenError as e:
        print(f"đổi code thất bại: {e}", file=sys.stderr)
        return None


def try_refresh(opener, token_endpoint) -> dict | None:
    """Gia hạn bằng refresh token. None nếu không có / không dùng được.

    Nuốt lỗi có chủ đích: refresh token hết hạn hay bị thu hồi thì đường này
    chỉ là tối ưu, còn `login()` vẫn chạy được. Không nuốt thì mỗi lần token
    hết hạn là phải nhớ xoá file.
    """
    rt = read_secret(REFRESH_PATH)
    if not rt:
        return None
    try:
        return token_call(
            opener, token_endpoint, {"grant_type": "refresh_token", "refresh_token": rt}
        )
    except (TokenError, OSError, urllib.error.URLError) as e:
        # Refresh token hết hạn / bị thu hồi / mất mạng. Đây chỉ là tối ưu,
        # nên báo rồi để `login()` thử tiếp — Dex nói rõ "has already been
        # claimed by another client", tức refresh token là **dùng một lần**.
        print(f"refresh token không dùng được ({e}) — đăng nhập lại", file=sys.stderr)
        return None


def read_secret(path: str) -> str:
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return ""


def write_secret(path: str, value: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(value)


def main() -> int:
    argv = sys.argv[1:]
    if "--help" in argv or "-h" in argv:
        print(__doc__)
        return 0

    opener = make_opener()
    auth_endpoint, token_endpoint = discover(opener)

    # Token hiện tại còn hạn và còn dư ngưỡng ⇒ không làm gì cả, kể cả network
    # call. Đây là trường hợp phổ biến nhất khi chạy script nhiều lần trong
    # ngày.
    current = read_secret(TOKEN_PATH)
    ttl = _seconds_left(current) if current else None
    if "--login" not in argv and ttl is not None and ttl > REFRESH_AHEAD_SECS:
        print(f"token còn {ttl / 3600:.1f} giờ (hết lúc {_expiry_text(current)}) — không cần làm mới")
        if "--print" in argv:
            print(current)
        return 0

    how = "login"
    resp = None
    if "--login" not in argv:
        resp = try_refresh(opener, token_endpoint)
        how = "refresh" if resp else "login"
    if resp is None:
        resp = login(opener, auth_endpoint, token_endpoint)
        if resp is None:
            return 1
    id_token = resp["id_token"]

    if "--print" in argv:
        # Chỉ in ra: để dán vào `env` của MCP client mà không đụng file.
        print(id_token)
        return 0

    write_secret(TOKEN_PATH, id_token)
    # Dex **xoay** refresh token mỗi lần dùng, nên phải lưu bản mới nhất —
    # lưu bản cũ thì lần sau nó đã bị thu hồi.
    if resp.get("refresh_token"):
        write_secret(REFRESH_PATH, resp["refresh_token"])

    # In luôn thời hạn: Dex cấp id_token **24 giờ**, nên đây là thứ quyết định
    # khi nào phải chạy lại script. Không in thì phải tự giải mã JWT mới biết.
    new_ttl = _seconds_left(id_token)
    who = _claim(id_token, "email") or "?"
    via = "refresh" if how == "refresh" else "đăng nhập"
    if new_ttl is None:
        print(f"wrote {TOKEN_PATH} ({len(id_token)} chars, mode 0600) — không đọc được hạn")
    elif new_ttl > 0:
        print(
            f"wrote {TOKEN_PATH} ({len(id_token)} chars, mode 0600) — {via}\n"
            f"  user={who}  còn {new_ttl / 3600:.1f} giờ (hết lúc {_expiry_text(id_token)})"
        )
    else:
        print(
            f"wrote {TOKEN_PATH} nhưng token **đã hết hạn** ({_expiry_text(id_token)}) — "
            f"kiểm tra lại Dex/issuer",
        )
    return 0


def _payload(token: str) -> dict:
    seg = token.split(".")[1]
    seg += "=" * (-len(seg) % 4)
    return json.loads(base64.urlsafe_b64decode(seg))


def _claim(token: str, name: str):
    try:
        return _payload(token).get(name)
    except (IndexError, ValueError, json.JSONDecodeError):
        return None


def _seconds_left(token: str):
    exp = _claim(token, "exp")
    if not isinstance(exp, (int, float)):
        return None
    return exp - time.time()


def _expiry_text(token: str) -> str:
    exp = _claim(token, "exp")
    if not isinstance(exp, (int, float)):
        return "?"
    return datetime.fromtimestamp(exp).strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    sys.exit(main())
