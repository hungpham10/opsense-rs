#!/usr/bin/env python3
"""Lấy Dex id_token và ghi ra `~/.config/opsense/token` — không cần trình duyệt.

Cần cho mọi thứ gọi vào gateway khi tenant bật OIDC: `opsense status`,
`opsense query`, `opsense orders`, MCP server (`opsense mcp`), REPL. Token đọc
theo thứ tự (`crates/opsense/src/client/graphql.rs:422`):

1. `OPSENSE_ACCESS_TOKEN` — thắng, kể cả khi sai (không fallback).
2. `~/.config/opsense/token` — file này tạo ra, mode 0600.

Dùng:
    ./scripts/mint-dex-token.py            # lấy token, ghi đè file
    ./scripts/mint-dex-token.py --print    # chỉ in ra, không ghi file

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


def main() -> int:
    opener = make_opener()
    _, body = fetch(
        opener, f"{DEX_ISSUER}/.well-known/openid-configuration"
    )
    disc = json.loads(body)
    auth_endpoint = disc["authorization_endpoint"]
    token_endpoint = disc["token_endpoint"]

    state = f"cli-state-{os.getpid()}"
    redirect_uri = f"{SERVE_URL}/callback"
    q = urllib.parse.urlencode(
        {
            "client_id": CLIENT_ID,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": "openid email profile",
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
        return 1

    action = form_action(html)
    approve_url = absolute(action) if action else approval_url
    form = hidden_fields(html)
    if not any(n == "approval" for n, _ in form):
        form.append(("approval", "approve"))
    final_url, body = fetch(opener, approve_url, data=form)

    if "code=" not in final_url:
        print(f"no code in {final_url}\n{body[:800]}", file=sys.stderr)
        return 1
    code = urllib.parse.parse_qs(urllib.parse.urlsplit(final_url).query)["code"][0]

    _, body = fetch(
        opener,
        token_endpoint,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": redirect_uri,
        },
    )
    id_token = json.loads(body)["id_token"]

    if "--print" in sys.argv[1:]:
        # Chỉ in ra: để dán vào `env` của MCP client mà không đụng file.
        print(id_token)
        return 0

    os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
    fd = os.open(TOKEN_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(id_token)

    # In luôn thời hạn: Dex cấp id_token **24 giờ**, nên đây là thứ quyết định
    # khi nào phải chạy lại script. Không in thì phải tự giải mã JWT mới biết.
    ttl = _seconds_left(id_token)
    who = _claim(id_token, "email") or "?"
    if ttl is None:
        print(f"wrote {TOKEN_PATH} ({len(id_token)} chars, mode 0600) — không đọc được hạn")
    elif ttl > 0:
        print(
            f"wrote {TOKEN_PATH} ({len(id_token)} chars, mode 0600)\n"
            f"  user={who}  còn {ttl / 3600:.1f} giờ (hết lúc {_expiry_text(id_token)})"
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
