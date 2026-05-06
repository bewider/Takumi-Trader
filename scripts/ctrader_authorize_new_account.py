"""Walk the user through cTrader OAuth re-authorization to grant access
to additional accounts under the same cTID (e.g. add 9984764 alongside
the existing 9895170 grant).

Flow (all automatic — just authorize in browser):
  1. Script starts a tiny HTTP server on http://localhost:8080
  2. Browser opens cTrader auth URL automatically
  3. User logs in, picks accounts (TICK 9984764), clicks Authorize
  4. cTrader redirects to http://localhost:8080/?code=XYZ
  5. Local server captures the code and shows a success page
  6. Script exchanges code → access_token + refresh_token
  7. Script verifies 9984764 appears, saves to QSettings
  8. Tells user to restart TAKUMI

Usage:
    python scripts/ctrader_authorize_new_account.py
"""
from __future__ import annotations

import http.server
import json
import socketserver
import sys
import threading
import urllib.parse
import urllib.request
import webbrowser
from urllib.parse import parse_qs, urlparse

sys.stdout.reconfigure(encoding="utf-8")

from PyQt6.QtCore import QSettings, QCoreApplication

TARGET_ACCOUNT_NUMBER = 9984764
REDIRECT_PORT = 8080
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}"


# ── Tiny HTTP server to capture the OAuth callback ────────────────────
class _AuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    captured_code: str | None = None
    captured_error: str | None = None

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        if "code" in qs:
            _AuthCallbackHandler.captured_code = qs["code"][0]
            body = (
                "<html><body style='font-family:sans-serif;padding:40px;"
                "background:#1b8a2a;color:white;text-align:center;'>"
                "<h1>✓ Authorization captured</h1>"
                "<p>You can close this window and return to the terminal.</p>"
                "</body></html>"
            )
        elif "error" in qs:
            _AuthCallbackHandler.captured_error = qs.get("error", ["?"])[0]
            body = (
                f"<html><body style='font-family:sans-serif;padding:40px;"
                f"background:#c62828;color:white;text-align:center;'>"
                f"<h1>✗ Authorization error</h1>"
                f"<p>{qs.get('error_description', ['(no description)'])[0]}</p>"
                f"</body></html>"
            )
        else:
            body = "<html><body><h1>No code received</h1></body></html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, *_args, **_kwargs):
        pass  # silence default access logs


def start_callback_server() -> tuple[socketserver.TCPServer, threading.Thread]:
    server = socketserver.TCPServer(("localhost", REDIRECT_PORT),
                                     _AuthCallbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def main():
    app = QCoreApplication.instance() or QCoreApplication(sys.argv)
    s = QSettings("TAKUMITrader", "TAKUMITrader")

    client_id = s.value("ctrader/client_id", "", type=str)
    client_secret = s.value("ctrader/client_secret", "", type=str)
    if not client_id or not client_secret:
        print("ERROR: client_id/client_secret missing from QSettings.")
        return 1

    print("=" * 78)
    print(f"  cTrader OAuth Re-Authorization (target account: {TARGET_ACCOUNT_NUMBER})")
    print("=" * 78)
    print()

    # Start local callback server
    print(f"Starting local callback server on {REDIRECT_URI} ...")
    try:
        server, _thread = start_callback_server()
    except OSError as e:
        print(f"  ✗ Failed to bind port {REDIRECT_PORT}: {e}")
        print(f"  Another process is using port {REDIRECT_PORT}. Stop it and rerun.")
        return 1
    print(f"  ✓ Listening")
    print()

    # Build authorization URL.
    # `product=web` and `prompt=consent` are added to FORCE cTrader to
    # re-display the account-selection consent screen even if the app
    # was previously authorized for this cTID. Without these, cTrader
    # may silently re-issue a token covering ONLY the previously-granted
    # accounts, missing any newly-added accounts (like 9984764).
    auth_params = urllib.parse.urlencode({
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "scope": "trading",
        "product": "web",
        "prompt": "consent",
    })
    auth_url = f"https://openapi.ctrader.com/apps/auth?{auth_params}"

    print("STEP 1: Browser will open the cTrader authorization page.")
    print(f"        URL: {auth_url}")
    print()
    print("STEP 2: On the cTrader page:")
    print("  • Log in with the SAME cTID user as before")
    print(f"  • If an account-selection screen appears, ★ TICK BOTH 9895170 AND {TARGET_ACCOUNT_NUMBER} ★")
    print("    (or just 'select all accounts')")
    print("  • Click Authorize")
    print()
    print("STEP 3: Browser will redirect to localhost — auto-captured here.")
    print("        (Page may briefly show this terminal-side success message.)")
    print()
    print("Opening browser ...")
    webbrowser.open(auth_url)
    print()

    # Wait for callback (timeout 5 min)
    print("Waiting for authorization callback (timeout 5 min) ...")
    import time
    deadline = time.time() + 300
    while time.time() < deadline:
        if _AuthCallbackHandler.captured_code:
            break
        if _AuthCallbackHandler.captured_error:
            break
        time.sleep(0.3)

    server.shutdown()

    if _AuthCallbackHandler.captured_error:
        print(f"\n  ✗ cTrader returned error: {_AuthCallbackHandler.captured_error}")
        return 1
    if not _AuthCallbackHandler.captured_code:
        print(f"\n  ✗ Timed out waiting for authorization. Re-run when ready.")
        return 1

    code = _AuthCallbackHandler.captured_code
    print(f"  ✓ Captured authorization code ({len(code)} chars)")
    print()

    # Exchange code for tokens
    print("Exchanging code for access_token + refresh_token ...")
    body = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": client_id,
        "client_secret": client_secret,
    }).encode()
    try:
        req = urllib.request.Request("https://openapi.ctrader.com/apps/token",
                                      data=body, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, timeout=15) as resp:
            tok = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"  ✗ Token exchange failed: HTTP {e.code} — {e.read().decode()}")
        return 1
    except Exception as e:
        print(f"  ✗ Network error: {e}")
        return 1

    new_access = tok.get("accessToken", "")
    new_refresh = tok.get("refreshToken", "")
    if not new_access:
        print(f"  ✗ Response missing accessToken: {tok}")
        return 1
    print(f"  ✓ Got new access_token + refresh_token (expires in {tok.get('expiresIn','?')}s)")
    print()

    # Query account list with new token
    print("Querying accounts visible with the new token ...")
    list_url = f"https://api.spotware.com/connect/tradingaccounts?access_token={new_access}"
    try:
        with urllib.request.urlopen(list_url, timeout=10) as resp:
            accounts = json.loads(resp.read().decode())
    except Exception as e:
        print(f"  ✗ Account-list query failed: {e}")
        return 1

    print(f"  Visible accounts:")
    for a in accounts.get("data", []):
        marker = "  ← TARGET" if a.get("accountNumber") == TARGET_ACCOUNT_NUMBER else ""
        bal = a.get("balance", 0) / (10 ** a.get("moneyDigits", 2))
        print(f"    - #{a.get('accountNumber'):<10}  cTID id={a.get('accountId'):<10}  "
              f"{a.get('depositCurrency','???'):<3}  bal={bal:>12,.2f}  "
              f"{'live' if a.get('live') else 'demo'}{marker}")

    target = next((a for a in accounts.get("data", [])
                   if a.get("accountNumber") == TARGET_ACCOUNT_NUMBER), None)
    if not target:
        print()
        print(f"  ╔══════════════════════════════════════════════════════════════╗")
        print(f"  ║  ✗ ABORTING — {TARGET_ACCOUNT_NUMBER} NOT in the granted accounts.        ║")
        print(f"  ╚══════════════════════════════════════════════════════════════╝")
        print()
        print(f"  This means cTrader did NOT show you an account-selection screen,")
        print(f"  OR the screen appeared but {TARGET_ACCOUNT_NUMBER} was not ticked,")
        print(f"  OR {TARGET_ACCOUNT_NUMBER} is owned by a different cTID user.")
        print()
        print(f"  REQUIRED FIX (in this exact order):")
        print(f"    1. Open https://id.ctrader.com/ in browser, log in")
        print(f"    2. Go to: Apps / Connected Applications")
        print(f"    3. Find 'TAKUMI Trader' → click Revoke / Disconnect")
        print(f"    4. Verify {TARGET_ACCOUNT_NUMBER} appears in your account list there")
        print(f"       (if not, that account is under a DIFFERENT cTID user)")
        print(f"    5. Re-run this script — cTrader will now show consent screen")
        print(f"       with both accounts; tick BOTH and approve")
        print()
        print(f"  Saving NOTHING. cTrader stays disabled until {TARGET_ACCOUNT_NUMBER} is granted.")
        # Do NOT save the token — we don't want 9895170 silently re-installed
        return 1

    print(f"\n  ✓ {TARGET_ACCOUNT_NUMBER} is accessible. cTID id = {target['accountId']}")
    print()

    # Save everything
    print("Saving credentials to QSettings ...")
    s.setValue("ctrader/access_token", new_access)
    s.setValue("ctrader/refresh_token", new_refresh)
    s.setValue("ctrader/account_id", str(target["accountId"]))
    s.setValue("ctrader/enabled", True)
    s.setValue("ctrader/auto_open", True)
    s.setValue("ctrader/auto_close", True)
    s.sync()

    print(f"  ✓ Saved.")
    print()
    print("Active TAKUMI cTrader settings:")
    for k in ["ctrader/enabled", "ctrader/auto_open", "ctrader/account_id",
              "ctrader/risk_pct", "ctrader/max_positions"]:
        print(f"    {k:<30} = {s.value(k)!r}")
    print()
    print(f"╔{'═'*60}╗")
    print(f"║  RESTART TAKUMI to connect to account {TARGET_ACCOUNT_NUMBER}  ║")
    print(f"╚{'═'*60}╝")
    return 0


if __name__ == "__main__":
    sys.exit(main())
