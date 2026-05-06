"""Get cTrader Open API production access token.

Usage: python scripts/get_ctrader_token.py

Opens your browser for authorization, catches the redirect,
exchanges the code for an access token, and prints it.
"""

import http.server
import threading
import webbrowser
import urllib.parse
import json
import urllib.request
import sys

# ── Your app credentials ──
CLIENT_ID = input("Enter Client ID: ").strip()
CLIENT_SECRET = input("Enter Client Secret: ").strip()
REDIRECT_URI = "http://localhost:8080"
SCOPE = "trading"

# ── Step 1: Open browser for user authorization ──
auth_url = (
    f"https://id.ctrader.com/my/settings/openapi/grantingaccess/"
    f"?client_id={CLIENT_ID}"
    f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
    f"&scope={SCOPE}"
    f"&product=web"
)

auth_code = None


class CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        auth_code = params.get("code", [None])[0]

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        if auth_code:
            self.wfile.write(b"<h1>Authorization successful!</h1><p>You can close this tab.</p>")
        else:
            self.wfile.write(b"<h1>Error: no code received</h1>")

    def log_message(self, *args):
        pass  # Suppress server logs


# Start local server to catch the redirect
server = http.server.HTTPServer(("localhost", 8080), CallbackHandler)
server_thread = threading.Thread(target=server.handle_request, daemon=True)
server_thread.start()

print(f"\nOpening browser for authorization...")
print(f"If browser doesn't open, go to:\n{auth_url}\n")
webbrowser.open(auth_url)

print("Waiting for authorization (click 'Allow' in the browser)...")
server_thread.join(timeout=120)
server.server_close()

if not auth_code:
    print("ERROR: No authorization code received. Timed out or denied.")
    sys.exit(1)

print(f"\nAuthorization code received!")

# ── Step 2: Exchange code for access token ──
token_url = (
    f"https://openapi.ctrader.com/apps/token"
    f"?grant_type=authorization_code"
    f"&code={auth_code}"
    f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
    f"&client_id={CLIENT_ID}"
    f"&client_secret={CLIENT_SECRET}"
)

try:
    req = urllib.request.Request(token_url, method="GET")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())

    access_token = data.get("accessToken", "")
    refresh_token = data.get("refreshToken", "")
    expires_in = data.get("expiresIn", 0)

    print(f"\n{'='*60}")
    print(f"ACCESS TOKEN:  {access_token}")
    print(f"REFRESH TOKEN: {refresh_token}")
    print(f"EXPIRES IN:    {expires_in} seconds ({expires_in//86400} days)")
    print(f"{'='*60}")
    print(f"\nCopy the ACCESS TOKEN into TAKUMI Settings > cTrader > Access Token")

except Exception as e:
    print(f"\nERROR exchanging code for token: {e}")
    sys.exit(1)
