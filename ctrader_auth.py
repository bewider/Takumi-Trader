"""cTrader OAuth2 Authorization Helper.

Run this ONCE to get your access token. It will:
1. Open a browser for you to authorize
2. You paste the redirect URL back
3. It exchanges for an access token and saves to ctrader_config.json

Usage: python ctrader_auth.py
"""

import json
import webbrowser
import urllib.request
import urllib.parse
from pathlib import Path

CONFIG_FILE = Path(__file__).parent / "data" / "ctrader_config.json"

def main():
    # Load config
    config = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    client_id = config["client_id"]
    client_secret = config["client_secret"]

    if client_id == "PASTE_YOUR_CLIENT_ID_HERE":
        print("ERROR: Please paste your Client ID and Secret into data/ctrader_config.json first!")
        return

    # Step 1: Open browser for authorization
    redirect_uri = "http://localhost:8080"
    auth_url = (
        f"https://connect.spotware.com/apps/auth"
        f"?client_id={client_id}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        f"&scope=trading"
        f"&response_type=code"
    )

    print("=" * 60)
    print("cTrader OAuth2 Authorization")
    print("=" * 60)
    print()
    print("Opening browser for authorization...")
    print("If it doesn't open, go to this URL manually:")
    print()
    print(auth_url)
    print()

    webbrowser.open(auth_url)

    print("After authorizing, you'll be redirected to a URL like:")
    print("  https://openapi.ctrader.com/apps/auth?code=XXXXXXXXXXXX")
    print()
    code = input("Paste the FULL redirect URL here: ").strip()

    # Extract code from URL
    if "code=" in code:
        code = code.split("code=")[1].split("&")[0]

    print(f"\nAuthorization code: {code[:10]}...")

    # Step 2: Exchange code for access token
    token_url = "https://openapi.ctrader.com/apps/token"
    data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
    }).encode("utf-8")

    req = urllib.request.Request(token_url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        resp = urllib.request.urlopen(req, timeout=30)
        result = json.loads(resp.read().decode("utf-8"))

        access_token = result.get("accessToken", result.get("access_token", ""))
        refresh_token = result.get("refreshToken", result.get("refresh_token", ""))

        print(f"\nAccess Token: {access_token[:20]}...")
        print(f"Refresh Token: {refresh_token[:20]}...")

        # Save to config
        config["access_token"] = access_token
        config["refresh_token"] = refresh_token
        CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")

        print(f"\nSaved to {CONFIG_FILE}")
        print("\nNow you need your Account ID.")
        print("Check cTrader — it's the number shown in your account list.")
        account_id = input("Enter your demo Account ID: ").strip()
        if account_id:
            config["account_id"] = account_id
            CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")
            print(f"Account ID saved: {account_id}")

        print("\n✅ Authorization complete! You can now enable live trading.")
        print('Set "enabled": true in ctrader_config.json when ready.')

    except Exception as e:
        print(f"\nERROR: {e}")
        print("Check your Client ID and Secret in ctrader_config.json")


if __name__ == "__main__":
    main()
