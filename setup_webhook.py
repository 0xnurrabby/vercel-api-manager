"""
Run this script to set Telegram webhook after deployment.
Usage: python setup_webhook.py <VERCEL_URL> <BOT_TOKEN>
Example: python setup_webhook.py https://vercel-api-manager.vercel.app 1234:TOKEN
"""
import sys
import urllib.request
import json

if len(sys.argv) < 3:
    print("Usage: python setup_webhook.py <VERCEL_URL> <BOT_TOKEN>")
    sys.exit(1)

vercel_url = sys.argv[1].rstrip("/")
bot_token = sys.argv[2]

webhook_url = f"{vercel_url}/api/webhook"
api_url = f"https://api.telegram.org/bot{bot_token}/setWebhook"

data = json.dumps({"url": webhook_url, "drop_pending_updates": True}).encode()
req = urllib.request.Request(api_url, data=data, headers={"Content-Type": "application/json"}, method="POST")

with urllib.request.urlopen(req) as resp:
    result = json.loads(resp.read().decode())
    print(json.dumps(result, indent=2))
