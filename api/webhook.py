"""
Webhook handler — receives Telegram updates via POST /api/webhook
"""
import json, asyncio, sys, os
from http.server import BaseHTTPRequestHandler

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bot import handle_update


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            update = json.loads(self.rfile.read(length))

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(handle_update(update))
            loop.close()

            self._ok({"ok": True})
        except Exception as e:
            import traceback
            self._ok({"ok": True, "internal_error": str(e), "trace": traceback.format_exc()})

    def do_GET(self):
        # Show allowed users for debugging (masked)
        allowed_raw = os.environ.get("ALLOWED_TELEGRAM_USERS", "NOT SET")
        self._ok({
            "status": "Vercel API Manager Bot running",
            "allowed_users_set": bool(allowed_raw and allowed_raw != "NOT SET"),
            "kv_url_set": bool(os.environ.get("KV_REST_API_URL") or os.environ.get("UPSTASH_REDIS_REST_URL")),
            "kv_token_set": bool(os.environ.get("KV_REST_API_TOKEN") or os.environ.get("UPSTASH_REDIS_REST_TOKEN")),
        })

    def _ok(self, body: dict):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def _err(self, msg: str):
        self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": False, "error": msg}).encode())

    def log_message(self, *_): pass
