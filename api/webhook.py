"""
Webhook handler — POST /api/webhook
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

            self._respond(200, {"ok": True})
        except Exception as e:
            # Always return 200 to Telegram so it doesn't retry
            self._respond(200, {"ok": True, "err": str(e)})

    def do_GET(self):
        self._respond(200, {"status": "running"})

    def _respond(self, code: int, body: dict):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, *_): pass
