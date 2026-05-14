"""
Vercel API Manager Bot - Webhook Handler
Serverless function for Vercel deployment.

Note: asyncio.create_task() background tasks (auto-delete) are awaited
via the pending_tasks list so they complete before the function exits.
"""

import json
import asyncio
from http.server import BaseHTTPRequestHandler
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bot import handle_update, run_pending_tasks


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            update = json.loads(body.decode("utf-8"))

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(handle_update(update))
            # Run any pending background tasks (e.g. delayed deletes)
            loop.run_until_complete(run_pending_tasks())
            loop.close()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True}).encode())
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "error": str(e)}).encode())

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "Vercel API Manager Bot is running"}).encode())

    def log_message(self, format, *args):
        pass
