"""
Vercel API Manager Bot
Telegram bot to manage Vercel AI Gateway API keys.

Architecture:
- Storage : Upstash Redis via REST API (serverless-safe, no persistent TCP)
- Encryption: Per-user AES-256 (Fernet) keys derived via HMAC-SHA256
- Access   : Username allowlist via ALLOWED_TELEGRAM_USERS env var
- Auto-del : Deferred deletes stored in Redis queue, executed by run_pending_tasks()
"""

from __future__ import annotations
import os
import json
import asyncio
import hashlib
import hmac
import base64
import time
from typing import Optional, List, Dict, Any

import httpx
from cryptography.fernet import Fernet


# ───────────────────────────────────────────────────────
# Config
# ───────────────────────────────────────────────────────
BOT_TOKEN       = os.environ["TELEGRAM_BOT_TOKEN"]
MASTER_KEY      = os.environ["MASTER_ENCRYPTION_KEY"]
ALLOWED_RAW     = os.environ.get("ALLOWED_TELEGRAM_USERS", "")
ALLOWED_USERS   = {u.strip().lower().lstrip("@") for u in ALLOWED_RAW.split(",") if u.strip()}

# Upstash / Vercel KV REST (works in serverless — no persistent socket)
KV_URL   = (os.environ.get("UPSTASH_REDIS_REST_URL") or os.environ.get("KV_REST_API_URL", "")).rstrip("/")
KV_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN") or os.environ.get("KV_REST_API_TOKEN", "")

TG_API       = f"https://api.telegram.org/bot{BOT_TOKEN}"
GATEWAY_URL  = "https://ai-gateway.vercel.sh/v1"

AUTO_DEL_SEC = 30   # seconds before auto-deleting output messages
ADD_DEL_SEC  = 3    # seconds before deleting "done" confirmation after add


# ───────────────────────────────────────────────────────
# Upstash Redis REST helpers  (GET /COMMAND/arg1/arg2…)
# ───────────────────────────────────────────────────────

async def _kv(command: list[str]) -> Any:
    """Execute a Redis command via Upstash REST API."""
    url = KV_URL + "/" + "/".join(
        # URL-encode each part so values with special chars are safe
        str(c).replace("/", "%2F").replace(" ", "%20")
        for c in command
    )
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, headers={"Authorization": f"Bearer {KV_TOKEN}"})
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"KV error: {data['error']}")
    return data.get("result")


async def kv_get(key: str) -> Optional[str]:
    return await _kv(["GET", key])

async def kv_set(key: str, val: str) -> None:
    await _kv(["SET", key, val])

async def kv_setex(key: str, ttl: int, val: str) -> None:
    await _kv(["SETEX", key, str(ttl), val])

async def kv_del(key: str) -> None:
    await _kv(["DEL", key])

async def kv_lpush(key: str, val: str) -> None:
    await _kv(["LPUSH", key, val])

async def kv_lrange(key: str, start: int, stop: int) -> list:
    result = await _kv(["LRANGE", key, str(start), str(stop)])
    return result or []

async def kv_delete(key: str) -> None:
    await _kv(["DEL", key])


# ───────────────────────────────────────────────────────
# Per-user encryption  (HMAC-derived Fernet key)
# ───────────────────────────────────────────────────────

def _user_fernet(username: str) -> Fernet:
    derived = hmac.new(
        MASTER_KEY.encode(),
        username.lower().encode(),
        hashlib.sha256,
    ).digest()
    return Fernet(base64.urlsafe_b64encode(derived))


def enc(username: str, plaintext: str) -> str:
    return _user_fernet(username).encrypt(plaintext.encode()).decode()

def dec(username: str, ciphertext: str) -> str:
    return _user_fernet(username).decrypt(ciphertext.encode()).decode()


# ───────────────────────────────────────────────────────
# Redis key helpers  (user-isolated, hash-based)
# ───────────────────────────────────────────────────────

def _uh(username: str) -> str:
    """Short user hash for Redis key namespacing."""
    return hashlib.sha256(username.lower().encode()).hexdigest()[:16]

def _kkeys(u: str)  -> str: return f"vam:keys:{_uh(u)}"
def _kstate(u: str) -> str: return f"vam:state:{_uh(u)}"
def _kdq(u: str)    -> str: return f"vam:delq:{_uh(u)}"   # delete queue


# ───────────────────────────────────────────────────────
# API key storage
# ───────────────────────────────────────────────────────

async def get_keys(username: str) -> List[str]:
    raw = await kv_get(_kkeys(username))
    if not raw:
        return []
    return [dec(username, e) for e in json.loads(raw)]

async def save_keys(username: str, keys: List[str]) -> None:
    await kv_set(_kkeys(username), json.dumps([enc(username, k) for k in keys]))

async def add_key(username: str, key: str) -> bool:
    keys = await get_keys(username)
    if key in keys:
        return False
    keys.append(key)
    await save_keys(username, keys)
    return True

async def remove_key(username: str, key: str) -> bool:
    keys = await get_keys(username)
    if key not in keys:
        return False
    keys.remove(key)
    await save_keys(username, keys)
    return True


# ───────────────────────────────────────────────────────
# State machine
# ───────────────────────────────────────────────────────

async def set_state(username: str, state: str) -> None:
    await kv_setex(_kstate(username), 300, state)

async def get_state(username: str) -> Optional[str]:
    return await kv_get(_kstate(username))

async def clear_state(username: str) -> None:
    await kv_del(_kstate(username))


# ───────────────────────────────────────────────────────
# Delete queue  (schedule messages for later deletion)
# Stored as JSON: {chat_id, message_id, delete_at}
# ───────────────────────────────────────────────────────

async def schedule_delete(username: str, chat_id: int, message_ids: List[int], delay: int) -> None:
    """Push deletion tasks into Redis for run_pending_tasks() to execute."""
    delete_at = int(time.time()) + delay
    for mid in message_ids:
        entry = json.dumps({"chat_id": chat_id, "message_id": mid, "delete_at": delete_at})
        await kv_lpush(_kdq(username), entry)


# Global delete queue used within a single request (non-serverless tasks).
_pending_deletes: List[Dict] = []

def queue_delete(chat_id: int, message_ids: List[int], delay: int) -> None:
    """Queue in-process deletion (for background within same request)."""
    delete_at = time.time() + delay
    for mid in message_ids:
        _pending_deletes.append({"chat_id": chat_id, "message_id": mid, "delete_at": delete_at})


async def run_pending_tasks() -> None:
    """
    Called by webhook handler after handle_update() completes.
    Executes all queued deletions (waits for their delay).
    """
    if not _pending_deletes:
        return

    # Sort by delete_at so shortest waits go first
    _pending_deletes.sort(key=lambda x: x["delete_at"])

    async def _delete_one(item: Dict) -> None:
        now = time.time()
        wait = max(0.0, item["delete_at"] - now)
        if wait > 0:
            await asyncio.sleep(wait)
        await tg("deleteMessage", {"chat_id": item["chat_id"], "message_id": item["message_id"]})

    await asyncio.gather(*[_delete_one(item) for item in _pending_deletes])
    _pending_deletes.clear()


# ───────────────────────────────────────────────────────
# Vercel AI Gateway — balance check
# ───────────────────────────────────────────────────────

async def check_balance(api_key: str) -> Optional[Dict[str, float]]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{GATEWAY_URL}/credits",
                headers={"Authorization": f"Bearer {api_key}"},
            )
        if resp.status_code == 200:
            d = resp.json()
            return {"balance": float(d.get("balance", 0)), "total_used": float(d.get("total_used", 0))}
    except Exception:
        pass
    return None


async def best_key(username: str) -> Optional[Dict]:
    """Key with highest balance."""
    keys = await get_keys(username)
    if not keys:
        return None
    bals = await asyncio.gather(*[check_balance(k) for k in keys])
    results = [
        {"key": k, "balance": b["balance"], "total_used": b["total_used"]}
        for k, b in zip(keys, bals) if b is not None
    ]
    if not results:
        return None
    results.sort(key=lambda x: x["balance"], reverse=True)
    return results[0]


async def all_balances(username: str) -> List[Dict]:
    keys = await get_keys(username)
    if not keys:
        return []
    bals = await asyncio.gather(*[check_balance(k) for k in keys])
    out = []
    for k, b in zip(keys, bals):
        out.append({
            "key": k,
            "masked": f"{k[:8]}...{k[-6:]}",
            "balance": b["balance"] if b else None,
            "total_used": b["total_used"] if b else None,
        })
    out.sort(key=lambda x: (x["balance"] or -1), reverse=True)
    return out


# ───────────────────────────────────────────────────────
# Telegram API helpers
# ───────────────────────────────────────────────────────

async def tg(method: str, data: dict) -> dict:
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(f"{TG_API}/{method}", json=data)
    return r.json()


async def send(chat_id: int, text: str, markup=None, parse_mode="Markdown") -> Optional[int]:
    payload: dict = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if markup:
        payload["reply_markup"] = markup
    r = await tg("sendMessage", payload)
    if r.get("ok"):
        return r["result"]["message_id"]
    return None


async def answer_cb(cq_id: str, text: str = "") -> None:
    await tg("answerCallbackQuery", {"callback_query_id": cq_id, "text": text})


# ───────────────────────────────────────────────────────
# Access control
# ───────────────────────────────────────────────────────

def allowed(username: Optional[str]) -> bool:
    if not username:
        return False
    return username.lower().lstrip("@") in ALLOWED_USERS


# ───────────────────────────────────────────────────────
# Main keyboard
# ───────────────────────────────────────────────────────

def main_kb() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "➕ Add Key",    "callback_data": "add"},
                {"text": "📊 Total APIs", "callback_data": "total"},
            ],
            [
                {"text": "🗑 Remove Key",  "callback_data": "remove"},
                {"text": "🔑 Get Best Key","callback_data": "get"},
            ],
            [
                {"text": "📈 Dashboard",  "callback_data": "dashboard"},
            ],
        ]
    }


# ───────────────────────────────────────────────────────
# Button handlers
# ───────────────────────────────────────────────────────

async def on_start(chat_id: int, username: str) -> None:
    await clear_state(username)
    await send(
        chat_id,
        "👋 *Vercel API Manager*\n\n"
        "Securely manage your Vercel AI Gateway API keys.\n"
        "All keys are encrypted — only you can access yours.\n\n"
        "Choose an action:",
        markup=main_kb(),
    )


async def on_add(chat_id: int, username: str, cq_id: str) -> None:
    await answer_cb(cq_id)
    await set_state(username, "awaiting_add")
    prompt_id = await send(
        chat_id,
        "🔑 *Add API Key*\n\n"
        "Send your Vercel AI Gateway API key now.\n"
        "Format: `vck_...`\n\n"
        "_Message will be deleted right after saving._",
    )
    if prompt_id:
        queue_delete(chat_id, [prompt_id], delay=60)


async def on_total(chat_id: int, username: str, cq_id: str) -> None:
    await answer_cb(cq_id)
    keys = await get_keys(username)
    n = len(keys)
    msg_id = await send(
        chat_id,
        f"📊 *Total API Keys*\n\n"
        f"You have *{n}* API key{'s' if n != 1 else ''} stored.\n\n"
        f"_Auto-deletes in {AUTO_DEL_SEC}s._",
    )
    if msg_id:
        queue_delete(chat_id, [msg_id], delay=AUTO_DEL_SEC)


async def on_remove(chat_id: int, username: str, cq_id: str) -> None:
    await answer_cb(cq_id)
    await set_state(username, "awaiting_remove")
    prompt_id = await send(
        chat_id,
        "🗑 *Remove API Key*\n\n"
        "Send the full API key you want to delete.\n\n"
        "_Prompt auto-deletes in 60s if no response._",
    )
    if prompt_id:
        queue_delete(chat_id, [prompt_id], delay=60)


async def on_get(chat_id: int, username: str, cq_id: str) -> None:
    await answer_cb(cq_id, text="Checking balances…")
    loading_id = await send(chat_id, "⏳ Finding best key by balance…")

    bk = await best_key(username)
    to_del = [loading_id] if loading_id else []

    if bk is None:
        result_id = await send(
            chat_id,
            "❌ No keys available or balance fetch failed.\n"
            "Add keys using the *➕ Add Key* button.",
        )
    else:
        result_id = await send(
            chat_id,
            f"🔑 *Best Key (Highest Balance)*\n\n"
            f"`{bk['key']}`\n\n"
            f"💰 Balance : *${bk['balance']:.4f}*\n"
            f"📉 Used    : *${bk['total_used']:.4f}*\n\n"
            f"_Auto-deletes in {AUTO_DEL_SEC}s._",
        )
    if result_id:
        to_del.append(result_id)
    queue_delete(chat_id, to_del, delay=AUTO_DEL_SEC)


async def on_dashboard(chat_id: int, username: str, cq_id: str) -> None:
    await answer_cb(cq_id, text="Loading dashboard…")
    loading_id = await send(chat_id, "⏳ Building dashboard…")

    data = await all_balances(username)
    to_del = [loading_id] if loading_id else []

    if not data:
        result_id = await send(
            chat_id,
            "📈 *Dashboard*\n\nNo API keys found.\n"
            "Use *➕ Add Key* to get started.",
        )
    else:
        total_bal  = sum(d["balance"] or 0 for d in data)
        total_used = sum(d["total_used"] or 0 for d in data)
        reachable  = sum(1 for d in data if d["balance"] is not None)
        n          = len(data)

        lines = [
            "📈 *Vercel API Manager — Dashboard*", "",
            f"👤 User       : @{username}",
            f"🔢 Total Keys : *{n}*",
            f"✅ Reachable  : *{reachable}/{n}*",
            f"💰 Total Bal  : *${total_bal:.4f}*",
            f"📉 Total Used : *${total_used:.4f}*", "",
            "━━━━━━━━━━━━━━━━━━━━━━━",
            "*Keys — sorted by balance (high→low)*", "",
        ]
        for i, d in enumerate(data, 1):
            icon = "🟢" if d["balance"] is not None else "🔴"
            bal  = f"${d['balance']:.4f}"  if d["balance"]    is not None else "N/A"
            used = f"${d['total_used']:.4f}" if d["total_used"] is not None else "N/A"
            lines += [
                f"{icon} *#{i}* `{d['masked']}`",
                f"   Balance: {bal}  |  Used: {used}", "",
            ]
        lines.append(f"_Auto-deletes in {AUTO_DEL_SEC}s._")
        result_id = await send(chat_id, "\n".join(lines))

    if result_id:
        to_del.append(result_id)
    queue_delete(chat_id, to_del, delay=AUTO_DEL_SEC)


# ───────────────────────────────────────────────────────
# Text input handler (state machine)
# ───────────────────────────────────────────────────────

async def on_text(chat_id: int, username: str, text: str, user_msg_id: int) -> None:
    state = await get_state(username)
    if not state:
        return

    await clear_state(username)

    if state == "awaiting_add":
        api_key = text.strip()
        if not (api_key.startswith("vck_") and len(api_key) >= 20):
            err_id = await send(
                chat_id,
                "❌ Invalid format.\n"
                "Vercel AI Gateway keys start with `vck_` and are 60+ chars.",
            )
            queue_delete(chat_id, [user_msg_id] + ([err_id] if err_id else []), delay=ADD_DEL_SEC)
            return

        added = await add_key(username, api_key)
        msg = "✅ *Done!* API key saved and encrypted." if added else "⚠️ Key already exists."
        done_id = await send(chat_id, msg)
        queue_delete(chat_id, [user_msg_id] + ([done_id] if done_id else []), delay=ADD_DEL_SEC)

    elif state == "awaiting_remove":
        api_key = text.strip()
        removed = await remove_key(username, api_key)
        msg = "✅ *Done!* API key removed." if removed else "❌ Key not found in your vault."
        done_id = await send(chat_id, msg)
        queue_delete(chat_id, [user_msg_id] + ([done_id] if done_id else []), delay=AUTO_DEL_SEC)


# ───────────────────────────────────────────────────────
# Main dispatcher
# ───────────────────────────────────────────────────────

BUTTON_MAP = {
    "add":       on_add,
    "total":     on_total,
    "remove":    on_remove,
    "get":       on_get,
    "dashboard": on_dashboard,
}


async def handle_update(update: dict) -> None:
    # ── Callback query ──────────────────────────────
    if "callback_query" in update:
        cq       = update["callback_query"]
        chat_id  = cq["message"]["chat"]["id"]
        cq_id    = cq["id"]
        username = cq["from"].get("username", "")

        if not allowed(username):
            await answer_cb(cq_id, text="Access denied.")
            return

        fn = BUTTON_MAP.get(cq.get("data", ""))
        if fn:
            await fn(chat_id, username, cq_id)
        else:
            await answer_cb(cq_id)
        return

    # ── Text message ────────────────────────────────
    if "message" in update:
        msg      = update["message"]
        chat_id  = msg["chat"]["id"]
        msg_id   = msg["message_id"]
        username = msg.get("from", {}).get("username", "")
        text     = msg.get("text", "")

        if not allowed(username):
            return  # silent

        if text.startswith("/start"):
            await on_start(chat_id, username)
        elif text:
            await on_text(chat_id, username, text, msg_id)
