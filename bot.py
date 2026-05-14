"""
Vercel API Manager Bot
A Telegram bot to manage Vercel AI Gateway API keys
with per-user encryption and access control.

Storage: Upstash Redis (REST API) — serverless-friendly, no persistent connection.
Security: Per-user AES-256 encryption via HMAC-derived keys (Fernet).
Access: Allowlist via ALLOWED_TELEGRAM_USERS env var.
"""

import os
import json
import asyncio
import hashlib
import hmac
import base64
import httpx
from typing import Optional, List, Dict, Any
from cryptography.fernet import Fernet


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_USERS_RAW = os.environ.get("ALLOWED_TELEGRAM_USERS", "")
ALLOWED_USERS = {u.strip().lower().lstrip("@") for u in ALLOWED_USERS_RAW.split(",") if u.strip()}

MASTER_ENCRYPTION_KEY = os.environ["MASTER_ENCRYPTION_KEY"]

# Upstash / Vercel KV REST API (serverless-compatible)
# Supports both Vercel KV naming and Upstash naming conventions
UPSTASH_REDIS_REST_URL = (
    os.environ.get("UPSTASH_REDIS_REST_URL") or
    os.environ.get("KV_REST_API_URL") or
    ""
)
UPSTASH_REDIS_REST_TOKEN = (
    os.environ.get("UPSTASH_REDIS_REST_TOKEN") or
    os.environ.get("KV_REST_API_TOKEN") or
    ""
)

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
VERCEL_GATEWAY = "https://ai-gateway.vercel.sh/v1"

AUTO_DELETE_SECONDS = 30


# ─────────────────────────────────────────────
# Upstash Redis REST Client (no persistent conn)
# ─────────────────────────────────────────────

async def _upstash(command: list) -> Any:
    """Execute a Redis command via Upstash REST API."""
    url = f"{UPSTASH_REDIS_REST_URL.rstrip('/')}/{'/'.join(str(c) for c in command)}"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            url,
            headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"},
        )
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"Redis error: {data['error']}")
        return data.get("result")


async def kv_get(key: str) -> Optional[str]:
    result = await _upstash(["GET", key])
    return result


async def kv_set(key: str, value: str) -> None:
    await _upstash(["SET", key, value])


async def kv_setex(key: str, ttl: int, value: str) -> None:
    await _upstash(["SETEX", key, str(ttl), value])


async def kv_del(key: str) -> None:
    await _upstash(["DEL", key])


# ─────────────────────────────────────────────
# Per-User Encryption
# ─────────────────────────────────────────────

def _derive_user_key(username: str) -> bytes:
    """Derive a unique Fernet-compatible key per user using HMAC-SHA256."""
    master = MASTER_ENCRYPTION_KEY.encode()
    user_bytes = username.lower().encode()
    derived = hmac.HMAC(master, user_bytes, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(derived)


def encrypt_value(username: str, plaintext: str) -> str:
    fernet = Fernet(_derive_user_key(username))
    return fernet.encrypt(plaintext.encode()).decode()


def decrypt_value(username: str, ciphertext: str) -> str:
    fernet = Fernet(_derive_user_key(username))
    return fernet.decrypt(ciphertext.encode()).decode()


# ─────────────────────────────────────────────
# Data Layer - isolated per user
# ─────────────────────────────────────────────

def _user_hash(username: str) -> str:
    return hashlib.sha256(username.lower().encode()).hexdigest()[:16]


def _keys_redis_key(username: str) -> str:
    return f"vam:keys:{_user_hash(username)}"


def _state_redis_key(username: str) -> str:
    return f"vam:state:{_user_hash(username)}"


async def get_user_keys(username: str) -> List[str]:
    """Return all decrypted API keys for the user."""
    raw = await kv_get(_keys_redis_key(username))
    if not raw:
        return []
    encrypted_list: List[str] = json.loads(raw)
    return [decrypt_value(username, e) for e in encrypted_list]


async def save_user_keys(username: str, keys: List[str]) -> None:
    """Encrypt and persist the key list."""
    encrypted_list = [encrypt_value(username, k) for k in keys]
    await kv_set(_keys_redis_key(username), json.dumps(encrypted_list))


async def add_user_key(username: str, api_key: str) -> bool:
    keys = await get_user_keys(username)
    if api_key in keys:
        return False
    keys.append(api_key)
    await save_user_keys(username, keys)
    return True


async def remove_user_key(username: str, api_key: str) -> bool:
    keys = await get_user_keys(username)
    if api_key not in keys:
        return False
    keys.remove(api_key)
    await save_user_keys(username, keys)
    return True


# ─────────────────────────────────────────────
# State Machine (await input from user)
# ─────────────────────────────────────────────

async def set_user_state(username: str, state: str) -> None:
    await kv_setex(_state_redis_key(username), 300, state)


async def get_user_state(username: str) -> Optional[str]:
    return await kv_get(_state_redis_key(username))


async def clear_user_state(username: str) -> None:
    await kv_del(_state_redis_key(username))


# ─────────────────────────────────────────────
# Vercel AI Gateway - Balance API
# ─────────────────────────────────────────────

async def check_balance(api_key: str) -> Optional[Dict[str, float]]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{VERCEL_GATEWAY}/credits",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            )
            if resp.status_code == 200:
                data = resp.json()
                return {
                    "balance": float(data.get("balance", 0)),
                    "total_used": float(data.get("total_used", 0)),
                }
    except Exception:
        pass
    return None


async def get_best_key(username: str) -> Optional[Dict[str, Any]]:
    """Return the key with the highest balance."""
    keys = await get_user_keys(username)
    if not keys:
        return None

    balances = await asyncio.gather(*[check_balance(k) for k in keys])
    results = [
        {"key": k, "balance": b["balance"], "total_used": b["total_used"]}
        for k, b in zip(keys, balances) if b is not None
    ]
    if not results:
        return None

    results.sort(key=lambda x: x["balance"], reverse=True)
    return results[0]


async def get_all_balances(username: str) -> List[Dict[str, Any]]:
    """Return balance info for all keys, sorted by balance descending."""
    keys = await get_user_keys(username)
    if not keys:
        return []

    balances = await asyncio.gather(*[check_balance(k) for k in keys])
    results = []
    for key, bal in zip(keys, balances):
        results.append({
            "key": key,
            "masked": f"{key[:8]}...{key[-6:]}",
            "balance": bal["balance"] if bal else None,
            "total_used": bal["total_used"] if bal else None,
        })
    results.sort(key=lambda x: (x["balance"] or -1), reverse=True)
    return results


# ─────────────────────────────────────────────
# Telegram API helpers
# ─────────────────────────────────────────────

async def tg(method: str, data: dict) -> dict:
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(f"{TELEGRAM_API}/{method}", json=data)
        return resp.json()


async def send_message(chat_id: int, text: str, reply_markup=None, parse_mode="Markdown") -> Optional[int]:
    payload: dict = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    resp = await tg("sendMessage", payload)
    if resp.get("ok"):
        return resp["result"]["message_id"]
    return None


async def delete_message(chat_id: int, message_id: int) -> None:
    await tg("deleteMessage", {"chat_id": chat_id, "message_id": message_id})


async def answer_callback(callback_query_id: str, text: str = "") -> None:
    await tg("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text})


async def delete_messages_later(chat_id: int, message_ids: List[int], delay: int = AUTO_DELETE_SECONDS) -> None:
    await asyncio.sleep(delay)
    for mid in message_ids:
        try:
            await delete_message(chat_id, mid)
        except Exception:
            pass


# ─────────────────────────────────────────────
# Access Control
# ─────────────────────────────────────────────

def is_allowed(username: Optional[str]) -> bool:
    if not username:
        return False
    return username.lower().lstrip("@") in ALLOWED_USERS


# ─────────────────────────────────────────────
# Keyboard
# ─────────────────────────────────────────────

def main_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "➕ Add Key", "callback_data": "btn_add"},
                {"text": "📊 Total APIs", "callback_data": "btn_total"},
            ],
            [
                {"text": "🗑 Remove Key", "callback_data": "btn_remove"},
                {"text": "🔑 Get Best Key", "callback_data": "btn_get"},
            ],
            [
                {"text": "📈 Dashboard", "callback_data": "btn_dashboard"},
            ],
        ]
    }


# ─────────────────────────────────────────────
# Action Handlers
# ─────────────────────────────────────────────

async def handle_start(chat_id: int, username: str) -> None:
    await clear_user_state(username)
    text = (
        "👋 *Vercel API Manager*\n\n"
        "Securely manage your Vercel AI Gateway API keys.\n"
        "Each key is encrypted and only accessible by you.\n\n"
        "Select an action:"
    )
    await send_message(chat_id, text, reply_markup=main_keyboard())


async def handle_btn_add(chat_id: int, username: str, cq_id: str) -> None:
    await answer_callback(cq_id)
    await set_user_state(username, "awaiting_add")
    prompt_id = await send_message(
        chat_id,
        "🔑 *Add API Key*\n\nSend your Vercel AI Gateway API key now.\n\n"
        "Example: `vck_87yuw9Jj...`\n\n"
        "_Your message will be deleted immediately after saving._",
    )
    # Auto-delete prompt if user doesn't respond in 60s
    asyncio.create_task(delete_messages_later(chat_id, [prompt_id], delay=60))


async def handle_btn_total(chat_id: int, username: str, cq_id: str) -> None:
    await answer_callback(cq_id)
    keys = await get_user_keys(username)
    count = len(keys)
    text = (
        f"📊 *Total API Keys*\n\n"
        f"You have *{count}* API key{'s' if count != 1 else ''} stored.\n\n"
        f"_Auto-deletes in {AUTO_DELETE_SECONDS}s._"
    )
    msg_id = await send_message(chat_id, text)
    if msg_id:
        asyncio.create_task(delete_messages_later(chat_id, [msg_id]))


async def handle_btn_remove(chat_id: int, username: str, cq_id: str) -> None:
    await answer_callback(cq_id)
    await set_user_state(username, "awaiting_remove")
    prompt_id = await send_message(
        chat_id,
        "🗑 *Remove API Key*\n\nSend the full API key you want to delete.\n\n"
        "_Auto-deletes in 60s if no response._",
    )
    asyncio.create_task(delete_messages_later(chat_id, [prompt_id], delay=60))


async def handle_btn_get(chat_id: int, username: str, cq_id: str) -> None:
    await answer_callback(cq_id, text="Checking balances...")
    loading_id = await send_message(chat_id, "⏳ Fetching best key by balance...")

    best = await get_best_key(username)
    to_delete = [loading_id] if loading_id else []

    if best is None:
        result_id = await send_message(
            chat_id,
            "❌ No keys available or balance check failed.\nAdd keys using *Add Key*.",
        )
    else:
        text = (
            f"🔑 *Best Key (Highest Balance)*\n\n"
            f"`{best['key']}`\n\n"
            f"💰 Balance: *${best['balance']:.4f}*\n"
            f"📉 Used: *${best['total_used']:.4f}*\n\n"
            f"_Auto-deletes in {AUTO_DELETE_SECONDS}s._"
        )
        result_id = await send_message(chat_id, text)

    if result_id:
        to_delete.append(result_id)
    asyncio.create_task(delete_messages_later(chat_id, to_delete))


async def handle_btn_dashboard(chat_id: int, username: str, cq_id: str) -> None:
    await answer_callback(cq_id, text="Loading dashboard...")
    loading_id = await send_message(chat_id, "⏳ Building dashboard...")

    all_data = await get_all_balances(username)
    to_delete = [loading_id] if loading_id else []

    if not all_data:
        result_id = await send_message(
            chat_id,
            "📈 *Dashboard*\n\nNo API keys stored yet.\nUse *Add Key* to get started.",
        )
    else:
        total_balance = sum(d["balance"] or 0 for d in all_data)
        total_used = sum(d["total_used"] or 0 for d in all_data)
        reachable = sum(1 for d in all_data if d["balance"] is not None)
        total_keys = len(all_data)

        lines = [
            "📈 *Vercel API Manager — Dashboard*",
            "",
            f"👤 User: @{username}",
            f"🔢 Total Keys: *{total_keys}*",
            f"✅ Reachable: *{reachable}/{total_keys}*",
            f"💰 Total Balance: *${total_balance:.4f}*",
            f"📉 Total Used: *${total_used:.4f}*",
            "",
            "━━━━━━━━━━━━━━━━━━━━━━━",
            "*Keys (sorted by balance)*",
            "",
        ]
        for i, d in enumerate(all_data, 1):
            bal = f"${d['balance']:.4f}" if d["balance"] is not None else "N/A"
            used = f"${d['total_used']:.4f}" if d["total_used"] is not None else "N/A"
            icon = "🟢" if d["balance"] is not None else "🔴"
            lines += [
                f"{icon} *#{i}* `{d['masked']}`",
                f"   Balance: {bal} | Used: {used}",
                "",
            ]

        lines.append(f"_Auto-deletes in {AUTO_DELETE_SECONDS}s._")
        result_id = await send_message(chat_id, "\n".join(lines))

    if result_id:
        to_delete.append(result_id)
    asyncio.create_task(delete_messages_later(chat_id, to_delete))


# ─────────────────────────────────────────────
# Text Message Handler (state machine)
# ─────────────────────────────────────────────

async def handle_user_text(chat_id: int, username: str, text: str, user_msg_id: int) -> None:
    state = await get_user_state(username)
    if not state:
        return  # Ignore messages outside of a flow

    await clear_user_state(username)

    if state == "awaiting_add":
        api_key = text.strip()
        if not api_key.startswith("vck_") or len(api_key) < 20:
            err_id = await send_message(
                chat_id,
                "❌ Invalid format. Vercel AI Gateway keys start with `vck_`.",
            )
            asyncio.create_task(delete_messages_later(chat_id, [user_msg_id, err_id]))
            return

        added = await add_user_key(username, api_key)
        msg = "✅ *Done!* API key saved and encrypted." if added else "⚠️ This key already exists."
        done_id = await send_message(chat_id, msg)
        asyncio.create_task(delete_messages_later(chat_id, [user_msg_id, done_id], delay=3))

    elif state == "awaiting_remove":
        api_key = text.strip()
        removed = await remove_user_key(username, api_key)
        msg = "✅ *Done!* API key removed." if removed else "❌ Key not found in your vault."
        done_id = await send_message(chat_id, msg)
        asyncio.create_task(delete_messages_later(chat_id, [user_msg_id, done_id]))


# ─────────────────────────────────────────────
# Update Router
# ─────────────────────────────────────────────

async def handle_update(update: dict) -> None:
    """Main entry point for all Telegram updates."""

    # Callback query (button press)
    if "callback_query" in update:
        cq = update["callback_query"]
        chat_id: int = cq["message"]["chat"]["id"]
        cq_id: str = cq["id"]
        username: str = cq["from"].get("username", "")

        if not is_allowed(username):
            await answer_callback(cq_id, text="Access denied.")
            return

        data = cq.get("data", "")
        dispatch = {
            "btn_add": handle_btn_add,
            "btn_total": handle_btn_total,
            "btn_remove": handle_btn_remove,
            "btn_get": handle_btn_get,
            "btn_dashboard": handle_btn_dashboard,
        }
        handler = dispatch.get(data)
        if handler:
            await handler(chat_id, username, cq_id)
        else:
            await answer_callback(cq_id)
        return

    # Regular text message
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        message_id: int = msg["message_id"]
        username = msg.get("from", {}).get("username", "")
        text: str = msg.get("text", "")

        if not is_allowed(username):
            return  # Silent reject

        if text.startswith("/start"):
            await handle_start(chat_id, username)
        elif text:
            await handle_user_text(chat_id, username, text, message_id)
