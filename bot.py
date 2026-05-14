"""
Vercel API Manager Bot
A Telegram bot to manage Vercel AI Gateway API keys
with per-user encryption and access control.
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
import redis.asyncio as aioredis


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_USERS_RAW = os.environ.get("ALLOWED_TELEGRAM_USERS", "")
ALLOWED_USERS = {u.strip().lower().lstrip("@") for u in ALLOWED_USERS_RAW.split(",") if u.strip()}

MASTER_ENCRYPTION_KEY = os.environ["MASTER_ENCRYPTION_KEY"]  # 32-byte base64 key
KV_URL = os.environ["KV_URL"]  # Vercel KV Redis URL

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
VERCEL_GATEWAY = "https://ai-gateway.vercel.sh/v1"

AUTO_DELETE_SECONDS = 30  # seconds before clearing chat


# ─────────────────────────────────────────────
# Redis / KV Store
# ─────────────────────────────────────────────
_redis_client: Optional[aioredis.Redis] = None


async def get_redis() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(KV_URL, decode_responses=True)
    return _redis_client


# ─────────────────────────────────────────────
# Per-User Encryption
# ─────────────────────────────────────────────

def _derive_user_key(username: str) -> bytes:
    """Derive a unique Fernet key for each user using HMAC-SHA256."""
    master = MASTER_ENCRYPTION_KEY.encode()
    user_bytes = username.lower().encode()
    derived = hmac.new(master, user_bytes, hashlib.sha256).digest()
    # Fernet needs 32 bytes URL-safe base64
    return base64.urlsafe_b64encode(derived)


def encrypt_key(username: str, api_key: str) -> str:
    """Encrypt an API key with a user-specific key."""
    fernet = Fernet(_derive_user_key(username))
    return fernet.encrypt(api_key.encode()).decode()


def decrypt_key(username: str, encrypted: str) -> str:
    """Decrypt an API key with a user-specific key."""
    fernet = Fernet(_derive_user_key(username))
    return fernet.decrypt(encrypted.encode()).decode()


# ─────────────────────────────────────────────
# Data Layer - per user Redis keys
# ─────────────────────────────────────────────

def _redis_key(username: str) -> str:
    """Redis key for a user's API key list (encrypted)."""
    user_hash = hashlib.sha256(username.lower().encode()).hexdigest()[:16]
    return f"vam:keys:{user_hash}"


async def get_user_keys(username: str) -> List[str]:
    """Get all API keys for a user (decrypted)."""
    r = await get_redis()
    raw = await r.get(_redis_key(username))
    if not raw:
        return []
    encrypted_list = json.loads(raw)
    return [decrypt_key(username, e) for e in encrypted_list]


async def save_user_keys(username: str, keys: List[str]) -> None:
    """Save API keys for a user (encrypted)."""
    r = await get_redis()
    encrypted_list = [encrypt_key(username, k) for k in keys]
    await r.set(_redis_key(username), json.dumps(encrypted_list))


async def add_user_key(username: str, api_key: str) -> bool:
    """Add an API key. Returns False if already exists."""
    keys = await get_user_keys(username)
    if api_key in keys:
        return False
    keys.append(api_key)
    await save_user_keys(username, keys)
    return True


async def remove_user_key(username: str, api_key: str) -> bool:
    """Remove an API key. Returns False if not found."""
    keys = await get_user_keys(username)
    if api_key not in keys:
        return False
    keys.remove(api_key)
    await save_user_keys(username, keys)
    return True


# ─────────────────────────────────────────────
# Vercel AI Gateway API
# ─────────────────────────────────────────────

async def check_balance(api_key: str) -> Optional[Dict[str, Any]]:
    """Check balance for a single API key. Returns dict or None on error."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{VERCEL_GATEWAY}/credits",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                }
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
    """
    Get the API key with the highest balance.
    Returns dict: { key, balance, total_used } or None.
    """
    keys = await get_user_keys(username)
    if not keys:
        return None

    results = []
    tasks = [check_balance(k) for k in keys]
    balances = await asyncio.gather(*tasks)

    for key, bal in zip(keys, balances):
        if bal is not None:
            results.append({"key": key, "balance": bal["balance"], "total_used": bal["total_used"]})

    if not results:
        return None

    # Sort descending by balance, return top one
    results.sort(key=lambda x: x["balance"], reverse=True)
    return results[0]


async def get_all_balances(username: str) -> List[Dict[str, Any]]:
    """Get balance info for all keys."""
    keys = await get_user_keys(username)
    if not keys:
        return []

    tasks = [check_balance(k) for k in keys]
    balances = await asyncio.gather(*tasks)
    results = []
    for key, bal in zip(keys, balances):
        masked = f"{key[:8]}...{key[-6:]}"
        results.append({
            "key": key,
            "masked": masked,
            "balance": bal["balance"] if bal else None,
            "total_used": bal["total_used"] if bal else None,
        })
    results.sort(key=lambda x: (x["balance"] or -1), reverse=True)
    return results


# ─────────────────────────────────────────────
# Telegram API helpers
# ─────────────────────────────────────────────

async def tg_request(method: str, data: dict) -> dict:
    """Make a Telegram Bot API request."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(f"{TELEGRAM_API}/{method}", json=data)
        return resp.json()


async def send_message(chat_id: int, text: str, reply_markup=None, parse_mode="Markdown") -> Optional[int]:
    """Send a message and return message_id."""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    resp = await tg_request("sendMessage", payload)
    if resp.get("ok"):
        return resp["result"]["message_id"]
    return None


async def delete_message(chat_id: int, message_id: int) -> None:
    """Delete a message."""
    await tg_request("deleteMessage", {"chat_id": chat_id, "message_id": message_id})


async def delete_messages_later(chat_id: int, message_ids: List[int], delay: int = AUTO_DELETE_SECONDS) -> None:
    """Schedule message deletion after delay seconds."""
    await asyncio.sleep(delay)
    for mid in message_ids:
        await delete_message(chat_id, mid)


async def answer_callback(callback_query_id: str, text: str = "") -> None:
    await tg_request("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text})


# ─────────────────────────────────────────────
# State management for awaiting input
# ─────────────────────────────────────────────

async def set_user_state(username: str, state: str, extra: str = "") -> None:
    r = await get_redis()
    user_hash = hashlib.sha256(username.lower().encode()).hexdigest()[:16]
    await r.setex(f"vam:state:{user_hash}", 300, json.dumps({"state": state, "extra": extra}))


async def get_user_state(username: str) -> Optional[Dict]:
    r = await get_redis()
    user_hash = hashlib.sha256(username.lower().encode()).hexdigest()[:16]
    raw = await r.get(f"vam:state:{user_hash}")
    return json.loads(raw) if raw else None


async def clear_user_state(username: str) -> None:
    r = await get_redis()
    user_hash = hashlib.sha256(username.lower().encode()).hexdigest()[:16]
    await r.delete(f"vam:state:{user_hash}")


# ─────────────────────────────────────────────
# Main Keyboard
# ─────────────────────────────────────────────

def main_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "➕ Add Key", "callback_data": "action_add"},
                {"text": "📊 Total APIs", "callback_data": "action_total"},
            ],
            [
                {"text": "🗑 Remove Key", "callback_data": "action_remove"},
                {"text": "🔑 Get Best Key", "callback_data": "action_get"},
            ],
            [
                {"text": "📈 Dashboard", "callback_data": "action_dashboard"},
            ]
        ]
    }


# ─────────────────────────────────────────────
# Access Control
# ─────────────────────────────────────────────

def is_allowed(username: Optional[str]) -> bool:
    if not username:
        return False
    return username.lower().lstrip("@") in ALLOWED_USERS


# ─────────────────────────────────────────────
# Handlers
# ─────────────────────────────────────────────

async def handle_start(chat_id: int, username: str) -> None:
    text = (
        "👋 *Welcome to Vercel API Manager*\n\n"
        "Manage your Vercel AI Gateway API keys securely.\n"
        "Your keys are encrypted and only accessible by you.\n\n"
        "Choose an action:"
    )
    await send_message(chat_id, text, reply_markup=main_keyboard())


async def handle_action_add(chat_id: int, username: str, callback_query_id: str, bot_msg_id: int) -> None:
    await answer_callback(callback_query_id)
    await set_user_state(username, "awaiting_add_key")
    prompt_msg_id = await send_message(
        chat_id,
        "🔑 *Add New API Key*\n\nPlease send your Vercel AI Gateway API key.\n\n"
        "_Your message and this prompt will be deleted automatically._"
    )
    # Schedule deletion of prompt after 60s if user doesn't respond
    asyncio.create_task(delete_messages_later(chat_id, [prompt_msg_id], delay=60))


async def handle_action_total(chat_id: int, username: str, callback_query_id: str) -> None:
    await answer_callback(callback_query_id)
    keys = await get_user_keys(username)
    count = len(keys)
    text = (
        f"📊 *Total API Keys*\n\n"
        f"You have *{count}* API key{'s' if count != 1 else ''} stored.\n\n"
        f"_This message will be deleted in {AUTO_DELETE_SECONDS} seconds._"
    )
    msg_id = await send_message(chat_id, text)
    if msg_id:
        asyncio.create_task(delete_messages_later(chat_id, [msg_id]))


async def handle_action_remove(chat_id: int, username: str, callback_query_id: str) -> None:
    await answer_callback(callback_query_id)
    await set_user_state(username, "awaiting_remove_key")
    prompt_id = await send_message(
        chat_id,
        "🗑 *Remove API Key*\n\nSend the full API key you want to remove.\n\n"
        "_This message will be deleted after action._"
    )
    asyncio.create_task(delete_messages_later(chat_id, [prompt_id], delay=60))


async def handle_action_get(chat_id: int, username: str, callback_query_id: str) -> None:
    await answer_callback(callback_query_id, text="Checking balances...")
    checking_id = await send_message(chat_id, "⏳ Checking balances for all keys...")

    best = await get_best_key(username)

    msgs_to_delete = []
    if checking_id:
        msgs_to_delete.append(checking_id)

    if best is None:
        result_id = await send_message(
            chat_id,
            "❌ No API keys found or unable to fetch balances.\nAdd keys using the *Add Key* button.",
        )
    else:
        balance_str = f"${best['balance']:.4f}"
        text = (
            f"🔑 *Best API Key (Highest Balance)*\n\n"
            f"`{best['key']}`\n\n"
            f"💰 Balance: *{balance_str}*\n"
            f"📉 Total Used: *${best['total_used']:.4f}*\n\n"
            f"_This message will be deleted in {AUTO_DELETE_SECONDS} seconds._"
        )
        result_id = await send_message(chat_id, text)

    if result_id:
        msgs_to_delete.append(result_id)

    asyncio.create_task(delete_messages_later(chat_id, msgs_to_delete))


async def handle_action_dashboard(chat_id: int, username: str, callback_query_id: str) -> None:
    await answer_callback(callback_query_id, text="Loading dashboard...")
    loading_id = await send_message(chat_id, "⏳ Fetching dashboard data...")

    keys = await get_user_keys(username)
    all_data = await get_all_balances(username)

    msgs_to_delete = []
    if loading_id:
        msgs_to_delete.append(loading_id)

    if not keys:
        result_id = await send_message(
            chat_id,
            "📈 *Dashboard*\n\nNo API keys stored yet.\nUse *Add Key* to add your first key.",
        )
    else:
        total_balance = sum(d["balance"] or 0 for d in all_data)
        total_used = sum(d["total_used"] or 0 for d in all_data)
        total_keys = len(all_data)
        reachable = sum(1 for d in all_data if d["balance"] is not None)

        lines = [
            "📈 *Vercel API Manager Dashboard*",
            "",
            f"👤 User: @{username}",
            f"🔢 Total Keys: *{total_keys}*",
            f"✅ Reachable: *{reachable}/{total_keys}*",
            f"💰 Total Balance: *${total_balance:.4f}*",
            f"📉 Total Used: *${total_used:.4f}*",
            "",
            "─────────────────────",
            "*Keys Overview (sorted by balance):*",
            "",
        ]

        for i, d in enumerate(all_data, 1):
            bal = f"${d['balance']:.4f}" if d["balance"] is not None else "N/A"
            used = f"${d['total_used']:.4f}" if d["total_used"] is not None else "N/A"
            status = "🟢" if d["balance"] is not None else "🔴"
            lines.append(f"{status} *Key {i}:* `{d['masked']}`")
            lines.append(f"   Balance: {bal} | Used: {used}")
            lines.append("")

        lines.append(f"_Dashboard will be deleted in {AUTO_DELETE_SECONDS} seconds._")
        text = "\n".join(lines)
        result_id = await send_message(chat_id, text)

    if result_id:
        msgs_to_delete.append(result_id)

    asyncio.create_task(delete_messages_later(chat_id, msgs_to_delete))


async def handle_user_text(chat_id: int, username: str, text: str, user_message_id: int) -> None:
    """Handle user text messages (used for add/remove states)."""
    state_data = await get_user_state(username)

    if not state_data:
        # Ignore unknown messages
        return

    state = state_data.get("state", "")
    await clear_user_state(username)

    if state == "awaiting_add_key":
        api_key = text.strip()
        # Basic validation: Vercel keys start with vck_
        if not api_key.startswith("vck_") or len(api_key) < 20:
            err_id = await send_message(
                chat_id,
                "❌ Invalid API key format.\nVercel AI Gateway keys start with `vck_`."
            )
            asyncio.create_task(delete_messages_later(chat_id, [user_message_id, err_id]))
            return

        added = await add_user_key(username, api_key)
        if added:
            done_id = await send_message(
                chat_id,
                "✅ *Done!*\n\nAPI key has been securely saved and encrypted.",
            )
        else:
            done_id = await send_message(
                chat_id,
                "⚠️ This API key already exists in your vault.",
            )
        # Delete user's key message + bot's response
        asyncio.create_task(delete_messages_later(chat_id, [user_message_id, done_id], delay=3))

    elif state == "awaiting_remove_key":
        api_key = text.strip()
        removed = await remove_user_key(username, api_key)
        if removed:
            done_id = await send_message(
                chat_id,
                "✅ *Done!*\n\nAPI key has been removed from your vault.",
            )
        else:
            done_id = await send_message(
                chat_id,
                "❌ API key not found in your vault.",
            )
        asyncio.create_task(delete_messages_later(chat_id, [user_message_id, done_id]))


# ─────────────────────────────────────────────
# Main Update Handler
# ─────────────────────────────────────────────

async def handle_update(update: dict) -> None:
    """Entry point for all Telegram updates."""

    # ── Callback Query (button press) ──
    if "callback_query" in update:
        cq = update["callback_query"]
        chat_id = cq["message"]["chat"]["id"]
        bot_msg_id = cq["message"]["message_id"]
        callback_id = cq["id"]
        username = cq["from"].get("username", "")

        if not is_allowed(username):
            await answer_callback(callback_id, text="Access denied.")
            return

        data = cq.get("data", "")

        if data == "action_add":
            await handle_action_add(chat_id, username, callback_id, bot_msg_id)
        elif data == "action_total":
            await handle_action_total(chat_id, username, callback_id)
        elif data == "action_remove":
            await handle_action_remove(chat_id, username, callback_id)
        elif data == "action_get":
            await handle_action_get(chat_id, username, callback_id)
        elif data == "action_dashboard":
            await handle_action_dashboard(chat_id, username, callback_id)
        else:
            await answer_callback(callback_id)
        return

    # ── Text Message ──
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        message_id = msg["message_id"]
        username = msg.get("from", {}).get("username", "")
        text = msg.get("text", "")

        if not is_allowed(username):
            # Silently ignore unauthorized users
            return

        if text.startswith("/start"):
            await handle_start(chat_id, username)
        elif text:
            await handle_user_text(chat_id, username, text, message_id)
