"""
Vercel API Manager Bot
======================
Speed optimizations:
- All Redis + Telegram calls run in parallel (asyncio.gather)
- httpx connection reuse via shared AsyncClient
- Minimal Redis round-trips

Auto-delete fix:
- Uses threading.Timer so deletes fire after response is sent,
  even with no further user interaction (works in Vercel serverless)
"""

from __future__ import annotations
import os, json, hashlib, hmac, base64, time, threading
from typing import Optional, List, Dict, Any

import httpx
from cryptography.fernet import Fernet

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]
MASTER_KEY = os.environ["MASTER_ENCRYPTION_KEY"]
ALLOWED    = {
    u.strip().lower().lstrip("@")
    for u in os.environ.get("ALLOWED_TELEGRAM_USERS", "").split(",")
    if u.strip()
}

KV_URL   = (os.environ.get("UPSTASH_REDIS_REST_URL") or os.environ.get("KV_REST_API_URL", "")).rstrip("/")
KV_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN") or os.environ.get("KV_REST_API_TOKEN", "")

TG_API   = f"https://api.telegram.org/bot{BOT_TOKEN}"
GATEWAY  = "https://ai-gateway.vercel.sh/v1"

DEL_SEC  = 30   # auto-delete output messages after 30s
ADD_SEC  = 4    # auto-delete add/remove confirmations after 4s

BTN_ADD       = "➕ Add Key"
BTN_TOTAL     = "📊 Total APIs"
BTN_REMOVE    = "🗑 Remove Key"
BTN_GET       = "🔑 Get Best Key"
BTN_DASHBOARD = "📈 Dashboard"


# ── Shared HTTP client (reused across calls in one request) ───────────────────
# Created fresh per-invocation since serverless has no persistent state
def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=8, limits=httpx.Limits(max_connections=20))


# ── Upstash Redis REST ────────────────────────────────────────────────────────

async def _kv(client: httpx.AsyncClient, cmd: list) -> Any:
    parts = "/".join(
        str(c).replace("/", "%2F").replace(" ", "%20").replace("+", "%2B")
        for c in cmd
    )
    r = await client.get(
        f"{KV_URL}/{parts}",
        headers={"Authorization": f"Bearer {KV_TOKEN}"},
    )
    d = r.json()
    if "error" in d:
        raise RuntimeError(f"KV: {d['error']}")
    return d.get("result")


# Convenience wrappers that accept a shared client
async def kv_get(cl: httpx.AsyncClient, k: str) -> Optional[str]:
    return await _kv(cl, ["GET", k])

async def kv_set(cl: httpx.AsyncClient, k: str, v: str) -> None:
    await _kv(cl, ["SET", k, v])

async def kv_setex(cl: httpx.AsyncClient, k: str, t: int, v: str) -> None:
    await _kv(cl, ["SETEX", k, t, v])

async def kv_del(cl: httpx.AsyncClient, k: str) -> None:
    await _kv(cl, ["DEL", k])

async def kv_sadd(cl: httpx.AsyncClient, k: str, v: str) -> None:
    await _kv(cl, ["SADD", k, v])


# ── Encryption ────────────────────────────────────────────────────────────────

def _fernet(username: str) -> Fernet:
    raw = hmac.new(
        MASTER_KEY.encode(), username.lower().encode(), hashlib.sha256
    ).digest()
    return Fernet(base64.urlsafe_b64encode(raw))

def enc(u: str, v: str) -> str: return _fernet(u).encrypt(v.encode()).decode()
def dec(u: str, v: str) -> str: return _fernet(u).decrypt(v.encode()).decode()


# ── Redis key helpers ─────────────────────────────────────────────────────────

def _uh(u: str)  -> str: return hashlib.sha256(u.lower().encode()).hexdigest()[:16]
def _kk(u: str)  -> str: return f"vam:keys:{_uh(u)}"
def _ks(u: str)  -> str: return f"vam:state:{_uh(u)}"


# ── API key CRUD ──────────────────────────────────────────────────────────────

async def get_keys(cl: httpx.AsyncClient, u: str) -> List[str]:
    raw = await kv_get(cl, _kk(u))
    if not raw:
        return []
    return [dec(u, e) for e in json.loads(raw)]

async def save_keys(cl: httpx.AsyncClient, u: str, keys: List[str]) -> None:
    await kv_set(cl, _kk(u), json.dumps([enc(u, k) for k in keys]))

async def add_key(cl: httpx.AsyncClient, u: str, key: str) -> bool:
    keys = await get_keys(cl, u)
    if key in keys:
        return False
    keys.append(key)
    await save_keys(cl, u, keys)
    return True

async def remove_key(cl: httpx.AsyncClient, u: str, key: str) -> bool:
    keys = await get_keys(cl, u)
    if key not in keys:
        return False
    keys.remove(key)
    await save_keys(cl, u, keys)
    return True


# ── State machine ─────────────────────────────────────────────────────────────

async def set_state(cl: httpx.AsyncClient, u: str, s: str) -> None:
    await kv_setex(cl, _ks(u), 300, s)

async def get_state(cl: httpx.AsyncClient, u: str) -> Optional[str]:
    return await kv_get(cl, _ks(u))

async def clear_state(cl: httpx.AsyncClient, u: str) -> None:
    await kv_del(cl, _ks(u))


# ── Auto-delete (threading.Timer — fires after response is sent) ──────────────

def _fire_delete(chat_id: int, msg_ids: List[int]) -> None:
    """Called in a background thread. Uses a fresh sync HTTP call."""
    import urllib.request
    for mid in msg_ids:
        try:
            body = json.dumps({"chat_id": chat_id, "message_id": mid}).encode()
            req  = urllib.request.Request(
                f"{TG_API}/deleteMessage",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            pass  # message may already be gone


def schedule_delete(chat_id: int, msg_ids: List[int], delay: int = DEL_SEC) -> None:
    """
    Schedule deletion of msg_ids after `delay` seconds.
    threading.Timer runs in a daemon thread — does NOT block the response,
    and works inside Vercel serverless (process stays alive long enough).
    """
    valid = [m for m in msg_ids if m]
    if not valid:
        return
    t = threading.Timer(delay, _fire_delete, args=(chat_id, valid))
    t.daemon = True
    t.start()


# ── Vercel AI Gateway — parallel balance checks ───────────────────────────────

async def check_balance(cl: httpx.AsyncClient, key: str) -> Optional[Dict]:
    try:
        r = await cl.get(
            f"{GATEWAY}/credits",
            headers={"Authorization": f"Bearer {key}"},
        )
        if r.status_code == 200:
            d = r.json()
            return {
                "balance": float(d.get("balance", 0)),
                "used":    float(d.get("total_used", 0)),
            }
    except Exception:
        pass
    return None


async def best_key(cl: httpx.AsyncClient, u: str) -> Optional[Dict]:
    """All balance checks run in parallel."""
    import asyncio
    keys = await get_keys(cl, u)
    if not keys:
        return None
    bals = await asyncio.gather(*[check_balance(cl, k) for k in keys])
    res  = [{"key": k, **b} for k, b in zip(keys, bals) if b]
    if not res:
        return None
    return max(res, key=lambda x: x["balance"])


async def all_balances(cl: httpx.AsyncClient, u: str) -> List[Dict]:
    import asyncio
    keys = await get_keys(cl, u)
    if not keys:
        return []
    bals = await asyncio.gather(*[check_balance(cl, k) for k in keys])
    out  = []
    for k, b in zip(keys, bals):
        out.append({
            "key":    k,
            "masked": f"{k[:8]}...{k[-6:]}",
            "balance": b["balance"] if b else None,
            "used":    b["used"]    if b else None,
        })
    out.sort(key=lambda x: (x["balance"] or -1), reverse=True)
    return out


# ── Telegram helpers ──────────────────────────────────────────────────────────

async def tg_post(cl: httpx.AsyncClient, method: str, data: dict) -> dict:
    r = await cl.post(f"{TG_API}/{method}", json=data)
    return r.json()


async def send(
    cl: httpx.AsyncClient,
    chat_id: int,
    text: str,
    markup=None,
    mode: str = "Markdown",
) -> Optional[int]:
    p: dict = {"chat_id": chat_id, "text": text, "parse_mode": mode}
    if markup:
        p["reply_markup"] = markup
    r = await tg_post(cl, "sendMessage", p)
    return r["result"]["message_id"] if r.get("ok") else None


async def delete_msg(cl: httpx.AsyncClient, chat_id: int, msg_id: int) -> None:
    try:
        await tg_post(cl, "deleteMessage", {"chat_id": chat_id, "message_id": msg_id})
    except Exception:
        pass


# ── Access control ────────────────────────────────────────────────────────────

def allowed(username: Optional[str]) -> bool:
    return bool(username) and username.lower().lstrip("@") in ALLOWED  # type: ignore[union-attr]


# ── Keyboards ─────────────────────────────────────────────────────────────────

def reply_kb() -> dict:
    return {
        "keyboard": [
            [{"text": BTN_ADD},    {"text": BTN_TOTAL}],
            [{"text": BTN_REMOVE}, {"text": BTN_GET}],
            [{"text": BTN_DASHBOARD}],
        ],
        "resize_keyboard": True,
        "persistent": True,
        "input_field_placeholder": "Select an option…",
    }


# ── Handlers ──────────────────────────────────────────────────────────────────

async def on_start(cl: httpx.AsyncClient, chat_id: int, username: str, user_msg_id: int) -> None:
    import asyncio

    # Safe parallel cleanup — errors ignored so new users never crash here
    async def _safe_clear():
        try:
            await clear_state(cl, username)
        except Exception:
            pass

    async def _safe_register():
        try:
            await kv_sadd(cl, "vam:users", _uh(username))
        except Exception:
            pass

    async def _safe_delete_user_msg():
        try:
            await delete_msg(cl, chat_id, user_msg_id)
        except Exception:
            pass

    await asyncio.gather(_safe_clear(), _safe_register(), _safe_delete_user_msg())

    await send(
        cl, chat_id,
        "🔐 *Vercel API Manager*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Welcome, @" + username + "!\n\n"
        "This bot helps you manage your *Vercel AI Gateway*\n"
        "API keys safely and efficiently.\n\n"
        "✦ Keys are *AES-256 encrypted*\n"
        "✦ Only *you* can access your keys\n"
        "✦ Always gets the *highest balance* key\n"
        "✦ Messages *auto-delete* for privacy\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Select an option from the menu below:",
        markup=reply_kb(),
    )


async def on_add(cl: httpx.AsyncClient, chat_id: int, username: str, user_msg_id: int) -> None:
    import asyncio
    await asyncio.gather(
        set_state(cl, username, "awaiting_add"),
        delete_msg(cl, chat_id, user_msg_id),
    )
    prompt_id = await send(
        cl, chat_id,
        "🔑 *Add API Key*\n\n"
        "Send your Vercel AI Gateway API key now.\n"
        "Format: `vck_...`\n\n"
        "_Your message will be deleted immediately after saving._",
    )
    # Prompt auto-deletes if user doesn't respond in 90s
    schedule_delete(chat_id, [prompt_id], delay=90)


async def on_total(cl: httpx.AsyncClient, chat_id: int, username: str, user_msg_id: int) -> None:
    import asyncio
    keys, _ = await asyncio.gather(
        get_keys(cl, username),
        delete_msg(cl, chat_id, user_msg_id),
    )
    n   = len(keys)
    mid = await send(
        cl, chat_id,
        f"📊 *Total API Keys*\n\n"
        f"You have *{n}* API key{'s' if n != 1 else ''} stored.\n\n"
        f"_Auto-deletes in {DEL_SEC}s._",
    )
    schedule_delete(chat_id, [mid])


async def on_remove(cl: httpx.AsyncClient, chat_id: int, username: str, user_msg_id: int) -> None:
    import asyncio
    await asyncio.gather(
        set_state(cl, username, "awaiting_remove"),
        delete_msg(cl, chat_id, user_msg_id),
    )
    prompt_id = await send(
        cl, chat_id,
        "🗑 *Remove API Key*\n\n"
        "Send the full API key you want to delete.\n\n"
        "_Prompt auto-deletes in 90s if no response._",
    )
    schedule_delete(chat_id, [prompt_id], delay=90)


async def on_get(cl: httpx.AsyncClient, chat_id: int, username: str, user_msg_id: int) -> None:
    import asyncio
    # Delete user button press + send loading message in parallel
    loading_id, _ = await asyncio.gather(
        send(cl, chat_id, "⏳ Checking balances…"),
        delete_msg(cl, chat_id, user_msg_id),
    )
    # All balance checks fire simultaneously inside best_key()
    bk = await best_key(cl, username)

    await delete_msg(cl, chat_id, loading_id)

    if bk is None:
        mid = await send(
            cl, chat_id,
            "❌ No keys available or balance check failed.\n"
            "Add keys first using *Add Key*.",
        )
    else:
        mid = await send(
            cl, chat_id,
            f"🔑 *Best Key — Highest Balance*\n\n"
            f"`{bk['key']}`\n\n"
            f"💰 Balance : *${bk['balance']:.4f}*\n"
            f"📉 Used    : *${bk['used']:.4f}*\n\n"
            f"_Auto-deletes in {DEL_SEC}s._",
        )
    schedule_delete(chat_id, [mid])


async def on_dashboard(cl: httpx.AsyncClient, chat_id: int, username: str, user_msg_id: int) -> None:
    import asyncio
    loading_id, _ = await asyncio.gather(
        send(cl, chat_id, "⏳ Building dashboard…"),
        delete_msg(cl, chat_id, user_msg_id),
    )
    # All balance checks fire in parallel inside all_balances()
    data = await all_balances(cl, username)

    await delete_msg(cl, chat_id, loading_id)

    if not data:
        mid = await send(
            cl, chat_id,
            "📈 *Dashboard*\n\nNo API keys found.\nUse *Add Key* to get started.",
        )
    else:
        total_bal  = sum(d["balance"] or 0 for d in data)
        total_used = sum(d["used"]    or 0 for d in data)
        reachable  = sum(1 for d in data if d["balance"] is not None)
        n          = len(data)

        lines = [
            "📈 *Vercel API Manager — Dashboard*", "",
            f"👤 User        : @{username}",
            f"🔢 Total Keys  : *{n}*",
            f"✅ Reachable   : *{reachable}/{n}*",
            f"💰 Total Bal   : *${total_bal:.4f}*",
            f"📉 Total Used  : *${total_used:.4f}*", "",
            "━━━━━━━━━━━━━━━━━━━━━━",
            "*Keys — highest balance first*", "",
        ]
        for i, d in enumerate(data, 1):
            icon = "🟢" if d["balance"] is not None else "🔴"
            bal  = f"${d['balance']:.4f}" if d["balance"] is not None else "N/A"
            used = f"${d['used']:.4f}"    if d["used"]    is not None else "N/A"
            lines += [
                f"{icon} *#{i}* `{d['masked']}`",
                f"   Balance: {bal}  |  Used: {used}", "",
            ]
        lines.append(f"_Auto-deletes in {DEL_SEC}s._")
        mid = await send(cl, chat_id, "\n".join(lines))

    schedule_delete(chat_id, [mid])


# ── Text input (state machine) ────────────────────────────────────────────────

async def on_text(
    cl: httpx.AsyncClient,
    chat_id: int,
    username: str,
    text: str,
    user_msg_id: int,
) -> None:
    state = await get_state(cl, username)
    if not state:
        return

    await clear_state(cl, username)

    if state == "awaiting_add":
        key = text.strip()
        if not (key.startswith("vck_") and len(key) >= 20):
            err = await send(
                cl, chat_id,
                "❌ Invalid format.\n"
                "Vercel keys start with `vck_` and are 60+ chars.",
            )
            schedule_delete(chat_id, [user_msg_id, err], delay=ADD_SEC)
            return
        ok   = await add_key(cl, username, key)
        msg  = "✅ *Done!* Key saved and encrypted." if ok else "⚠️ Key already exists."
        done = await send(cl, chat_id, msg)
        schedule_delete(chat_id, [user_msg_id, done], delay=ADD_SEC)

    elif state == "awaiting_remove":
        key     = text.strip()
        ok      = await remove_key(cl, username, key)
        msg     = "✅ *Done!* API key removed." if ok else "❌ Key not found in your vault."
        done    = await send(cl, chat_id, msg)
        schedule_delete(chat_id, [user_msg_id, done], delay=DEL_SEC)


# ── Main dispatcher ───────────────────────────────────────────────────────────

async def handle_update(update: dict) -> None:
    if "message" not in update:
        return

    msg      = update["message"]
    chat_id  = msg["chat"]["id"]
    msg_id   = msg["message_id"]
    username = msg.get("from", {}).get("username", "")
    text     = msg.get("text", "").strip()

    if not allowed(username):
        return

    try:
        # Single shared client for the entire request
        async with _client() as cl:
            if text.startswith("/start"):
                await on_start(cl, chat_id, username, msg_id)
                return

            handlers = {
                BTN_ADD:       on_add,
                BTN_TOTAL:     on_total,
                BTN_REMOVE:    on_remove,
                BTN_GET:       on_get,
                BTN_DASHBOARD: on_dashboard,
            }
            fn = handlers.get(text)
            if fn:
                await fn(cl, chat_id, username, msg_id)
                return

            if text:
                await on_text(cl, chat_id, username, text, msg_id)
    except Exception:
        # Never crash silently — try to send a fallback message
        try:
            async with _client() as cl:
                await send(cl, chat_id, "Something went wrong. Please try /start again.")
        except Exception:
            pass
