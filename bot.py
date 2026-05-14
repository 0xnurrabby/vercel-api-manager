"""
Vercel API Manager Bot
======================
- Reply Keyboard (persistent bottom bar, like the reference screenshot)
- Auto-delete via Redis-persisted queue + a /tick cron endpoint
- Per-user AES-256 encryption (HMAC-derived Fernet)
- Username allowlist, strict isolation
"""

from __future__ import annotations
import os, json, hashlib, hmac, base64, time
from typing import Optional, List, Dict, Any

import httpx
from cryptography.fernet import Fernet

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ["TELEGRAM_BOT_TOKEN"]
MASTER_KEY   = os.environ["MASTER_ENCRYPTION_KEY"]
ALLOWED_RAW  = os.environ.get("ALLOWED_TELEGRAM_USERS", "")
ALLOWED      = {u.strip().lower().lstrip("@") for u in ALLOWED_RAW.split(",") if u.strip()}

KV_URL   = (os.environ.get("UPSTASH_REDIS_REST_URL") or os.environ.get("KV_REST_API_URL", "")).rstrip("/")
KV_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN") or os.environ.get("KV_REST_API_TOKEN", "")

TG       = f"https://api.telegram.org/bot{BOT_TOKEN}"
GATEWAY  = "https://ai-gateway.vercel.sh/v1"

DEL_SEC  = 30   # auto-delete delay for output messages
ADD_SEC  = 4    # auto-delete delay for add/remove confirmations

# Button labels (used both for keyboard display and routing)
BTN_ADD       = "➕ Add Key"
BTN_TOTAL     = "📊 Total APIs"
BTN_REMOVE    = "🗑 Remove Key"
BTN_GET       = "🔑 Get Best Key"
BTN_DASHBOARD = "📈 Dashboard"


# ── Upstash Redis REST ────────────────────────────────────────────────────────

async def _kv(cmd: list) -> Any:
    parts = "/".join(str(c).replace("/", "%2F").replace(" ", "%20") for c in cmd)
    url   = f"{KV_URL}/{parts}"
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(url, headers={"Authorization": f"Bearer {KV_TOKEN}"})
    d = r.json()
    if "error" in d:
        raise RuntimeError(f"KV: {d['error']}")
    return d.get("result")

async def kv_get(k: str) -> Optional[str]:          return await _kv(["GET", k])
async def kv_set(k: str, v: str) -> None:           await _kv(["SET", k, v])
async def kv_setex(k: str, t: int, v: str) -> None: await _kv(["SETEX", k, t, v])
async def kv_del(k: str) -> None:                   await _kv(["DEL", k])
async def kv_rpush(k: str, v: str) -> None:         await _kv(["RPUSH", k, v])
async def kv_llen(k: str) -> int:
    r = await _kv(["LLEN", k]); return int(r or 0)
async def kv_lrange(k: str, s: int, e: int) -> list:
    r = await _kv(["LRANGE", k, s, e]); return r or []
async def kv_ltrim(k: str, s: int, e: int) -> None: await _kv(["LTRIM", k, s, e])


# ── Encryption ────────────────────────────────────────────────────────────────

def _fernet(username: str) -> Fernet:
    raw = hmac.new(MASTER_KEY.encode(), username.lower().encode(), hashlib.sha256).digest()
    return Fernet(base64.urlsafe_b64encode(raw))

def enc(u: str, v: str) -> str: return _fernet(u).encrypt(v.encode()).decode()
def dec(u: str, v: str) -> str: return _fernet(u).decrypt(v.encode()).decode()


# ── Redis key namespacing ─────────────────────────────────────────────────────

def _uh(u: str)  -> str: return hashlib.sha256(u.lower().encode()).hexdigest()[:16]
def _kk(u: str)  -> str: return f"vam:keys:{_uh(u)}"
def _ks(u: str)  -> str: return f"vam:state:{_uh(u)}"
def _kdq(u: str) -> str: return f"vam:delq:{_uh(u)}"


# ── API key storage ───────────────────────────────────────────────────────────

async def get_keys(u: str) -> List[str]:
    raw = await kv_get(_kk(u))
    if not raw: return []
    return [dec(u, e) for e in json.loads(raw)]

async def save_keys(u: str, keys: List[str]) -> None:
    await kv_set(_kk(u), json.dumps([enc(u, k) for k in keys]))

async def add_key(u: str, key: str) -> bool:
    keys = await get_keys(u)
    if key in keys: return False
    keys.append(key); await save_keys(u, keys); return True

async def remove_key(u: str, key: str) -> bool:
    keys = await get_keys(u)
    if key not in keys: return False
    keys.remove(key); await save_keys(u, keys); return True


# ── State machine ─────────────────────────────────────────────────────────────

async def set_state(u: str, s: str)   -> None: await kv_setex(_ks(u), 300, s)
async def get_state(u: str)           -> Optional[str]: return await kv_get(_ks(u))
async def clear_state(u: str)         -> None: await kv_del(_ks(u))


# ── Delete queue (Redis-persisted, processed by /api/tick) ───────────────────

async def enqueue_delete(u: str, chat_id: int, msg_ids: List[int], delay: int) -> None:
    """Push deletion jobs into Redis list. Processed by tick()."""
    at = int(time.time()) + delay
    for mid in msg_ids:
        entry = json.dumps({"chat_id": chat_id, "message_id": mid, "at": at})
        await kv_rpush(_kdq(u), entry)


async def process_delete_queue(u: str) -> None:
    """
    Process pending deletions for a user whose jobs are due.
    Called from /api/tick (Vercel Cron, every 10s) or inline after short delays.
    """
    key = _kdq(u)
    n   = await kv_llen(key)
    if n == 0:
        return
    items = await kv_lrange(key, 0, n - 1)
    now   = int(time.time())
    keep  = []
    for raw in items:
        try:
            job = json.loads(raw)
            if job["at"] <= now:
                await tg("deleteMessage", {"chat_id": job["chat_id"], "message_id": job["message_id"]})
            else:
                keep.append(raw)
        except Exception:
            pass  # message already deleted or invalid — skip
    # Rewrite list with only future jobs
    await kv_del(key)
    for item in keep:
        await kv_rpush(key, item)


# ── All active user hashes (for cron fan-out) ─────────────────────────────────

async def register_user(u: str) -> None:
    """Keep a set of active user hashes so the cron can iterate them."""
    await _kv(["SADD", "vam:users", _uh(u)])

async def all_user_hashes() -> List[str]:
    result = await _kv(["SMEMBERS", "vam:users"])
    return result or []


# ── Vercel AI Gateway ─────────────────────────────────────────────────────────

async def check_balance(key: str) -> Optional[Dict[str, float]]:
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{GATEWAY}/credits", headers={"Authorization": f"Bearer {key}"})
        if r.status_code == 200:
            d = r.json()
            return {"balance": float(d.get("balance", 0)), "used": float(d.get("total_used", 0))}
    except Exception:
        pass
    return None

async def best_key(u: str) -> Optional[Dict]:
    import asyncio
    keys = await get_keys(u)
    if not keys: return None
    bals = await asyncio.gather(*[check_balance(k) for k in keys])
    res  = [{"key": k, **b} for k, b in zip(keys, bals) if b]
    if not res: return None
    return max(res, key=lambda x: x["balance"])

async def all_balances(u: str) -> List[Dict]:
    import asyncio
    keys = await get_keys(u)
    if not keys: return []
    bals = await asyncio.gather(*[check_balance(k) for k in keys])
    out  = [{"key": k, "masked": f"{k[:8]}...{k[-6:]}", **(b or {"balance": None, "used": None})}
            for k, b in zip(keys, bals)]
    out.sort(key=lambda x: (x["balance"] or -1), reverse=True)
    return out


# ── Telegram helpers ──────────────────────────────────────────────────────────

async def tg(method: str, data: dict) -> dict:
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"{TG}/{method}", json=data)
    return r.json()

async def send(chat_id: int, text: str, markup=None, mode="Markdown") -> Optional[int]:
    p: dict = {"chat_id": chat_id, "text": text, "parse_mode": mode}
    if markup: p["reply_markup"] = markup
    r = await tg("sendMessage", p)
    return r["result"]["message_id"] if r.get("ok") else None

async def delete_msg(chat_id: int, msg_id: int) -> None:
    await tg("deleteMessage", {"chat_id": chat_id, "message_id": msg_id})

async def answer_cb(cq_id: str, text: str = "") -> None:
    await tg("answerCallbackQuery", {"callback_query_id": cq_id, "text": text})


# ── Keyboards ─────────────────────────────────────────────────────────────────

def reply_kb() -> dict:
    """
    Persistent bottom Reply Keyboard — always visible, like the reference bot.
    resize_keyboard=True  → compact size
    """
    return {
        "keyboard": [
            [{"text": BTN_ADD},     {"text": BTN_TOTAL}],
            [{"text": BTN_REMOVE},  {"text": BTN_GET}],
            [{"text": BTN_DASHBOARD}],
        ],
        "resize_keyboard": True,
        "persistent": True,
        "input_field_placeholder": "Select an option…",
    }

def remove_kb() -> dict:
    """Temporarily hide the keyboard (shown while awaiting text input)."""
    return {"remove_keyboard": True}


# ── Access control ────────────────────────────────────────────────────────────

def allowed(username: Optional[str]) -> bool:
    if not username: return False
    return username.lower().lstrip("@") in ALLOWED


# ── Handlers ──────────────────────────────────────────────────────────────────

async def on_start(chat_id: int, username: str, user_msg_id: int) -> None:
    await clear_state(username)
    await register_user(username)
    # Delete the user's /start message to keep chat clean
    await delete_msg(chat_id, user_msg_id)
    await send(
        chat_id,
        "👋 *Vercel API Manager*\n\n"
        "Manage your Vercel AI Gateway API keys securely.\n"
        "Your keys are *encrypted* — only you can access them.\n\n"
        "Use the buttons below:",
        markup=reply_kb(),
    )


async def on_add(chat_id: int, username: str) -> None:
    await set_state(username, "awaiting_add")
    prompt_id = await send(
        chat_id,
        "🔑 *Add API Key*\n\n"
        "Send your Vercel AI Gateway API key.\n"
        "Format: `vck_...`\n\n"
        "_Your key message will be deleted immediately after saving._",
    )
    if prompt_id:
        await enqueue_delete(username, chat_id, [prompt_id], delay=90)


async def on_total(chat_id: int, username: str, user_msg_id: int) -> None:
    await delete_msg(chat_id, user_msg_id)
    keys = await get_keys(username)
    n    = len(keys)
    mid  = await send(
        chat_id,
        f"📊 *Total API Keys*\n\n"
        f"You have *{n}* API key{'s' if n != 1 else ''} stored.\n\n"
        f"_Auto-deletes in {DEL_SEC}s._",
    )
    if mid:
        await enqueue_delete(username, chat_id, [mid], delay=DEL_SEC)


async def on_remove(chat_id: int, username: str) -> None:
    await set_state(username, "awaiting_remove")
    prompt_id = await send(
        chat_id,
        "🗑 *Remove API Key*\n\n"
        "Send the full API key you want to delete.\n\n"
        "_Prompt auto-deletes in 90s if no response._",
    )
    if prompt_id:
        await enqueue_delete(username, chat_id, [prompt_id], delay=90)


async def on_get(chat_id: int, username: str, user_msg_id: int) -> None:
    await delete_msg(chat_id, user_msg_id)
    loading = await send(chat_id, "⏳ Checking balances for all keys…")

    bk      = await best_key(username)
    to_del  = [loading] if loading else []

    if bk is None:
        mid = await send(
            chat_id,
            "❌ No keys available or balance check failed.\n"
            "Add keys first using *Add Key*.",
        )
    else:
        mid = await send(
            chat_id,
            f"🔑 *Best Key — Highest Balance*\n\n"
            f"`{bk['key']}`\n\n"
            f"💰 Balance : *${bk['balance']:.4f}*\n"
            f"📉 Used    : *${bk['used']:.4f}*\n\n"
            f"_Auto-deletes in {DEL_SEC}s._",
        )
    if mid: to_del.append(mid)
    await enqueue_delete(username, chat_id, [i for i in to_del if i], delay=DEL_SEC)


async def on_dashboard(chat_id: int, username: str, user_msg_id: int) -> None:
    await delete_msg(chat_id, user_msg_id)
    loading = await send(chat_id, "⏳ Building dashboard…")

    data    = await all_balances(username)
    to_del  = [loading] if loading else []

    if not data:
        mid = await send(
            chat_id,
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
        mid = await send(chat_id, "\n".join(lines))

    if mid: to_del.append(mid)
    await enqueue_delete(username, chat_id, [i for i in to_del if i], delay=DEL_SEC)


# ── Text input (state machine) ────────────────────────────────────────────────

async def on_text(chat_id: int, username: str, text: str, user_msg_id: int) -> None:
    state = await get_state(username)
    if not state:
        return  # unknown free text — ignore

    await clear_state(username)

    if state == "awaiting_add":
        key = text.strip()
        if not (key.startswith("vck_") and len(key) >= 20):
            err = await send(
                chat_id,
                "❌ Invalid format.\n"
                "Vercel AI Gateway keys start with `vck_` (60+ chars).",
            )
            ids = [user_msg_id] + ([err] if err else [])
            await enqueue_delete(username, chat_id, ids, delay=ADD_SEC)
            return

        ok  = await add_key(username, key)
        msg = "✅ *Done!* Key saved and encrypted." if ok else "⚠️ Key already exists in your vault."
        done = await send(chat_id, msg)
        ids  = [user_msg_id] + ([done] if done else [])
        await enqueue_delete(username, chat_id, ids, delay=ADD_SEC)

    elif state == "awaiting_remove":
        key     = text.strip()
        ok      = await remove_key(username, key)
        msg     = "✅ *Done!* API key removed." if ok else "❌ Key not found in your vault."
        done    = await send(chat_id, msg)
        ids     = [user_msg_id] + ([done] if done else [])
        await enqueue_delete(username, chat_id, ids, delay=DEL_SEC)


# ── Main dispatcher ───────────────────────────────────────────────────────────

BUTTON_HANDLERS = {
    BTN_ADD:       on_add,
    BTN_TOTAL:     on_total,
    BTN_REMOVE:    on_remove,
    BTN_GET:       on_get,
    BTN_DASHBOARD: on_dashboard,
}

async def handle_update(update: dict) -> None:
    if "message" not in update:
        return

    msg      = update["message"]
    chat_id  = msg["chat"]["id"]
    msg_id   = msg["message_id"]
    username = msg.get("from", {}).get("username", "")
    text     = msg.get("text", "").strip()

    if not allowed(username):
        return  # silent reject

    # ── Process any pending auto-deletes FIRST (on every interaction) ─────────
    # This is the serverless-safe way: delete due messages at the start of
    # the next request instead of waiting in a background coroutine.
    await process_delete_queue(username)

    # /start command
    if text.startswith("/start"):
        await on_start(chat_id, username, msg_id)
        return

    # Bottom reply keyboard buttons
    if text == BTN_ADD:
        await on_add(chat_id, username)
        return

    if text == BTN_TOTAL:
        await on_total(chat_id, username, msg_id)
        return

    if text == BTN_REMOVE:
        await on_remove(chat_id, username)
        return

    if text == BTN_GET:
        await on_get(chat_id, username, msg_id)
        return

    if text == BTN_DASHBOARD:
        await on_dashboard(chat_id, username, msg_id)
        return

    # Free text → state machine
    if text:
        await on_text(chat_id, username, text, msg_id)


# ── Cron tick — called by /api/tick every ~10s via Vercel Cron ───────────────

async def tick() -> None:
    """Process all pending delete queues for all registered users."""
    hashes = await all_user_hashes()
    for uh in hashes:
        key = f"vam:delq:{uh}"
        n   = await kv_llen(key)
        if n == 0:
            continue
        items = await kv_lrange(key, 0, n - 1)
        now   = int(time.time())
        keep  = []
        for raw in items:
            try:
                job = json.loads(raw)
                if job["at"] <= now:
                    await tg("deleteMessage", {
                        "chat_id":    job["chat_id"],
                        "message_id": job["message_id"],
                    })
                else:
                    keep.append(raw)
            except Exception:
                pass
        await kv_del(key)
        for item in keep:
            await kv_rpush(key, item)
