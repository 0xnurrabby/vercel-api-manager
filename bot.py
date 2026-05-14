"""
Vercel API Manager Bot
======================
- Every action instantly deletes ALL previous bot+user messages
- Dashboard pagination (10/page, Prev/Next, page X/Y)
- Copy Key: send prefix -> get full key monospace
- Export: Yes/No confirm -> .txt file (cleared on next action)
- Parallel async calls, shared httpx client
"""

from __future__ import annotations
import os, json, hashlib, hmac, base64, asyncio
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

TG_API    = f"https://api.telegram.org/bot{BOT_TOKEN}"
GATEWAY   = "https://ai-gateway.vercel.sh/v1"
PAGE_SIZE = 10

BTN_ADD       = "➕ Add Key"
BTN_TOTAL     = "📊 Total APIs"
BTN_REMOVE    = "🗑 Remove Key"
BTN_GET       = "🔑 Get Best Key"
BTN_DASHBOARD = "📈 Dashboard"
BTN_COPY      = "🔍 Copy Key"
BTN_EXPORT    = "📤 Export Keys"


# ── HTTP client ───────────────────────────────────────────────────────────────
def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=8, limits=httpx.Limits(max_connections=30))


# ── Upstash Redis REST ────────────────────────────────────────────────────────
async def _kv(cl: httpx.AsyncClient, cmd: list) -> Any:
    parts = "/".join(
        str(c).replace("/", "%2F").replace(" ", "%20").replace("+", "%2B")
        for c in cmd
    )
    r = await cl.get(f"{KV_URL}/{parts}", headers={"Authorization": f"Bearer {KV_TOKEN}"})
    d = r.json()
    if "error" in d:
        raise RuntimeError(f"KV: {d['error']}")
    return d.get("result")

async def kv_get(cl, k):          return await _kv(cl, ["GET", k])
async def kv_set(cl, k, v):       await _kv(cl, ["SET", k, v])
async def kv_setex(cl, k, t, v):  await _kv(cl, ["SETEX", k, t, v])
async def kv_del(cl, k):          await _kv(cl, ["DEL", k])
async def kv_sadd(cl, k, v):      await _kv(cl, ["SADD", k, v])


# ── Encryption ────────────────────────────────────────────────────────────────
def _fernet(u: str) -> Fernet:
    raw = hmac.new(MASTER_KEY.encode(), u.lower().encode(), hashlib.sha256).digest()
    return Fernet(base64.urlsafe_b64encode(raw))

def enc(u, v): return _fernet(u).encrypt(v.encode()).decode()
def dec(u, v): return _fernet(u).decrypt(v.encode()).decode()


# ── Redis key helpers ─────────────────────────────────────────────────────────
def _uh(u):   return hashlib.sha256(u.lower().encode()).hexdigest()[:16]
def _kk(u):   return f"vam:keys:{_uh(u)}"
def _ks(u):   return f"vam:state:{_uh(u)}"
def _km(u):   return f"vam:msgs:{_uh(u)}"    # tracked message IDs
def _kdash(u):return f"vam:dash:{_uh(u)}"    # dashboard cache


# ── API key CRUD ──────────────────────────────────────────────────────────────
async def get_keys(cl, u) -> List[str]:
    raw = await kv_get(cl, _kk(u))
    if not raw: return []
    return [dec(u, e) for e in json.loads(raw)]

async def save_keys(cl, u, keys):
    await kv_set(cl, _kk(u), json.dumps([enc(u, k) for k in keys]))

async def add_key(cl, u, key) -> bool:
    keys = await get_keys(cl, u)
    if key in keys: return False
    keys.append(key); await save_keys(cl, u, keys); return True

async def remove_key(cl, u, key) -> bool:
    keys = await get_keys(cl, u)
    if key not in keys: return False
    keys.remove(key); await save_keys(cl, u, keys); return True


# ── State machine ─────────────────────────────────────────────────────────────
async def set_state(cl, u, s):  await kv_setex(cl, _ks(u), 300, s)
async def get_state(cl, u):     return await kv_get(cl, _ks(u))
async def clear_state(cl, u):   await kv_del(cl, _ks(u))


# ── Message tracker ───────────────────────────────────────────────────────────
# Stores list of message IDs (both bot and user) to delete on next action.

async def _track(cl, u: str, chat_id: int, *msg_ids: Optional[int]) -> None:
    """Add message IDs to the tracked list for this user."""
    valid = [m for m in msg_ids if m]
    if not valid:
        return
    raw  = await kv_get(cl, _km(u))
    data = json.loads(raw) if raw else {"chat_id": chat_id, "ids": []}
    data["ids"].extend(valid)
    data["ids"] = list(set(data["ids"]))   # dedup
    await kv_setex(cl, _km(u), 3600, json.dumps(data))


async def _clear_all(cl, u: str) -> None:
    """
    Instantly delete ALL tracked messages and clear the tracker.
    Called at the start of every action so the chat is always clean.
    """
    raw = await kv_get(cl, _km(u))
    if not raw:
        return
    data = json.loads(raw)
    chat_id  = data.get("chat_id")
    msg_ids  = data.get("ids", [])
    # Delete all in parallel, ignore errors (already deleted etc.)
    await kv_del(cl, _km(u))
    if chat_id and msg_ids:
        await asyncio.gather(
            *[_tg_delete(cl, chat_id, mid) for mid in msg_ids],
            return_exceptions=True,
        )


async def _tg_delete(cl, chat_id: int, msg_id: int) -> None:
    try:
        await cl.post(f"{TG_API}/deleteMessage",
                      json={"chat_id": chat_id, "message_id": msg_id})
    except Exception:
        pass


# ── Vercel AI Gateway ─────────────────────────────────────────────────────────
async def check_balance(cl, key: str) -> Optional[Dict]:
    try:
        r = await cl.get(f"{GATEWAY}/credits", headers={"Authorization": f"Bearer {key}"})
        if r.status_code == 200:
            d = r.json()
            return {"balance": float(d.get("balance", 0)), "used": float(d.get("total_used", 0))}
    except Exception:
        pass
    return None

async def best_key(cl, u: str) -> Optional[Dict]:
    keys = await get_keys(cl, u)
    if not keys: return None
    bals = await asyncio.gather(*[check_balance(cl, k) for k in keys])
    res  = [{"key": k, **b} for k, b in zip(keys, bals) if b]
    if not res: return None
    return max(res, key=lambda x: x["balance"])

async def all_balances(cl, u: str) -> List[Dict]:
    keys = await get_keys(cl, u)
    if not keys: return []
    bals = await asyncio.gather(*[check_balance(cl, k) for k in keys])
    out  = []
    for k, b in zip(keys, bals):
        out.append({"key": k, "masked": f"{k[:8]}...{k[-6:]}",
                    "balance": b["balance"] if b else None,
                    "used":    b["used"]    if b else None})
    out.sort(key=lambda x: (x["balance"] or -1), reverse=True)
    return out


# ── Telegram helpers ──────────────────────────────────────────────────────────
async def tg(cl, method: str, data: dict) -> dict:
    r = await cl.post(f"{TG_API}/{method}", json=data)
    return r.json()

async def send(cl, chat_id: int, text: str,
               markup=None, mode="Markdown") -> Optional[int]:
    p: dict = {"chat_id": chat_id, "text": text, "parse_mode": mode}
    if markup: p["reply_markup"] = markup
    r = await tg(cl, "sendMessage", p)
    return r["result"]["message_id"] if r.get("ok") else None

async def send_doc(cl, chat_id: int, filename: str, content: bytes) -> Optional[int]:
    import io
    r = await cl.post(f"{TG_API}/sendDocument",
                      data={"chat_id": str(chat_id)},
                      files={"document": (filename, io.BytesIO(content), "text/plain")})
    d = r.json()
    return d["result"]["message_id"] if d.get("ok") else None

async def edit_msg(cl, chat_id: int, msg_id: int, text: str, markup=None) -> None:
    p: dict = {"chat_id": chat_id, "message_id": msg_id,
                "text": text, "parse_mode": "Markdown"}
    if markup: p["reply_markup"] = markup
    try: await tg(cl, "editMessageText", p)
    except Exception: pass


# ── Access control ────────────────────────────────────────────────────────────
def allowed(username) -> bool:
    return bool(username) and username.lower().lstrip("@") in ALLOWED


# ── Keyboards ─────────────────────────────────────────────────────────────────
def reply_kb() -> dict:
    return {
        "keyboard": [
            [{"text": BTN_ADD},    {"text": BTN_TOTAL}],
            [{"text": BTN_REMOVE}, {"text": BTN_GET}],
            [{"text": BTN_DASHBOARD}, {"text": BTN_COPY}],
            [{"text": BTN_EXPORT}],
        ],
        "resize_keyboard": True,
        "persistent": True,
        "input_field_placeholder": "Select an option…",
    }

def dash_nav_kb(page: int, total: int) -> dict:
    row = []
    if page > 1:
        row.append({"text": "◀ Prev", "callback_data": f"dash:{page-1}"})
    row.append({"text": f"📄 {page}/{total}", "callback_data": "noop"})
    if page < total:
        row.append({"text": "Next ▶", "callback_data": f"dash:{page+1}"})
    return {"inline_keyboard": [row]}

def export_kb() -> dict:
    return {"inline_keyboard": [[
        {"text": "✅ Yes, Export", "callback_data": "export:yes"},
        {"text": "❌ No",          "callback_data": "export:no"},
    ]]}


# ── Dashboard page text ───────────────────────────────────────────────────────
def dash_page(data, page, total_pages, total_bal, total_used, reachable, username) -> str:
    start  = (page - 1) * PAGE_SIZE
    chunk  = data[start: start + PAGE_SIZE]
    lines  = [
        "📈 *Vercel API Manager — Dashboard*", "",
        f"👤 User       : @{username}",
        f"🔢 Total Keys : *{len(data)}*",
        f"✅ Reachable  : *{reachable}/{len(data)}*",
        f"💰 Total Bal  : *${total_bal:.4f}*",
        f"📉 Total Used : *${total_used:.4f}*", "",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"*Keys (page {page}/{total_pages})*", "",
    ]
    for i, d in enumerate(chunk, start + 1):
        icon = "🟢" if d["balance"] is not None else "🔴"
        bal  = f"${d['balance']:.4f}" if d["balance"] is not None else "N/A"
        used = f"${d['used']:.4f}"    if d["used"]    is not None else "N/A"
        lines += [f"{icon} *#{i}* `{d['masked']}`",
                  f"   Balance: {bal}  |  Used: {used}", ""]
    return "\n".join(lines)


# ── Handlers ──────────────────────────────────────────────────────────────────

async def on_start(cl, chat_id: int, u: str) -> None:
    # /start does NOT clear previous messages — keyboard stays,
    # but we do register the user and reset state
    try:
        await asyncio.gather(
            clear_state(cl, u),
            kv_sadd(cl, "vam:users", _uh(u)),
        )
    except Exception:
        pass
    mid = await send(cl, chat_id,
        "🔐 *Vercel API Manager*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Welcome, @" + u + "!\n\n"
        "This bot helps you manage your *Vercel AI Gateway*\n"
        "API keys safely and efficiently.\n\n"
        "✦ Keys are *AES-256 encrypted*\n"
        "✦ Only *you* can access your keys\n"
        "✦ Always gets the *highest balance* key\n"
        "✦ Messages *auto-delete* on next action\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Select an option from the menu below:",
        markup=reply_kb(),
    )
    await _track(cl, u, chat_id, mid)


async def on_add(cl, chat_id: int, u: str, user_msg_id: int) -> None:
    await asyncio.gather(
        _clear_all(cl, u),
        set_state(cl, u, "awaiting_add"),
    )
    mid = await send(cl, chat_id,
        "🔑 *Add API Key*\n\n"
        "Send your Vercel AI Gateway API key now.\n"
        "Format: `vck_...`\n\n"
        "_Your key message will be deleted right after saving._",
    )
    await _track(cl, u, chat_id, mid, user_msg_id)


async def on_total(cl, chat_id: int, u: str, user_msg_id: int) -> None:
    keys, _ = await asyncio.gather(
        get_keys(cl, u),
        _clear_all(cl, u),
    )
    n   = len(keys)
    mid = await send(cl, chat_id,
        f"📊 *Total API Keys*\n\n"
        f"You have *{n}* API key{'s' if n != 1 else ''} stored.\n\n"
        "_Clears on next action._",
    )
    await _track(cl, u, chat_id, mid, user_msg_id)


async def on_remove(cl, chat_id: int, u: str, user_msg_id: int) -> None:
    await asyncio.gather(
        _clear_all(cl, u),
        set_state(cl, u, "awaiting_remove"),
    )
    mid = await send(cl, chat_id,
        "🗑 *Remove API Key*\n\n"
        "Send the full API key you want to delete.\n\n"
        "_Your message will be deleted after action._",
    )
    await _track(cl, u, chat_id, mid, user_msg_id)


async def on_get(cl, chat_id: int, u: str, user_msg_id: int) -> None:
    loading_id, _ = await asyncio.gather(
        send(cl, chat_id, "⏳ Checking balances…"),
        _clear_all(cl, u),
    )
    bk = await best_key(cl, u)
    await _tg_delete(cl, chat_id, loading_id)

    if bk is None:
        mid = await send(cl, chat_id,
            "❌ No keys available or balance check failed.\n"
            "Add keys first using *Add Key*.")
    else:
        mid = await send(cl, chat_id,
            f"🔑 *Best Key — Highest Balance*\n\n"
            f"`{bk['key']}`\n\n"
            f"💰 Balance : *${bk['balance']:.4f}*\n"
            f"📉 Used    : *${bk['used']:.4f}*\n\n"
            "_Clears on next action._")
    await _track(cl, u, chat_id, mid, user_msg_id)


async def on_dashboard(cl, chat_id: int, u: str, user_msg_id: int, page: int = 1) -> None:
    loading_id, _ = await asyncio.gather(
        send(cl, chat_id, "⏳ Building dashboard…"),
        _clear_all(cl, u),
    )
    data = await all_balances(cl, u)
    await _tg_delete(cl, chat_id, loading_id)

    if not data:
        mid = await send(cl, chat_id,
            "📈 *Dashboard*\n\nNo API keys found.\nUse *Add Key* to get started.")
        await _track(cl, u, chat_id, mid, user_msg_id)
        return

    total_bal   = sum(d["balance"] or 0 for d in data)
    total_used  = sum(d["used"]    or 0 for d in data)
    reachable   = sum(1 for d in data if d["balance"] is not None)
    total_pages = max(1, (len(data) + PAGE_SIZE - 1) // PAGE_SIZE)
    page        = max(1, min(page, total_pages))

    text = dash_page(data, page, total_pages, total_bal, total_used, reachable, u)
    mid  = await send(cl, chat_id, text, markup=dash_nav_kb(page, total_pages))
    await _track(cl, u, chat_id, mid, user_msg_id)

    # Cache for Prev/Next navigation
    await kv_setex(cl, _kdash(u), 300, json.dumps({
        "data": data, "total_bal": total_bal, "total_used": total_used,
        "reachable": reachable, "total_pages": total_pages,
        "msg_id": mid, "username": u, "chat_id": chat_id,
    }))


async def on_copy(cl, chat_id: int, u: str, user_msg_id: int) -> None:
    await asyncio.gather(
        _clear_all(cl, u),
        set_state(cl, u, "awaiting_copy"),
    )
    mid = await send(cl, chat_id,
        "🔍 *Copy Key*\n\n"
        "Send the *prefix* shown in the dashboard.\n"
        "Example: `vck_4b0e`\n\n"
        "_Your message will be deleted after lookup._",
    )
    await _track(cl, u, chat_id, mid, user_msg_id)


async def on_export(cl, chat_id: int, u: str, user_msg_id: int) -> None:
    await asyncio.gather(
        _clear_all(cl, u),
        set_state(cl, u, "awaiting_export_confirm"),
    )
    mid = await send(cl, chat_id,
        "📤 *Export API Keys*\n\n"
        "This will send a `.txt` file with all your keys.\n"
        "Do you want to continue?",
        markup=export_kb(),
    )
    await _track(cl, u, chat_id, mid, user_msg_id)


# ── Callback query handler ────────────────────────────────────────────────────
async def handle_callback(cl, chat_id: int, u: str, msg_id: int,
                          data: str, cq_id: str) -> None:
    await tg(cl, "answerCallbackQuery", {"callback_query_id": cq_id})

    if data.startswith("dash:"):
        page = int(data.split(":")[1])
        raw  = await kv_get(cl, _kdash(u))
        if not raw: return
        cache       = json.loads(raw)
        total_pages = cache["total_pages"]
        page        = max(1, min(page, total_pages))
        text        = dash_page(
            cache["data"], page, total_pages,
            cache["total_bal"], cache["total_used"],
            cache["reachable"], cache["username"],
        )
        await edit_msg(cl, chat_id, msg_id, text, markup=dash_nav_kb(page, total_pages))

    elif data == "noop":
        pass

    elif data == "export:yes":
        await asyncio.gather(
            _clear_all(cl, u),
            clear_state(cl, u),
        )
        keys = await get_keys(cl, u)
        if not keys:
            mid = await send(cl, chat_id, "❌ No keys to export.")
            await _track(cl, u, chat_id, mid)
            return
        content = "\n".join(keys).encode("utf-8")
        mid = await send_doc(cl, chat_id,
                             filename=f"vercel_keys_{u}.txt",
                             content=content)
        await _track(cl, u, chat_id, mid)

    elif data == "export:no":
        await asyncio.gather(
            _clear_all(cl, u),
            clear_state(cl, u),
        )


# ── Text input (state machine) ────────────────────────────────────────────────
async def on_text(cl, chat_id: int, u: str, text: str, user_msg_id: int) -> None:
    state = await get_state(cl, u)
    if not state:
        return

    # Clear everything including user's own message
    await asyncio.gather(
        _clear_all(cl, u),
        clear_state(cl, u),
    )

    if state == "awaiting_add":
        key = text.strip()
        if not (key.startswith("vck_") and len(key) >= 20):
            mid = await send(cl, chat_id,
                "❌ Invalid format.\n"
                "Vercel keys start with `vck_` and are 60+ chars.")
            await _track(cl, u, chat_id, mid, user_msg_id)
            return
        ok  = await add_key(cl, u, key)
        mid = await send(cl, chat_id,
            "✅ *Done!* Key saved and encrypted." if ok else "⚠️ Key already exists.")
        # Track only done message — user's key message already deleted via _clear_all
        await _track(cl, u, chat_id, mid, user_msg_id)

    elif state == "awaiting_remove":
        key = text.strip()
        ok  = await remove_key(cl, u, key)
        mid = await send(cl, chat_id,
            "✅ *Done!* API key removed." if ok else "❌ Key not found in your vault.")
        await _track(cl, u, chat_id, mid, user_msg_id)

    elif state == "awaiting_copy":
        prefix = text.strip()
        keys   = await get_keys(cl, u)
        match  = next((k for k in keys if k.startswith(prefix)), None)
        if match is None:
            mid = await send(cl, chat_id,
                f"❌ No key found starting with `{prefix}`.\n"
                "Check the prefix from the dashboard.")
        else:
            bal  = await check_balance(cl, match)
            bstr = f"${bal['balance']:.4f}" if bal else "N/A"
            ustr = f"${bal['used']:.4f}"    if bal else "N/A"
            mid  = await send(cl, chat_id,
                f"🔑 *Full API Key*\n\n"
                f"`{match}`\n\n"
                f"💰 Balance : *{bstr}*\n"
                f"📉 Used    : *{ustr}*\n\n"
                "_Clears on next action._")
        await _track(cl, u, chat_id, mid, user_msg_id)

    elif state == "awaiting_export_confirm":
        # User typed instead of tapping button — just clear
        pass


# ── Main dispatcher ───────────────────────────────────────────────────────────
async def handle_update(update: dict) -> None:

    # Callback query (inline button)
    if "callback_query" in update:
        cq       = update["callback_query"]
        username = cq["from"].get("username", "")
        chat_id  = cq["message"]["chat"]["id"]
        msg_id   = cq["message"]["message_id"]
        data     = cq.get("data", "")
        cq_id    = cq["id"]
        if not allowed(username): return
        try:
            async with _client() as cl:
                await handle_callback(cl, chat_id, username, msg_id, data, cq_id)
        except Exception:
            pass
        return

    # Regular message
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
        async with _client() as cl:
            if text.startswith("/start"):
                await on_start(cl, chat_id, username)
                return

            handlers = {
                BTN_ADD:       on_add,
                BTN_TOTAL:     on_total,
                BTN_REMOVE:    on_remove,
                BTN_GET:       on_get,
                BTN_DASHBOARD: lambda cl, c, u, m: on_dashboard(cl, c, u, m, page=1),
                BTN_COPY:      on_copy,
                BTN_EXPORT:    on_export,
            }
            fn = handlers.get(text)
            if fn:
                await fn(cl, chat_id, username, msg_id)
                return

            if text:
                await on_text(cl, chat_id, username, text, msg_id)
    except Exception:
        try:
            async with _client() as cl:
                await send(cl, chat_id,
                    "Something went wrong. Please try again.")
        except Exception:
            pass
