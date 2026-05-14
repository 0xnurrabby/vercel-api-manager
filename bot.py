"""
Vercel API Manager Bot
======================
Features:
- Dashboard with pagination (10/page, Prev/Next, page X/Y)
- Copy Key: send masked prefix, get full key in monospace
- Export: Yes/No confirm -> sends .txt file with all keys
- All Redis + Telegram calls run in parallel
- threading.Timer for true background auto-delete
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

DEL_SEC      = 30
ADD_SEC      = 4
PAGE_SIZE    = 10

BTN_ADD       = "➕ Add Key"
BTN_TOTAL     = "📊 Total APIs"
BTN_REMOVE    = "🗑 Remove Key"
BTN_GET       = "🔑 Get Best Key"
BTN_DASHBOARD = "📈 Dashboard"
BTN_COPY      = "🔍 Copy Key"
BTN_EXPORT    = "📤 Export Keys"


# ── HTTP client ───────────────────────────────────────────────────────────────
def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=8, limits=httpx.Limits(max_connections=20))


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

async def kv_get(cl: httpx.AsyncClient, k: str) -> Optional[str]:   return await _kv(cl, ["GET", k])
async def kv_set(cl: httpx.AsyncClient, k: str, v: str) -> None:    await _kv(cl, ["SET", k, v])
async def kv_setex(cl: httpx.AsyncClient, k: str, t: int, v: str) -> None: await _kv(cl, ["SETEX", k, t, v])
async def kv_del(cl: httpx.AsyncClient, k: str) -> None:            await _kv(cl, ["DEL", k])
async def kv_sadd(cl: httpx.AsyncClient, k: str, v: str) -> None:   await _kv(cl, ["SADD", k, v])


# ── Encryption ────────────────────────────────────────────────────────────────
def _fernet(u: str) -> Fernet:
    raw = hmac.new(MASTER_KEY.encode(), u.lower().encode(), hashlib.sha256).digest()
    return Fernet(base64.urlsafe_b64encode(raw))

def enc(u: str, v: str) -> str: return _fernet(u).encrypt(v.encode()).decode()
def dec(u: str, v: str) -> str: return _fernet(u).decrypt(v.encode()).decode()


# ── Redis key helpers ─────────────────────────────────────────────────────────
def _uh(u: str) -> str: return hashlib.sha256(u.lower().encode()).hexdigest()[:16]
def _kk(u: str) -> str: return f"vam:keys:{_uh(u)}"
def _ks(u: str) -> str: return f"vam:state:{_uh(u)}"


# ── API key CRUD ──────────────────────────────────────────────────────────────
async def get_keys(cl: httpx.AsyncClient, u: str) -> List[str]:
    raw = await kv_get(cl, _kk(u))
    if not raw: return []
    return [dec(u, e) for e in json.loads(raw)]

async def save_keys(cl: httpx.AsyncClient, u: str, keys: List[str]) -> None:
    await kv_set(cl, _kk(u), json.dumps([enc(u, k) for k in keys]))

async def add_key(cl: httpx.AsyncClient, u: str, key: str) -> bool:
    keys = await get_keys(cl, u)
    if key in keys: return False
    keys.append(key); await save_keys(cl, u, keys); return True

async def remove_key(cl: httpx.AsyncClient, u: str, key: str) -> bool:
    keys = await get_keys(cl, u)
    if key not in keys: return False
    keys.remove(key); await save_keys(cl, u, keys); return True


# ── State machine ─────────────────────────────────────────────────────────────
async def set_state(cl: httpx.AsyncClient, u: str, s: str) -> None:
    await kv_setex(cl, _ks(u), 300, s)

async def get_state(cl: httpx.AsyncClient, u: str) -> Optional[str]:
    return await kv_get(cl, _ks(u))

async def clear_state(cl: httpx.AsyncClient, u: str) -> None:
    await kv_del(cl, _ks(u))


# ── Auto-delete via threading.Timer ──────────────────────────────────────────
def _fire_delete(chat_id: int, msg_ids: List[int]) -> None:
    import urllib.request
    for mid in msg_ids:
        try:
            body = json.dumps({"chat_id": chat_id, "message_id": mid}).encode()
            req  = urllib.request.Request(
                f"{TG_API}/deleteMessage", data=body,
                headers={"Content-Type": "application/json"}, method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            pass

def schedule_delete(chat_id: int, msg_ids: List[int], delay: int = DEL_SEC) -> None:
    valid = [m for m in msg_ids if m]
    if not valid: return
    t = threading.Timer(delay, _fire_delete, args=(chat_id, valid))
    t.daemon = True
    t.start()


# ── Vercel AI Gateway ─────────────────────────────────────────────────────────
async def check_balance(cl: httpx.AsyncClient, key: str) -> Optional[Dict]:
    try:
        r = await cl.get(f"{GATEWAY}/credits", headers={"Authorization": f"Bearer {key}"})
        if r.status_code == 200:
            d = r.json()
            return {"balance": float(d.get("balance", 0)), "used": float(d.get("total_used", 0))}
    except Exception:
        pass
    return None

async def best_key(cl: httpx.AsyncClient, u: str) -> Optional[Dict]:
    import asyncio
    keys = await get_keys(cl, u)
    if not keys: return None
    bals = await asyncio.gather(*[check_balance(cl, k) for k in keys])
    res  = [{"key": k, **b} for k, b in zip(keys, bals) if b]
    if not res: return None
    return max(res, key=lambda x: x["balance"])

async def all_balances(cl: httpx.AsyncClient, u: str) -> List[Dict]:
    import asyncio
    keys = await get_keys(cl, u)
    if not keys: return []
    bals = await asyncio.gather(*[check_balance(cl, k) for k in keys])
    out  = []
    for k, b in zip(keys, bals):
        out.append({
            "key":     k,
            "masked":  f"{k[:8]}...{k[-6:]}",
            "balance": b["balance"] if b else None,
            "used":    b["used"]    if b else None,
        })
    out.sort(key=lambda x: (x["balance"] or -1), reverse=True)
    return out


# ── Telegram helpers ──────────────────────────────────────────────────────────
async def tg_post(cl: httpx.AsyncClient, method: str, data: dict) -> dict:
    r = await cl.post(f"{TG_API}/{method}", json=data)
    return r.json()

async def send(cl: httpx.AsyncClient, chat_id: int, text: str,
               markup=None, mode: str = "Markdown") -> Optional[int]:
    p: dict = {"chat_id": chat_id, "text": text, "parse_mode": mode}
    if markup: p["reply_markup"] = markup
    r = await tg_post(cl, "sendMessage", p)
    return r["result"]["message_id"] if r.get("ok") else None

async def send_document(cl: httpx.AsyncClient, chat_id: int,
                        filename: str, content: bytes, caption: str = "") -> Optional[int]:
    """Send a text file as document."""
    import io
    files = {"document": (filename, io.BytesIO(content), "text/plain")}
    data  = {"chat_id": str(chat_id)}
    if caption: data["caption"] = caption
    r = await cl.post(f"{TG_API}/sendDocument", data=data, files=files)
    d = r.json()
    return d["result"]["message_id"] if d.get("ok") else None

async def edit_msg(cl: httpx.AsyncClient, chat_id: int, msg_id: int,
                   text: str, markup=None) -> None:
    p: dict = {"chat_id": chat_id, "message_id": msg_id,
                "text": text, "parse_mode": "Markdown"}
    if markup: p["reply_markup"] = markup
    try:
        await tg_post(cl, "editMessageText", p)
    except Exception:
        pass

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
            [{"text": BTN_DASHBOARD}, {"text": BTN_COPY}],
            [{"text": BTN_EXPORT}],
        ],
        "resize_keyboard": True,
        "persistent": True,
        "input_field_placeholder": "Select an option…",
    }

def dash_nav_kb(page: int, total_pages: int) -> dict:
    """Inline Prev/Next buttons for dashboard pagination."""
    row = []
    if page > 1:
        row.append({"text": "◀ Prev", "callback_data": f"dash:{page-1}"})
    row.append({"text": f"📄 {page}/{total_pages}", "callback_data": "noop"})
    if page < total_pages:
        row.append({"text": "Next ▶", "callback_data": f"dash:{page+1}"})
    return {"inline_keyboard": [row]}

def export_confirm_kb() -> dict:
    return {
        "inline_keyboard": [[
            {"text": "✅ Yes, Export", "callback_data": "export:yes"},
            {"text": "❌ No",          "callback_data": "export:no"},
        ]]
    }


# ── Dashboard page builder ────────────────────────────────────────────────────
def build_dash_page(data: List[Dict], page: int, total_pages: int,
                    total_bal: float, total_used: float,
                    reachable: int, username: str) -> str:
    start = (page - 1) * PAGE_SIZE
    end   = start + PAGE_SIZE
    slice_ = data[start:end]

    lines = [
        "📈 *Vercel API Manager — Dashboard*", "",
        f"👤 User       : @{username}",
        f"🔢 Total Keys : *{len(data)}*",
        f"✅ Reachable  : *{reachable}/{len(data)}*",
        f"💰 Total Bal  : *${total_bal:.4f}*",
        f"📉 Total Used : *${total_used:.4f}*", "",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"*Keys (page {page}/{total_pages})*", "",
    ]
    for i, d in enumerate(slice_, start + 1):
        icon = "🟢" if d["balance"] is not None else "🔴"
        bal  = f"${d['balance']:.4f}" if d["balance"] is not None else "N/A"
        used = f"${d['used']:.4f}"    if d["used"]    is not None else "N/A"
        lines += [
            f"{icon} *#{i}* `{d['masked']}`",
            f"   Balance: {bal}  |  Used: {used}", "",
        ]
    lines.append(f"_Auto-deletes in {DEL_SEC}s._")
    return "\n".join(lines)


# ── Handlers ──────────────────────────────────────────────────────────────────

async def on_start(cl: httpx.AsyncClient, chat_id: int, username: str, user_msg_id: int) -> None:
    import asyncio
    try:
        await asyncio.gather(
            clear_state(cl, username),
            kv_sadd(cl, "vam:users", _uh(username)),
        )
    except Exception:
        pass

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
    loading_id, _ = await asyncio.gather(
        send(cl, chat_id, "⏳ Checking balances…"),
        delete_msg(cl, chat_id, user_msg_id),
    )
    bk = await best_key(cl, username)
    await delete_msg(cl, chat_id, loading_id)

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
            f"_Auto-deletes in {DEL_SEC}s._")
    schedule_delete(chat_id, [mid])


async def on_dashboard(cl: httpx.AsyncClient, chat_id: int, username: str,
                       user_msg_id: int, page: int = 1) -> None:
    import asyncio
    loading_id, _ = await asyncio.gather(
        send(cl, chat_id, "⏳ Building dashboard…"),
        delete_msg(cl, chat_id, user_msg_id),
    )
    data = await all_balances(cl, username)
    await delete_msg(cl, chat_id, loading_id)

    if not data:
        mid = await send(cl, chat_id,
            "📈 *Dashboard*\n\nNo API keys found.\nUse *Add Key* to get started.")
        schedule_delete(chat_id, [mid])
        return

    total_bal   = sum(d["balance"] or 0 for d in data)
    total_used  = sum(d["used"]    or 0 for d in data)
    reachable   = sum(1 for d in data if d["balance"] is not None)
    total_pages = max(1, (len(data) + PAGE_SIZE - 1) // PAGE_SIZE)
    page        = max(1, min(page, total_pages))

    text = build_dash_page(data, page, total_pages, total_bal, total_used, reachable, username)
    mid  = await send(cl, chat_id, text, markup=dash_nav_kb(page, total_pages))
    schedule_delete(chat_id, [mid])

    # Cache dashboard data + page msg_id for callback navigation
    cache = json.dumps({
        "data": data, "total_bal": total_bal, "total_used": total_used,
        "reachable": reachable, "total_pages": total_pages,
        "msg_id": mid, "username": username,
    })
    await kv_setex(cl, f"vam:dash:{_uh(username)}", 120, cache)


async def on_copy_key(cl: httpx.AsyncClient, chat_id: int, username: str, user_msg_id: int) -> None:
    import asyncio
    await asyncio.gather(
        set_state(cl, username, "awaiting_copy"),
        delete_msg(cl, chat_id, user_msg_id),
    )
    prompt_id = await send(
        cl, chat_id,
        "🔍 *Copy Key*\n\n"
        "Send the *prefix* of the key you want to copy.\n"
        "You can find it in the dashboard.\n\n"
        "Example: `vck_4b0e`\n\n"
        "_Prompt auto-deletes in 90s._",
    )
    schedule_delete(chat_id, [prompt_id], delay=90)


async def on_export(cl: httpx.AsyncClient, chat_id: int, username: str, user_msg_id: int) -> None:
    import asyncio
    await asyncio.gather(
        set_state(cl, username, "awaiting_export_confirm"),
        delete_msg(cl, chat_id, user_msg_id),
    )
    mid = await send(
        cl, chat_id,
        "📤 *Export API Keys*\n\n"
        "This will send a `.txt` file with all your keys.\n"
        "Do you want to export?",
        markup=export_confirm_kb(),
    )
    # Store confirm msg_id so we can delete it after
    await kv_setex(cl, f"vam:exportmsg:{_uh(username)}", 120, str(mid))
    schedule_delete(chat_id, [mid], delay=120)


# ── Callback query handler ────────────────────────────────────────────────────

async def handle_callback(cl: httpx.AsyncClient, chat_id: int, username: str,
                          msg_id: int, data: str, cq_id: str) -> None:
    # Answer callback immediately to remove loading spinner
    await tg_post(cl, "answerCallbackQuery", {"callback_query_id": cq_id})

    # Dashboard pagination
    if data.startswith("dash:"):
        page = int(data.split(":")[1])
        raw  = await kv_get(cl, f"vam:dash:{_uh(username)}")
        if not raw:
            return
        cache       = json.loads(raw)
        total_pages = cache["total_pages"]
        page        = max(1, min(page, total_pages))
        text        = build_dash_page(
            cache["data"], page, total_pages,
            cache["total_bal"], cache["total_used"],
            cache["reachable"], cache["username"],
        )
        await edit_msg(cl, chat_id, msg_id, text, markup=dash_nav_kb(page, total_pages))

    elif data == "noop":
        pass  # page counter button — do nothing

    # Export confirm
    elif data == "export:yes":
        import asyncio
        await asyncio.gather(
            delete_msg(cl, chat_id, msg_id),
            clear_state(cl, username),
        )
        keys = await get_keys(cl, username)
        if not keys:
            mid = await send(cl, chat_id, "❌ No keys to export.")
            schedule_delete(chat_id, [mid])
            return
        content  = "\n".join(keys).encode("utf-8")
        file_mid = await send_document(
            cl, chat_id,
            filename=f"vercel_keys_{username}.txt",
            content=content,
            caption=f"📤 {len(keys)} API key(s) exported.",
        )
        # Export file stays until next command (no auto-delete)
        await kv_setex(cl, f"vam:exportfile:{_uh(username)}", 3600, str(file_mid or ""))

    elif data == "export:no":
        import asyncio
        await asyncio.gather(
            delete_msg(cl, chat_id, msg_id),
            clear_state(cl, username),
        )


# ── Text input (state machine) ────────────────────────────────────────────────

async def on_text(cl: httpx.AsyncClient, chat_id: int, username: str,
                  text: str, user_msg_id: int) -> None:
    state = await get_state(cl, username)
    if not state:
        return

    await clear_state(cl, username)

    # Delete old export file when user does next action
    import asyncio
    exp_raw = await kv_get(cl, f"vam:exportfile:{_uh(username)}")
    if exp_raw and exp_raw.isdigit():
        await asyncio.gather(
            delete_msg(cl, chat_id, int(exp_raw)),
            kv_del(cl, f"vam:exportfile:{_uh(username)}"),
        )

    if state == "awaiting_add":
        key = text.strip()
        if not (key.startswith("vck_") and len(key) >= 20):
            err = await send(cl, chat_id,
                "❌ Invalid format.\n"
                "Vercel keys start with `vck_` and are 60+ chars.")
            schedule_delete(chat_id, [user_msg_id, err], delay=ADD_SEC)
            return
        ok   = await add_key(cl, username, key)
        msg  = "✅ *Done!* Key saved and encrypted." if ok else "⚠️ Key already exists."
        done = await send(cl, chat_id, msg)
        schedule_delete(chat_id, [user_msg_id, done], delay=ADD_SEC)

    elif state == "awaiting_remove":
        key  = text.strip()
        ok   = await remove_key(cl, username, key)
        msg  = "✅ *Done!* API key removed." if ok else "❌ Key not found in your vault."
        done = await send(cl, chat_id, msg)
        schedule_delete(chat_id, [user_msg_id, done], delay=DEL_SEC)

    elif state == "awaiting_copy":
        prefix = text.strip()
        keys   = await get_keys(cl, username)
        # Find key that starts with given prefix
        match  = next((k for k in keys if k.startswith(prefix)), None)
        if match is None:
            err = await send(cl, chat_id,
                f"❌ No key found starting with `{prefix}`.\n"
                "Check the prefix from the dashboard.")
            schedule_delete(chat_id, [user_msg_id, err], delay=DEL_SEC)
        else:
            bal_info = await check_balance(cl, match)
            bal_str  = f"${bal_info['balance']:.4f}" if bal_info else "N/A"
            used_str = f"${bal_info['used']:.4f}"    if bal_info else "N/A"
            done = await send(cl, chat_id,
                f"🔑 *Full API Key*\n\n"
                f"`{match}`\n\n"
                f"💰 Balance : *{bal_str}*\n"
                f"📉 Used    : *{used_str}*\n\n"
                f"_Auto-deletes in {DEL_SEC}s._")
            schedule_delete(chat_id, [user_msg_id, done], delay=DEL_SEC)

    elif state == "awaiting_export_confirm":
        # User typed instead of clicking button — cancel
        exp_msg_raw = await kv_get(cl, f"vam:exportmsg:{_uh(username)}")
        if exp_msg_raw and exp_msg_raw.isdigit():
            await delete_msg(cl, chat_id, int(exp_msg_raw))
        schedule_delete(chat_id, [user_msg_id], delay=2)


# ── Main dispatcher ───────────────────────────────────────────────────────────

async def handle_update(update: dict) -> None:
    # ── Callback query (inline button press) ──────────────────────────────────
    if "callback_query" in update:
        cq       = update["callback_query"]
        username = cq["from"].get("username", "")
        chat_id  = cq["message"]["chat"]["id"]
        msg_id   = cq["message"]["message_id"]
        data     = cq.get("data", "")
        cq_id    = cq["id"]

        if not allowed(username):
            await _client().__aenter__()  # no-op, just ignore
            return

        try:
            async with _client() as cl:
                await handle_callback(cl, chat_id, username, msg_id, data, cq_id)
        except Exception:
            pass
        return

    # ── Regular message ───────────────────────────────────────────────────────
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
                await on_start(cl, chat_id, username, msg_id)
                return

            handlers = {
                BTN_ADD:       on_add,
                BTN_TOTAL:     on_total,
                BTN_REMOVE:    on_remove,
                BTN_GET:       on_get,
                BTN_DASHBOARD: lambda cl, c, u, m: on_dashboard(cl, c, u, m, page=1),
                BTN_COPY:      on_copy_key,
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
                await send(cl, chat_id, "Something went wrong. Please try /start again.")
        except Exception:
            pass
