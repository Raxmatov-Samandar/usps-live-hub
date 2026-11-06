"""
USPS Live Hub — bitta dastur ichida:
  * usps.uz WebSocket'dan yangi yuklarni oladi (+ REST orqali o'tkazib yuborilganlarni)
  * SQLite bazaga saqlaydi (loads.db)
  * Telegram kanalga post qiladi (Map + Board tugmalari)
  * /board sahifasi va /api/loads, /ws orqali yuklarni real vaqtda ko'rsatadi
Ishga tushirish:  python app.py   ->  http://localhost:8000/board
"""

import asyncio
import html
import json
import logging
import os
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, urlparse
from zoneinfo import ZoneInfo

import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from websockets.asyncio.client import connect

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"]
CHANNEL_ID = os.environ["CHANNEL_ID"]
WS_URL = os.getenv("WS_URL", "wss://usps.uz/ws/loads/")
REST_URL = os.getenv("REST_URL", "https://usps.uz/api/loads/?page=1&page_size=50")
BOARD_URL = os.getenv("BOARD_URL", "").strip()  # o'z saytingiz: https://domen.uz/board
FOOTER_LINKS = os.getenv("FOOTER_LINKS", "")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "3"))
LOAD_TTL_MIN = int(os.getenv("LOAD_TTL_MIN", "30"))  # yuk necha daqiqada tugaydi
MAX_AGE_MIN = int(os.getenv("MAX_AGE_MIN", "25"))  # bundan eski yuk kanalga yuborilmaydi
HISTORY_HOURS = int(os.getenv("HISTORY_HOURS", "24"))  # sahifadagi tarix
KEEP_DAYS = int(os.getenv("KEEP_DAYS", "30"))  # bazada saqlash muddati
DB_PATH = os.getenv("DB_PATH", "loads.db")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
SOURCE = os.getenv("SOURCE", "telegram").strip().lower()  # telegram (Sean kanali) | usps_uz (eski, Ben)
TG_SOURCE = os.getenv("TG_SOURCE", "usps_live_sean").strip().lstrip("@")  # manba: ochiq Telegram kanal
POLL_SEC = float(os.getenv("POLL_SEC", "2"))  # kanal necha soniyada bir tekshiriladi
# Manbani bot orqali ham olish: bot Sean kanalida admin bo'lsa, har post darhol `channel_post` bo'lib keladi.
# Lokal DRY_RUN da o'chiq — getUpdates bir xil token bilan ikki joyda ishlasa, yangilanishlarni bir-biridan "o'g'irlaydi".
TG_BOT_SOURCE = os.getenv("TG_BOT_SOURCE", "0" if DRY_RUN else "1") == "1"
SRC_BOT_TOKEN = os.getenv("SRC_BOT_TOKEN", "").strip() or BOT_TOKEN  # alohida bot bo'lsa; aks holda post qiluvchi bot

SRC_HEADERS = {"Origin": "https://usps.uz", "Referer": "https://usps.uz/board", "User-Agent": "Mozilla/5.0"}
STATIC = Path(__file__).parent / "static"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hub")

# posted holatlari
PENDING, POSTED, SKIPPED = 0, 1, 2


def now() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(s: str | None) -> datetime:
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return now()


# ---------- Baza ----------
class Store:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS loads (
                order_id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                created_at TEXT NOT NULL,
                posted INTEGER NOT NULL DEFAULT 0,
                tg_message_id INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_loads_created ON loads(created_at);
            """
        )
        self.db.commit()

    def is_empty(self) -> bool:
        return self.db.execute("SELECT 1 FROM loads LIMIT 1").fetchone() is None

    def insert(self, load: dict, posted: int) -> bool:
        """Yangi bo'lsa True qaytaradi. Mavjud yuk qayta yozilmaydi (idempotent)."""
        created = parse_ts(load.get("created_at")).isoformat()
        cur = self.db.execute(
            "INSERT OR IGNORE INTO loads(order_id, data, created_at, posted) VALUES (?,?,?,?)",
            (str(load["order_id"]), json.dumps(load), created, posted),
        )
        self.db.commit()
        return cur.rowcount == 1

    def set_posted(self, oid: str, status: int, message_id: int | None = None):
        self.db.execute(
            "UPDATE loads SET posted=?, tg_message_id=COALESCE(?, tg_message_id) WHERE order_id=?",
            (status, message_id, oid),
        )
        self.db.commit()

    def pending(self) -> list[dict]:
        rows = self.db.execute("SELECT data FROM loads WHERE posted=? ORDER BY created_at", (PENDING,))
        return [json.loads(r["data"]) for r in rows]

    def recent(self, hours: int) -> list[dict]:
        since = (now() - timedelta(hours=hours)).isoformat()
        rows = self.db.execute(
            "SELECT * FROM loads WHERE created_at >= ? ORDER BY created_at DESC LIMIT 2000", (since,)
        )
        return [to_public(r) for r in rows]

    def get(self, oid: str):
        r = self.db.execute("SELECT * FROM loads WHERE order_id=?", (oid,)).fetchone()
        return to_public(r) if r else None

    def cleanup(self):
        cutoff = (now() - timedelta(days=KEEP_DAYS)).isoformat()
        self.db.execute("DELETE FROM loads WHERE created_at < ?", (cutoff,))
        self.db.commit()


def tg_post_url(message_id: int | None) -> str | None:
    if not message_id:
        return None
    if CHANNEL_ID.startswith("@"):
        return f"https://t.me/{CHANNEL_ID[1:]}/{message_id}"
    if CHANNEL_ID.startswith("-100"):
        return f"https://t.me/c/{CHANNEL_ID[4:]}/{message_id}"
    return None


def to_public(row) -> dict:
    d = json.loads(row["data"])
    return {
        "order_id": row["order_id"],
        "distance": d.get("distance"),
        "pickup_time": d.get("pickup_time"),
        "delivery_time": d.get("delivery_time"),
        "stops": d.get("stops") or [],
        "stop_addresses": d.get("stop_addresses") or [],
        "pickup_tz": d.get("pickup_tz"),
        "ends_at": d.get("ends_at"),
        "state_code": d.get("state_code"),
        "load_type": d.get("load_type"),
        "route_url": d.get("route_url"),
        "created_at": row["created_at"],
        "tg_url": tg_post_url(row["tg_message_id"]),
    }


# ---------- Brauzerlarga real vaqtda uzatish ----------
class Hub:
    def __init__(self):
        self.clients: set[WebSocket] = set()

    async def broadcast(self, msg: dict):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


# ---------- Telegram ----------
def footer() -> str:
    parts = []
    for item in filter(None, (x.strip() for x in FOOTER_LINKS.split(","))):
        name, _, url = item.partition("|")
        name, url = name.strip(), url.strip()
        parts.append(f'<a href="{html.escape(url)}">{html.escape(name)}</a>' if url else html.escape(name))
    return " | ".join(parts)


POST_HEADER = os.getenv("POST_HEADER", "✉️ USPS by MIKE")
ET = ZoneInfo("America/New_York")


def short_dt(s) -> str:
    """'10/05/2026 10:00 AM' -> '10/05 10:00 AM'"""
    m = re.match(r"\s*(\d{1,2}/\d{1,2})/\d{4}\s+(.+)", str(s or ""))
    return f"{m.group(1)} {m.group(2).strip()}" if m else str(s or "—")


def city_state(stop: str) -> tuple[str, str]:
    """'MEMPHIS, TN 38118' -> ('MEMPHIS, TN', 'TN')"""
    m = re.match(r"\s*(.+?),\s*([A-Z]{2})\b", str(stop or ""))
    return (f"{m.group(1).strip()}, {m.group(2)}", m.group(2)) if m else (str(stop or "—"), "")


def miles_text(distance) -> str:
    try:
        n = round(float(re.sub(r"[^\d.]", "", str(distance))))
        return f"{n:,} miles"
    except ValueError:
        return str(distance or "—")


def format_load(load: dict) -> str:
    e = html.escape
    ltype = str(load.get("load_type") or "").upper()
    icon = "👥" if ltype == "TEAM" else "👤"
    stops = [city_state(s) for s in (load.get("stops") or [])]
    if load.get("ends_at"):  # manba tugash vaqtini o'zi bergan
        ends = parse_ts(load["ends_at"]).astimezone(ET)
    else:
        ends = (parse_ts(load.get("created_at")) + timedelta(minutes=LOAD_TTL_MIN)).astimezone(ET)
    pu_tz = f" {load['pickup_tz']}" if load.get("pickup_tz") else ""

    lines = [e(POST_HEADER), "", f"{icon} {e(ltype or '—')} — <code>{e(str(load.get('order_id')))}</code>", ""]
    lines += [f"📍 {e(name)}" for name, _ in stops]
    lines += [
        "",
        f"⏰ PU: {e(short_dt(load.get('pickup_time')) + pu_tz)}",
    ]
    if load.get("delivery_time"):  # Sean kanalida delivery vaqti yo'q
        lines.append(f"📦 DEL: {e(short_dt(load.get('delivery_time')))}")
    lines += [
        "",
        f"🚛 Distance: {e(miles_text(load.get('distance')))}",
        f"⏳ Bidding Ends at: {ends.strftime('%I:%M %p')} ET",
    ]
    tags = [ltype] if ltype else []
    for st in (stops[0][1] if stops else "", stops[-1][1] if stops else ""):
        if st and st not in tags:
            tags.append(st)
    if tags:
        lines += ["", " ".join(f"#{e(t)}" for t in tags)]
    if f := footer():
        lines += ["", f]
    return "\n".join(lines)


def board_link_ok() -> bool:
    # Telegram localhost va http linklarni tugmada qabul qilmaydi
    return BOARD_URL.startswith("https://") and "localhost" not in BOARD_URL and "127.0.0.1" not in BOARD_URL


def keyboard(load: dict) -> dict:
    row = []
    if load.get("route_url"):
        row.append({"text": "📍 Map", "url": load["route_url"]})
    if board_link_ok():
        sep = "&" if "?" in BOARD_URL else "?"
        row.append({"text": "📋 Board", "url": f"{BOARD_URL}{sep}load={load['order_id']}"})
    return {"inline_keyboard": [row]} if row else {}


async def send_to_tg(session: aiohttp.ClientSession, load: dict) -> tuple[bool, int | None]:
    if DRY_RUN:
        print("\n" + "=" * 40 + "\n" + format_load(load) + "\n" + json.dumps(keyboard(load)))
        return True, None
    payload = {
        "chat_id": CHANNEL_ID,
        "text": format_load(load),
        "parse_mode": "HTML",
        "link_preview_options": {"is_disabled": True},
    }
    if kb := keyboard(load):
        payload["reply_markup"] = kb
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for attempt in range(5):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                if data.get("ok"):
                    return True, data["result"]["message_id"]
                if r.status == 429:
                    wait = data.get("parameters", {}).get("retry_after", 5)
                    log.warning("Telegram limit, %ss kutilyapti", wait)
                    await asyncio.sleep(wait + 1)
                    continue
                log.error("Telegram xato: %s", data.get("description"))
                if r.status == 400:
                    return False, None
        except Exception as ex:
            log.warning("Telegram'ga ulanishda xato (%s), qayta urinish", safe_err(ex))
        await asyncio.sleep(2 * (attempt + 1))
    return False, None


def safe_err(ex: Exception) -> str:
    """Xato matni logga tokensiz tushsin (aiohttp xatolari URL'ni, ya'ni tokenni ham yozishi mumkin)."""
    msg = f"{type(ex).__name__}: {ex}"
    for tok in {BOT_TOKEN, SRC_BOT_TOKEN}:
        if tok:
            msg = msg.replace(tok, "***")
    return msg


def is_fresh(load: dict) -> bool:
    return now() - parse_ts(load.get("created_at")) < timedelta(minutes=MAX_AGE_MIN)


# ---------- Manba: ochiq Telegram kanal (t.me/s/<kanal>) ----------
TG_WEB = "https://t.me/s/"
TG_HEADERS = {"User-Agent": "Mozilla/5.0"}


def tg_text(fragment: str) -> str:
    """Post HTML'i -> oddiy matn: <br> yangi qatorga aylanadi, teglar olib tashlanadi."""
    return html.unescape(re.sub(r"<[^>]+>", "", re.sub(r"<br\s*/?>", "\n", fragment)))


def tg_addresses(route_url: str, n: int) -> list[str]:
    """Map linkidagi to'liq manzillar (origin, waypoints, destination) — faqat soni stoplarga teng
    va hammasi ko'cha manzili bo'lsa. Ko'pincha linkda faqat shahar turadi, unda bo'sh ro'yxat."""
    q = parse_qs(urlparse(route_url or "").query)
    parts = q.get("origin", []) + [w for x in q.get("waypoints", []) for w in x.split("|")] + q.get("destination", [])
    parts = [p.strip() for p in parts if p.strip()]
    return parts if len(parts) == n and all(re.match(r"\d", p) for p in parts) else []


def tg_post_to_load(text: str, posted: datetime, route_url: str | None) -> dict | None:
    """Sean kanalidagi post matnini usps.uz formatidagi yukka o'giradi; yuk posti bo'lmasa None.
    Namuna:  👥 TEAM - 129178208 / 📍 HENDERSON, NV / 📍 MEMPHIS, TN /
             ⏰ PU: 11/06 04:30 AM PT / 🚛 Distance: 1,594.2 miles / ⏳ Bidding Ends at: 05:14 AM ET"""
    head = re.search(r"\b(TEAM|SOLO)\b\s*[-–—]\s*(\d{6,})", text)
    stops = [s.strip() for s in re.findall(r"📍\s*([^\n]+)", text) if s.strip()]
    if not head or len(stops) < 2:
        return None
    posted_et = posted.astimezone(ET)

    pickup_time = pickup_tz = None
    if m := re.search(r"PU:\s*(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})\s*([AP]M)(?:[ \t]+([A-Z]{1,4})\b)?", text):
        mo, d = int(m.group(1)), int(m.group(2))
        year = posted_et.year + (1 if mo < posted_et.month - 6 else 0)  # dekabrda chiqqan yanvar yuki
        pickup_time = f"{mo:02d}/{d:02d}/{year} {int(m.group(3)):02d}:{m.group(4)} {m.group(5)}"
        pickup_tz = m.group(6)

    ends_at = None
    if m := re.search(r"Bidding Ends at:\s*(\d{1,2}):(\d{2})\s*([AP]M)", text):
        h = int(m.group(1)) % 12 + (12 if m.group(3) == "PM" else 0)
        end = posted_et.replace(hour=h, minute=int(m.group(2)), second=0, microsecond=0)
        if end < posted_et - timedelta(hours=2):
            end += timedelta(days=1)  # yarim tundan o'tib tugaydi
        ends_at = end.astimezone(timezone.utc).isoformat()

    dist = re.search(r"Distance:\s*([\d,.]+)\s*mi", text)
    if not route_url:
        route_url = ("https://www.google.com/maps/dir/?api=1&origin=" + quote_plus(stops[0])
                     + "&destination=" + quote_plus(stops[-1]) + "&travelmode=driving")
    return {
        "order_id": head.group(2),
        "load_type": head.group(1),
        "stops": stops,
        "stop_addresses": tg_addresses(route_url, len(stops)),
        "pickup_time": pickup_time,
        "pickup_tz": pickup_tz,
        "delivery_time": None,
        "distance": f"{dist.group(1)} miles" if dist else None,
        "state_code": city_state(stops[0])[1],
        "route_url": route_url,
        "created_at": posted.astimezone(timezone.utc).isoformat(),
        "ends_at": ends_at,
    }


def tg_parse_page(page: str) -> tuple[list[dict], int, int]:
    """t.me/s sahifasi -> (yuklar eskidan yangiga, eng katta post id, postlar soni)."""
    loads, top, count = [], 0, 0
    for block in re.split(r'(?=<div class="tgme_widget_message_wrap)', page)[1:]:
        pid = re.search(r'data-post="[^"/]+/(\d+)"', block)
        if not pid:
            continue
        count += 1
        top = max(top, int(pid.group(1)))
        body = re.search(r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', block, re.S)
        when = re.search(r'<time datetime="([^"]+)"', block)
        if not (body and when):
            continue
        btn = re.search(r'<a\b[^>]*tgme_widget_message_inline_button[^>]*>', block)
        href = re.search(r'href="([^"]+)"', btn.group(0)) if btn else None
        load = tg_post_to_load(tg_text(body.group(1)), parse_ts(when.group(1)),
                               html.unescape(href.group(1)) if href else None)
        if load:
            loads.append(load)
    return loads, top, count


def bot_post_to_load(post: dict) -> dict | None:
    """Bot API `channel_post` -> yuk. Matn sahifadagi bilan bir xil, Map linki inline tugmada."""
    route_url = None
    for row in (post.get("reply_markup") or {}).get("inline_keyboard", []):
        for b in row:
            if not route_url and "google.com/maps" in str(b.get("url", "")):
                route_url = b["url"]
    posted = datetime.fromtimestamp(int(post.get("date") or 0), timezone.utc)
    return tg_post_to_load(post.get("text") or post.get("caption") or "", posted, route_url)


# ---------- Asosiy ish oqimi ----------
class Engine:
    def __init__(self):
        self.store = Store(DB_PATH)
        self.hub = Hub()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.session: aiohttp.ClientSession | None = None
        self.source_online = False
        # bot orqali manba holati — /health da ko'rinadi
        self.bot_src = {"enabled": SOURCE == "telegram" and TG_BOT_SOURCE, "bot": None, "admin": None,
                        "ok": False, "last_post": None}

    async def accept(self, load: dict, first_run: bool = False):
        if not load.get("order_id"):
            return
        fresh = is_fresh(load)
        status = SKIPPED if (first_run or not fresh) else PENDING
        if not self.store.insert(load, status):
            return  # allaqachon bor
        await self.hub.broadcast({"type": "load", "load": self.store.get(str(load["order_id"]))})
        if status == PENDING:
            self.queue.put_nowait(load)

    async def catch_up(self):
        try:
            async with self.session.get(REST_URL, headers=SRC_HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
        except Exception as ex:
            log.warning("REST xato: %s", ex)
            return
        loads = data.get("results", data) if isinstance(data, dict) else data
        first = self.store.is_empty()
        for ld in reversed(loads):  # eskidan yangiga
            await self.accept(ld, first_run=first)
        if first:
            log.info("Birinchi ishga tushish: %d ta mavjud yuk bazaga yozildi (kanalga yuborilmaydi)", len(loads))

    async def listen(self):
        backoff = 1
        while True:
            try:
                async with connect(WS_URL, additional_headers=SRC_HEADERS, open_timeout=15, ping_interval=20) as ws:
                    log.info("Manbaga ulandi: %s", WS_URL)
                    self.source_online = True
                    await self.hub.broadcast({"type": "source", "online": True})
                    backoff = 1
                    await self.catch_up()
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        load = msg if isinstance(msg, dict) and msg.get("order_id") else None
                        if load is None and isinstance(msg, dict):
                            for k in ("data", "load", "payload"):
                                if isinstance(msg.get(k), dict) and msg[k].get("order_id"):
                                    load = msg[k]
                        if load:
                            await self.accept(load)
            except Exception as ex:
                log.warning("Manba uzildi (%s). %ss dan keyin qayta ulanish", ex, backoff)
            self.source_online = False
            await self.hub.broadcast({"type": "source", "online": False})
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    async def set_online(self, ok: bool):
        if ok != self.source_online:
            self.source_online = ok
            await self.hub.broadcast({"type": "source", "online": ok})

    async def tg_fetch(self, after: int | None) -> str:
        url = TG_WEB + TG_SOURCE
        timeout = aiohttp.ClientTimeout(total=15)
        if after is None:  # birinchi so'rov: oxirgi ~20 post
            async with self.session.get(url, headers=TG_HEADERS, timeout=timeout) as r:
                r.raise_for_status()
                return await r.text()
        # faqat `after` dan keyingi postlar; yangi post bo'lmasa javob '""'
        headers = {**TG_HEADERS, "X-Requested-With": "XMLHttpRequest"}
        async with self.session.post(f"{url}?after={after}", data=b"", headers=headers, timeout=timeout) as r:
            r.raise_for_status()
            body = await r.text()
        return json.loads(body) if body.strip() else ""

    async def listen_tg(self):
        log.info("Manba: https://t.me/s/%s (har %ss)", TG_SOURCE, POLL_SEC)
        last, backoff = None, POLL_SEC
        while True:
            try:
                loads, top, count = tg_parse_page(await self.tg_fetch(last))
                if last is None and not top:
                    raise RuntimeError("kanal sahifasida post topilmadi")
                first = last is None and self.store.is_empty()
                for ld in loads:
                    await self.accept(ld, first_run=first)
                if first:
                    log.info("Birinchi ishga tushish: %d ta mavjud yuk bazaga yozildi (kanalga yuborilmaydi)", len(loads))
                last = max(last or 0, top)
                await self.set_online(True)
                backoff = POLL_SEC
                if count >= 20:
                    continue  # bir so'rovga sig'madi — darhol keyingisini olamiz
            except Exception as ex:
                await self.set_online(False)
                backoff = min(backoff * 2, 60)
                log.warning("Telegram manba xato (%s). %ss dan keyin qayta", ex, backoff)
                await asyncio.sleep(backoff)
                continue
            await asyncio.sleep(POLL_SEC)

    async def bot_api(self, method: str, **params):
        url = f"https://api.telegram.org/bot{SRC_BOT_TOKEN}/{method}"
        timeout = aiohttp.ClientTimeout(total=params.get("timeout", 0) + 15)
        async with self.session.post(url, json=params, timeout=timeout) as r:
            data = await r.json()
        if not data.get("ok"):
            raise RuntimeError(f"{method}: {data.get('error_code')} {data.get('description')}")
        return data["result"]

    async def check_bot_admin(self):
        try:
            me = await self.bot_api("getMe")
            self.bot_src["bot"] = "@" + me.get("username", "")
            m = await self.bot_api("getChatMember", chat_id="@" + TG_SOURCE, user_id=me["id"])
            self.bot_src["admin"] = m.get("status") in ("administrator", "creator")
        except Exception as ex:
            self.bot_src["admin"] = False
            log.info("Bot manba tekshiruvi: %s", safe_err(ex))
        if self.bot_src["admin"]:
            log.info("Bot %s @%s kanalida admin — postlar darhol keladi", self.bot_src["bot"], TG_SOURCE)
        else:
            log.warning("Bot %s @%s kanalida admin emas — manba hozircha faqat sahifa orqali (har %ss)",
                        self.bot_src["bot"], TG_SOURCE, POLL_SEC)

    async def listen_bot(self):
        await self.check_bot_admin()
        offset, backoff = None, 2
        while True:
            try:
                updates = await self.bot_api("getUpdates", offset=offset, timeout=50, allowed_updates=["channel_post"])
                self.bot_src["ok"], backoff = True, 2
                for u in updates:
                    offset = u["update_id"] + 1
                    post = u.get("channel_post") or {}
                    if str((post.get("chat") or {}).get("username", "")).lower() != TG_SOURCE.lower():
                        continue  # boshqa kanal, masalan bot post qiladigan o'zimizning kanal
                    self.bot_src["admin"] = True
                    if load := bot_post_to_load(post):
                        self.bot_src["last_post"] = load["created_at"]
                        await self.accept(load)
            except Exception as ex:
                # 409 — shu token bilan boshqa joyda ham getUpdates ishlayapti (yoki webhook o'rnatilgan)
                self.bot_src["ok"] = False
                log.warning("Bot manba xato (%s). %ss dan keyin qayta", safe_err(ex), backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def sender(self):
        while True:
            load = await self.queue.get()
            oid = str(load["order_id"])
            if not is_fresh(load):
                self.store.set_posted(oid, SKIPPED)
                continue
            ok, mid = await send_to_tg(self.session, load)
            self.store.set_posted(oid, POSTED if ok else SKIPPED, mid)
            if ok:
                log.info("Post qilindi: %s (%s)", oid, load.get("state_code"))
                await self.hub.broadcast({"type": "load", "load": self.store.get(oid)})
            else:
                log.error("Post qilinmadi: %s", oid)
            await asyncio.sleep(SEND_INTERVAL)

    async def start(self):
        self.session = aiohttp.ClientSession()
        self.store.cleanup()
        for ld in self.store.pending():  # oldingi ishga tushishda yuborilmay qolganlar
            self.queue.put_nowait(ld)
        if not board_link_ok():
            log.warning("BOARD_URL https bo'lmagani uchun kanaldagi postlarda Board tugmasi chiqmaydi")
        source = self.listen_tg() if SOURCE == "telegram" else self.listen()
        self.tasks = [asyncio.create_task(source), asyncio.create_task(self.sender())]
        if self.bot_src["enabled"]:
            self.tasks.append(asyncio.create_task(self.listen_bot()))

    async def stop(self):
        for t in self.tasks:
            t.cancel()
        await self.session.close()


engine = Engine()


@asynccontextmanager
async def lifespan(_app):
    await engine.start()
    yield
    await engine.stop()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")  # rasm, network.json


@app.get("/")
async def root():
    return RedirectResponse("/board")


@app.get("/board")
async def board():
    return FileResponse(STATIC / "index.html")


@app.get("/api/loads")
async def api_loads(hours: int = HISTORY_HOURS):
    hours = max(1, min(hours, 24 * 7))
    return JSONResponse(
        {
            "ttl_min": LOAD_TTL_MIN,
            "server_time": now().isoformat(),
            "source_online": engine.source_online,
            "loads": engine.store.recent(hours),
        }
    )


@app.get("/health")
async def health():
    return {"ok": True, "source_online": engine.source_online, "queue": engine.queue.qsize(),
            "bot_source": engine.bot_src}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    engine.hub.clients.add(ws)
    try:
        while True:
            await ws.receive_text()  # klientdan ping
    except WebSocketDisconnect:
        pass
    finally:
        engine.hub.clients.discard(ws)


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, ws="wsproto", log_level="warning")
