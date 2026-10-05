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
from zoneinfo import ZoneInfo

import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
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
    ends = (parse_ts(load.get("created_at")) + timedelta(minutes=LOAD_TTL_MIN)).astimezone(ET)

    lines = [e(POST_HEADER), "", f"{icon} {e(ltype or '—')} — <code>{e(str(load.get('order_id')))}</code>", ""]
    lines += [f"📍 {e(name)}" for name, _ in stops]
    lines += [
        "",
        f"⏰ PU: {e(short_dt(load.get('pickup_time')))}",
        f"📦 DEL: {e(short_dt(load.get('delivery_time')))}",
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
            log.warning("Telegram'ga ulanishda xato (%s), qayta urinish", ex)
        await asyncio.sleep(2 * (attempt + 1))
    return False, None


def is_fresh(load: dict) -> bool:
    return now() - parse_ts(load.get("created_at")) < timedelta(minutes=MAX_AGE_MIN)


# ---------- Asosiy ish oqimi ----------
class Engine:
    def __init__(self):
        self.store = Store(DB_PATH)
        self.hub = Hub()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.session: aiohttp.ClientSession | None = None
        self.source_online = False

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
        self.tasks = [asyncio.create_task(self.listen()), asyncio.create_task(self.sender())]

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
    return {"ok": True, "source_online": engine.source_online, "queue": engine.queue.qsize()}


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
