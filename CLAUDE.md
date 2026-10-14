# USPS Live Hub — loyiha konteksti

Bu fayl Claude Code uchun. U loyihani ochganda shu faylni avtomatik o'qiydi.
Muallif: Samandar (vibe coder — kodni AI orqali yozadi). Javoblarni **o'zbek tilida**, qisqa va aniq ber; texnik atamalar inglizcha qolsin.

## Loyiha nima qiladi

USPS spot yuklarini (USPS Freight Auction / Load Bid) real vaqtda olib:
1. **Telegram kanalga** (`@USPS_LIVE_HUB`) har bir yangi yukni chiroyli formatda post qiladi (📍 Map + 📋 Board tugmalari bilan).
2. **O'z web board'ida** (`https://www.uspshub.uz/board`) jonli jadval ko'rinishida ko'rsatadi.

Ma'lumot manbai — `usps.uz` (Ben'ning tizimi). Ben'ning ukasi bilan birga ishlanmoqda, ya'ni manbadan foydalanish ular bilan kelishilgan. Yuklar USPS'da **30 daqiqada** tugaydi, shuning uchun tezlik eng muhim talab.

Brend: **HUB Logistics** (logo: oq "HUB" harflari + qizil uchburchak qism, `#FF101C`).

## Arxitektura

Hammasi **bitta Python jarayon** (`app.py`) ichida, asyncio + FastAPI:

```
usps.uz WebSocket ──► Engine.listen() ──► Store (SQLite) ──► Hub.broadcast() ──► brauzerlar (/ws)
usps.uz REST (catch-up) ─┘                     │
                                               └──► queue ──► Engine.sender() ──► Telegram Bot API
```

- **`Engine.listen()`** — `wss://usps.uz/ws/loads/` ga ulanadi. Uzilsa 1→2→4…60s backoff bilan qayta ulanadi. Har ulanishda `catch_up()` REST orqali oxirgi 50 yukni tekshiradi (uzilish paytida o'tkazib yuborilganlar uchun).
- **`Engine.accept()`** — yukni bazaga yozadi (`order_id` PRIMARY KEY → takrorlanish yo'q), brauzerlarga uzatadi, yangi va "fresh" bo'lsa Telegram navbatiga qo'yadi.
- **Birinchi ishga tushish** (baza bo'sh): mavjud 50 yuk `SKIPPED` deb yoziladi, kanalga **yuborilmaydi** (kanalni eski yuklar bilan to'ldirmaslik uchun).
- **`Engine.sender()`** — navbatdan olib Telegram'ga yuboradi, postlar orasida `SEND_INTERVAL` (3s) — Telegram kanal limiti ~20 xabar/daqiqa. 429 bo'lsa `retry_after` kutadi. Yuborishdan oldin yana `is_fresh()` tekshiradi.
- **`Hub`** — board sahifasidagi brauzerlarga WebSocket orqali `{"type":"load"}` va `{"type":"source","online":bool}` xabarlarini uzatadi.

### Manba ma'lumot formati (usps.uz WS xabari va REST `results[]` elementi)
```json
{
  "id": 12399, "order_id": "125978026",
  "distance": "577.3 miles",
  "pickup_time": "10/05/2026 07:00 AM", "delivery_time": "10/05/2026 08:17 PM",
  "stops": ["PHILADELPHIA, PA 19116", "GASTONIA, NC 28054"],
  "stop_addresses": ["1900 BYBERRY RD STE 1 PHILADELPHIA, PA, USA 19116", "..."],
  "state_code": "PA", "load_type": "TEAM",
  "route_url": "https://www.google.com/maps/dir/...",
  "created_at": "2026-10-05T07:42:42.642668Z"
}
```
- `pickup_time`/`delivery_time` — **vaqt mintaqasisiz** mahalliy vaqt (stop shtatiniki).
- `created_at` — UTC. Bidding tugash vaqti = `created_at + 30 daqiqa`.
- REST: `GET https://usps.uz/api/loads/?page=1&page_size=50` → Django formatida `{count, next, results}`.
- Manbaga so'rovlarda `Origin`/`Referer` headerlari `https://usps.uz` bo'lishi kerak (`SRC_HEADERS`).

### Baza (SQLite, `DB_PATH`)
```sql
loads(order_id TEXT PRIMARY KEY, data TEXT /*to'liq JSON*/, created_at TEXT, posted INTEGER, tg_message_id INTEGER)
```
`posted`: `0` PENDING, `1` POSTED, `2` SKIPPED. Restartda PENDING'lar qayta navbatga qo'yiladi. `KEEP_DAYS` (30) dan eskilari o'chiriladi.

## Fayllar

| Fayl | Vazifasi |
|---|---|
| `app.py` | Butun backend: manba, baza, Telegram, FastAPI |
| `static/index.html` | Board — bitta faylda HTML+CSS+JS, framework yo'q |
| `static/hub-logo.svg` | Asl HUB Logistics vektor logosi (manba). Header logosi va favicon `index.html` ichiga inline yozilgan |
| `telegram-avatar.png` | Kanal avatari (repoda shart emas) |
| `requirements.txt` | aiohttp, websockets, python-dotenv, fastapi, uvicorn, wsproto, tzdata |
| `railway.json` | Railway: `python app.py`, healthcheck `/health`, restart ALWAYS |
| `.env` | Lokal sozlamalar — **hech qachon commit qilinmaydi** |

## HTTP endpointlar

- `GET /` → `/board` ga redirect (⚠️ hozir `?load=` query'ni yo'qotadi)
- `GET /board` → `static/index.html`. `?load=<order_id>` bo'lsa o'sha yukning batafsil oynasi ochiladi
- `GET /api/loads?hours=24` → `{ttl_min, server_time, source_online, loads:[...]}` (max 2000, `hours` 1–168)
- `GET /health` → `{ok, source_online, queue}`
- `WS /ws` → brauzer uchun jonli yangilanishlar (klient har 25s `ping` yuboradi)

## Telegram post formati (`format_load()`, HTML parse_mode)

```
✉️ USPS by MIKE

👥 TEAM — <code>125980141</code>      (SOLO bo'lsa 👤)

📍 MEMPHIS, TN
📍 DENVER, CO                          (har bir stop alohida qator)

⏰ PU: 10/05 10:00 AM
📦 DEL: 10/06 07:17 AM

🚛 Distance: 1,103 miles               (butun songa yaxlitlangan)
⏳ Bidding Ends at: 04:41 AM ET        (created_at + 30 min, America/New_York)

#TEAM #TN #CO                          (tur, birinchi va oxirgi stop shtati, takrorsiz)

USPS_LIVE | USPS_HUB | DISPATCH_SERVICE   (FOOTER_LINKS dan)
```
Tugmalar: `📍 Map` → `route_url`; `📋 Board` → `{BOARD_URL}?load={order_id}`.
**Board tugmasi faqat `BOARD_URL` `https://` bilan boshlansa va localhost bo'lmasa qo'shiladi** (Telegram aks holda rad etadi). Aks holda logda `WARNING BOARD_URL https bo'lmagani uchun...` chiqadi.

## Board (`static/index.html`)

- Dizayn: tungi indigo fon (`--bg #10102A`), aksent firuza `#5EE6D0`; holatlar: open `#4BE3A2`, soon `#FFC24B`, expiring `#FF5D7D`; TEAM binafsha, SOLO havorang. `prefers-color-scheme: light` da och mavzuga o'tadi. Shriftlar: Barlow / Barlow Condensed.
- Holat (frontendda hisoblanadi, `left = created_at + TTL − now`): `open` >15 min, `soon` 5–15, `exp` <5, `ended` ≤0.
- Yuqorida 3 panel: Live on the board (status ulushi chizig'i, filtr), Driver type (Team/Solo), Closing next (eng tez tugaydigan yuk + oxirgi post vaqti).
- Jadval ustunlari: Load #, State (shtatga xos doimiy rang — `hue()`), Origin, Destination, Pickup (shtat bo'yicha CT/ET/MT/PT), Miles, Type, Ends in, Status, Posted ET, Map/Telegram tugmalari. Har qator ostida "fuse" chizig'i — qolgan vaqt.
- Filtrlar: status, Team/Solo, bir nechta shtat, qidiruv (raqam/shahar/shtat/ZIP), saralash, "Show ended loads". Filtrlar `localStorage` da (`hub:` prefiksi, try/catch bilan).
- Yangi yuk: 4s ajralib turadi, ixtiyoriy ovoz (WebAudio), tab sarlavhasida `(n)` hisoblagich.
- Yuk bosilsa o'ng tomondan drawer: to'liq manzillar, Google Maps, Telegram posti, nusxalash.
- Server bilan soat farqi `clockSkew` orqali to'g'rilanadi. Mobil (<820px) da qatorlar kartochkaga aylanadi.

## Environment o'zgaruvchilari

| Nomi | Majburiy | Default | Izoh |
|---|---|---|---|
| `BOT_TOKEN` | ✅ | — | @BotFather tokeni. Bot kanalda admin bo'lishi shart |
| `CHANNEL_ID` | ✅ | — | `@USPS_LIVE_HUB` yoki `-100...` |
| `BOARD_URL` | | `""` | `https://www.uspshub.uz/board` — qo'shtirnoqsiz, probelsiz |
| `FOOTER_LINKS` | | `""` | `NOM|URL,NOM|URL` |
| `POST_HEADER` | | `✉️ USPS by MIKE` | |
| `PORT` | | `8000` | Railway domenlari ham 8000-portga yo'naltirilgan — mos bo'lishi shart |
| `DB_PATH` | | `loads.db` | Railway'da `/data/loads.db` (Volume) |
| `WS_URL`, `REST_URL` | | usps.uz | |
| `LOAD_TTL_MIN` 30, `MAX_AGE_MIN` 25, `SEND_INTERVAL` 3, `HISTORY_HOURS` 24, `KEEP_DAYS` 30 | | | |
| `DRY_RUN` | | `0` | `1` → Telegram'ga yubormaydi, konsolga chiqaradi |

## Deploy (production)

- **Railway** (Hobby/Pro), GitHub repo `Raxmatov-Samandar/usps-live-hub` (private bo'lishi kerak), `main` ga push → avtomatik deploy.
- **Volume** `/data` ga ulangan — baza restartda saqlanadi.
- **Domen:** `uspshub.uz` (registrator Billur Com) → nameserverlar **Cloudflare** (`dane`/`melissa.ns.cloudflare.com`). Cloudflare'da `CNAME www → 0nm0cpq3.up.railway.app` va TXT `_railway-verify.www`. Proxy **DNS only (kulrang bulut)** bo'lishi shart — aks holda Railway SSL chiqara olmaydi.
- Railway domen porti: **8000**.

## Muhim qoidalar (o'zgartirishdan oldin o'qi)

1. **Ikki nusxa bir vaqtda ishlamasin.** Lokal `python app.py` va Railway bir vaqtda ishlasa, har bir yuk kanalga 2 marta tushadi. Lokal test har doim `DRY_RUN=1` bilan, alohida `DB_PATH` bilan.
2. **Takrorlanishni nazorat qilish** `order_id` PRIMARY KEY + `INSERT OR IGNORE` ga tayanadi — buni buzma.
3. **Tezlik** — yuk chiqqandan postgacha bir necha soniya. Sinxron/bloklovchi chaqiruvlar qo'shma (hammasi async).
4. **Sirlar** (`BOT_TOKEN`) hech qachon kodga, logga yoki commitga tushmasin. `.gitignore` da `.env` va `*.db` bor.
5. `static/index.html` — framework'siz bitta fayl. Tashqi kutubxona kerak bo'lsa CDN orqali, build bosqichisiz.
6. Python 3.10+ (`dict | None` sintaksisi), loyiha 3.12 da ishlaydi. Windows'da `tzdata` kerak.
7. Kod izohlari va log matnlari o'zbek tilida — shu uslubni saqla.

## Lokal ishga tushirish

```powershell
cd "D:\Dasturlar\usps-live-hub"
pip install -r requirements.txt
# .env: BOT_TOKEN, CHANNEL_ID, DRY_RUN=1, DB_PATH=test.db
python app.py
# http://localhost:8000/board
```
Tekshirish: `curl http://localhost:8000/health`.

## Ma'lum kamchiliklar / keyingi ishlar

- `GET /` redirect `?load=` ni yo'qotadi — `/` ham query'ni saqlab redirect qilsin.
- `uspshub.uz` (www siz) hali ishlamaydi — Cloudflare'da `@` uchun CNAME flattening yoki redirect rule kerak.
- Testlar yo'q — `format_load`, `accept` (dedup), `is_fresh` uchun pytest qo'shish foydali.
- Apple touch icon (PNG) hali yo'q.
- G'oyalar: har bir dispatcherga shaxsiy filtr (shtat, TEAM/SOLO) bo'yicha alohida bot orqali yuborish; yuklar tarixidan statistika sahifasi.
