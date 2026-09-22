# JCAP Macro Dashboard

Single-page web dashboard that pulls ~175 global macro & equity-factor
indicators from a logged-in Bloomberg Terminal, caches them in SQLite,
and serves them over a small async Python HTTP server. Front end is one
`index.html` (Chart.js + vanilla JS, no build step).

---

## 1. Deploy in one go (Claude Code)

The following sequence takes a fresh Windows machine from "files copied"
to "dashboard live". Run it from a **logged-in Bloomberg Terminal**.

```powershell
# ---- 0. Variables ---------------------------------------------------------
$PROJECT = "C:\Users\mchso\OneDrive - Jebsen & Co Ltd\Desktop\Agg\Macro Dashboard"
$PORT    = 8083
$SUBDOM  = "jcapmacrodashboard.ngrok.app"   # reserved ngrok subdomain

# ---- 1. Prerequisites check -----------------------------------------------
python --version                  # need 3.11+ (3.13 verified)
ngrok version                     # need ngrok with reserved subdomain configured
# Bloomberg Terminal must be running and logged in (DAPI on localhost:8194)

# ---- 2. Create virtualenv -------------------------------------------------
cd $PROJECT
python -m venv .venv

# ---- 3. Install dependencies ---------------------------------------------
# blpapi lives on Bloomberg's own index, hence --extra-index-url
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt `
    --extra-index-url https://blpapi.bloomberg.com/repository/releases/python/simple/

# ---- 4. Smoke test the BBG connection ------------------------------------
.\.venv\Scripts\python.exe -c "from xbbg import blp; print(blp.bdp('SPX Index','PX_LAST'))"

# ---- 5. Start the server --------------------------------------------------
.\.venv\Scripts\python.exe server_bbg.py
# (or double-click  "START SERVER (Python BBG).bat")

# ---- 6. Bootstrap the cache (first run only — ~5 min for 10y × ~175 tickers)
# In a second terminal:
curl "http://localhost:$PORT/api/refresh?force=1"
curl "http://localhost:$PORT/api/warm?years=10"

# ---- 7. Open ngrok tunnel (optional — for sharing) -----------------------
Start-Process ngrok -ArgumentList "http","--url=$SUBDOM","$PORT" -WindowStyle Minimized

# ---- 8. Verify ------------------------------------------------------------
curl                    "http://localhost:$PORT/api/macro" | Select-String '"status"|"lastRefresh"'
curl -H "ngrok-skip-browser-warning: 1" "https://$SUBDOM/api/macro" | Select-String '"lastRefresh"'
# Then open https://jcapmacrodashboard.ngrok.app  (or http://localhost:8083)
```

After step 5 the cache fills automatically on each chart request (gap-fill
on read — see §5). Steps 6+7 are optional but recommended for first deploy.

---

## 2. Prerequisites

| Component | Minimum | Notes |
|---|---|---|
| Windows | 10 / 11 | only platform tested (Bloomberg DAPI) |
| Python | 3.11+ | tested on 3.13 |
| Bloomberg Terminal | logged in | DAPI on `localhost:8194` |
| `blpapi` | 3.26.x | installs from Bloomberg's index (Step 3) |
| ngrok | any recent | only needed for external sharing; reserved subdomain `jcapmacrodashboard.ngrok.app` recommended |
| disk | ~250 MB | cache.db grows to ~50 MB over 10y of data |

---

## 3. File layout

```
Macro Dashboard/
├── .claude/launch.json          # Claude Code preview config (port + venv path)
├── .venv/                       # virtualenv  (recreate per machine)
├── tools/
│   ├── __init__.py
│   └── bloomberg_api.py         # xbbg/blpapi wrapper (BDP/BDH async funcs)
├── cache.db                     # SQLite: series + snapshots, 10y of bars
├── cache.py                     # cache module (range/upsert/staleness)
├── server_bbg.py                # async HTTP server (port 8083) + ticker config
├── index.html                   # single-page front end (Chart.js + vanilla JS)
├── jcap-logo.png                # header logo
├── requirements.txt             # pinned deps incl. blpapi==3.26.3.1
├── START SERVER (Python BBG).bat # double-click launcher
└── README.md                    # this file
```

The dashboard has **five tabs**:
- **Position** — Regime banner, Key Variables, Macro Regime Quadrant, Research Consensus (editable notes), Global Macro Pulse charts, Signal Heatmap
- **Economy** — Growth, Labour, Inflation, Sentiment, Consumer Credit, Housing, Recession Probability, China / Europe / Japan / EM macros, External / Trade
- **Liquidity Risk** — UST curve (3M/2Y/5Y/10Y/30Y), real yields & breakevens, funding rates, Bund / JGB curves, China policy & liquidity
- **Data** — Equity indices, vol (VIX term structure + SKEW), credit (CDX / iTraxx / OAS), cross-asset corr, FX, FX vol, commodities, valuation (CAPE / Buffett / AAII)
- **Equity** — 6 scatter charts (Industry + 5 regional factor lenses), WoW vs YoY returns

---

## 4. Data flow

```
Bloomberg Terminal (DAPI :8194)
        │  xbbg.bdp / xbbg.bdh
        ▼
tools/bloomberg_api.py        ← async wrappers
        │
        ▼
server_bbg.py                 ← /api/macro, /api/series, /api/refresh, /api/warm
   ├─ MACRO_TICKERS list      ← canonical universe (~175 tickers)
   ├─ TICKER_LABEL dict       ← human-readable names (used in tables/export)
   ├─ PERIODICITY dict        ← natural cadence (D/W/M/Q) per ticker
   └─ cache.db (SQLite)       ← snapshots + historical bars
        │
        ▼  HTTP :8083
index.html (Chart.js)
        │
        ▼
ngrok :443 → jcapmacrodashboard.ngrok.app  (optional)
```

---

## 5. Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /` | static `index.html` |
| `GET /api/macro` | latest snapshot for every ticker (`PX_LAST`, `CHG_NET_1D`, `LAST_UPDATE_DT`, `YLD_YTM_MID`) — cache-first |
| `GET /api/refresh?force=1` | force-pull fresh snapshots from BBG |
| `GET /api/series/<TICKER>?field=PX_LAST&years=N&periodicity={DAILY,WEEKLY,MONTHLY,QUARTERLY}` | historical bars; **auto gap-fills** from cache + BBG |
| `GET /api/warm?years=10` | pre-fill the historical-series cache for every ticker at its natural cadence |
| `GET /api/export?years=10` | full Excel dump (one sheet per periodicity bucket) |

Gap-fill logic on every `/api/series` request:
1. Look up cached `min/max(date)` for `(ticker, field, periodicity)`
2. If newest cached bucket is stale vs current period boundary → pull `cached_max+1 → today`
3. If user asks deeper than `cached_min` → pull `start_required → cached_min-1`
4. UPSERT new bars, return everything ≥ `start_required`

This means **no manual refresh is needed** for chart loads — first hit on a
day auto-fetches the day's bar, subsequent hits return from cache.

---

## 6. Maintenance ops

### Adding a new ticker

1. **`server_bbg.py`**
   - Add the BBG mnemonic to `MACRO_TICKERS` (top of the list, in the right region block)
   - Add a human-readable label to `TICKER_LABEL`
   - Add a periodicity bucket to `PERIODICITY` (`D`/`W`/`M`/`Q`) — omit to default to `D`
2. **`index.html`** — add a KPI card and/or chart entry in the relevant `CFG.<tab>.sections[…]` block
3. **Restart cycle**:
   - Stop `server_bbg.py`
   - Restart `server_bbg.py` (re-reads ticker list)
   - `curl "http://localhost:8083/api/refresh?force=1"` (fresh snapshot)
   - `curl "http://localhost:8083/api/warm?years=10"` (10y history)
   - Restart ngrok (`taskkill /F /IM ngrok.exe` then `ngrok http --url=jcapmacrodashboard.ngrok.app 8083`)
   - Hard-refresh the browser tab (Ctrl+Shift+R)

### Restart cycle (any code change)

Both processes must restart to pick up changes — `index.html` edits land on
hard-refresh, but `server_bbg.py` edits need a server restart, and ngrok's
tunnel needs to re-connect after the local server cycles.

---

## 7. Resilience features (already wired)

The front end is defensive against ngrok free-tier flakiness:

- **Fetch shim** (top of `index.html`): every `/api/*` request gets
  `ngrok-skip-browser-warning: 1` header + cache-buster query param
- **Auto-retry** on 502/503/504: up to 3 retries with 400/800/1200 ms backoff
- **In-memory series memo cache**: same `/api/series` URL within a session = Map hit (no refetch)
- **Concurrency cap** on scatter chart fetches: 4 in-flight max (ngrok-friendly)
- **Per-ticker error isolation**: a single failing ticker won't break the section it's in

Equity factor indices that are entitlement-gated on this Terminal are
proxied via ETFs and sector composites (see the `Factor proxies` block in
`MACRO_TICKERS`).

---

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Error: Unexpected token '<', "<!DOCTYPE"` on a chart | ngrok 502 / HTML splash | Already auto-retries; if persistent, hard-refresh; check `tasklist \| findstr ngrok` |
| KPI shows `—` (em dash) | Snapshot endpoint not warmed | `curl "http://localhost:8083/api/refresh?force=1"` |
| Chart shows "No Bloomberg data" | Ticker not in cache & BBG refused | Check Terminal entitlements; ticker may need an ETF proxy |
| `blpapi` import error | Bloomberg index missed during install | Re-run `pip install -r requirements.txt --extra-index-url https://blpapi.bloomberg.com/repository/releases/python/simple/` |
| `All securities failed: <TICKER>` in `/api/refresh` errors | Ticker mnemonic wrong or not entitled | Validate via `DES <GO>` in Terminal; replace with the canonical mnemonic |
| OneDrive sync conflicts on `cache.db` | OneDrive trying to sync a hot SQLite file | In Explorer → right-click `cache.db` → "Free up space" to stop syncing it; or move project off OneDrive |
| `port 8083 already in use` | Previous server didn't release | `taskkill /F /IM python.exe` then restart |

---

## 9. Quick reference — most-used commands

```powershell
# start everything
.\.venv\Scripts\python.exe server_bbg.py
Start-Process ngrok -ArgumentList "http","--url=jcapmacrodashboard.ngrok.app","8083" -WindowStyle Minimized

# force-pull fresh snapshots + warm 10y cache
curl "http://localhost:8083/api/refresh?force=1"
curl "http://localhost:8083/api/warm?years=10"

# stop everything
taskkill /F /IM python.exe
taskkill /F /IM ngrok.exe

# inspect cache contents
.\.venv\Scripts\python.exe -c "import sqlite3;c=sqlite3.connect('cache.db');print(c.execute('SELECT COUNT(DISTINCT ticker), COUNT(*) FROM series').fetchone())"

# excel dump
curl -o macro_export.xlsx "http://localhost:8083/api/export?years=10"
```

---

*Last verified: 2026-06-25 · Python 3.13 · blpapi 3.26.3.1 · ngrok reserved subdomain `jcapmacrodashboard.ngrok.app` · port 8083.*
