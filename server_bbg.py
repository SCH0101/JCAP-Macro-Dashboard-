"""JCAP Macro Dashboard — Python server backed by a local Bloomberg data agent.

This replaces the PowerShell .NET blpapi backend (server.ps1) with a proven
xbbg/blpapi data agent vendored into this project (tools/bloomberg_api.py).
The front end (index.html) is unchanged: it still calls /api/macro,
/api/refresh and /api/series/{ticker}.

How it works
------------
- The Bloomberg primitive layer lives locally at ./tools/bloomberg_api.py.
  We import `_xbbg_bdp` (reference/snapshot), `_xbbg_bdh` (historical) and
  `is_bloomberg_available` from it. These talk to a logged-in Bloomberg Terminal
  over local DAPI (port 8194), as documented in
  HOW_TO_IMPLEMENT_BBG_DATA_AGENT.md (also in this folder).
- This file owns only the HTTP layer + the macro ticker list. All Bloomberg
  I/O is delegated to the local data agent.

Self-contained: the project ships its own .venv (xbbg + blpapi + httpx + pandas,
pinned in requirements.txt). Just double-click "START SERVER (Python BBG).bat",
or run:

    ".venv/Scripts/python.exe" server_bbg.py
"""
from __future__ import annotations

import asyncio
import gzip as _gzip
import json
import math
import os
import subprocess
import sys
import threading
import time
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

# Force line-buffered stdout so [REFRESH]/[SERIES]/[MACRO] log lines appear in
# real time even when the server is launched via Start-Process with stdout
# redirected to a file (default would be block-buffered).
try:
    sys.stdout.reconfigure(line_buffering=True)
except AttributeError:
    pass

# ── Configuration ─────────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", 8083))
WEB_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_pool_env() -> None:
    """Self-load JCAP_POOL_* from a project-local or legacy sibling .env.

    This lets the JCAP-pool data agent work regardless of how the server is
    launched (.bat, launch.json, manual shell, scheduled task). The
    bloomberg_api module reads these at import time, so this must run first.
    """
    # Short-circuit only when the full pair is already present — a launcher that
    # sets JCAP_POOL_URL but not the API key must still pick the key up from .env
    # (os.environ.setdefault below never clobbers an already-set value).
    if os.environ.get("JCAP_POOL_URL") and os.environ.get("JCAP_POOL_API_KEY"):
        return
    local_env_path = os.path.join(WEB_DIR, ".env")
    legacy_env_path = os.path.join(os.path.dirname(WEB_DIR), "equity_swarm_V5", ".env")
    env_path = local_env_path if os.path.isfile(local_env_path) else legacy_env_path
    try:
        with open(env_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                if k in ("JCAP_POOL_URL", "JCAP_POOL_API_KEY", "JCAP_POOL_TIMEOUT"):
                    os.environ.setdefault(k, v.strip().strip('"').strip("'"))
    except OSError:
        pass  # no .env — fall back to local DAPI / cache-only


_load_pool_env()

# ── Import the LOCAL data agent ─────────────────────────────────────────────
# The Bloomberg primitive layer lives inside this project at
# tools/bloomberg_api.py. Running `python server_bbg.py` from this folder puts
# WEB_DIR on sys.path[0], so `from tools import bloomberg_api` resolves locally.
if WEB_DIR not in sys.path:
    sys.path.insert(0, WEB_DIR)

_AGENT_IMPORT_ERROR: str | None = None
try:
    from tools import bloomberg_api as bbg  # local: ./tools/bloomberg_api.py
except Exception as exc:  # pragma: no cover — environment dependent
    bbg = None  # type: ignore[assignment]
    _AGENT_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

import cache  # local SQLite cache module
DB_PATH = os.path.join(WEB_DIR, "cache.db")
cache.init(DB_PATH)

# ── Vanda Positioning (CSV-backed, SharePoint folder) ───────────────────────
# One subfolder per instrument; each holds a daily-refreshed CSV named
# <CODE>_<YYYYMMDD>.csv with header `date,combined_positioning`. The upstream
# job rewrites these ~06:00 daily, so we read the newest dated file per
# instrument on each request (short TTL cache to avoid disk thrash).
VANDA_DIR = os.environ.get(
    "VANDA_DIR",
    r"C:\Users\mchso\OneDrive - Jebsen & Co Ltd\JCAPSharePoint - Documents"
    r"\Equity Team\Trading\Macro & Sectors\Vanda Positioning",
)
VANDA_META = {
    "USEQCOMB":      {"name": "US Equity",     "group": "Equities",    "order": 1},
    "JPEQCOMB":      {"name": "Japan Equity",  "group": "Equities",    "order": 2},
    "CNEQASHRCOMB":  {"name": "China A-Shares","group": "Equities",    "order": 3},
    "CNEQHSHRCOMB":  {"name": "China H-Shares","group": "Equities",    "order": 4},
    "EMEQCOMB":      {"name": "EM ex-China",   "group": "Equities",    "order": 5},
    "GLCOMCOMB-GC1": {"name": "Gold",          "group": "Commodities", "order": 6},
    "GLCOMCOMB-SI1": {"name": "Silver",        "group": "Commodities", "order": 7},
    "GLCOMCOMB-CO1": {"name": "Brent Crude",   "group": "Commodities", "order": 8},
    "GLCOMCOMB-CL1": {"name": "WTI Crude",     "group": "Commodities", "order": 9},
}
_VANDA_TTL = 300.0  # seconds — bypass with ?fresh=1
_vanda_cache: dict = {"ts": 0.0, "years": None, "payload": None}

# ── Master ticker list (canonical, user-provided 2026-06-22) ────────────────
MACRO_TICKERS = [
    # ── US Macro ────────────────────────────────────────────────────────────
    # Growth & Activity
    "GDP CQOQ Index", "NFP TCH Index", "USURTOT Index", "NAPMPMI Index",
    "NAPMNMI Index", "RSTAMOM Index", "RSTAYOY Index", "NHSPSTOT Index",
    "CONCCONF Index", "CONSSENT Index",
    # Inflation
    "CPI YOY Index", "CPI CHNG Index", "PCE CYOY Index", "PCEPILFE Index",
    "CPIQCSAN Index",        # US CPI quarterly SAAR
    # Money, Credit & Financial Conditions
    "M2% YOY Index", "BFCIUS Index", "NFCI Index", "BLCITOTL Index",
    "TOMOTCSO Index", "ECST T SLDETIGT Index",

    # ── China Macro ─────────────────────────────────────────────────────────
    # Growth & Activity
    "CHVAIOY Index", "CNRSCYOY Index", "CNFAYOY Index", "CNPMIMFG Index",
    "CNPMISRV Index", "SHSZ300 Index",
    # Inflation
    "CNCPIYOY Index", "CHEFTYOY Index",
    # Money, Credit & Social Financing
    "CNLNASF Index", "CNMSM2 Index", "CNLNNNML Index", "CNMMTTL Index",
    "CHLRLPR1 Index",
    # External / Trade
    "CNGFOREX Index", "CNFRBAL$ Index", "USDCNH Curncy",

    # ── Europe / UK Macro ───────────────────────────────────────────────────
    "MPMIEZCA Index", "MPMIEZMA Index", "KXEZCPMF Index", "UKCPIYOY Index",
    "UMRTEMU Index", "EUTRBAL Index",
    "EURR002W Index", "EURDEPO Index",
    "ITRX EUR CDSI GEN 5Y Corp", "ITRX XOVER CDSI GEN 5Y Corp",
    "FWISEU55 Index",

    # ── Japan Macro ─────────────────────────────────────────────────────────
    "BJFXINOP Index", "JNMBCABE Index", "ECOYBJPN Index",
    "GTJPY2Y Govt", "GTJPY10Y Govt", "USDJPY Curncy", "JPMVG71M Index",

    # ── Asia EM Macro ───────────────────────────────────────────────────────
    "EHGDEMGY Index", "EHPIEMG Index", "MPMIKRMA Index", "KOGDPQOQ Index",
    "KOEAUERS Index", "MPMIEMCA Index", "JPMVXYEM Index",
    "CDX EM CDSI GEN 5Y SPRD Corp",

    # ── Global Rates ────────────────────────────────────────────────────────
    "GT2 Govt", "GT5 Govt", "GT10 Govt", "GT30 Govt",
    "GTDEM2Y Govt", "GTDEM10Y Govt", "GTDEM30Y Govt",
    "MOVE Index",

    # ── Global FX ───────────────────────────────────────────────────────────
    "DXY Curncy", "EURUSD Curncy", "GBPUSD Curncy",
    "CVIX Index", "EURUSDV1M BGN Curncy", "USDJPYV1M BGN Curncy",

    # ── US Equity & Vol (SPX + VIX term structure) ──────────────────────────
    "SPX Index",
    "VIX Index", "VIX3M Index", "VIX6M Index", "VIX1Y Index",

    # ── US Credit OAS (Aaa / Baa — used to compute Baa-Aaa spread) ──────────
    "LCA3OAS Index", "LCB1OAS Index",

    # ── Global Credit ───────────────────────────────────────────────────────
    "CDX IG CDSI GEN 5Y Corp", "CDX HY CDSI GEN 5Y SPRD Corp",

    # ── Commodities ─────────────────────────────────────────────────────────
    "CL1 Comdty", "GC1 Comdty", "HG1 Comdty",
    "XAU Curncy",            # Spot gold (used in quadrant)
    "SPBDU1BT Index",        # S&P U.S. Treasury 7-10Y Total Return (used in quadrant)
    "SPTR Index",            # S&P 500 Total Return (used in quadrant — rule is TR basis)

    # ── US Labour deep-dive (additions) ────────────────────────────────────
    "INJCJC Index",          # Initial Jobless Claims (weekly)
    "JOLTTOTL Index",        # JOLTS Job Openings (monthly)

    # ── US Real yields & inflation expectations ────────────────────────────
    "USGGT10Y Index",        # 10Y Real Yield (TIPS)
    "USGGBE10 Index",        # 10Y Breakeven Inflation
    "USGGBE05 Index",        # 5Y Breakeven Inflation
    "ACMTP10 Index",         # ACM 10Y Term Premium

    # ── US Funding rates / front-end ───────────────────────────────────────
    "SOFRRATE Index",        # SOFR Overnight Rate
    "USGG3M Index",          # 3M T-Bill Yield

    # ── DM / EM equity (ex-US, ex-China) ───────────────────────────────────
    "SXXP Index", "NKY Index", "HSI Index", "MXEF Index",
    "NDX Index", "RIY Index",
    # Added DM/EM benchmarks
    "UKX Index", "DAX Index", "CAC Index", "RTY Index", "TPX Index",
    "KOSPI Index", "NIFTY Index", "STI Index", "TWSE Index",

    # ── Risk / sentiment ───────────────────────────────────────────────────
    "SKEW Index",            # CBOE SKEW

    # ── Global growth pulse / commodities (additions) ──────────────────────
    "BDIY Index",            # Baltic Dry Index (global shipping)
    "IOE1 Comdty",           # SGX Iron Ore (CNY) — China construction proxy
    "CO1 Comdty",            # Brent Crude
    "NG1 Comdty",            # Natural Gas (Henry Hub)
    ".SHAUPREM G Index",     # Shanghai Gold Premium vs COMEX

    # ── Region-specific ────────────────────────────────────────────────────
    "GRIFPBUS Index",        # Germany IFO Business Climate
    "JNTSMFG Index",         # Japan Tankan Large Mfg (quarterly)

    # ── Equity tab — MSCI World GICS Sectors (11) ──────────────────────────
    "MXWO0EN Index", "MXWO0MT Index", "MXWO0IN Index", "MXWO0CD Index",
    "MXWO0CS Index", "MXWO0HC Index", "MXWO0FN Index", "MXWO0IT Index",
    "MXWO0TC Index", "MXWO0UT Index", "MXWO0RL Index",

    # ── Equity tab — MSCI World Factor Indices (13) ────────────────────────
    "MXWOMOM Index", "MXWO000V Index", "MXWO000G Index", "MXWOQU Index",
    "MXWDHDVD Index", "MXWDY Index", "MXWOBUY Index", "MXWOCYC Index",
    "MXWODEF Index", "MXWOLVE Index", "MXWOLC Index", "MXWOSC Index",
    "MXWOSIZ Index",

    # ── Equity tab — MSCI USA Factor Indices (11) ──────────────────────────
    "MXUSMOM Index", "MXUSVL Index", "MXUSGR Index", "MXUSQU Index",
    "MXUSDY Index", "MXUSBUY Index", "MXUSCYC Index", "MXUSDEF Index",
    "MXUSLEV Index", "MXUSLC Index", "MXUSSC Index",

    # ── Equity tab — MSCI Europe Factor Indices (9) ────────────────────────
    "MXEU000M Index", "MXEU000V Index", "MXEU000G Index", "MXEU000Q Index",
    "MXEUHDY Index", "MXEUCYC Index", "MXEUDEF Index", "MXEULC Index",
    "MXEUSC Index",

    # ── Equity tab — MSCI Asia Pacific Factor Indices (8) ──────────────────
    "M1APMMT Index", "MXAP000V Index", "MXAP000G Index", "M1APQU Index",
    "M1APHDY Index", "MXAPLC Index", "MXAPSC Index", "MXAP Index",

    # ── Equity tab — MSCI EM Factor Indices (7) ────────────────────────────
    "MXEF000M Index", "MXEF000V Index", "MXEF000G Index", "MXEF000Q Index",
    "MXEFHDY Index", "MXEFLC Index", "MXEFSC Index",

    # ── Equity tab — Factor proxies (when original MSCI factor tickers
    #    are not entitled on this Terminal) ───────────────────────────────
    # US factor ETFs (track MSCI USA factors near-perfectly):
    "MTUM US Equity", "VLUE US Equity", "IVW US Equity", "SDY US Equity",
    "PKW US Equity",  "USMV US Equity",
    # S&P 500 sector indices used as Cyclicals / Defensive proxies for USA:
    "S5COND Index", "S5INDU Index",
    "S5CONS Index", "S5HLTH Index", "S5UTIL Index",
    # S&P 500 Buyback (replacement for MSCI World Buyback):
    "SPBUYUP Index",
    # Europe momentum ETF + MSCI Europe sector indices for Cyc/Def composites:
    "CEMR GY Equity",
    "MXEU0CD Index", "MXEU0IN Index",
    "MXEU0CS Index", "MXEU0HC Index", "MXEU0UT Index",
    # Asia Pacific momentum ETF + Japan/EM factor pairs:
    "AAXJ US Equity",
    "MXJP000V Index", "MXJP000G Index",
    "M4JPQU Index",   "M1EFQU Index",
    "M4JPDY Index",   "M1APJDY Index",
    # EM momentum proxy ETF:
    "EEMV US Equity",

    # ── New 2026-06-24 indicator additions ─────────────────────────────────
    # US recession / credit stress
    "NYFYPROB Index",        # NY Fed Recession Probability (NTM)
    # US labour deeper
    "NAPMNEMP Index",        # ISM Services PMI — Employment sub-index
    # US consumer
    "USDECRED Index",        # Credit Card Delinquencies (quarterly)
    "TDBCTOTL Index",        # Total Consumer Credit (Fed)
    "RSTATOTL Index",        # Retail Sales level
    # US small business surveys
    "SBOITOTL Index",        # NFIB Small Business Optimism
    "SBOIHIRE Index",        # NFIB Hiring Plans
    # Conference Board components
    "CONCPSIT Index",        # Present Situation
    "CONCPEXP Index",        # Expectations (6-month)
    # US Housing extras
    "NHSLTOT Index",         # New Home Sales
    "NHSLMSPL Index",        # Months Supply
    "CNSTPUSA Index",        # Construction Spending (% YoY)
    # Europe Services PMI (composite + mfg already exist)
    "MPMIEZSA Index",        # Eurozone Services PMI (S&P Global)
    # Japan PMIs
    "MPMIJPMA Index",        # Japan PMI Manufacturing
    "MPMIJPSA Index",        # Japan PMI Services
    # China Caixin composite
    "MPMICNCA Index",        # China Caixin Composite PMI
    # Valuation / sentiment
    "SHILLERPE Index",       # Shiller CAPE PE
    "MCGDUS Index",          # Buffett Indicator (Mkt Cap / GDP)
    "AAIIBULL Index",        # AAII Bullish %
    "AAIIBEAR Index",        # AAII Bearish %
]
SNAPSHOT_FIELDS = ["PX_LAST", "CHG_NET_1D", "LAST_UPDATE_DT", "YLD_YTM_MID"]

# Bond instruments (GT*/GTDEM*/GTJPY*) report PX_LAST as price; the dashboard
# wants the yield. For these, expose YLD_YTM_MID as the metric value.
YIELD_TICKERS = {
    "GT2 Govt", "GT5 Govt", "GT10 Govt", "GT30 Govt",
    "GTDEM2Y Govt", "GTDEM10Y Govt", "GTDEM30Y Govt",
    "GTJPY2Y Govt", "GTJPY10Y Govt",
}

# Natural publishing cadence per ticker; drives the cache's stale-detection.
#   D = daily market series   W = weekly release
#   M = monthly release       Q = quarterly release
# Any ticker not listed here defaults to "D".
PERIODICITY: dict[str, str] = {
    # ── US — monthly economic releases ──────────────────────────────────────
    "NFP TCH Index": "M", "USURTOT Index": "M", "NAPMPMI Index": "M",
    "NAPMNMI Index": "M", "RSTAMOM Index": "M", "RSTAYOY Index": "M",
    "NHSPSTOT Index": "M", "CONCCONF Index": "M", "CONSSENT Index": "M",
    "CPI YOY Index": "M", "CPI CHNG Index": "M", "PCE CYOY Index": "M",
    "PCEPILFE Index": "M", "M2% YOY Index": "M",
    # US weekly / quarterly
    "GDP CQOQ Index": "Q", "NFCI Index": "W", "BLCITOTL Index": "W",
    "ECST T SLDETIGT Index": "Q",
    # US daily (rates/conditions)
    "BFCIUS Index": "D", "TOMOTCSO Index": "D",

    # ── China — monthly ─────────────────────────────────────────────────────
    "CHVAIOY Index": "M", "CNRSCYOY Index": "M", "CNFAYOY Index": "M",
    "CNPMIMFG Index": "M", "CNPMISRV Index": "M",
    "CNCPIYOY Index": "M", "CHEFTYOY Index": "M",
    "CNLNASF Index": "M", "CNMSM2 Index": "M", "CNLNNNML Index": "M",
    "CNMMTTL Index": "M", "CHLRLPR1 Index": "M",
    "CNGFOREX Index": "M", "CNFRBAL$ Index": "M",

    # ── Europe / UK ─────────────────────────────────────────────────────────
    "MPMIEZCA Index": "M", "MPMIEZMA Index": "M", "KXEZCPMF Index": "M",
    "UKCPIYOY Index": "M", "UMRTEMU Index": "M", "EUTRBAL Index": "M",
    "EURR002W Index": "M", "EURDEPO Index": "M",

    # ── Japan ───────────────────────────────────────────────────────────────
    "BJFXINOP Index": "M", "JNMBCABE Index": "M", "ECOYBJPN Index": "M",

    # ── Asia EM ─────────────────────────────────────────────────────────────
    "EHGDEMGY Index": "Q", "KOGDPQOQ Index": "Q",
    "EHPIEMG Index": "M", "MPMIKRMA Index": "M",
    "KOEAUERS Index": "M", "MPMIEMCA Index": "M",

    # ── New additions ──────────────────────────────────────────────────────
    "INJCJC Index": "W",     # Initial Jobless Claims
    "JOLTTOTL Index": "M",   # JOLTS Job Openings
    "GRIFPBUS Index": "M",   # Germany IFO
    "JNTSMFG Index": "Q",    # Japan Tankan
    "CPIQCSAN Index": "Q",   # US CPI quarterly SAAR

    # ── New 2026-06-24 additions ──────────────────────────────────────────
    "NYFYPROB Index": "M",   # NY Fed Recession Probability (monthly)
    "NAPMNEMP Index": "M",   # ISM Services Employment
    "USDECRED Index": "Q",   # Credit Card Delinquencies (quarterly)
    "TDBCTOTL Index": "M",   # Total Consumer Credit
    "RSTATOTL Index": "M",   # Retail Sales level
    "SBOITOTL Index": "M",   # NFIB Optimism
    "SBOIHIRE Index": "M",   # NFIB Hiring
    "CONCPSIT Index": "M",   # CB Present Situation
    "CONCPEXP Index": "M",   # CB Expectations
    "NHSLTOT Index":  "M",   # New Home Sales
    "NHSLMSPL Index": "M",   # Months Supply
    "CNSTPUSA Index": "M",   # Construction Spending
    "MPMIEZSA Index": "M",   # EA Services PMI
    "MPMIJPMA Index": "M",   # Japan Mfg PMI
    "MPMIJPSA Index": "M",   # Japan Services PMI
    "MPMICNCA Index": "M",   # China Caixin Composite
    "SHILLERPE Index":"M",   # Shiller CAPE
    "MCGDUS Index":   "Q",   # Buffett Indicator (quarterly GDP)
    "AAIIBULL Index": "W",   # AAII Bullish (weekly)
    "AAIIBEAR Index": "W",   # AAII Bearish (weekly)
    # (everything else — equity indices, FX, FX vol, credit indices, govt
    #  bonds, swaps, commodities, real yields, breakevens, SOFR — falls
    #  through to the default "D".)
}


def _period(ticker: str) -> str:
    return PERIODICITY.get(ticker, "D")


# Human-readable label per ticker (drives the Excel column headers, so a
# reader doesn't have to know that LCA3OAS Index means "US AAA Corp OAS").
TICKER_LABEL: dict[str, str] = {
    # US — rates & money
    "GT2 Govt": "2Y UST Yield",
    "GT5 Govt": "5Y UST Yield",
    "GT10 Govt": "10Y UST Yield",
    "GT30 Govt": "30Y UST Yield",
    "M2% YOY Index": "US M2 YoY",
    "TOMOTCSO Index": "Fed O/N Reverse Repo Rate",
    "BLCITOTL Index": "US Bank Loans (H.8)",
    # US — credit
    "CDX IG CDSI GEN 5Y Corp": "CDX IG 5Y Spread (bps)",
    "CDX HY CDSI GEN 5Y SPRD Corp": "CDX HY 5Y Spread (bps)",
    "LCA3OAS Index": "US AAA Corp OAS (bps)",
    "LCB1OAS Index": "US Baa Corp OAS (bps)",
    # US — growth
    "NAPMPMI Index": "ISM Manufacturing PMI",
    "NAPMNMI Index": "ISM Services PMI",
    "NFP TCH Index": "Nonfarm Payrolls (K)",
    "USURTOT Index": "US Unemployment Rate",
    "RSTAMOM Index": "US Retail Sales MoM",
    "RSTAYOY Index": "US Retail Sales YoY",
    "NHSPSTOT Index": "US Housing Starts (K)",
    "CONCCONF Index": "US Consumer Confidence",
    "CONSSENT Index": "U-Mich Consumer Sentiment",
    "GDP CQOQ Index": "US GDP QoQ Annualised",
    # US — inflation
    "CPI YOY Index": "US CPI YoY",
    "CPI CHNG Index": "US CPI MoM",
    "PCE CYOY Index": "US Core PCE YoY",
    "PCEPILFE Index": "US Core PCE Price Index",
    "CPIQCSAN Index": "US CPI Quarterly SAAR",
    # US — equity & vol
    "SPX Index": "S&P 500",
    "RTY Index": "Russell 2000",
    "UKX Index": "FTSE 100",
    "DAX Index": "DAX 40",
    "CAC Index": "CAC 40",
    "TPX Index": "TOPIX",
    "KOSPI Index": "KOSPI",
    "NIFTY Index": "NIFTY 50",
    "STI Index": "Straits Times",
    "TWSE Index": "Taiwan TAIEX",
    "VIX Index": "VIX (1M)",
    "VIX3M Index": "VIX 3M",
    "VIX6M Index": "VIX 6M",
    "VIX1Y Index": "VIX 12M",
    "MOVE Index": "MOVE Rates Vol",
    "BFCIUS Index": "Bloomberg US Financial Conditions",
    "NFCI Index": "Chicago Fed NFCI",
    "ECST T SLDETIGT Index": "SLOOS C&I Loan Tightening",
    # China
    "CHLRLPR1 Index": "China 1Y Loan Prime Rate",
    "CNMMTTL Index": "PBoC Balance Sheet",
    "CNMSM2 Index": "China M2 Money Supply",
    "USDCNH Curncy": "USD/CNH",
    "CHVAIOY Index": "China Industrial Production YoY",
    "CNRSCYOY Index": "China Retail Sales YoY",
    "CNFAYOY Index": "China Fixed Asset Investment YoY",
    "SHSZ300 Index": "CSI 300",
    "CNLNASF Index": "China Total Social Financing",
    "CNLNNNML Index": "China New Yuan Loans",
    "CNCPIYOY Index": "China CPI YoY",
    "CHEFTYOY Index": "China PPI YoY",
    "CNGFOREX Index": "China FX Reserves",
    "CNFRBAL$ Index": "China Trade Balance (USD)",
    "CNPMIMFG Index": "Caixin Manufacturing PMI",
    "CNPMISRV Index": "Caixin Services PMI",
    # Europe / UK
    "MPMIEZCA Index": "EA Composite PMI",
    "MPMIEZMA Index": "EA Manufacturing PMI",
    "UMRTEMU Index": "EA Unemployment Rate",
    "EURR002W Index": "ECB Policy Rate",
    "EURDEPO Index": "ECB Deposit Rate",
    "GTDEM2Y Govt": "2Y Bund Yield",
    "GTDEM10Y Govt": "10Y Bund Yield",
    "GTDEM30Y Govt": "30Y Bund Yield",
    "FWISEU55 Index": "EUR 5y5y Inflation Swap",
    "ITRX EUR CDSI GEN 5Y Corp": "iTraxx Europe IG 5Y (bps)",
    "ITRX XOVER CDSI GEN 5Y Corp": "iTraxx Crossover 5Y (bps)",
    "EUTRBAL Index": "EU Trade Balance",
    "KXEZCPMF Index": "EA HICP Flash",
    "UKCPIYOY Index": "UK CPI YoY",
    # Japan
    "BJFXINOP Index": "BoJ Balance Sheet",
    "JNMBCABE Index": "BoJ Current Account Balances",
    "ECOYBJPN Index": "Japan Trade Balance",
    "GTJPY2Y Govt": "2Y JGB Yield",
    "GTJPY10Y Govt": "10Y JGB Yield",
    "USDJPY Curncy": "USD/JPY",
    # Asia EM
    "EHGDEMGY Index": "EM Real GDP YoY",
    "EHPIEMG Index": "EM CPI YoY",
    "MPMIEMCA Index": "EM Composite PMI",
    "MPMIKRMA Index": "Korea Manufacturing PMI",
    "KOGDPQOQ Index": "Korea GDP QoQ",
    "KOEAUERS Index": "Korea Unemployment Rate",
    "CDX EM CDSI GEN 5Y SPRD Corp": "CDX EM 5Y Spread (bps)",
    "JPMVG71M Index": "JPM G7 FX Vol (1M)",
    "JPMVXYEM Index": "JPM EM FX Vol",
    # FX
    "DXY Curncy": "DXY Dollar Index",
    "EURUSD Curncy": "EUR/USD",
    "GBPUSD Curncy": "GBP/USD",
    "CVIX Index": "DB CVIX FX Vol",
    "EURUSDV1M BGN Curncy": "EUR/USD 1M Implied Vol",
    "USDJPYV1M BGN Curncy": "USD/JPY 1M Implied Vol",
    # Commodities
    "CL1 Comdty": "WTI Crude ($)",
    "GC1 Comdty": "Gold Future ($/oz)",
    "XAU Curncy": "Gold Spot ($/oz)",
    "SPBDU1BT Index": "S&P U.S. Treasury 7-10Y TR",
    "SPTR Index": "S&P 500 Total Return",
    "HG1 Comdty": "Copper",
    "CO1 Comdty": "Brent Crude ($)",
    "NG1 Comdty": "Natural Gas (Henry Hub, $)",
    "IOE1 Comdty": "Iron Ore (SGX, CNY)",
    "BDIY Index":  "Baltic Dry Index",
    ".SHAUPREM G Index": "Shanghai Gold Premium vs COMEX ($/oz)",
    # US labour deep-dive
    "INJCJC Index":   "US Initial Jobless Claims (K)",
    "JOLTTOTL Index": "JOLTS Job Openings",
    # US real yields & inflation expectations
    "USGGT10Y Index": "US 10Y Real Yield (TIPS)",
    "USGGBE10 Index": "US 10Y Breakeven Inflation",
    "USGGBE05 Index": "US 5Y Breakeven Inflation",
    "ACMTP10 Index":  "ACM 10Y Term Premium",
    # US funding / front-end
    "SOFRRATE Index": "SOFR Overnight Rate",
    "USGG3M Index":   "US 3M T-Bill Yield",
    # DM / EM equity
    "SXXP Index": "Stoxx 600",
    "NKY Index":  "Nikkei 225",
    "HSI Index":  "Hang Seng Index",
    "MXEF Index": "MSCI EM Equity",
    "NDX Index":  "Nasdaq 100",
    "RIY Index":  "Russell 1000",
    # Risk
    "SKEW Index":     "CBOE SKEW",
    # Region-specific
    "GRIFPBUS Index": "Germany IFO Business Climate",
    "JNTSMFG Index":  "Japan Tankan Large Mfg",

    # ── 2026-06-24 additions ──────────────────────────────────────────────
    # US recession / credit stress
    "NYFYPROB Index": "NY Fed Recession Probability (NTM)",
    # US labour deeper
    "NAPMNEMP Index": "ISM Services Employment Sub-index",
    # US consumer
    "USDECRED Index": "US Credit Card Delinquencies (%)",
    "TDBCTOTL Index": "US Total Consumer Credit ($Bn)",
    "RSTATOTL Index": "US Retail Sales ($Bn)",
    # NFIB
    "SBOITOTL Index": "NFIB Small Business Optimism",
    "SBOIHIRE Index": "NFIB Small Business Hiring Plans",
    # Conference Board components
    "CONCPSIT Index": "CB Present Situation",
    "CONCPEXP Index": "CB Expectations (6-month)",
    # US Housing
    "NHSLTOT Index":  "US New Home Sales (K)",
    "NHSLMSPL Index": "US New Home Months Supply",
    "CNSTPUSA Index": "US Construction Spending",
    # Europe / Japan / China PMIs
    "MPMIEZSA Index": "EA Services PMI",
    "MPMIJPMA Index": "Japan Manufacturing PMI",
    "MPMIJPSA Index": "Japan Services PMI",
    "MPMICNCA Index": "China Caixin Composite PMI",
    # Valuation / sentiment
    "SHILLERPE Index":"Shiller CAPE PE",
    "MCGDUS Index":   "Buffett Indicator (Mkt Cap / GDP)",
    "AAIIBULL Index": "AAII Bullish %",
    "AAIIBEAR Index": "AAII Bearish %",

    # Equity — MSCI World GICS Sectors
    "MXWO0EN Index": "MSCI World Energy",
    "MXWO0MT Index": "MSCI World Materials",
    "MXWO0IN Index": "MSCI World Industrials",
    "MXWO0CD Index": "MSCI World Consumer Discretionary",
    "MXWO0CS Index": "MSCI World Consumer Staples",
    "MXWO0HC Index": "MSCI World Health Care",
    "MXWO0FN Index": "MSCI World Financials",
    "MXWO0IT Index": "MSCI World Information Technology",
    "MXWO0TC Index": "MSCI World Communication Services",
    "MXWO0UT Index": "MSCI World Utilities",
    "MXWO0RL Index": "MSCI World Real Estate",
    # Equity — MSCI World Factor Indices
    "MXWOMOM Index":  "MSCI World Momentum",
    "MXWO000V Index": "MSCI World Value",
    "MXWO000G Index": "MSCI World Growth",
    "MXWOQU Index":   "MSCI World Quality",
    "MXWDHDVD Index": "MSCI World High Dividend Yield",
    "MXWDY Index":    "MSCI World Dividend Yield",
    "MXWOBUY Index":  "MSCI World Buyback Yield",
    "MXWOCYC Index":  "MSCI World Cyclicals",
    "MXWODEF Index":  "MSCI World Defensive",
    "MXWOLVE Index":  "MSCI World Low Leverage",
    "MXWOLC Index":   "MSCI World Large Cap",
    "MXWOSC Index":   "MSCI World Small Cap",
    "MXWOSIZ Index":  "MSCI World Size",
    # Equity — MSCI USA
    "MXUSMOM Index": "MSCI USA Momentum",
    "MXUSVL Index":  "MSCI USA Value",
    "MXUSGR Index":  "MSCI USA Growth",
    "MXUSQU Index":  "MSCI USA Quality",
    "MXUSDY Index":  "MSCI USA Dividend Yield",
    "MXUSBUY Index": "MSCI USA Buyback Yield",
    "MXUSCYC Index": "MSCI USA Cyclicals",
    "MXUSDEF Index": "MSCI USA Defensive",
    "MXUSLEV Index": "MSCI USA Low Leverage",
    "MXUSLC Index":  "MSCI USA Large Cap",
    "MXUSSC Index":  "MSCI USA Small Cap",
    # Equity — MSCI Europe
    "MXEU000M Index": "MSCI Europe Momentum",
    "MXEU000V Index": "MSCI Europe Value",
    "MXEU000G Index": "MSCI Europe Growth",
    "MXEU000Q Index": "MSCI Europe Quality",
    "MXEUHDY Index":  "MSCI Europe High Dividend Yield",
    "MXEUCYC Index":  "MSCI Europe Cyclicals",
    "MXEUDEF Index":  "MSCI Europe Defensive",
    "MXEULC Index":   "MSCI Europe Large Cap",
    "MXEUSC Index":   "MSCI Europe Small Cap",
    # Equity — MSCI Asia Pacific
    "M1APMMT Index": "MSCI AC Asia Pacific Momentum",
    "MXAP000V Index":"MSCI AC Asia Pacific Value",
    "MXAP000G Index":"MSCI AC Asia Pacific Growth",
    "M1APQU Index":  "MSCI AC Asia Pacific Quality",
    "M1APHDY Index": "MSCI AC Asia Pacific High Dividend Yield",
    "MXAPLC Index":  "MSCI AC Asia Pacific Large Cap",
    "MXAPSC Index":  "MSCI AC Asia Pacific Small Cap",
    "MXAP Index":    "MSCI AC Asia Pacific Broad",
    # Equity — MSCI EM
    "MXEF000M Index":"MSCI EM Momentum",
    "MXEF000V Index":"MSCI EM Value",
    "MXEF000G Index":"MSCI EM Growth",
    "MXEF000Q Index":"MSCI EM Quality",
    "MXEFHDY Index": "MSCI EM High Dividend Yield",
    "MXEFLC Index":  "MSCI EM Large Cap",
    "MXEFSC Index":  "MSCI EM Small Cap",
    # Equity factor proxies (ETFs and sector indices used when the MSCI
    # factor index isn't entitled on this Terminal)
    "MTUM US Equity": "iShares MSCI USA Momentum ETF",
    "VLUE US Equity": "iShares MSCI USA Value ETF",
    "IVW US Equity":  "iShares S&P 500 Growth ETF",
    "SDY US Equity":  "SPDR S&P Dividend ETF",
    "PKW US Equity":  "Invesco BuyBack Achievers ETF",
    "USMV US Equity": "iShares MSCI USA Min Vol ETF (Low Lev proxy)",
    "S5COND Index":   "S&P 500 Consumer Discretionary",
    "S5INDU Index":   "S&P 500 Industrials",
    "S5CONS Index":   "S&P 500 Consumer Staples",
    "S5HLTH Index":   "S&P 500 Health Care",
    "S5UTIL Index":   "S&P 500 Utilities",
    "SPBUYUP Index":  "S&P 500 Buyback Index",
    "CEMR GY Equity": "iShares Edge MSCI Europe Momentum ETF",
    "MXEU0CD Index":  "MSCI Europe Consumer Discretionary",
    "MXEU0IN Index":  "MSCI Europe Industrials",
    "MXEU0CS Index":  "MSCI Europe Consumer Staples",
    "MXEU0HC Index":  "MSCI Europe Health Care",
    "MXEU0UT Index":  "MSCI Europe Utilities",
    "AAXJ US Equity": "iShares MSCI All Country Asia ex-Japan ETF",
    "MXJP000V Index": "MSCI Japan Value",
    "MXJP000G Index": "MSCI Japan Growth",
    "M4JPQU Index":   "MSCI Japan Quality",
    "M1EFQU Index":   "MSCI EM Quality (Net TR)",
    "M4JPDY Index":   "MSCI Japan Dividend Yield",
    "M1APJDY Index":  "MSCI AC Asia Pacific ex-Japan High Div Yld",
    "EEMV US Equity": "iShares MSCI EM Min Vol ETF (Momentum proxy)",
}


def _label(ticker: str) -> str:
    """Human-readable name for a ticker (falls back to the ticker itself)."""
    return TICKER_LABEL.get(ticker, ticker)


# Tickers where Bloomberg returns the value in percent / decimal but the
# dashboard (and Excel) reports them in basis points. Multiplier applied
# in /api/export so the spreadsheet values match the dashboard.
SCALE_X100 = {"LCA3OAS Index", "LCB1OAS Index"}


# Map BDH periodicity strings → cache bucket period codes.
_BDH_TO_PERIOD = {"DAILY": "D", "WEEKLY": "W", "MONTHLY": "M", "QUARTERLY": "Q"}

MIME = {
    ".html": "text/html; charset=utf-8", ".js": "application/javascript",
    ".css": "text/css", ".json": "application/json; charset=utf-8",
    ".png": "image/png", ".jpg": "image/jpeg", ".ico": "image/x-icon",
}


# ── Bloomberg work (delegated to the data agent, cached in SQLite) ──────────
async def _refresh_macro(force: bool = False) -> dict:
    """Pull tickers from Bloomberg; serve unchanged tickers from cache.

    force=False (default): pull only tickers whose bucket has flipped — the
        normal cache-aware path. Cheap when called repeatedly.
    force=True:  pull every ticker regardless of bucket — what the Refresh
        button uses so the user explicitly gets the very latest values
        (e.g. intraday yields that already updated today).
    """
    tickers = list(dict.fromkeys(MACRO_TICKERS))
    now = datetime.now()
    period_map = {t: _period(t) for t in tickers}
    if force:
        stale = tickers
        print(f"  [REFRESH][FORCE]  BBG_PULL {len(tickers)} tickers "
              f"(button: pulling everything regardless of bucket)")
    else:
        stale = cache.stale_tickers(period_map, now)
        if not stale:
            print(f"  [REFRESH][AUTO]   CACHE_HIT all {len(tickers)} tickers "
                  f"in current bucket - 0 BBG calls")
        else:
            fresh = len(tickers) - len(stale)
            sample = ', '.join(stale[:5])
            more = f" +{len(stale)-5} more" if len(stale) > 5 else ""
            print(f"  [REFRESH][AUTO]   BBG_PULL {len(stale)} stale "
                  f"(CACHE_HIT {fresh}) - stale: {sample}{more}")

    if stale:
        raw = await bbg._post("/api/v1/bloomberg/bdp",
                              {"securities": stale, "fields": SNAPSHOT_FIELDS})
        new_metrics: dict[str, dict] = {}
        for t in stale:
            fields = raw.get(t) or {}
            if fields:
                px = fields.get("PX_LAST")
                yld = fields.get("YLD_YTM_MID")
                value = yld if (t in YIELD_TICKERS and yld is not None) else px
                new_metrics[t] = {
                    "value": value,
                    "chgAbs": fields.get("CHG_NET_1D"),
                    "lastUpdate": fields.get("LAST_UPDATE_DT"),
                    "error": None,
                }
            else:
                # Bloomberg returned nothing (release not out yet, or unknown
                # security). Fall back to whatever the cache already holds and
                # just bump cached_at so we don't keep retrying inside the same
                # bucket. Existing value/chgAbs/lastUpdate are preserved by the
                # next branch.
                new_metrics[t] = {
                    "value": None, "chgAbs": None, "lastUpdate": None,
                    "error": "Not found",
                }
        # If Bloomberg returned nothing fresher than what we already have, keep
        # the cached values (the "fallback to N-1" behaviour). Only the cached_at
        # gets bumped so the stale check moves us forward.
        cached_now, _ = cache.get_snapshot_all()
        for t, m in list(new_metrics.items()):
            if m["value"] is None and t in cached_now and cached_now[t]["value"] is not None:
                new_metrics[t] = cached_now[t] | {"error": None}
        cache.upsert_snapshot(new_metrics, period_map, now)

    metrics, latest_iso = cache.get_snapshot_all()
    if latest_iso:
        ts = datetime.fromisoformat(latest_iso)
        last_refresh = ts.strftime("%d %b %Y %H:%M")
    else:
        last_refresh = now.strftime("%d %b %Y %H:%M")
    return {"lastRefresh": last_refresh, "metrics": metrics}


async def _fetch_series(ticker: str, field: str, years: float,
                        periodicity: str, topup: bool = True,
                        start: date | None = None) -> dict:
    """Cached BDH lookup.

    Algorithm:
      1. Look up min/max(date) for (ticker, field, periodicity) in cache.
      2. Pull only the gaps:
         - FULL pull if the cache is empty (first-time population — always).
         - Newer top-up (cached_max+1 → today) ONLY on the topup path. This is
           the expensive daily pull; chart reads pass topup=False so they serve
           straight from SQLite (instant). The 06:05 scheduler and the Refresh
           button pass topup=True to fetch today's bar.
         - Older deepen if the caller requests a window older than cached
           (topup path only; window-expand). The daily refresh uses years=10,
           so for the 1990-anchored quadrant (cached_min ≪ start_required ≈
           now−10y) deepen never fires — only the newest bar tops up.
      3. UPSERT new bars; return everything in cache from start_required.

    `start` (ISO date) pins an explicit window start and overrides `years`; the
    quadrant anchors at 1990-01-01.
    """
    # For bond instruments, swap default PX_LAST → YLD_YTM_MID so charts show yields.
    if ticker in YIELD_TICKERS and field.upper() == "PX_LAST":
        field = "YLD_YTM_MID"

    now = datetime.now()
    today = date.today()
    start_required = start if start is not None else today - timedelta(days=int(365 * years))
    bucket_period = _BDH_TO_PERIOD.get(periodicity.upper(), "D")

    cached_min, cached_max = cache.series_range(ticker, field, periodicity)
    # "Did the daily update already run this calendar day for this series?" The
    # top-up is gated to ONCE PER DAY (not per D/W/M/Q bucket): a monthly series
    # whose current-month bar isn't published yet is "stale" all month, so
    # without this gate every update would re-pull it and find nothing — the
    # classic try-N/fallback-N-1 thrash. series_meta records the last attempt.
    meta_at = cache.series_meta_cached_at(ticker, field, periodicity)
    tried_today = (meta_at is not None) and (not cache.is_stale(meta_at, "D", now))
    pulls: list[tuple[date, date]] = []
    if cached_min is None:
        pulls.append((start_required, today))           # first-time — always pull
    elif topup and not tried_today:
        # topup is the live path (06:05 scheduler + Refresh). Normal chart reads
        # pass topup=False → NEVER pull (instant cache read). Gating on
        # tried_today caps live pulls at one per series per day.
        cached_max_dt = datetime.combine(cached_max, datetime.min.time())
        if cache.is_stale(cached_max_dt, bucket_period, now):
            pulls.append((cached_max + timedelta(days=1), today))   # missing days + today
        if cached_min > start_required:
            pulls.append((start_required, cached_min - timedelta(days=1)))  # deepen window once

    if not pulls:
        print(f"  [SERIES]          CACHE_HIT {ticker} ({periodicity}, "
              f"{years:g}y) - 0 BBG calls")
    else:
        kind = ("FULL"      if cached_min is None
                else "GAP_FILL")
        ranges = ' + '.join(f"{s}..{e}" for (s, e) in pulls)
        print(f"  [SERIES]          BBG_PULL {ticker} ({periodicity}, "
              f"{years:g}y) [{kind}] {ranges}")

    bbg_error: str | None = None
    for (start, end) in pulls:
        if start > end:
            continue
        try:
            # Always send an ISO date — the JCAP-pool BDH wrapper hangs on the
            # literal string "today" that local xbbg would otherwise accept.
            iso_end = (today if end >= today else end).strftime("%Y-%m-%d")
            raw = await bbg._post("/api/v1/bloomberg/bdh", {
                "securities": [ticker],
                "fields": [field],
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": iso_end,
                "periodicity": periodicity,
            })
        except Exception as exc:
            bbg_error = f"{type(exc).__name__}: {exc}"
            print(f"  [SERIES][BBG_DOWN] {ticker} ({periodicity}, {years:g}y) "
                  f"- serving cache only: {bbg_error}")
            break
        rows = raw.get(ticker) or []
        new_rows: list[tuple[date, float]] = []
        for r in rows:
            v = r.get(field.upper())
            d = r.get("date")
            if v is None or d is None:
                continue
            try:
                dt = date.fromisoformat(str(d)[:10])
                new_rows.append((dt, float(v)))
            except (ValueError, TypeError):
                continue
        cache.upsert_series_rows(ticker, field, periodicity, new_rows)

    # Stamp "checked today" only on the live (topup) path so a successful update
    # suppresses further same-day pulls. Cache-only chart reads must NOT stamp it
    # (they never pull, so they haven't actually refreshed the series).
    if topup and bbg_error is None:
        cache.upsert_series_meta(ticker, field, periodicity, now)
    points = cache.get_series_points(ticker, field, periodicity, start_required)
    result = {"ticker": ticker, "field": field, "points": points}
    if bbg_error:
        result["stale"] = True
        result["bbgError"] = bbg_error
    return result


_BUCKET_TO_BDH = {"D": "DAILY", "W": "WEEKLY", "M": "MONTHLY", "Q": "QUARTERLY"}


async def _warm_cache(years: float = 10.0) -> dict:
    """Pre-fill the historical-series cache for every ticker at its natural
    cadence (D/W/M/Q). Uses the existing _fetch_series gap-fill path, so
    anything already cached for that (ticker, field, periodicity, range)
    tuple is skipped — only the missing portion hits Bloomberg.
    """
    tickers = list(dict.fromkeys(MACRO_TICKERS))
    print(f"  [WARM] starting cache warm for {len(tickers)} tickers, {years:g}y")
    warmed: list[dict] = []
    errors: list[dict] = []
    for t in tickers:
        p = _period(t)
        bdh_p = _BUCKET_TO_BDH[p]
        field = "YLD_YTM_MID" if t in YIELD_TICKERS else "PX_LAST"
        try:
            r = await _fetch_series(t, field, years, bdh_p, topup=True)
            warmed.append({"ticker": t, "periodicity": p, "points": len(r["points"])})
        except Exception as exc:
            errors.append({"ticker": t, "error": f"{type(exc).__name__}: {exc}"})
            print(f"  [WARM][ERR] {t}: {exc}")
    print(f"  [WARM] done — {len(warmed)} ticker series cached, {len(errors)} errors")
    return {"years": years, "warmed": warmed, "errors": errors}


async def _warm_known() -> dict:
    """Gap-fill EVERY (ticker, field, periodicity) the dashboard has ever shown.

    The charts request many tickers at multiple periodicities (DAILY/WEEKLY/…),
    so warming only each ticker's natural cadence (_warm_cache) leaves the other
    cadences stale. The series table records exactly the (ticker, field,
    periodicity) tuples the charts use, so we gap-fill each one at its own
    cadence. years=10, but _fetch_series only tops up the missing newer bars
    (cached_max+1 → today), deepening once if a series is shallower than 10y —
    never a full re-pull (the once-per-day gate prevents thrash). This is the
    routine behind both the 06:05 scheduler and the Refresh button.
    """
    keys = cache.distinct_series_keys()
    print(f"  [WARM_KNOWN] gap-filling {len(keys)} displayed series (today top-up)")
    warmed = 0
    errors: list[dict] = []
    # Run concurrently (cap 8) so the per-series pool round-trips overlap. Safe:
    # _fetch_series never holds a SQLite connection across an await (the only
    # awaits are the network pulls), so there's no DB-lock contention — only the
    # network waits are parallelised. ~245 series go from minutes to ~tens of s.
    sem = asyncio.Semaphore(8)

    async def _one(t: str, f: str, p: str) -> None:
        nonlocal warmed
        async with sem:
            try:
                # years=10 so a series shallower than its deepest chart window
                # gets deepened once (then chart window-buttons read from cache).
                # The once-per-day gate in _fetch_series prevents re-pull thrash.
                await _fetch_series(t, f, 10, p, topup=True)
                warmed += 1
            except Exception as exc:
                errors.append({"ticker": t, "periodicity": p,
                               "error": f"{type(exc).__name__}: {exc}"})
                print(f"  [WARM_KNOWN][ERR] {t} ({p}): {exc}")

    await asyncio.gather(*[_one(t, f, p) for (t, f, p) in keys])
    print(f"  [WARM_KNOWN] done — {warmed} series topped up, {len(errors)} errors")
    return {"series": len(keys), "warmed": warmed, "errors": errors}


# ── Gold-to-S&P 500 Dividend ratio (Gavekal) ─────────────────────────────────
# The price of 1g of gold expressed as a multiple of the trailing SPX dividend
# per share (G/D — dimensionless; NOT "grams per dollar", that's the reciprocal):
#   div/share = PX_LAST(SPX) × EQY_DVD_YLD_12M(SPX) / 100   (yield×price ⇒ $ DPS)
#   gold/gram = PX_LAST(XAU Curncy) / 31.1034768
#   ratio     = gold_per_gram / div_per_share      (long-run mean ≈ 1)
# High ratio = gold expensive / shares cheap vs gold; low = the reverse. Rule:
# ratio confirmed above its 7y MA → remove gold hedge; below → maintain it.
# Field note: the pool serves EQY_DVD_YLD_12M (trailing 12M yield) for SPX back
# to 1993; EQY_DVD_YLD_IND returns nothing and IDX_EST_DVD_YLD only from 2006.
# Chart shows the ratio from 2000 with a 7-year MA; data is fetched from 1993 so
# the MA is full-window at the 2000 start. Cache-first via _fetch_series like
# every other series — once cached, _warm_known gap-fills all three legs daily.
GRAMS_PER_TROY_OZ = 31.1034768
_DIVGOLD_FETCH_START = date(1993, 1, 1)   # 2000 minus the 7y MA window
_DIVGOLD_CHART_START = date(2000, 1, 1)
_DIVGOLD_MA_DAYS = 1764                   # ≈ 7 years of trading days
_divgold_cache: dict = {"ts": 0.0, "years": None, "payload": None}
_DIVGOLD_TTL = 300.0

# NBER US recessions since 2000 — fallback when USRINDEX has no data.
_NBER_FALLBACK = [["2001-03-01", "2001-11-30"],
                  ["2007-12-01", "2009-06-30"],
                  ["2020-02-01", "2020-04-30"]]


async def _recession_bands(topup: bool = False) -> list[list[str]]:
    """[[start_iso, end_iso], ...]. Prefer Bloomberg's USRINDEX Index (NBER
    recession indicator, monthly 0/1) so future recessions appear without a
    code change; fall back to the static NBER list."""
    try:
        r = await _fetch_series("USRINDEX Index", "PX_LAST", 0, "MONTHLY",
                                topup=topup, start=_DIVGOLD_CHART_START)
        bands, cur = [], None
        for p in r.get("points", []):
            d = datetime.fromtimestamp(p["t"], tz=timezone.utc).date()
            if p["v"] >= 1:
                cur = [d, d] if cur is None else [cur[0], d]
            elif cur is not None:
                bands.append([cur[0].isoformat(), cur[1].isoformat()])
                cur = None
        if cur is not None:
            bands.append([cur[0].isoformat(), cur[1].isoformat()])
        if bands:
            return bands
    except Exception as exc:
        print(f"  [DIVGOLD] USRINDEX unavailable ({exc}) — static NBER dates")
    return [list(b) for b in _NBER_FALLBACK]


async def _divgold(years: float = 0.0, topup: bool = False) -> dict:
    """Gold-to-dividend payload. years=0 → full history from 2000; N → last N
    years. topup=True (Refresh) pulls today's bars; plain reads are cache-only
    (the first-ever call still populates EQY_DVD_YLD_IND with a full pull)."""
    xau = await _fetch_series("XAU Curncy", "PX_LAST", 0, "DAILY",
                              topup=topup, start=_DIVGOLD_FETCH_START)
    spx = await _fetch_series("SPX Index", "PX_LAST", 0, "DAILY",
                              topup=topup, start=_DIVGOLD_FETCH_START)
    yld = await _fetch_series("SPX Index", "EQY_DVD_YLD_12M", 0, "DAILY",
                              topup=topup, start=_DIVGOLD_FETCH_START)

    import pandas as pd

    def _ser(res: dict) -> "pd.Series":
        pts = res.get("points", [])
        idx = pd.to_datetime([datetime.fromtimestamp(p["t"], tz=timezone.utc).date()
                              for p in pts])
        return pd.Series([p["v"] for p in pts], index=idx)

    df = pd.DataFrame({"xau": _ser(xau), "spx": _ser(spx), "yld": _ser(yld)})
    # The yield can lag price by a bar around publication: carry values forward
    # briefly so one missing leg doesn't punch holes, then require all three.
    df = df.ffill(limit=5).dropna()
    if df.empty:
        return {"error": "no overlapping XAU / SPX / dividend-yield history in cache",
                "points": [], "stale": True}

    div_ps = df["spx"] * df["yld"] / 100.0
    ratio = (df["xau"] / GRAMS_PER_TROY_OZ) / div_ps
    ma = ratio.rolling(_DIVGOLD_MA_DAYS,
                       min_periods=int(_DIVGOLD_MA_DAYS * 0.75)).mean()

    start = (pd.Timestamp(date.today() - timedelta(days=int(365 * years)))
             if years and years > 0 else pd.Timestamp(_DIVGOLD_CHART_START))
    ratio_w, ma_w = ratio[ratio.index >= start], ma[ma.index >= start]

    last_r = float(ratio.iloc[-1])
    last_m = None if math.isnan(ma.iloc[-1]) else float(ma.iloc[-1])
    cheap = last_m is not None and last_r > last_m
    points = [{"d": d.strftime("%Y-%m-%d"), "r": round(float(r), 5),
               "m": None if math.isnan(m) else round(float(m), 5)}
              for (d, r), m in zip(ratio_w.items(), ma_w)]
    return {
        "points": points,
        "asOf": ratio.index[-1].strftime("%Y-%m-%d"),
        "last": {"ratio": round(last_r, 4),
                 "ma7y": None if last_m is None else round(last_m, 4)},
        "signal": {"sharesCheapVsGold": cheap,
                   "text": ("Ratio above 7y MA — gold expensive / shares cheap vs gold → remove gold hedge"
                            if cheap else
                            "Ratio below 7y MA — gold cheap / shares expensive vs gold → maintain gold hedge")},
        "recessions": await _recession_bands(topup=topup),
        "stale": bool(xau.get("stale") or spx.get("stale") or yld.get("stale")),
    }


async def _build_export(years: float = 5.0) -> bytes:
    """Pull every ticker at its natural cadence (cache-aware) and emit an
    Excel workbook with four sheets, data-scientist style:

      Snapshot      — current value per ticker (one row each)
      Monthly_Wide  — date x ticker, resampled to month-end, forward-filled
                      so every cell carries the latest as-of value
      Daily_Wide    — date x ticker for daily-cadence tickers only
      Long          — tidy (date, ticker, periodicity, value)
    """
    import io
    import pandas as pd

    tickers = list(dict.fromkeys(MACRO_TICKERS))
    snapshot, _ = cache.get_snapshot_all()

    # Snapshot sheet — lead with the human-readable name; ticker code follows.
    snap_rows = []
    for t in tickers:
        m = snapshot.get(t, {})
        v = m.get("value")
        if t in SCALE_X100 and v is not None:
            v = v * 100  # convert percent -> bps for the spreadsheet
        snap_rows.append({
            "name": _label(t),
            "ticker": t,
            "periodicity": _period(t),
            "value": v,
            "chg_abs": m.get("chgAbs"),
            "last_update": m.get("lastUpdate"),
            "error": m.get("error"),
        })
    # Derived: Baa-AAA credit spread (Baa OAS minus AAA OAS, both in bps
    # AFTER the x100 conversion above).
    aaa = snapshot.get("LCA3OAS Index", {}).get("value")
    baa = snapshot.get("LCB1OAS Index", {}).get("value")
    if aaa is not None and baa is not None:
        snap_rows.append({
            "name": "US Baa-Aaa Credit Spread (bps)",
            "ticker": "(LCB1OAS - LCA3OAS)",
            "periodicity": "D",
            "value": (baa - aaa) * 100,
            "chg_abs": None,
            "last_update": None,
            "error": None,
        })
    snap_df = pd.DataFrame(snap_rows)

    # Historical at natural cadence (uses /api/series cache + gap-fill path)
    long_rows = []
    for t in tickers:
        p = _period(t)
        field = "YLD_YTM_MID" if t in YIELD_TICKERS else "PX_LAST"
        try:
            result = await _fetch_series(t, field, years, _BUCKET_TO_BDH[p])
        except Exception as exc:
            print(f"  [EXPORT]          skip {t}: {type(exc).__name__}: {exc}")
            continue
        scale = 100 if t in SCALE_X100 else 1
        for pt in result.get("points", []):
            d = datetime.fromtimestamp(pt["t"], tz=timezone.utc).date()
            long_rows.append({"date": pd.Timestamp(d), "ticker": t,
                              "periodicity": p, "value": pt["v"] * scale})

    long_df = pd.DataFrame(long_rows)
    if long_df.empty:
        # Still produce a workbook so the UI doesn't break.
        daily_wide = pd.DataFrame()
        monthly_wide = pd.DataFrame()
    else:
        # Drop accidental duplicates so pivot doesn't blow up
        long_df = long_df.drop_duplicates(subset=["date", "ticker"], keep="last")
        wide = long_df.pivot(index="date", columns="ticker",
                             values="value").sort_index()
        # Monthly grid: align everything to month-end, forward-fill so each
        # cell is the latest as-of observation up to that month.
        monthly_wide = wide.resample("ME").last().ffill()
        # Daily grid: only tickers with D periodicity, reindexed onto a
        # complete calendar-day grid so weekends and holidays carry the
        # prior trading day's value via forward-fill.
        d_tickers = [t for t in tickers if _period(t) == "D"]
        d_long = long_df[long_df["ticker"].isin(d_tickers)]
        if not d_long.empty:
            daily_raw = d_long.pivot(index="date", columns="ticker",
                                     values="value").sort_index()
            full_idx = pd.date_range(daily_raw.index.min(),
                                     daily_raw.index.max(), freq="D")
            daily_wide = daily_raw.reindex(full_idx).ffill()
            daily_wide.index.name = "date"
        else:
            daily_wide = pd.DataFrame()

        # Derived series: US Baa-Aaa credit spread, appended to both grids
        # so analysts get the spread without subtracting columns by hand.
        if {"LCA3OAS Index", "LCB1OAS Index"}.issubset(monthly_wide.columns):
            monthly_wide["US Baa-Aaa Credit Spread (bps)"] = (
                monthly_wide["LCB1OAS Index"] - monthly_wide["LCA3OAS Index"])
        if not daily_wide.empty and {"LCA3OAS Index", "LCB1OAS Index"}.issubset(daily_wide.columns):
            daily_wide["US Baa-Aaa Credit Spread (bps)"] = (
                daily_wide["LCB1OAS Index"] - daily_wide["LCA3OAS Index"])

        # Rename ticker columns -> human-readable labels in both wide sheets.
        monthly_wide = monthly_wide.rename(columns=TICKER_LABEL)
        if not daily_wide.empty:
            daily_wide = daily_wide.rename(columns=TICKER_LABEL)

        # Add a "name" column to the Long sheet too so the tidy view is readable.
        long_df = long_df.assign(name=long_df["ticker"].map(_label))
        long_df = long_df[["date", "name", "ticker", "periodicity", "value"]]

    print(f"  [EXPORT]          wrote {len(snap_df)} snapshot rows, "
          f"{len(long_df)} historical rows, "
          f"{len(monthly_wide)} monthly grid rows, "
          f"{len(daily_wide)} daily grid rows")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        snap_df.to_excel(writer, sheet_name="Snapshot", index=False)
        if not monthly_wide.empty:
            monthly_wide.to_excel(writer, sheet_name="Monthly_Wide")
        if not daily_wide.empty:
            daily_wide.to_excel(writer, sheet_name="Daily_Wide")
        if not long_df.empty:
            long_df.to_excel(writer, sheet_name="Long", index=False)
    return buf.getvalue()


# ── Vanda Positioning loaders ───────────────────────────────────────────────
def _vanda_latest_csv(folder: str) -> str | None:
    """Newest <CODE>_<YYYYMMDD>.csv in a folder (by trailing date token)."""
    try:
        files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
    except OSError:
        return None
    if not files:
        return None

    def _key(fn: str) -> str:
        tok = os.path.splitext(fn)[0].rsplit("_", 1)[-1]
        return tok if tok.isdigit() else "0"

    files.sort(key=_key)
    return os.path.join(folder, files[-1])


def _read_vanda_csv(path: str, start: date | None) -> list[dict]:
    """Parse `date,combined_positioning` → [{t,v}] (UTC epoch seconds)."""
    import csv as _csv
    out: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as fh:
            for row in _csv.DictReader(fh):
                ds = (row.get("date") or "").strip()
                vs = (row.get("combined_positioning") or "").strip()
                if not ds or not vs:
                    continue
                try:
                    d = date.fromisoformat(ds[:10])
                    v = float(vs)
                except (ValueError, TypeError):
                    continue
                if start and d < start:
                    continue
                dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
                out.append({"t": int(dt.timestamp()), "v": v})
    except (OSError, UnicodeDecodeError, _csv.Error):
        # A non-UTF-8 or malformed CSV must skip THIS instrument only, not 500
        # the whole /api/vanda endpoint. (UnicodeDecodeError is a ValueError
        # subclass but NOT an OSError; csv.Error derives from Exception.)
        return []
    out.sort(key=lambda p: p["t"])
    return out


def _load_vanda(years: float) -> dict:
    """Build the Vanda payload: one series per instrument, trimmed to `years`.

    years <= 0 returns full history. Instruments are discovered by scanning
    VANDA_DIR subfolders so newly-added instruments appear automatically;
    known codes get a friendly name/group/order from VANDA_META.
    """
    start = (date.today() - timedelta(days=int(365 * years))) if years and years > 0 else None
    instruments: list[dict] = []
    as_of: str | None = None
    try:
        subdirs = [d for d in os.listdir(VANDA_DIR)
                   if os.path.isdir(os.path.join(VANDA_DIR, d))]
    except OSError as exc:
        return {"error": f"Vanda dir unreadable: {exc}", "instruments": []}
    for sub in subdirs:
        csv_path = _vanda_latest_csv(os.path.join(VANDA_DIR, sub))
        if not csv_path:
            continue
        # CODE = folder suffix after " - ", else the filename stem before _date.
        code = (sub.split(" - ")[-1].strip() if " - " in sub
                else os.path.basename(csv_path).rsplit("_", 1)[0])
        meta = VANDA_META.get(code, {"name": code, "group": "Other", "order": 99})
        points = _read_vanda_csv(csv_path, start)
        if not points:
            continue
        last_iso = datetime.fromtimestamp(
            points[-1]["t"], tz=timezone.utc).strftime("%Y-%m-%d")
        if as_of is None or last_iso > as_of:
            as_of = last_iso
        instruments.append({
            "code": code, "name": meta["name"], "group": meta["group"],
            "order": meta["order"], "file": os.path.basename(csv_path),
            "points": points,
        })
    instruments.sort(key=lambda x: (x["order"], x["name"]))
    return {"asOf": as_of, "years": years, "instruments": instruments}


def _run(coro):
    """Run an async data-agent coroutine from this sync request thread."""
    return asyncio.run(coro)


# ── Regime-favored asset backtest ─────────────────────────────────────────────
_regime_picks_cache: dict = {"ts": 0.0, "payload": None}
_REGIME_PICKS_TTL = 3600.0

# Investable universe (index/security with meaningful price returns). Vanda
# code maps positioning weighting; "" = no positioning series → neutral weight.
# Assets are ranked; only usable-history tickers survive the backtest.
_PICK_UNIVERSE = [
    ("USEQCOMB",      "SPX Index",      "S&P 500"),
    ("",              "NDX Index",      "Nasdaq 100"),
    ("",              "SXXP Index",     "Stoxx 600"),
    ("JPEQCOMB",      "NKY Index",      "Nikkei 225"),
    ("CNEQASHRCOMB",  "SHSZ300 Index",  "CSI 300"),
    ("CNEQHSHRCOMB",  "HSI Index",      "Hang Seng"),
    ("EMEQCOMB",      "MXEF Index",     "MSCI EM"),
    ("GLCOMCOMB-GC1", "XAU Curncy",     "Gold"),
    ("GLCOMCOMB-CO1", "CO1 Comdty",     "Brent Oil"),
    ("GLCOMCOMB-CL1", "CL1 Comdty",     "WTI Oil"),
    ("GLCOMCOMB-SI1", "XAG Curncy",     "Silver"),
    ("",              "NG1 Comdty",     "Natural Gas"),
    ("",              "HG1 Comdty",     "Copper"),
    ("",              "DXY Curncy",     "Dollar Index"),
]
_REGIME_NAMES = {0: "Deflationary Bust", 1: "Deflationary Boom",
                 2: "Inflationary Bust",  3: "Inflationary Boom"}

# ── Backtest knobs (deterministic; adjust here, no other dependency) ──────────
_FWD_MONTHS = 3          # forward-return horizon
_H_REGIME = 1.0          # regime-similarity kernel bandwidth (standardized units)
_H_VANDA = 1.0           # positioning-similarity kernel bandwidth (z-score units)
_REGIME_LOOKBACK_Y = 7   # trailing window for the growth/inflation scores
_THIN_EPISODES = 2       # ≤ this many distinct regime episodes → flag "thin"
_THIN_EFFN = 4.0         # effective sample (Kish) below this → flag "thin"
_THIN_TSTAT = 1.0        # |t| below this → not statistically distinguishable


def _month_add(ym: tuple, k: int) -> tuple:
    """Add k months to a (year, month) tuple."""
    idx = ym[0] * 12 + (ym[1] - 1) + k
    return (idx // 12, idx % 12 + 1)


def _compute_regime_picks() -> dict:
    """Deterministic, kernel-weighted backtest of asset performance conditioned
    on the current macro regime AND current Vanda positioning.

    Fully systematic — no randomness, no LLM, no bootstrap. Method:

      1. REGIME COORDINATES. Each month is scored by 7-year simple returns:
           growth    = SPX 7y return − Oil 7y return
           inflation = Gold 7y return − Bond(7-10y UST TR) 7y return
         The hard 4-quadrant label (sign of each) is kept only for display and
         for counting distinct episodes.

      2. SOFT REGIME SIMILARITY (replaces the hard same-quadrant filter so
         near-boundary months still contribute, down-weighted — grows the
         effective sample and removes the knife-edge at 0). Each month gets a
         Gaussian weight on its standardized distance from TODAY's coordinates:
           Kr = exp(−½ · (dist / H_REGIME)²),
           dist² = ((g−g₀)/σ_g)² + ((inf−inf₀)/σ_inf)².

      3. SOFT VANDA SIMILARITY. Gaussian weight on positioning z-score distance
         to today:  Kv = exp(−½ · ((z₀−z_hist)/H_VANDA)²). Months without a
         Vanda reading (pre-2010) take Kv = 1 (regime-only weighting).

      4. NON-OVERLAPPING 3-month forward windows (stride = horizon, anchored at
         the most recent complete window). Independent observations → the
         effective-N, t-stat and information ratio are statistically honest
         rather than inflated by overlapping-window pseudo-replication.

      5. CROSS-SECTIONAL EXCESS RETURN. Each window, subtract the mean forward
         return of the assets trading that month → ranks RELATIVE outperformance
         (removes the "everything rises in a bull regime" illusion).

      6. PER-ASSET STATS (combined weight w = Kr·Kv):
           excess3M  = Σw·ex / Σw                         (weighted mean excess)
           vol       = sqrt(Σw·(ex−excess3M)² / Σw)       (weighted excess vol)
           infoRatio = excess3M / vol                     (risk-adjusted, #3)
           effN      = (Σw)² / Σw²                         (Kish effective N, #1)
           tStat     = excess3M / (vol / sqrt(effN))      (significance, #1)
           winRate   = Σ(w over r>0) / Σw                 (chance it rose)
           episodes  = distinct contiguous runs of the current regime (#1)
         Ranked by excess3M; assets resting on ≤2 episodes / tiny effN / |t|<1
         are flagged `thin` (statistically weak, not hidden).
    """
    _bt_start = date(1990, 1, 1)
    _epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _to_monthly(ticker: str) -> dict:
        # Month-end value (last daily print in each calendar month).
        pts = cache.get_series_points(ticker, "PX_LAST", "DAILY", _bt_start)
        monthly: dict = {}
        for p in pts:
            if p["v"] is None:
                continue
            dt = _epoch + timedelta(seconds=p["t"])
            monthly[(dt.year, dt.month)] = p["v"]   # later print overwrites → month-end
        return monthly

    spx_m  = _to_monthly("SPX Index")
    oil_m  = _to_monthly("CO1 Comdty")
    gold_m = _to_monthly("XAU Curncy")
    bond_m = _to_monthly("SPBDU1BT Index")

    common = sorted(set(spx_m) & set(oil_m) & set(gold_m) & set(bond_m))
    if len(common) < 85:
        return {"error": "insufficient regime data"}

    # 1. Regime coordinates + hard label for every month with a 7y lookback.
    coords: dict = {}   # ym -> (growth, inflation, hard_regime)
    for ym in common:
        past = _month_add(ym, -12 * _REGIME_LOOKBACK_Y)
        if past not in spx_m or past not in oil_m:
            continue
        if past not in gold_m or past not in bond_m:
            continue
        g = spx_m[ym] / spx_m[past] - 1 - (oil_m[ym] / oil_m[past] - 1)
        inf = gold_m[ym] / gold_m[past] - 1 - (bond_m[ym] / bond_m[past] - 1)
        coords[ym] = (g, inf, (1 if g > 0 else 0) | (2 if inf > 0 else 0))

    if not coords:
        return {"error": "no regime data after 7y lookback"}
    current_ym = max(coords)
    g0, inf0, current_regime = coords[current_ym]

    # Standardize the regime axes (population std) so the 2-D distance is
    # scale-free — growth and inflation differentials have different spreads.
    def _pstd(xs: list) -> float:
        n = len(xs)
        if n < 2:
            return 0.0
        mu = sum(xs) / n
        return math.sqrt(sum((x - mu) ** 2 for x in xs) / n)
    sg = _pstd([v[0] for v in coords.values()]) or 1.0
    si = _pstd([v[1] for v in coords.values()]) or 1.0

    def _regime_kernel(ym: tuple) -> float:
        g, inf, _ = coords[ym]
        d2 = ((g - g0) / sg) ** 2 + ((inf - inf0) / si) ** 2
        return math.exp(-0.5 * d2 / (_H_REGIME ** 2))

    # 3. Vanda positioning history + today's z per instrument.
    vanda = _load_vanda(0)
    current_z: dict = {}
    vanda_m: dict = {}
    for inst in vanda.get("instruments", []):
        code = inst["code"]
        pts = inst.get("points", [])
        if not pts:
            continue
        current_z[code] = pts[-1]["v"]
        mm: dict = {}
        for p in pts:
            dt = _epoch + timedelta(seconds=p["t"])
            mm[(dt.year, dt.month)] = p["v"]
        vanda_m[code] = mm

    # 4. Non-overlapping anchor months: stride back by the horizon from the most
    # recent month that still has a forward window (current_ym − FWD).
    anchors: list = []
    ym = _month_add(current_ym, -_FWD_MONTHS)
    while ym in coords:
        anchors.append(ym)
        ym = _month_add(ym, -_FWD_MONTHS)
    anchors.sort()

    asset_monthly = {t: _to_monthly(t) for _, t, _ in _PICK_UNIVERSE}

    # 5. Per anchor: each asset's forward return + cross-sectional mean → excess.
    #    Collect (excess, weight, raw_return) rows per asset.
    rows: dict = {t: [] for _, t, _ in _PICK_UNIVERSE}
    for a in anchors:
        fwd = _month_add(a, _FWD_MONTHS)
        kr = _regime_kernel(a)
        month_rets: dict = {}
        for vcode, ticker, _ in _PICK_UNIVERSE:
            am = asset_monthly[ticker]
            if a not in am or fwd not in am or am[a] == 0:
                continue
            month_rets[ticker] = am[fwd] / am[a] - 1
        if not month_rets:
            continue
        xbar = sum(month_rets.values()) / len(month_rets)   # cross-sectional mean
        for vcode, ticker, _ in _PICK_UNIVERSE:
            if ticker not in month_rets:
                continue
            r = month_rets[ticker]
            kv = 1.0
            if vcode in current_z and vcode in vanda_m:
                hz = vanda_m[vcode].get(a)
                if hz is not None:
                    kv = math.exp(-0.5 * ((current_z[vcode] - hz) / _H_VANDA) ** 2)
            rows[ticker].append((r - xbar, kr * kv, r))

    # 6. Episode counter: distinct contiguous runs of the current regime over the
    #    months where the asset has a spot price (honest "how many occurrences").
    def _episodes(am: dict) -> int:
        months = sorted(ym for ym in coords
                        if ym in am and coords[ym][2] == current_regime)
        if not months:
            return 0
        eps = 1
        for prev, cur in zip(months, months[1:]):
            gap = (cur[0] - prev[0]) * 12 + (cur[1] - prev[1])
            if gap != 1:
                eps += 1
        return eps

    results = []
    for vcode, ticker, name in _PICK_UNIVERSE:
        data = rows[ticker]
        if not data:
            continue
        W = sum(w for _, w, _ in data)
        if W <= 0:
            continue
        excess = sum(w * ex for ex, w, _ in data) / W
        var = sum(w * (ex - excess) ** 2 for ex, w, _ in data) / W
        vol = math.sqrt(var)
        effn = (W * W) / sum(w * w for _, w, _ in data)
        se = (vol / math.sqrt(effn)) if effn > 0 else float("inf")
        tstat = (excess / se) if se > 0 else 0.0
        ir = (excess / vol) if vol > 0 else 0.0
        win = sum(w for _, w, r in data if r > 0) / W
        avg_abs = sum(w * r for _, w, r in data) / W
        eps = _episodes(asset_monthly[ticker])
        thin = (eps <= _THIN_EPISODES or effn < _THIN_EFFN
                or abs(tstat) < _THIN_TSTAT)
        # Current trailing 3-month return (latest month-end vs 3 months prior).
        am = asset_monthly[ticker]
        cur3m = None
        if am:
            last = max(am)
            prev = _month_add(last, -_FWD_MONTHS)
            if prev in am and am[prev]:
                cur3m = am[last] / am[prev] - 1
        results.append({
            "ticker": ticker, "name": name,
            "current3M": round(cur3m, 4) if cur3m is not None else None,  # trailing 3M
            "excess3M": round(excess, 4),    # relative outperformance
            "avg3M": round(avg_abs, 4),       # historical forward return (rank key)
            "infoRatio": round(ir, 3),        # risk-adjusted
            "tStat": round(tstat, 2),         # significance given effective N
            "winRate": round(win, 3),         # chance it rose
            "nWindows": len(data),            # non-overlapping windows used
            "effN": round(effn, 1),           # Kish effective sample size
            "episodes": eps,                  # distinct regime occurrences
            "thin": thin,
            "z": round(current_z.get(vcode, 0.0), 2),
        })

    results.sort(key=lambda x: x["avg3M"], reverse=True)
    return {
        "regime": _REGIME_NAMES.get(current_regime, "?"),
        "growth": round(g0, 3),
        "inflation": round(inf0, 3),
        "horizonMonths": _FWD_MONTHS,
        "nAnchors": len(anchors),
        "lookbackY": _REGIME_LOOKBACK_Y,
        "hRegime": _H_REGIME,
        "hVanda": _H_VANDA,
        "assets": results,
        "computedAt": datetime.now().isoformat(timespec="seconds"),
    }


# ── Daily update routine + scheduler (browser-independent) ───────────────────
# ONE update routine, run two ways: automatically at 06:05 local (this thread)
# and on demand from the Refresh button (POST /api/update). It (1) re-reads the
# Vanda CSVs (the 06:00 scraper has just written them), (2) pulls a fresh BBG
# snapshot, and (3) gap-fills EVERY displayed series (missing days + today; or
# today only if no gap). Chart reads stay cache-only (fast); this keeps that
# cache fresh whether or not a browser is open.
DAILY_REFRESH_HOUR = int(os.environ.get("DAILY_REFRESH_HOUR", 6))
DAILY_REFRESH_MIN = int(os.environ.get("DAILY_REFRESH_MIN", 5))


def _seconds_until(hour: int, minute: int) -> float:
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


# Vanda live-scrape: run the Playwright downloader on demand (Refresh button)
# so the dashboard pulls FRESH CSVs from vandaxasset.com, not just whatever the
# 06:00 scheduled task last wrote. Same interpreter as this server (system
# Python 3.13 has playwright+dotenv) and the same browser cache path the
# scheduled task uses. Best-effort + single-flight: a slow/failed scrape never
# blocks the rest of the update.
_WEB_AUTOSCRAP = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "web_autoscrap"))
_VANDA_SCRAPER = os.path.join(_WEB_AUTOSCRAP, "vandax_csv_download.py")
_PLAYWRIGHT_BROWSERS = r"C:\Users\mchso\PlaywrightBrowsers"
_scrape_lock = threading.Lock()


def _scrape_vanda(timeout: float = 300.0) -> dict:
    """Download fresh Vanda CSVs from the website (subprocess). Writes into
    VANDA_DIR per web_autoscrap/settings.txt; _load_vanda then re-reads them."""
    if not os.path.exists(_VANDA_SCRAPER):
        return {"ok": False, "error": f"scraper not found: {_VANDA_SCRAPER}"}
    if not _scrape_lock.acquire(blocking=False):
        return {"ok": False, "error": "a scrape is already running"}
    try:
        env = dict(os.environ)
        env["PLAYWRIGHT_BROWSERS_PATH"] = _PLAYWRIGHT_BROWSERS
        env["PYTHONIOENCODING"] = "utf-8"
        print(f"  [VANDA-SCRAPE] launching fresh download (timeout {timeout:.0f}s)")
        try:
            proc = subprocess.run(
                [sys.executable, _VANDA_SCRAPER],
                cwd=_WEB_AUTOSCRAP, env=env,
                capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            print(f"  [VANDA-SCRAPE][ERR] timed out after {timeout:.0f}s")
            return {"ok": False, "error": f"timed out ({timeout:.0f}s)"}
        except Exception as exc:
            print(f"  [VANDA-SCRAPE][ERR] {type(exc).__name__}: {exc}")
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        tail = [ln for ln in (proc.stdout or "").splitlines() if ln.strip()][-1:]
        ok = proc.returncode == 0
        print(f"  [VANDA-SCRAPE] rc={proc.returncode} "
              f"{'OK' if ok else 'FAIL'} — {tail[0] if tail else ''}")
        return {"ok": ok, "returncode": proc.returncode,
                "detail": tail[0] if tail else ""}
    finally:
        _scrape_lock.release()


def _daily_refresh_once(scrape: bool = False) -> dict:
    """One full update pass: (optional live Vanda scrape →) reload Vanda CSVs +
    BBG snapshot + gap-fill all series.

    `scrape=True` (Refresh button) first re-downloads the Vanda CSVs from the
    website; the 06:05 scheduler passes scrape=False since the 06:00 task already
    scrapes. Returns a small status dict so the Refresh button can report back.
    """
    print(f"  [UPDATE] {datetime.now():%Y-%m-%d %H:%M} update starting"
          f"{' (with live Vanda scrape)' if scrape else ''}")
    out: dict = {"status": "ok", "vandaAsOf": None, "snapshot": False,
                 "seriesWarmed": 0, "bbg": False, "vandaScrape": None}
    # Optional: pull fresh CSVs from vandaxasset.com before re-reading them.
    if scrape:
        out["vandaScrape"] = _scrape_vanda()
    # Vanda: drop the TTL cache, re-parse the (freshly-scraped or 06:00) CSVs, and
    # store the result back so the next /api/vanda hit is a warm cache hit.
    _vanda_cache.update(ts=0.0, years=None, payload=None)
    try:
        payload = _load_vanda(2)
        _vanda_cache.update(ts=time.time(), years=2, payload=payload)
        out["vandaAsOf"] = payload.get("asOf")
    except Exception as exc:
        print(f"  [UPDATE][VANDA][ERR] {exc}")
    # BBG: refresh snapshot (current values) + gap-fill every displayed series.
    if bbg is not None:
        try:
            if bbg.is_bloomberg_available():
                _run(_refresh_macro(force=True))
                out["snapshot"] = True
                warm = _run(_warm_known())
                out["seriesWarmed"] = warm.get("warmed", 0)
                out["bbg"] = True
            else:
                out["status"] = "bbg_unavailable"
                print("  [UPDATE] Bloomberg unavailable — skipped BBG refresh "
                      "(charts serve last cache)")
        except Exception as exc:
            out["status"] = "error"
            out["error"] = f"{type(exc).__name__}: {exc}"
            print(f"  [UPDATE][BBG][ERR] {exc}")
    print(f"  [UPDATE] {datetime.now():%H:%M} update done — "
          f"snapshot={out['snapshot']} series={out['seriesWarmed']} "
          f"vandaAsOf={out['vandaAsOf']}")
    return out


def _daily_refresh_loop() -> None:
    now = datetime.now()
    cutoff = now.replace(hour=DAILY_REFRESH_HOUR, minute=DAILY_REFRESH_MIN,
                         second=0, microsecond=0)
    if now >= cutoff:
        print(f"  [SCHED] boot catch-up — past "
              f"{DAILY_REFRESH_HOUR:02d}:{DAILY_REFRESH_MIN:02d}, refreshing cache")
        try:
            _daily_refresh_once()
        except Exception as exc:
            print(f"  [SCHED][ERR] boot catch-up: {exc}")
    while True:
        time.sleep(_seconds_until(DAILY_REFRESH_HOUR, DAILY_REFRESH_MIN))
        try:
            _daily_refresh_once()
        except Exception as exc:
            print(f"  [SCHED][ERR] {exc}")
        time.sleep(90)  # clear the trigger minute so we don't double-fire


# ── HTTP handler ────────────────────────────────────────────────────────────
class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=WEB_DIR, **kw)

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        # Force the browser to revalidate every response. Without this, an
        # old index.html cached on a previous visit (before the fetch shim
        # existed) is served forever — meaning every /api/* call goes out
        # without the ngrok-skip header and the front-end gets HTML back.
        self.send_header("Cache-Control",
                         "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _json(self, body: bytes, status: int = 200):
        ae = self.headers.get("Accept-Encoding", "")
        if "gzip" in ae and len(body) > 1024:
            body = _gzip.compress(body, compresslevel=1)
            self.send_response(status)
            self.send_header("Content-Encoding", "gzip")
        else:
            self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _agent_down(self) -> bytes | None:
        if bbg is None:
            return json.dumps({
                "status": "error",
                "message": f"Local data agent import failed: {_AGENT_IMPORT_ERROR}. "
                           f"Ensure ./tools/bloomberg_api.py exists and xbbg is installed.",
            }).encode()
        return None

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)

        # /api/macro — serve cached snapshot from SQLite
        if path == "/api/macro":
            metrics, latest_iso = cache.get_snapshot_all()
            if latest_iso:
                ts = datetime.fromisoformat(latest_iso)
                last_refresh = ts.strftime("%d %b %Y %H:%M")
            else:
                last_refresh = None
            print(f"  [MACRO]           CACHE_HIT {len(metrics)} tickers "
                  f"(snapshot served from SQLite, no BBG)")
            body = json.dumps({"lastRefresh": last_refresh,
                               "lastRefreshIso": latest_iso,
                               "metrics": metrics},
                              separators=(",", ":")).encode()
            return self._json(body)

        # /api/vanda?years=N[&fresh=1] — Vanda positioning series from CSV
        if path == "/api/vanda":
            try:
                years = float((qs.get("years") or ["3"])[0])
            except ValueError:
                years = 3.0
            fresh = (qs.get("fresh") or ["0"])[0] in ("1", "true", "yes")
            now_t = time.time()
            c = _vanda_cache
            if (not fresh and c["payload"] is not None and c["years"] == years
                    and (now_t - c["ts"]) < _VANDA_TTL):
                print(f"  [VANDA]           CACHE_HIT ({years:g}y)")
                return self._json(json.dumps(
                    c["payload"], separators=(",", ":")).encode())
            payload = _load_vanda(years)
            c.update(ts=now_t, years=years, payload=payload)
            n = len(payload.get("instruments", []))
            print(f"  [VANDA]           LOADED {n} instruments "
                  f"({years:g}y) asOf={payload.get('asOf')}"
                  f"{' fresh' if fresh else ''}")
            return self._json(json.dumps(
                payload, separators=(",", ":")).encode())

        # /api/regime-picks — backtested asset ranking for the current regime
        if path == "/api/regime-picks":
            try:
                c = _regime_picks_cache
                now_t = time.time()
                if c["payload"] is not None and (now_t - c["ts"]) < _REGIME_PICKS_TTL:
                    return self._json(json.dumps(
                        c["payload"], separators=(",", ":")).encode())
                payload = _compute_regime_picks()
                c.update(ts=now_t, payload=payload)
                n = len(payload.get("assets", []))
                print(f"  [REGIME-PICKS]    computed {n} assets, "
                      f"regime={payload.get('regime')}")
                return self._json(json.dumps(
                    payload, separators=(",", ":")).encode())
            except Exception as exc:
                return self._json(json.dumps({
                    "error": str(exc)}).encode(), 500)

        # /api/divgold?years=N[&refresh=1] — gold-to-S&P dividend ratio (Gavekal)
        if path == "/api/divgold":
            down = self._agent_down()
            if down:
                return self._json(down, 503)
            try:
                years = float((qs.get("years") or ["0"])[0])
            except ValueError:
                years = 0.0
            refresh = (qs.get("refresh") or ["0"])[0] in ("1", "true", "yes")
            now_t = time.time()
            c = _divgold_cache
            if (not refresh and c["payload"] is not None and c["years"] == years
                    and (now_t - c["ts"]) < _DIVGOLD_TTL):
                print(f"  [DIVGOLD]         CACHE_HIT ({years:g}y)")
                return self._json(json.dumps(
                    c["payload"], separators=(",", ":")).encode())
            try:
                payload = _run(_divgold(years=years, topup=refresh))
                c.update(ts=now_t, years=years, payload=payload)
                print(f"  [DIVGOLD]         computed {len(payload.get('points', []))} pts "
                      f"asOf={payload.get('asOf')} signal="
                      f"{payload.get('signal', {}).get('sharesCheapVsGold')}")
                return self._json(json.dumps(
                    payload, separators=(",", ":")).encode())
            except Exception as exc:
                return self._json(json.dumps({
                    "error": str(exc)}).encode(), 500)

        # /api/warm — pre-fill series cache for every ticker at natural cadence
        if path == "/api/warm":
            down = self._agent_down()
            if down:
                return self._json(down, 503)
            try:
                years = float((qs.get("years") or ["10"])[0])
                payload = _run(_warm_cache(years=years))
                return self._json(json.dumps({
                    "status": "ok",
                    "years": payload["years"],
                    "warmed_count": len(payload["warmed"]),
                    "errors_count": len(payload["errors"]),
                    "errors": payload["errors"],
                }, separators=(",", ":")).encode())
            except Exception as exc:
                return self._json(json.dumps({
                    "status": "error", "message": str(exc),
                }).encode(), 500)

        # /api/export — build a multi-sheet Excel workbook with all data
        if path == "/api/export":
            down = self._agent_down()
            if down:
                return self._json(down, 503)
            try:
                years = float((qs.get("years") or ["5"])[0])
                xlsx = _run(_build_export(years=years))
                fname = f"macro_dashboard_{datetime.now():%Y%m%d_%H%M}.xlsx"
                self.send_response(200)
                self.send_header(
                    "Content-Type",
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet")
                self.send_header(
                    "Content-Disposition",
                    f'attachment; filename="{fname}"')
                self.send_header("Content-Length", str(len(xlsx)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(xlsx)
                return
            except Exception as exc:
                return self._json(json.dumps({
                    "status": "error", "message": str(exc),
                }).encode(), 500)

        # /api/refresh — pull a fresh snapshot from Bloomberg
        if path == "/api/refresh":
            down = self._agent_down()
            if down:
                return self._json(down, 503)
            try:
                if not bbg.is_bloomberg_available():
                    return self._json(json.dumps({
                        "status": "error",
                        "message": "Bloomberg Terminal not available on this machine.",
                    }).encode(), 503)
                force = (qs.get("force") or ["0"])[0] in ("1", "true", "yes")
                payload = _run(_refresh_macro(force=force))
                return self._json(b'{"status":"ok"}')
            except Exception as exc:
                return self._json(json.dumps({
                    "status": "error", "message": str(exc),
                }).encode(), 500)

        # /api/update — the full daily update on demand (same routine the 06:05
        # scheduler runs): fresh Vanda + snapshot + gap-fill every displayed
        # series (missing days + today, or today only if no gap).
        if path == "/api/update":
            down = self._agent_down()
            if down:
                return self._json(down, 503)
            try:
                scrape = (qs.get("scrape") or ["0"])[0] in ("1", "true", "yes")
                result = _daily_refresh_once(scrape=scrape)
                return self._json(json.dumps(result, separators=(",", ":")).encode())
            except Exception as exc:
                return self._json(json.dumps({
                    "status": "error", "message": str(exc),
                }).encode(), 500)

        # /api/series/{ticker}?field=&years=&periodicity=&start=
        if path.startswith("/api/series/"):
            down = self._agent_down()
            if down:
                return self._json(down, 503)
            try:
                ticker = urllib.parse.unquote(path[len("/api/series/"):])
                field = (qs.get("field") or ["PX_LAST"])[0]
                years = float((qs.get("years") or ["5"])[0])
                periodicity = (qs.get("periodicity") or ["MONTHLY"])[0]
                # Optional explicit window start (ISO date); overrides `years`.
                # The quadrant pins start=1990-01-01.
                start_q = (qs.get("start") or [""])[0]
                start = date.fromisoformat(start_q) if start_q else None
                # Default = cache-only (fast). ?refresh=1 forces the newer-bar pull.
                topup = (qs.get("refresh") or ["0"])[0] in ("1", "true", "yes")
                result = _run(_fetch_series(ticker, field, years, periodicity,
                                            topup=topup, start=start))
                return self._json(json.dumps(result, separators=(",", ":")).encode())
            except Exception as exc:
                return self._json(json.dumps({"error": str(exc)}).encode(), 500)

        # Static files
        if path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_OPTIONS(self):
        # NB: do NOT re-send Access-Control-Allow-Origin here — end_headers()
        # already emits it. Sending it twice yields "multiple values '*, *'",
        # which browsers reject, failing the CORS preflight (the cross-origin
        # ngrok case, where the fetch shim sets custom request headers).
        self.send_response(204)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Cache-Control, Pragma, ngrok-skip-browser-warning")
        self.end_headers()

    def log_message(self, fmt, *args):
        # Apply the format so multi-arg error/timeout logs render fully and an
        # arg-less call can't IndexError.
        try:
            msg = fmt % args if args else fmt
        except Exception:
            msg = " ".join(str(a) for a in args) if args else str(fmt)
        print(f"  [{datetime.now():%H:%M:%S}] {msg}")


if __name__ == "__main__":
    print("\n  JCAP Macro Dashboard — Bloomberg data agent (xbbg/blpapi)")
    print("  =========================================================")
    print(f"  http://localhost:{PORT}")
    print(f"  Data agent: {os.path.join(WEB_DIR, 'tools', 'bloomberg_api.py')} (local)")
    print(f"  Cache:      {DB_PATH} (SQLite — period-bucketed)")
    if bbg is None:
        print(f"  !! Data agent import FAILED: {_AGENT_IMPORT_ERROR}")
        print("     /api/macro still serves cached data; refresh/series disabled.")
    else:
        print("  Data agent loaded OK (xbbg ready).")
        print(f"  Pool:       {bbg.POOL_URL or '(local DAPI only)'}")
    print(f"  Scheduler:  daily update at {DAILY_REFRESH_HOUR:02d}:{DAILY_REFRESH_MIN:02d} "
          f"(Vanda CSVs + BBG snapshot + gap-fill all displayed series)")
    print("  Charts:     served cache-only (fast); freshness via scheduler / Refresh")
    threading.Thread(target=_daily_refresh_loop, daemon=True).start()
    print("  Press Ctrl+C to stop\n")
    ThreadingHTTPServer(("localhost", PORT), Handler).serve_forever()
