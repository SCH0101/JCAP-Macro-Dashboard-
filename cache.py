"""SQLite cache for the JCAP Macro Dashboard.

Two tables:

  snapshot(ticker PK, value, chg_abs, last_update, cached_at, periodicity, error)
    One row per ticker; the dashboard's KPI values live here.

  series(ticker, date, field, periodicity, value)  PK (ticker, date, field, periodicity)
    Historical bars for chart panels. Periodicity is part of the key because
    daily/weekly/monthly all come from distinct BDH calls.

Refresh decision uses "period bucket" comparison:
  - D: same calendar day  → not stale
  - W: same ISO year-week → not stale
  - M: same year-month    → not stale
  - Q: same year-quarter  → not stale

If the bucket has changed we pull. If Bloomberg returns nothing newer (release
hasn't happened yet) we still bump `cached_at` so we don't keep hammering BBG
inside the same bucket. That gives the "try N → fallback to N-1" behaviour.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Iterable

_DB_PATH: str | None = None


def init(db_path: str) -> None:
    """Open / create the cache DB at db_path. Call once at server start."""
    global _DB_PATH
    _DB_PATH = db_path
    with _conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS snapshot (
                ticker       TEXT PRIMARY KEY,
                value        REAL,
                chg_abs      REAL,
                last_update  TEXT,
                cached_at    TEXT NOT NULL,
                periodicity  TEXT NOT NULL,
                error        TEXT
            );
            CREATE TABLE IF NOT EXISTS series (
                ticker       TEXT NOT NULL,
                date         TEXT NOT NULL,
                field        TEXT NOT NULL,
                periodicity  TEXT NOT NULL,
                value        REAL NOT NULL,
                PRIMARY KEY (ticker, date, field, periodicity)
            );
            CREATE INDEX IF NOT EXISTS idx_series_lookup
                ON series (ticker, field, periodicity, date);
            CREATE TABLE IF NOT EXISTS series_meta (
                ticker       TEXT NOT NULL,
                field        TEXT NOT NULL,
                periodicity  TEXT NOT NULL,
                cached_at    TEXT NOT NULL,
                PRIMARY KEY (ticker, field, periodicity)
            );
        """)


@contextmanager
def _conn():
    """Yield a SQLite connection that commits on success, rolls back on error,
    and is ALWAYS closed. (`with sqlite3.connect() as c` only commits/rolls back
    — it never closes — so a long-running server would otherwise lean on GC to
    reclaim handles.) All call sites use `with _conn() as c:`.
    """
    if _DB_PATH is None:
        raise RuntimeError("cache.init(db_path) must be called first")
    conn = sqlite3.connect(_DB_PATH)
    try:
        with conn:            # transaction: commit on clean exit, rollback on raise
            yield conn
    finally:
        conn.close()


# ── Bucket logic ────────────────────────────────────────────────────────────
def bucket(dt: datetime, period: str) -> str:
    if period == "D":
        return dt.strftime("%Y-%m-%d")
    if period == "W":
        iso = dt.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    if period == "M":
        return dt.strftime("%Y-%m")
    if period == "Q":
        return f"{dt.year}-Q{(dt.month - 1) // 3 + 1}"
    return dt.strftime("%Y-%m-%d")


def is_stale(cached_at: datetime | None, period: str, now: datetime) -> bool:
    if cached_at is None:
        return True
    return bucket(cached_at, period) != bucket(now, period)


# ── Snapshot helpers ────────────────────────────────────────────────────────
def get_snapshot_all() -> tuple[dict, str | None]:
    """Return ({ticker: {value, chgAbs, lastUpdate, error, periodicity}}, latest_cached_at_iso).

    periodicity (D/W/M/Q) is exposed so the front end can pick the right BDH
    periodicity per ticker (monthly series don't accept DAILY/WEEKLY queries).
    """
    with _conn() as c:
        rows = c.execute(
            "SELECT ticker, value, chg_abs, last_update, error, cached_at, periodicity "
            "FROM snapshot"
        ).fetchall()
    metrics: dict[str, dict] = {}
    latest: datetime | None = None
    for (t, v, chg, lu, e, cached_at, period) in rows:
        metrics[t] = {"value": v, "chgAbs": chg, "lastUpdate": lu,
                      "error": e, "periodicity": period}
        try:
            ts = datetime.fromisoformat(cached_at)
            if latest is None or ts > latest:
                latest = ts
        except (ValueError, TypeError):
            pass
    return metrics, (latest.isoformat(timespec="seconds") if latest else None)


def stale_tickers(period_map: dict[str, str], now: datetime) -> list[str]:
    """Return the subset of period_map's keys whose cached_at is in a past bucket."""
    if not period_map:
        return []
    with _conn() as c:
        rows = c.execute("SELECT ticker, cached_at FROM snapshot").fetchall()
    cached: dict[str, datetime] = {}
    for (t, ca) in rows:
        try:
            cached[t] = datetime.fromisoformat(ca)
        except (ValueError, TypeError):
            pass
    return [t for t, p in period_map.items() if is_stale(cached.get(t), p, now)]


def upsert_snapshot(metrics: dict, period_map: dict, now: datetime) -> None:
    """UPSERT the supplied tickers' metrics; stamp cached_at = now for each."""
    if not metrics:
        return
    rows = []
    now_iso = now.isoformat(timespec="seconds")
    for t, m in metrics.items():
        rows.append((
            t,
            m.get("value"),
            m.get("chgAbs"),
            str(m.get("lastUpdate")) if m.get("lastUpdate") is not None else None,
            now_iso,
            period_map.get(t, "D"),
            m.get("error"),
        ))
    with _conn() as c:
        c.executemany("""
            INSERT INTO snapshot
              (ticker, value, chg_abs, last_update, cached_at, periodicity, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (ticker) DO UPDATE SET
              value       = excluded.value,
              chg_abs     = excluded.chg_abs,
              last_update = excluded.last_update,
              cached_at   = excluded.cached_at,
              periodicity = excluded.periodicity,
              error       = excluded.error
        """, rows)


# ── Series helpers ──────────────────────────────────────────────────────────
def distinct_series_keys() -> list[tuple[str, str, str]]:
    """Every (ticker, field, periodicity) the dashboard has ever displayed.

    The series table is keyed by exactly the (ticker, field, periodicity) tuples
    the charts request, so this is the authoritative set of "series on screen" —
    used by the daily update to gap-fill each one at the cadence it's shown at.
    """
    with _conn() as c:
        rows = c.execute(
            "SELECT DISTINCT ticker, field, periodicity FROM series"
        ).fetchall()
    return [(t, f, p) for (t, f, p) in rows]


def series_range(ticker: str, field: str, periodicity: str
                 ) -> tuple[date | None, date | None]:
    with _conn() as c:
        r = c.execute(
            "SELECT MIN(date), MAX(date) FROM series "
            "WHERE ticker=? AND field=? AND periodicity=?",
            (ticker, field, periodicity)
        ).fetchone()
    if r and r[0]:
        return (date.fromisoformat(r[0]), date.fromisoformat(r[1]))
    return (None, None)


def get_series_points(ticker: str, field: str, periodicity: str,
                      start: date) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT date, value FROM series "
            "WHERE ticker=? AND field=? AND periodicity=? AND date >= ? "
            "ORDER BY date",
            (ticker, field, periodicity, start.isoformat())
        ).fetchall()
    out = []
    for (d, v) in rows:
        try:
            dt = datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
            out.append({"t": int(dt.timestamp()), "v": float(v)})
        except (ValueError, TypeError):
            continue
    return out


def upsert_series_rows(ticker: str, field: str, periodicity: str,
                       points: Iterable[tuple[date, float]]) -> int:
    rows = [(ticker, d.isoformat(), field, periodicity, v) for (d, v) in points]
    if not rows:
        return 0
    with _conn() as c:
        c.executemany(
            "INSERT OR REPLACE INTO series "
            "(ticker, date, field, periodicity, value) VALUES (?, ?, ?, ?, ?)",
            rows
        )
    return len(rows)


def series_meta_cached_at(ticker: str, field: str, periodicity: str
                          ) -> datetime | None:
    with _conn() as c:
        r = c.execute(
            "SELECT cached_at FROM series_meta "
            "WHERE ticker=? AND field=? AND periodicity=?",
            (ticker, field, periodicity)
        ).fetchone()
    if not r:
        return None
    try:
        return datetime.fromisoformat(r[0])
    except (ValueError, TypeError):
        return None


def upsert_series_meta(ticker: str, field: str, periodicity: str,
                       now: datetime) -> None:
    with _conn() as c:
        c.execute("""
            INSERT INTO series_meta (ticker, field, periodicity, cached_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (ticker, field, periodicity) DO UPDATE SET
                cached_at = excluded.cached_at
        """, (ticker, field, periodicity, now.isoformat(timespec="seconds")))
