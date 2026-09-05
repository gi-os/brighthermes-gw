"""
One SQLite file, three tables, no ORM.

`tiles` is the latest payload per tile id — whoever last wrote it wins, whether that was the
weather fetcher, a Hermes cron job curling `/tiles/digest`, or a Home Assistant automation.
`layouts` is a phone's arrangement of the deck. `journal` is the ingest pipe: every event the
Bright* apps report, appended and never rewritten, so June has something to read later.

WAL mode because the WebSocket handler reads tiles while the refresher writes them, and the
default rollback journal would make one of them wait.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS tiles (
    id          TEXT PRIMARY KEY,
    payload     TEXT NOT NULL,          -- JSON: {label, value, sub, action?, ...}
    updated_at  REAL NOT NULL,
    stale_at    REAL                    -- when the phone should draw it dimmed; NULL = never
);
CREATE TABLE IF NOT EXISTS layouts (
    device      TEXT PRIMARY KEY,
    layout      TEXT NOT NULL,          -- JSON: [{id, span}]
    updated_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS journal (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    device      TEXT NOT NULL,
    app         TEXT NOT NULL,
    type        TEXT NOT NULL,
    ts          REAL NOT NULL,          -- when it happened, phone clock, epoch seconds
    received_at REAL NOT NULL,          -- when it arrived here
    payload     TEXT NOT NULL           -- JSON, whatever the app sent, at most a few KB
);
CREATE INDEX IF NOT EXISTS journal_app_ts ON journal(app, ts);
"""


class Store:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    # -- tiles -------------------------------------------------------------------------------

    def put_tile(self, tile_id: str, payload: dict, stale_after_s: Optional[float] = None) -> float:
        now = time.time()
        stale_at = now + stale_after_s if stale_after_s else None
        with self._lock:
            self._conn.execute(
                "INSERT INTO tiles(id, payload, updated_at, stale_at) VALUES(?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at, "
                "stale_at=excluded.stale_at",
                (tile_id, json.dumps(payload, ensure_ascii=False), now, stale_at),
            )
        return now

    def get_tile(self, tile_id: str) -> Optional[dict]:
        row = self._conn.execute("SELECT * FROM tiles WHERE id=?", (tile_id,)).fetchone()
        return _tile_row(row) if row else None

    def all_tiles(self) -> dict[str, dict]:
        rows = self._conn.execute("SELECT * FROM tiles").fetchall()
        return {r["id"]: _tile_row(r) for r in rows}

    def tiles_updated_at(self) -> float:
        row = self._conn.execute("SELECT MAX(updated_at) AS m FROM tiles").fetchone()
        return float(row["m"] or 0.0)

    # -- layouts -----------------------------------------------------------------------------

    def get_layout(self, device: str) -> Optional[list[dict]]:
        row = self._conn.execute("SELECT layout FROM layouts WHERE device=?", (device,)).fetchone()
        return json.loads(row["layout"]) if row else None

    def put_layout(self, device: str, layout: list[dict]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO layouts(device, layout, updated_at) VALUES(?,?,?) "
                "ON CONFLICT(device) DO UPDATE SET layout=excluded.layout, updated_at=excluded.updated_at",
                (device, json.dumps(layout), time.time()),
            )

    # -- journal -----------------------------------------------------------------------------

    def append(self, device: str, events: Iterable[dict]) -> int:
        now = time.time()
        rows = []
        for e in events:
            app = str(e.get("app", ""))[:64]
            typ = str(e.get("type", ""))[:64]
            if not app or not typ:
                continue
            try:
                ts = float(e.get("ts", now))
            except (TypeError, ValueError):
                ts = now
            payload = json.dumps(e.get("payload", {}), ensure_ascii=False)[:8192]
            rows.append((device, app, typ, ts, now, payload))
        if not rows:
            return 0
        with self._lock:
            self._conn.executemany(
                "INSERT INTO journal(device, app, type, ts, received_at, payload) VALUES(?,?,?,?,?,?)", rows
            )
        return len(rows)

    def recent(self, limit: int = 100, app: Optional[str] = None) -> list[dict]:
        if app:
            rows = self._conn.execute(
                "SELECT * FROM journal WHERE app=? ORDER BY seq DESC LIMIT ?", (app, limit)
            ).fetchall()
        else:
            rows = self._conn.execute("SELECT * FROM journal ORDER BY seq DESC LIMIT ?", (limit,)).fetchall()
        return [
            {
                "seq": r["seq"],
                "device": r["device"],
                "app": r["app"],
                "type": r["type"],
                "ts": r["ts"],
                "payload": json.loads(r["payload"]),
            }
            for r in rows
        ]


def _tile_row(r: sqlite3.Row) -> dict:
    d = json.loads(r["payload"])
    d["id"] = r["id"]
    d["updated_at"] = r["updated_at"]
    if r["stale_at"] is not None:
        d["stale_at"] = r["stale_at"]
    return d
