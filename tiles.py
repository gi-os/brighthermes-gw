"""
The deck's tiles: what exists, who fills them, and how often.

A tile is `label (tiny) + value (large) + sub (small) + optional action`. That shape is the whole
design — a tile that needs more than that is not a tile, it is a screen, and the phone will open
one when the `action` deep link is tapped.

Two kinds of tile, and the phone treats them differently:

- **Remote** tiles are filled here and read from `/deck`. Weather, the calendar, the home, and
  the agent digest. Their payloads sit in the store; the fetchers below refresh the ones with
  a source of their own, and anything else (the digest, a Home Assistant automation's tile) is
  *pushed* by whoever knows — `POST /tiles/{id}` with the payload.
- **Local** tiles the phone fills itself — clock, now playing, next transit, LightPods. The
  server only knows their ids so the edit-mode palette can offer them. Their `local: true` in
  the catalog is the phone's cue to never wait on the network for them.

Layout is a flat ordered list of `{id, span}` on a two-column grid, span 1 or 2. The plan drew
`x, y, w, h` on the 27×31 LightGrid; six tiles do not need that many degrees of freedom and an
edit mode driven by a scroll wheel cannot use them.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional
from zoneinfo import ZoneInfo

import httpx

from store import Store

log = logging.getLogger("brighthermes.tiles")


@dataclass(frozen=True)
class TileKind:
    id: str
    name: str  # for the palette
    local: bool = False
    # How long a payload is trusted before the phone should dim it. None: never dims (pushed
    # tiles have no schedule of their own).
    stale_after_s: Optional[float] = None
    # A widget: the payload carries `html` and the phone renders it in a WebView instead of the
    # label/value/sub shape. June writes these wholesale — see `POST /widgets/{n}` in app.py.
    html: bool = False


CATALOG: list[TileKind] = [
    TileKind("clock", "Clock", local=True),
    TileKind("weather", "Weather", stale_after_s=45 * 60),
    TileKind("air", "Air", stale_after_s=30 * 60),
    TileKind("next", "Next up", stale_after_s=30 * 60),
    TileKind("transit", "Transit", local=True),
    TileKind("home", "Home", stale_after_s=10 * 60),
    TileKind("digest", "June"),
    TileKind("music", "Now playing", local=True),
    TileKind("pods", "LightPods", local=True),
    # Three surfaces June owns outright. Anything HTML, any height, live JS, swapped whole.
    TileKind("web1", "Widget 1", html=True),
    TileKind("web2", "Widget 2", html=True),
    TileKind("web3", "Widget 3", html=True),
]
KINDS = {k.id: k for k in CATALOG}

# What a phone gets before it has arranged anything. The digest is the full-width row: it is the
# one tile no other app can draw, so it gets the most room. Order is reading order.
# The widgets sit at the end, full width; the phone draws one only once it has HTML in it, so an
# empty widget costs no space. June filling one is what makes it appear.
DEFAULT_LAYOUT: list[dict] = [
    {"id": "clock", "span": 1},
    {"id": "weather", "span": 1},
    {"id": "next", "span": 1},
    {"id": "transit", "span": 1},
    {"id": "home", "span": 1},
    {"id": "digest", "span": 2},
    {"id": "web1", "span": 2},
    {"id": "web2", "span": 2},
    {"id": "web3", "span": 2},
]

# Widget HTML is capped: the deck is fetched on every screen-on, and three of these ride in it.
WIDGET_MAX_HTML = 64 * 1024
WIDGET_DEFAULT_HEIGHT = 8   # LightGrid units of 15dp → 120dp
WIDGET_MAX_HEIGHT = 24


def widget_payload(body: dict, kind: TileKind) -> dict:
    """Validate what June sent for a widget. `html` may be a fragment or a whole document."""
    html = body.get("html", "")
    if not isinstance(html, str):
        raise ValueError("html must be a string")
    if len(html) > WIDGET_MAX_HTML:
        raise ValueError(f"html is over {WIDGET_MAX_HTML} bytes")
    height = body.get("height", WIDGET_DEFAULT_HEIGHT)
    try:
        height = int(height)
    except (TypeError, ValueError):
        raise ValueError("height must be an integer number of grid units")
    height = max(2, min(WIDGET_MAX_HEIGHT, height))
    return {
        "label": str(body.get("label") or kind.name)[:24],
        "value": "",
        "sub": str(body.get("sub", ""))[:64],
        "html": html,
        "height": height,
        **({"action": str(body["action"])[:256]} if body.get("action") else {}),
    }

# What the digest says until June has said anything. Deliberately not "0 done": a number that
# is always zero is a number nobody reads, and this tile has to earn its glance.
DEFAULT_DIGEST = {"label": "June", "value": "Quiet", "sub": "nothing waiting on you", "action": "brighthermes://chat"}


def validate_layout(layout) -> list[dict]:
    """Accept only ids the catalog knows and spans of 1 or 2; drop the rest silently."""
    if not isinstance(layout, list):
        raise ValueError("layout must be a list")
    out, seen = [], set()
    for item in layout[:24]:
        if not isinstance(item, dict):
            continue
        tid = str(item.get("id", ""))
        if tid not in KINDS or tid in seen:
            continue
        span = 2 if item.get("span") == 2 else 1
        out.append({"id": tid, "span": span})
        seen.add(tid)
    return out


# -- hourly rotation --------------------------------------------------------------------------

ROTATE_ANCHORS = ["clock", "weather"]  # glance tiles that never move
ROTATE_QUIET_HOURS = (1, 5)  # local time: no rotation 01:00–05:00


def in_quiet_hours(now: datetime) -> bool:
    return ROTATE_QUIET_HOURS[0] <= now.hour < ROTATE_QUIET_HOURS[1]


def rotation_order(now: datetime) -> list[str]:
    """Anchors first; the rest of the catalog rotated deterministically by the hour — stable
    all hour, new on the hour. New catalog tiles sweep in automatically."""
    rng = random.Random(now.replace(minute=0, second=0, microsecond=0).isoformat())
    pool = [k.id for k in CATALOG if k.id not in ROTATE_ANCHORS]
    rng.shuffle(pool)
    return ROTATE_ANCHORS + pool


class Rotator:
    """Hourly deck rotation: reorders every phone's layout and announces the change. Spans are
    preserved from the current layout; tiles new to the device default to span 1."""

    def __init__(self, store: Store, cfg: "Config"):
        self.store = store
        self.cfg = cfg
        self._task: Optional[asyncio.Task] = None
        self.on_change: Optional[Callable[[str], None]] = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="deck-rotator")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            self.tick()

    def tick(self) -> None:
        now = datetime.now(ZoneInfo(self.cfg.tz))
        if in_quiet_hours(now):
            return
        order = rotation_order(now)
        for device in self.store.devices():
            cur = self.store.get_layout(device) or DEFAULT_LAYOUT
            span = {t["id"]: t.get("span", 1) for t in cur}
            layout = [{"id": tid, "span": span.get(tid, 1)} for tid in order]
            if layout != cur:
                self.store.put_layout(device, layout)
                if self.on_change:
                    self.on_change("clock")  # any id; the frame carries the bumped updated_at


# -- fetchers --------------------------------------------------------------------------------


@dataclass
class Fetcher:
    tile_id: str
    every_s: float
    run: Callable[[], "asyncio.Future[Optional[dict]]"]
    next_at: float = field(default=0.0)


class Refresher:
    """
    One background task that keeps the remote tiles with a source of their own fresh.

    A fetcher that raises is logged and retried on its next tick; a fetcher returning None
    leaves the last good payload in place (and lets it go stale, which the phone draws dimmed).
    That is the right shape for a glance surface: an old temperature beats a blank tile.
    """

    def __init__(self, store: Store, cfg: "Config"):
        self.store = store
        self.cfg = cfg
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        self.fetchers: list[Fetcher] = []
        self._task: Optional[asyncio.Task] = None
        self.on_change: Optional[Callable[[str], None]] = None

        if cfg.lat is not None and cfg.lon is not None:
            self.fetchers.append(Fetcher("weather", 10 * 60, self.weather))
            self.fetchers.append(Fetcher("air", 30 * 60, self.air))
        if cfg.ics_path:
            self.fetchers.append(Fetcher("next", 5 * 60, self.calendar))
        if cfg.ha_url and cfg.ha_token and cfg.ha_entity:
            self.fetchers.append(Fetcher("home", 60, self.home))

        if self.store.get_tile("digest") is None:
            self.store.put_tile("digest", DEFAULT_DIGEST)

    def start(self) -> None:
        if self.fetchers and self._task is None:
            self._task = asyncio.create_task(self._loop(), name="tile-refresher")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.http.aclose()

    async def _loop(self) -> None:
        while True:
            now = time.time()
            for f in self.fetchers:
                if now < f.next_at:
                    continue
                f.next_at = now + f.every_s
                try:
                    payload = await f.run()
                except Exception as e:  # noqa: BLE001 — one bad source must not stop the others
                    log.warning("tile %s: %s", f.tile_id, e)
                    continue
                if payload is not None:
                    self.store.put_tile(f.tile_id, payload, KINDS[f.tile_id].stale_after_s)
                    if self.on_change:
                        self.on_change(f.tile_id)
            await asyncio.sleep(5)

    async def refresh_now(self) -> None:
        for f in self.fetchers:
            f.next_at = 0.0

    # -- weather: Open-Meteo, no key, one call --------------------------------------------------

    async def weather(self) -> Optional[dict]:
        r = await self.http.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": self.cfg.lat,
                "longitude": self.cfg.lon,
                "current": "temperature_2m,precipitation,weather_code",
                "hourly": "precipitation_probability,weather_code",
                "temperature_unit": "fahrenheit",
                "forecast_days": 1,
                "timezone": self.cfg.tz,
            },
        )
        r.raise_for_status()
        return weather_payload(r.json(), self.cfg.place, now=datetime.now(ZoneInfo(self.cfg.tz)))

    # -- air: Open-Meteo air quality, no key, same coordinates ----------------------------------

    async def air(self) -> Optional[dict]:
        r = await self.http.get(
            "https://air-quality-api.open-meteo.com/v1/air-quality",
            params={
                "latitude": self.cfg.lat,
                "longitude": self.cfg.lon,
                "current": "us_aqi,pm2_5",
            },
        )
        r.raise_for_status()
        return air_payload(r.json(), self.cfg.place)

    # -- calendar: the .ics LightSync already writes for this phone ----------------------------

    async def calendar(self) -> Optional[dict]:
        p = Path(self.cfg.ics_path)
        if not p.exists():
            return None
        text = await asyncio.to_thread(p.read_text, "utf-8", "replace")
        return next_payload(parse_ics(text), now=datetime.now(ZoneInfo(self.cfg.tz)))

    # -- home: one Home Assistant entity ------------------------------------------------------

    async def home(self) -> Optional[dict]:
        r = await self.http.get(
            f"{self.cfg.ha_url.rstrip('/')}/api/states/{self.cfg.ha_entity}",
            headers={"Authorization": f"Bearer {self.cfg.ha_token}"},
        )
        r.raise_for_status()
        return home_payload(r.json(), self.cfg.ha_label)


# -- pure formatting, tested ---------------------------------------------------------------------

_WMO_RAIN = {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82, 95, 96, 99}
_WMO_SNOW = {71, 73, 75, 77, 85, 86}


def weather_payload(data: dict, place: str, now: datetime) -> dict:
    cur = data.get("current", {})
    temp = cur.get("temperature_2m")
    value = f"{round(temp)}°" if isinstance(temp, (int, float)) else "—"

    # The sub-line answers the one question a glance asks about weather: do I need a jacket
    # or an umbrella before I go out. So: the first coming hour with a real chance of
    # precipitation, or nothing.
    sub = ""
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    probs = hourly.get("precipitation_probability", [])
    codes = hourly.get("weather_code", [])
    for t, p, c in zip(times, probs, codes):
        try:
            when = datetime.fromisoformat(t)
        except ValueError:
            continue
        if when.tzinfo is None:
            when = when.replace(tzinfo=now.tzinfo)
        if when < now:
            continue
        if isinstance(p, (int, float)) and p >= 50:
            what = "snow" if c in _WMO_SNOW else "rain"
            sub = f"{what} {_hour(when)}"
            break
    if not sub:
        code = cur.get("weather_code")
        sub = "raining" if code in _WMO_RAIN else "snowing" if code in _WMO_SNOW else "clear"
    return {"label": place, "value": value, "sub": sub, "action": "brighthermes://tile/weather"}


def air_payload(data: dict, place: str) -> dict:
    """US AQI + pm2.5. Value is the number; the sub-line is what it means, which is the whole
    point of a glance tile — nobody remembers the AQI bands, so the tile says them."""
    cur = data.get("current", {})
    aqi = cur.get("us_aqi")
    if not isinstance(aqi, (int, float)):
        return {"label": place, "value": "—", "sub": "", "action": "brighthermes://tile/air"}
    bands = [
        (50, "good"), (100, "moderate"), (150, "unhealthy-sens"),
        (200, "unhealthy"), (300, "very unhealthy"), (float("inf"), "hazardous"),
    ]
    word = next(w for hi, w in bands if aqi <= hi)
    pm = cur.get("pm2_5")
    pm_s = f" · pm2.5 {round(pm)}" if isinstance(pm, (int, float)) else ""
    return {"label": place, "value": str(round(aqi)), "sub": f"{word}{pm_s}", "action": "brighthermes://tile/air"}


@dataclass(frozen=True)
class VEvent:
    start: datetime
    end: Optional[datetime]
    summary: str
    location: str = ""
    all_day: bool = False


_ICS_LINE = re.compile(r"^([A-Z\-]+)(;[^:]*)?:(.*)$")


def parse_ics(text: str) -> list[VEvent]:
    """Just enough iCalendar: VEVENT blocks with DTSTART/DTEND/SUMMARY/LOCATION. Folded lines unfolded."""
    unfolded = re.sub(r"\r?\n[ \t]", "", text)
    events: list[VEvent] = []
    cur: Optional[dict] = None
    for raw in unfolded.splitlines():
        line = raw.strip()
        if line == "BEGIN:VEVENT":
            cur = {}
            continue
        if line == "END:VEVENT":
            if cur and "DTSTART" in cur and cur.get("SUMMARY"):
                start, all_day = cur["DTSTART"]
                end = cur.get("DTEND", (None, False))[0]
                events.append(VEvent(start, end, cur["SUMMARY"], cur.get("LOCATION", ""), all_day))
            cur = None
            continue
        if cur is None:
            continue
        m = _ICS_LINE.match(line)
        if not m:
            continue
        key, params, val = m.group(1), m.group(2) or "", m.group(3)
        if key in ("DTSTART", "DTEND"):
            dt = _ics_dt(val, params)
            if dt:
                cur[key] = dt
        elif key in ("SUMMARY", "LOCATION"):
            cur[key] = val.replace("\\,", ",").replace("\\;", ";").replace("\\n", " ").strip()
    events.sort(key=lambda e: e.start)
    return events


def _ics_dt(val: str, params: str) -> Optional[tuple[datetime, bool]]:
    val = val.strip()
    try:
        if "VALUE=DATE" in params or len(val) == 8:
            return datetime.strptime(val, "%Y%m%d").replace(tzinfo=timezone.utc), True
        if val.endswith("Z"):
            return datetime.strptime(val, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc), False
        tzm = re.search(r"TZID=([^;:]+)", params)
        tz = ZoneInfo(tzm.group(1)) if tzm else timezone.utc
        return datetime.strptime(val, "%Y%m%dT%H%M%S").replace(tzinfo=tz), False
    except (ValueError, KeyError):
        return None


def next_payload(events: list[VEvent], now: datetime, horizon: timedelta = timedelta(hours=48)) -> dict:
    """The first event that has not ended yet, within the horizon — or a quiet tile."""
    tz = now.tzinfo
    for e in events:
        if e.all_day:
            # All-day dates are calendar dates, not instants: never shift them through a
            # zone, or a Saturday birthday reads "Today" on Friday evening.
            day = e.start.date()
            end_day = (e.end or e.start + timedelta(days=1)).date()
            if end_day <= now.date():
                continue
            if day > (now + horizon).date():
                break
            value = "Today" if day <= now.date() else day.strftime("%a")
            return {"label": "Next", "value": value, "sub": e.summary[:48], "action": "brighthermes://tile/next"}
        end = e.end or (e.start + timedelta(hours=1))
        if end <= now:
            continue
        if e.start > now + horizon:
            break
        start_local = e.start.astimezone(tz)
        if e.start <= now:
            value = "Now"
        elif start_local.date() == now.date():
            value = _hour(start_local)
        else:
            value = f"{_weekday(start_local)} {_hour(start_local)}"
        sub = e.summary if not e.location or e.location.lower().startswith("microsoft teams") else f"{e.summary}, {e.location}"
        return {"label": "Next", "value": value, "sub": sub[:48], "action": "brighthermes://tile/next"}
    return {"label": "Next", "value": "Free", "sub": "nothing in 48h", "action": "brighthermes://tile/next"}


def home_payload(state: dict, label: str) -> dict:
    """One HA entity → a tile. Lights read as a percentage; anything else reads its state."""
    attrs = state.get("attributes", {}) or {}
    s = str(state.get("state", ""))
    name = label or str(attrs.get("friendly_name", "Home"))
    if "brightness" in attrs and s == "on":
        pct = round(float(attrs["brightness"]) / 255 * 100)
        return {"label": "Home", "value": f"{pct}%", "sub": name, "action": "brighthermes://tile/home"}
    if s in ("on", "off"):
        return {"label": "Home", "value": s.capitalize(), "sub": name, "action": "brighthermes://tile/home"}
    unit = attrs.get("unit_of_measurement", "")
    return {"label": "Home", "value": f"{s}{unit}", "sub": name, "action": "brighthermes://tile/home"}


def _hour(dt: datetime) -> str:
    # "7:30p", "12p", "8a" — the shortest string that is still a time. The design sets these
    # at 24sp in a one-column tile, and "7:30 PM" does not fit there.
    h = dt.hour % 12 or 12
    ap = "a" if dt.hour < 12 else "p"
    return f"{h}{ap}" if dt.minute == 0 else f"{h}:{dt.minute:02d}{ap}"


def _weekday(dt: datetime) -> str:
    return dt.strftime("%a")


# -- config --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    token: str
    hermes_url: str
    hermes_key: str
    hermes_model: str
    hermes_reasoning: str
    data_dir: Path
    tz: str
    place: str
    lat: Optional[float]
    lon: Optional[float]
    ics_path: str
    ha_url: str
    ha_token: str
    ha_entity: str
    ha_label: str
    chips: list[str]

    @staticmethod
    def from_env() -> "Config":
        def f(name: str) -> Optional[float]:
            v = os.environ.get(name, "").strip()
            try:
                return float(v) if v else None
            except ValueError:
                return None

        chips = [c.strip() for c in os.environ.get("BRIGHTHERMES_CHIPS", "summarize my day|lights off|snooze 10m").split("|")]
        return Config(
            token=os.environ.get("BRIGHTHERMES_TOKEN", ""),
            hermes_url=os.environ.get("HERMES_URL", "http://172.17.0.1:8642"),
            hermes_key=os.environ.get("HERMES_API_KEY", ""),
            hermes_model=os.environ.get("HERMES_MODEL", ""),
            hermes_reasoning=os.environ.get("HERMES_REASONING", "low"),
            data_dir=Path(os.environ.get("BRIGHTHERMES_DIR", "/data")),
            tz=os.environ.get("TZ", "America/New_York"),
            place=os.environ.get("WEATHER_PLACE", "NYC"),
            lat=f("WEATHER_LAT"),
            lon=f("WEATHER_LON"),
            ics_path=os.environ.get("CALENDAR_ICS", ""),
            ha_url=os.environ.get("HA_URL", ""),
            ha_token=os.environ.get("HA_TOKEN", ""),
            ha_entity=os.environ.get("HA_ENTITY", ""),
            ha_label=os.environ.get("HA_LABEL", ""),
            chips=[c for c in chips if c],
        )
