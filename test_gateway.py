"""
Pure-logic tests plus one end-to-end pass against a fake Hermes.

Run: `pip install fastapi httpx pytest pytest-asyncio uvicorn` then `pytest -q`.
"""

import asyncio
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

os.environ.setdefault("BRIGHTHERMES_TOKEN", "t0k")
os.environ.setdefault("HERMES_API_KEY", "june-key")
os.environ["BRIGHTHERMES_DIR"] = tempfile.mkdtemp()

import tiles as T  # noqa: E402
from hermes import Event, parse_sse, plain, transcript  # noqa: E402
from store import Store  # noqa: E402

NY = ZoneInfo("America/New_York")


# -- tiles -----------------------------------------------------------------------------------------


def test_weather_picks_first_wet_hour():
    now = datetime(2026, 9, 4, 12, 0, tzinfo=NY)
    data = {
        "current": {"temperature_2m": 63.4, "weather_code": 2},
        "hourly": {
            "time": ["2026-09-04T11:00", "2026-09-04T15:00", "2026-09-04T19:00"],
            "precipitation_probability": [90, 20, 70],
            "weather_code": [61, 2, 61],
        },
    }
    p = T.weather_payload(data, "NYC", now)
    assert p == {"label": "NYC", "value": "63°", "sub": "rain 7p", "action": "brighthermes://tile/weather"}


def test_weather_dry_day_reads_current_sky():
    now = datetime(2026, 9, 4, 12, 0, tzinfo=NY)
    data = {"current": {"temperature_2m": 80, "weather_code": 0}, "hourly": {}}
    assert T.weather_payload(data, "NYC", now)["sub"] == "clear"


def test_air_bands_and_pm():
    base = {"current": {"pm2_5": 11.4}}
    assert T.air_payload({**base, "current": {**base["current"], "us_aqi": 42}}, "NYC") == {
        "label": "NYC", "value": "42", "sub": "good · pm2.5 11", "action": "brighthermes://tile/air"}
    assert T.air_payload({**base, "current": {**base["current"], "us_aqi": 101}}, "NYC")["sub"].startswith("unhealthy-sens")
    assert T.air_payload({**base, "current": {**base["current"], "us_aqi": 151}}, "NYC")["sub"].startswith("unhealthy ·")
    assert T.air_payload({"current": {}}, "NYC")["value"] == "—"


ICS = """BEGIN:VCALENDAR
BEGIN:VEVENT
DTSTART:20260904T233000Z
DTEND:20260905T010000Z
LOCATION:Keens
SUMMARY:Dinner
END:VEVENT
BEGIN:VEVENT
DTSTART:20260904T140000Z
DTEND:20260904T143000Z
LOCATION:Microsoft Teams Meeting
SUMMARY:Standup\\, weekly
END:VEVENT
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260906
SUMMARY:Alex birthday
END:VEVENT
END:VCALENDAR
"""


def test_ics_parses_and_sorts():
    ev = T.parse_ics(ICS)
    assert [e.summary for e in ev] == ["Standup, weekly", "Dinner", "Alex birthday"]
    assert ev[2].all_day


def test_next_skips_finished_and_formats_short_time():
    ev = T.parse_ics(ICS)
    now = datetime(2026, 9, 4, 15, 0, tzinfo=NY)  # standup (10a) is over
    p = T.next_payload(ev, now)
    assert p["value"] == "7:30p" and p["sub"] == "Dinner, Keens"


def test_next_marks_running_event_as_now_and_hides_teams_location():
    ev = T.parse_ics(ICS)
    now = datetime(2026, 9, 4, 10, 10, tzinfo=NY)
    p = T.next_payload(ev, now)
    assert p["value"] == "Now" and p["sub"] == "Standup, weekly"


def test_next_all_day_tomorrow_says_weekday():
    ev = T.parse_ics(ICS)
    now = datetime(2026, 9, 5, 9, 0, tzinfo=NY)
    assert T.next_payload(ev, now)["value"] == "Sun"


def test_next_quiet_when_nothing():
    now = datetime(2026, 9, 20, 9, 0, tzinfo=NY)
    assert T.next_payload(T.parse_ics(ICS), now)["value"] == "Free"


def test_home_light_reads_percent():
    st = {"state": "on", "attributes": {"brightness": 102, "friendly_name": "Living room"}}
    assert T.home_payload(st, "")["value"] == "40%"


def test_layout_validation_drops_unknown_and_dupes():
    out = T.validate_layout([{"id": "clock", "span": 1}, {"id": "nope"}, {"id": "clock"}, {"id": "digest", "span": 3}])
    assert out == [{"id": "clock", "span": 1}, {"id": "digest", "span": 1}]


# -- store -----------------------------------------------------------------------------------------


def test_store_roundtrip(tmp_path):
    s = Store(tmp_path / "x.db")
    s.put_tile("weather", {"label": "NYC", "value": "63°", "sub": "rain"}, stale_after_s=60)
    t = s.get_tile("weather")
    assert t["value"] == "63°" and t["stale_at"] > t["updated_at"]
    assert s.get_layout("d1") is None
    s.put_layout("d1", [{"id": "clock", "span": 1}])
    assert s.get_layout("d1") == [{"id": "clock", "span": 1}]
    n = s.append("d1", [{"app": "brightmusic", "type": "play", "ts": 1.0, "payload": {"t": "x"}}, {"bad": 1}])
    assert n == 1 and s.recent()[0]["app"] == "brightmusic"
    s.close()


# -- hermes parsing --------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sse_parser_handles_names_keepalives_and_multiline():
    async def lines():
        for l in [
            ": keepalive",
            "",
            "event: assistant.delta",
            'data: {"delta":',
            'data: "hello"}',
            "",
            "event: done",
            "data: {}",
            "",
        ]:
            yield l

    got = [e async for e in parse_sse(lines())]
    assert got[0].name == "assistant.delta" and got[0].data == {"delta": "hello"}
    assert got[1] == Event("done", {})


def test_transcript_keeps_text_turns_oldest_first():
    rows = [
        {"role": "assistant", "content": [{"type": "text", "text": "Done."}], "created_at": 30},
        {"role": "tool", "content": "{}", "created_at": 20},
        {"role": "user", "content": "lights off", "created_at": 10},
    ]
    t = transcript(rows)
    assert [m["content"] for m in t] == ["lights off", "Done."]
    assert plain(None) == "" and plain("x") == "x"


# -- end to end against a fake June ----------------------------------------------------------------


@pytest.fixture
def fake_june():
    """A tiny aiohttp-free Hermes stand-in on a real port, using the same FastAPI stack."""
    from fastapi import FastAPI, Request
    from fastapi.responses import StreamingResponse
    import uvicorn, threading, socket

    j = FastAPI()
    j.state.sessions = set()
    # Every route is also mounted under /p/work, the way a multiplexed Hermes profile is.
    _get, _post = j.get, j.post
    j.get = lambda path, **kw: (lambda f: (_get("/p/work" + path, **kw)(f), _get(path, **kw)(f))[1])
    j.post = lambda path, **kw: (lambda f: (_post("/p/work" + path, **kw)(f), _post(path, **kw)(f))[1])

    @j.get("/health")
    def h():
        return {"ok": True}

    from fastapi.responses import JSONResponse

    @j.get("/api/sessions/{sid}")
    def get_session(sid: str):
        if sid not in j.state.sessions:
            return JSONResponse({"error": "no"}, status_code=404)
        return {"session": {"id": sid, "model": "deepseek/deepseek-v4-pro"}}

    @j.post("/v1/chat/completions")
    async def completions(req: Request):
        # The first turn of a session comes through here; the header names the session.
        sid = req.headers["x-hermes-session-id"]
        j.state.sessions.add(sid)
        b = await req.json()

        async def gen():
            for piece in ["Hi ", "there."]:
                yield f'data: {json.dumps({"choices": [{"delta": {"content": piece}}]})}\n\n'
            yield "data: [DONE]\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")

    @j.get("/api/sessions/{sid}/messages")
    def msgs(sid: str):
        return {"data": [{"role": "assistant", "content": "Hi.", "created_at": 2}, {"role": "user", "content": "hey", "created_at": 1}]}

    @j.post("/api/sessions/{sid}/chat/stream")
    async def chat(sid: str, req: Request):
        b = await req.json()

        async def gen():
            yield 'event: run.started\ndata: {}\n\n'
            yield 'event: tool.started\ndata: {"tool_name": "homeassistant"}\n\n'
            yield 'event: tool.completed\ndata: {"tool_name": "homeassistant"}\n\n'
            for piece in ["done — ", "warm 2700k ✓"]:
                yield f'event: assistant.delta\ndata: {json.dumps({"delta": piece}, ensure_ascii=False)}\n\n'
                await asyncio.sleep(0.01)
            yield f'event: assistant.completed\ndata: {json.dumps({"content": "done — warm 2700k ✓ (" + b["message"] + ")"}, ensure_ascii=False)}\n\n'
            yield 'event: done\ndata: {}\n\n'

        return StreamingResponse(gen(), media_type="text/event-stream")

    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    cfg = uvicorn.Config(j, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(cfg)
    th = threading.Thread(target=server.run, daemon=True); th.start()
    import time
    for _ in range(100):
        if server.started: break
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    th.join(timeout=3)


@pytest.mark.asyncio
async def test_gateway_end_to_end(fake_june, monkeypatch):
    import importlib
    monkeypatch.setenv("HERMES_URL", fake_june)
    monkeypatch.setenv("WEATHER_LAT", "")  # no network fetchers in tests
    monkeypatch.setenv("WEATHER_LON", "")
    monkeypatch.setenv("BRIGHTHERMES_DIR", tempfile.mkdtemp())
    monkeypatch.setenv("BOTS", json.dumps([{"id": "work", "name": "Work", "url": fake_june + "/p/work"}]))
    import app as appmod
    appmod = importlib.reload(appmod)
    from fastapi.testclient import TestClient

    with TestClient(appmod.app) as c:
        H = {"Authorization": "Bearer t0k", "X-Device": "lp3-test"}
        assert c.get("/deck").status_code == 401
        d = c.get("/deck", headers=H).json()
        assert d["layout"] == T.DEFAULT_LAYOUT and d["tiles"]["digest"]["value"] == "Quiet"
        assert any(k["id"] == "clock" and k["local"] for k in d["catalog"])

        r = c.post("/tiles/digest", headers=H, json={"value": "3 done", "sub": "1 waiting on you"})
        assert r.status_code == 200 and r.json()["label"] == "June"
        assert c.post("/tiles/clock", headers=H, json={"value": "x"}).status_code == 400

        r = c.put("/deck/layout", headers=H, json={"layout": [{"id": "digest", "span": 2}, {"id": "clock"}]})
        assert r.json()["layout"] == [{"id": "digest", "span": 2}, {"id": "clock", "span": 1}]
        assert c.get("/deck", headers=H).json()["layout"][0]["id"] == "digest"

        r = c.post("/ingest", headers=H, json={"events": [{"app": "brightmusic", "type": "play", "ts": 1, "payload": {}}]})
        assert r.json() == {"accepted": 1}
        assert c.get("/journal", headers=H).json()["events"][0]["device"] == "lp3-test"

        assert [m["content"] for m in c.get("/thread", headers=H).json()["messages"]] == ["hey", "Hi."]

        with c.websocket_connect("/ws?token=t0k") as ws:
            ws.send_text(json.dumps({"type": "hello", "v": 1, "device": "lp3-test"}))
            ok = ws.receive_json()
            assert ok["type"] == "ok" and ok["session"] == "brighthermes_lp3-test" and ok["chips"]
            # First turn: bootstrapped through completions, so text only.
            ws.send_text(json.dumps({"type": "user", "id": "u0", "text": "hello"}))
            first = []
            while True:
                m = ws.receive_json()
                first.append(m)
                if m["type"] == "done":
                    break
            assert [m["type"] for m in first] == ["start", "delta", "delta", "done"] and first[-1]["text"] == "Hi there."
            ws.send_text(json.dumps({"type": "user", "id": "u1", "text": "lights to 40%"}))
            seen = []
            while True:
                m = ws.receive_json()
                seen.append(m)
                if m["type"] == "done":
                    break
            types = [m["type"] for m in seen]
            assert types[0] == "start" and "tool" in types and types.count("delta") == 2
            assert seen[-1]["text"].startswith("done — warm 2700k ✓")
            # A tile pushed while connected is announced on the socket.
            c.post("/tiles/digest", headers=H, json={"value": "4 done"})
            assert ws.receive_json()["type"] == "deck"
            ws.send_text('{"type":"ping"}')
            assert ws.receive_json() == {"type": "pong"}

            # Another Hermes agent: same frames, its own session, its own transcript.
            assert [b["id"] for b in ok["bots"]] == ["june", "work"]
            ws.send_text(json.dumps({"type": "user", "id": "b0", "text": "hello work", "bot": "work"}))
            got = []
            while True:
                m = ws.receive_json()
                got.append(m)
                if m["type"] == "done":
                    break
            # (The fake shares one session set across prefixes, so this lands on the sessions path.)
            assert got[0]["bot"] == "work" and got[-1]["text"].endswith("(hello work)")
            assert [m["content"] for m in c.get("/thread?bot=work", headers=H).json()["messages"]] == ["hey", "Hi."]
            ws.send_text(json.dumps({"type": "user", "id": "b9", "text": "x", "bot": "nope"}))
            assert ws.receive_json()["type"] == "start"
            assert ws.receive_json()["type"] == "error"
        assert c.get("/thread?bot=nope", headers=H).status_code == 404
        assert c.get("/bots", headers=H).json()["bots"][1]["name"] == "Work"

        # Widgets: HTML in, HTML out, sized in grid units, capped, and refused on the tiles path.
        r = c.post("/widgets/1", headers=H, json={"html": "<b>hi</b>", "height": 99, "label": "Garage"})
        assert r.status_code == 200 and r.json()["height"] == T.WIDGET_MAX_HEIGHT and r.json()["label"] == "Garage"
        r = c.post("/widgets/2", headers={**H, "Content-Type": "text/html"}, params={"height": "4"}, content="<i>raw</i>")
        assert r.status_code == 200 and r.json()["height"] == 4
        assert c.get("/widgets/2", headers=H).text == "<i>raw</i>"
        d = c.get("/deck", headers=H).json()
        assert d["tiles"]["web1"]["html"] == "<b>hi</b>" and d["tiles"]["web2"]["height"] == 4
        assert any(k["id"] == "web1" and k["html"] for k in d["catalog"])
        assert c.post("/widgets/4", headers=H, json={"html": ""}).status_code == 404
        assert c.post("/widgets/1", headers=H, json={"html": "x" * (T.WIDGET_MAX_HTML + 1)}).status_code == 400
        assert c.post("/tiles/web1", headers=H, json={"value": "x"}).status_code == 400
        assert c.delete("/widgets/1", headers=H).json() == {"ok": True}
        assert c.get("/widgets/1", headers=H).text == ""
