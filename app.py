"""
BrightHermes gateway: the one URL the phone talks to.

Sits between the Light Phone and June (a Hermes Agent on the same box). Owns the deck, relays
chat over a WebSocket, and takes the Bright* apps' journal. It is deliberately thin — the agent
is Hermes, the memory is Hermes, the cron is Hermes. What lives here is the part Hermes does not
have: a glanceable surface and a phone-shaped protocol for it.

    GET  /health                    liveness; also says whether June is reachable
    GET  /deck                      layout + every remote tile's latest payload, one round trip
    PUT  /deck/layout               the phone's arrangement (per device)
    GET  /tiles/{id}                one tile
    POST /tiles/{id}                push a tile payload — June's cron, HA, curl, anyone with the token
    GET  /thread?limit=&bot=        transcript, oldest first, from that agent's own session store
    GET  /bots                      the roster: June plus every other Hermes agent in BOTS
    POST /ingest                    batch of journal events from any Bright* app
    GET  /journal?limit=&app=       read the journal back (for June, or for you with curl)
    WS   /ws                        chat; protocol below

Auth is one bearer token in `Authorization: Bearer …` (or `?token=` for the WebSocket, since
Android's WebSocket clients handle headers fine but curl/wscat testing is easier this way).
`X-Device` identifies the phone: it scopes the session and the layout. A phone without one is
`default`, which is fine for a phone that is the only phone.

WebSocket protocol, one JSON object per text frame:

    → {"type":"hello","v":1,"device":"…"}                       first frame, always
    ← {"type":"ok","session":"…","chips":[…],"bots":[{"id":"june","name":"June"},…],"deck_updated_at":…}
    → {"type":"user","id":"u1","text":"lights to 40%","bot":"june"}   bot optional; default june
    ← {"type":"start","id":"u1","reply":"r1"}
    ← {"type":"delta","id":"r1","text":"done — war"}           repeated
    ← {"type":"tool","id":"r1","name":"homeassistant","state":"started"|"done"|"failed"}
    ← {"type":"thinking","id":"r1"}                             June is reasoning; show the glyph
    ← {"type":"done","id":"r1","text":"<whole reply>"}
    ← {"type":"error","id":"r1","message":"…"}
    → {"type":"stop","id":"r1"}                                 cancel the turn
    ← {"type":"deck","updated_at":…}                            a tile changed; re-GET /deck
    → {"type":"ping"}  ← {"type":"pong"}
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

import tiles as T
from bots import JUNE, Bots, bots_from_env
from hermes import Hermes
from store import Store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("brighthermes")

cfg = T.Config.from_env()
store = Store(cfg.data_dir / "brighthermes.db")
hermes = Hermes(cfg.hermes_url, cfg.hermes_key, cfg.hermes_model)
bots = Bots(bots_from_env(), june=hermes)
refresher = T.Refresher(store, cfg)

# Every open phone socket, so a tile change can be announced instead of polled for.
_sockets: set[WebSocket] = set()


def _announce_deck(tile_id: str) -> None:
    payload = json.dumps({"type": "deck", "tile": tile_id, "updated_at": store.tiles_updated_at()})
    for ws in list(_sockets):
        asyncio.create_task(_send_quiet(ws, payload))


async def _send_quiet(ws: WebSocket, text: str) -> None:
    try:
        await ws.send_text(text)
    except Exception:  # noqa: BLE001 — a dead socket is removed on its own read loop
        pass


refresher.on_change = _announce_deck


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI):
    if not cfg.token:
        log.error("BRIGHTHERMES_TOKEN is empty; every request will be refused")
    if not cfg.hermes_key:
        log.warning("HERMES_API_KEY is empty; chat will fail with 401 from June")
    refresher.start()
    try:
        yield
    finally:
        await refresher.stop()
        await hermes.aclose()
        await bots.aclose()
        store.close()


app = FastAPI(title="BrightHermes", lifespan=lifespan, docs_url=None, redoc_url=None)


# -- auth ----------------------------------------------------------------------------------------


def _check(token: Optional[str]) -> None:
    # Same argument as LightSync: constant-time compare would be theatre. Refusing to run
    # with no token set is the part that matters.
    if not cfg.token:
        raise HTTPException(500, "server has no BRIGHTHERMES_TOKEN")
    if token != cfg.token:
        raise HTTPException(401, "bad token")


def auth(authorization: Optional[str] = Header(default=None)) -> None:
    tok = None
    if authorization and authorization.lower().startswith("bearer "):
        tok = authorization[7:].strip()
    _check(tok)


def device_of(x_device: Optional[str] = Header(default=None)) -> str:
    return _clean_device(x_device)


def _clean_device(v: Optional[str]) -> str:
    v = (v or "").strip()
    if not v:
        return "default"
    # Session ids become file names inside Hermes; keep this boring.
    return "".join(ch for ch in v if ch.isalnum() or ch in "-_")[:64] or "default"


# -- health --------------------------------------------------------------------------------------


@app.get("/health")
async def health():
    return {"ok": True, "june": await hermes.healthy(), "tiles_updated_at": store.tiles_updated_at(), "ts": time.time()}


# -- deck ----------------------------------------------------------------------------------------


def _deck_for(device: str) -> dict:
    layout = store.get_layout(device) or T.DEFAULT_LAYOUT
    return {
        "v": 1,
        "layout": layout,
        "tiles": store.all_tiles(),
        "catalog": [{"id": k.id, "name": k.name, "local": k.local} for k in T.CATALOG],
        "chips": cfg.chips,
        "updated_at": store.tiles_updated_at(),
    }


@app.get("/deck", dependencies=[Depends(auth)])
async def deck(device: str = Depends(device_of)):
    return _deck_for(device)


@app.put("/deck/layout", dependencies=[Depends(auth)])
async def put_layout(request: Request, device: str = Depends(device_of)):
    body = await request.json()
    try:
        layout = T.validate_layout(body.get("layout") if isinstance(body, dict) else body)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if not layout:
        raise HTTPException(400, "empty layout")
    store.put_layout(device, layout)
    return {"layout": layout}


@app.get("/tiles/{tile_id}", dependencies=[Depends(auth)])
async def get_tile(tile_id: str):
    t = store.get_tile(tile_id)
    if t is None:
        raise HTTPException(404, "no such tile")
    return t


@app.post("/tiles/{tile_id}", dependencies=[Depends(auth)])
async def post_tile(tile_id: str, request: Request):
    """
    Push a payload. This is how the digest gets its number: a Hermes cron job ends with

        curl -X POST -H "Authorization: Bearer $T" hermes.basilnet.com/tiles/digest \\
             -d '{"value":"3 done","sub":"1 waiting on you"}'

    Unknown ids are accepted too — a tile the catalog does not know is one the phone can
    still draw once someone adds it to a layout by hand — but only known ones get the
    palette. `label` defaults to the catalog name so a bare `{"value": …}` renders.
    """
    kind = T.KINDS.get(tile_id)
    if kind and kind.local:
        raise HTTPException(400, f"{tile_id} is filled on the phone, not here")
    if not (tile_id.isidentifier() and len(tile_id) <= 32):
        raise HTTPException(400, "tile id: letters, digits, underscore, ≤32")
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(400, "payload must be an object")
    payload = {
        "label": str(body.get("label") or (kind.name if kind else tile_id))[:24],
        "value": str(body.get("value", ""))[:24],
        "sub": str(body.get("sub", ""))[:64],
    }
    if body.get("action"):
        payload["action"] = str(body["action"])[:256]
    stale = body.get("stale_after_s")
    store.put_tile(tile_id, payload, float(stale) if isinstance(stale, (int, float)) else (kind.stale_after_s if kind else None))
    _announce_deck(tile_id)
    return store.get_tile(tile_id)


@app.post("/deck/refresh", dependencies=[Depends(auth)])
async def refresh():
    await refresher.refresh_now()
    return {"ok": True}


# -- thread --------------------------------------------------------------------------------------


@app.get("/thread", dependencies=[Depends(auth)])
async def thread(
    limit: int = Query(default=60, ge=1, le=200),
    bot: str = Query(default=JUNE),
    device: str = Depends(device_of),
):
    agent = bots.client(bot)
    if agent is None:
        raise HTTPException(404, "no such bot")
    try:
        return {"bot": bot, "messages": await agent.messages(device, limit)}
    except Exception as e:  # noqa: BLE001
        log.warning("thread %s: %s", bot, e)
        raise HTTPException(502, f"{bots.name(bot)} did not answer")


@app.get("/bots", dependencies=[Depends(auth)])
async def roster():
    return {"bots": bots.roster()}


# -- ingest --------------------------------------------------------------------------------------


@app.post("/ingest", dependencies=[Depends(auth)])
async def ingest(request: Request, device: str = Depends(device_of)):
    body = await request.json()
    events = body.get("events") if isinstance(body, dict) else body
    if not isinstance(events, list):
        raise HTTPException(400, "events must be a list")
    if len(events) > 500:
        raise HTTPException(413, "at most 500 events per batch")
    n = store.append(device, events)
    return {"accepted": n}


@app.get("/journal", dependencies=[Depends(auth)])
async def journal(limit: int = Query(default=100, ge=1, le=1000), app_id: Optional[str] = Query(default=None, alias="app")):
    return {"events": store.recent(limit, app_id)}


# -- chat ----------------------------------------------------------------------------------------


@app.websocket("/ws")
async def ws(websocket: WebSocket, token: Optional[str] = Query(default=None)):
    # Token comes in the query for wscat's sake, or in the header from the app; either works.
    hdr = websocket.headers.get("authorization", "")
    if hdr.lower().startswith("bearer "):
        token = token or hdr[7:].strip()
    try:
        _check(token)
    except HTTPException as e:
        await websocket.close(code=4401, reason=e.detail)
        return
    await websocket.accept()

    # First frame must be hello. Anything else is a client we do not know.
    try:
        first = json.loads(await asyncio.wait_for(websocket.receive_text(), timeout=10))
    except (asyncio.TimeoutError, json.JSONDecodeError, WebSocketDisconnect):
        await websocket.close(code=4400, reason="hello first")
        return
    if first.get("type") != "hello":
        await websocket.close(code=4400, reason="hello first")
        return
    device = _clean_device(first.get("device") or websocket.headers.get("x-device"))

    try:
        session = await hermes.ensure_session(device)
    except Exception as e:  # noqa: BLE001
        log.warning("ensure_session: %s", e)
        await websocket.send_text(json.dumps({"type": "error", "message": "June is not reachable"}))
        await websocket.close(code=1011)
        return

    await websocket.send_text(
        json.dumps({
            "type": "ok",
            "session": session,
            "chips": cfg.chips,
            "bots": bots.roster(),
            "deck_updated_at": store.tiles_updated_at(),
        })
    )
    _sockets.add(websocket)
    turns: dict[str, asyncio.Task] = {}

    async def run_turn(user_id: str, text: str, bot_id: str) -> None:
        reply_id = "r" + uuid.uuid4().hex[:8]
        await websocket.send_text(json.dumps({"type": "start", "id": user_id, "reply": reply_id, "bot": bot_id}))
        full: list[str] = []
        agent = bots.client(bot_id)
        if agent is None:
            await websocket.send_text(json.dumps({"type": "error", "id": reply_id, "message": f"No bot called {bot_id}"}))
            turns.pop(user_id, None)
            return
        try:
            async for ev in agent.stream(device, text):
                n, d = ev.name, ev.data
                if n == "assistant.delta":
                    delta = d.get("delta", "")
                    if delta:
                        full.append(delta)
                        await websocket.send_text(json.dumps({"type": "delta", "id": reply_id, "text": delta}, ensure_ascii=False))
                elif n == "tool.progress":
                    if d.get("tool_name") == "_thinking":
                        await websocket.send_text(json.dumps({"type": "thinking", "id": reply_id}))
                elif n in ("tool.started", "tool.completed", "tool.failed"):
                    state = {"tool.started": "started", "tool.completed": "done", "tool.failed": "failed"}[n]
                    await websocket.send_text(
                        json.dumps({"type": "tool", "id": reply_id, "name": d.get("tool_name") or "", "state": state})
                    )
                elif n == "assistant.completed":
                    content = d.get("content") or "".join(full)
                    await websocket.send_text(
                        json.dumps({"type": "done", "id": reply_id, "text": content, "bot": bot_id}, ensure_ascii=False)
                    )
                elif n == "error":
                    await websocket.send_text(
                        json.dumps({"type": "error", "id": reply_id, "message": d.get("message", f"{bots.name(bot_id)} hit an error")})
                    )
        except asyncio.CancelledError:
            await _send_quiet(websocket, json.dumps({"type": "done", "id": reply_id, "text": "".join(full), "stopped": True}))
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("turn %s (%s): %s", user_id, bot_id, e)
            await _send_quiet(websocket, json.dumps({"type": "error", "id": reply_id, "message": f"{bots.name(bot_id)} is not reachable"}))
        finally:
            turns.pop(user_id, None)

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            t = msg.get("type")
            if t == "ping":
                await websocket.send_text('{"type":"pong"}')
            elif t == "user":
                uid = str(msg.get("id") or "u" + uuid.uuid4().hex[:6])
                text = str(msg.get("text", "")).strip()
                if not text:
                    continue
                # One turn at a time per socket: a second message while June is mid-reply
                # cancels the first, which is what a person interrupting means.
                for task in list(turns.values()):
                    task.cancel()
                bot_id = str(msg.get("bot") or JUNE)
                turns[uid] = asyncio.create_task(run_turn(uid, text, bot_id))
            elif t == "stop":
                for task in list(turns.values()):
                    task.cancel()
    except WebSocketDisconnect:
        pass
    finally:
        _sockets.discard(websocket)
        for task in turns.values():
            task.cancel()


# -- errors as JSON, always ------------------------------------------------------------------------


@app.exception_handler(HTTPException)
async def _http_error(_: Request, exc: HTTPException):
    return JSONResponse({"error": exc.detail}, status_code=exc.status_code)
