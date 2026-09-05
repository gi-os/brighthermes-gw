# brighthermes-gw

The one URL the Light Phone talks to. Sits between BrightHermes and June (a Hermes Agent on the
same box), owns the deck, relays chat over a WebSocket, and takes the Bright* apps' journal.
Thin on purpose: the agent, the memory and the cron are Hermes's. What lives here is the part
Hermes does not have — a glanceable surface and a phone-shaped protocol for it.

Runs on BasilNet at `/volume1/docker/brighthermes`, LAN `:8650`, public `hermes.basilnet.com`
through the Cloudflare tunnel (a public hostname → `http://brighthermes:8650`).

```
GET  /health                 liveness; says whether June is reachable
GET  /deck                   layout + every remote tile, one round trip
PUT  /deck/layout            the phone's arrangement (per X-Device)
GET  /tiles/{id}             one tile
POST /tiles/{id}             push a payload: {"value": "3 done", "sub": "1 waiting on you"}
POST /deck/refresh           re-fetch weather / calendar / home now
GET  /thread?limit=          transcript, oldest first, from June's own session
POST /ingest                 {"events": [{app, type, ts, payload}, …]}
GET  /journal?limit=&app=    read the journal back
WS   /ws?token=              chat — protocol at the top of app.py
```

Auth is `Authorization: Bearer $BRIGHTHERMES_TOKEN`. `X-Device` names the phone and scopes its
June session (`brighthermes_<device>`) and its layout.

## Bots

`BOTS` lists other Hermes agents — a second profile on June's gateway (`/p/<profile>`) or a
Hermes on another box — as `{id, name, url, api_key}`. Each is a whole agent behind the same
client, so the phone gets sessions, tool markers and a server-kept transcript from every one of
them. The roster rides on the socket's `ok` frame; a user frame picks one with `"bot"`.

## Tiles

`weather` (Open-Meteo, no key), `next` (the `.ics` LightSync already writes for the phone,
mounted read-only), `home` (one Home Assistant entity, off until `HA_*` are set) refresh on
their own. `digest` and anything else are **pushed** — that is how June's cron job puts its
number on the deck. Local tiles (`clock`, `transit`, `music`, `pods`) are the phone's; the
gateway only lists them for the edit palette.

## Widgets

`web1`–`web3` are surfaces June draws herself. `POST /widgets/{n}` with JSON
`{"html": …, "height": <grid units, default 8>, "label": …}` or a raw `text/html` body
(`?height=&label=` on the query); `GET` reads it back; `DELETE` blanks it, and a blank widget is
not drawn. The phone renders it in a WebView with JavaScript on and hands the page
`window.brighthermes = {server, token, device}` plus `brighthermes.fetch(path, opts)` with the
auth headers filled in, so a widget can call this gateway back. HTML is capped at 64KB each
because all three ride in every `/deck`.

## The lock face

`POST /lock {"title", "text", "ttl_s" (default 30 min, max 24 h), "action"?}` puts one card on
the phone's lock screen — BrightControl draws it where the music player goes, in place of the
player, until `DELETE /lock` or the time runs out. `GET /lock` reads it; `{}` when nothing. One
slot on purpose: this is for the very important thing, not a shade.

## Running

```sh
cp .env.example .env   # BRIGHTHERMES_TOKEN (openssl rand -hex 24), HERMES_API_KEY (June's API_SERVER_KEY)
docker compose up -d --build
curl -s localhost:8650/health
```

Tests: `pip install fastapi httpx uvicorn pytest pytest-asyncio && pytest -q`.

## One Hermes quirk

`POST /api/sessions` without a `model` stores the literal string `hermes-agent`, and the session
chat path then hands that to the provider and gets a 404. So the first turn of a new phone goes
through `/v1/chat/completions` with `X-Hermes-Session-Id`, which writes the resolved default
model; every later turn uses `/api/sessions/{id}/chat/stream` and gets the structured tool events.
See `hermes.py`.
