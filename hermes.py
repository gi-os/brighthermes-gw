"""
The one client that talks to June.

June is a Hermes Agent (nousresearch/hermes-agent) with its API Server adapter on `:8642`. It
already owns the things an agent needs — sessions, memory, tools, cron — so this module does
none of that. It creates one persisted session per phone, streams a turn through
`/api/sessions/{id}/chat/stream`, and reads the transcript back. Everything else the phone
sees is shaped by `app.py`.

Why the sessions API and not `/v1/chat/completions` with `X-Hermes-Session-Id`, which is what
LightChat's June screen uses: the sessions path keeps the transcript on the server, so the
phone never has to persist a thread of its own, and it streams *structured* events — tool
started, tool finished, thinking available — which is what the chat's quiet tool marker is
drawn from. The completions path flattens all of that into one text stream.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import httpx

log = logging.getLogger("brighthermes.hermes")


@dataclass(frozen=True)
class Event:
    """One server-sent event from a session chat stream, already parsed."""

    name: str
    data: dict


class Hermes:
    def __init__(self, base_url: str, api_key: str, model: str = "", timeout_s: float = 600.0, reasoning: str = ""):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        # Empty means "whatever the gateway's default is". A model alias here (Hermes
        # `model_routes`) is how a fast local model gets picked for voice commands later.
        self.model = model
        # Per-turn reasoning effort — none / low / medium / high — sent as `model_options`.
        # This is the single biggest lever on time-to-first-word for a phone that asks for the
        # lights to go off: June's default is `medium`, and a glance surface wants `low`.
        self.reasoning = reasoning.strip().lower()
        self.timeout_s = timeout_s
        # Session ids known to exist with a real model — see [ensure_session].
        self._known: set[str] = set()
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            # Long reads: a turn that runs tools can sit quiet for a while. Connect stays short
            # so a dead Hermes is reported in seconds, not minutes.
            timeout=httpx.Timeout(connect=5.0, read=timeout_s, write=30.0, pool=5.0),
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    # -- health ------------------------------------------------------------------------------

    async def healthy(self) -> bool:
        try:
            r = await self._client.get("/health", timeout=5.0)
            return r.status_code == 200
        except httpx.HTTPError:
            return False

    # -- sessions ----------------------------------------------------------------------------

    @staticmethod
    def session_id_for(device: str) -> str:
        # Deterministic, so the same phone always lands in the same transcript, and so a
        # gateway wiped and redeployed finds the old conversation rather than starting a new
        # one. The prefix keeps them findable in Hermes's own session list.
        return f"brighthermes_{device}"

    async def ensure_session(self, device: str) -> str:
        """
        Make sure the phone's session exists, and say which id it has.

        Deliberately does *not* `POST /api/sessions` to create it. Hermes 0.20 stores whatever
        `model` that call was given — and with none given it stores the virtual name
        `hermes-agent`, which the session chat path then hands to the provider as if it were
        real and gets a 404 back. A session born from `/v1/chat/completions` with
        `X-Hermes-Session-Id` is written with the resolved default model, so the first turn
        goes through that door (see [stream]) and every later one through the sessions API.
        """
        sid = self.session_id_for(device)
        if sid in self._known:
            return sid
        r = await self._client.get(f"/api/sessions/{sid}", timeout=10.0)
        if r.status_code == 200:
            model = (r.json().get("session") or {}).get("model") or ""
            if model and model != "hermes-agent":
                self._known.add(sid)
            elif model == "hermes-agent":
                # A session made the broken way. Nothing in it is worth keeping over a
                # working chat, so start over.
                await self._client.delete(f"/api/sessions/{sid}", timeout=10.0)
        return sid

    async def messages(self, device: str, limit: int = 60) -> list[dict]:
        """The last `limit` turns, oldest first, as `{role, content, ts}`."""
        sid = self.session_id_for(device)
        r = await self._client.get(
            f"/api/sessions/{sid}/messages", params={"limit": limit, "order": "latest"}, timeout=10.0
        )
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return transcript(r.json().get("data", []))

    # -- chat --------------------------------------------------------------------------------

    async def stream(self, device: str, text: str, system: Optional[str] = None) -> AsyncIterator[Event]:
        """
        Run one turn and yield its events as they arrive.

        Names worth handling: `assistant.delta` (text), `tool.started` / `tool.completed` /
        `tool.failed` (name in `tool_name`), `tool.progress` with `tool_name == "_thinking"`
        (reasoning preview — the phone shows a glyph, never the text), `assistant.completed`
        (`content` is the whole reply), `error`, `done`.

        The first turn of a new session goes through `/v1/chat/completions` — see
        [ensure_session] for why — and its OpenAI-shaped chunks are translated into the same
        event names, minus the tool events that path does not carry.
        """
        sid = await self.ensure_session(device)
        if sid not in self._known:
            async for ev in self._stream_completions(sid, text, system):
                yield ev
            self._known.add(sid)
            return
        body: dict = {"message": text}
        if system:
            body["system_message"] = system
        if self.model:
            body["model"] = self.model
        if self.reasoning:
            body["model_options"] = {"reasoning_effort": self.reasoning}
        async with self._client.stream("POST", f"/api/sessions/{sid}/chat/stream", json=body) as r:
            if r.status_code == 404:
                # Session vanished under us (Hermes DB reset). Bootstrap again.
                await r.aread()
                self._known.discard(sid)
                async for ev in self._stream_completions(sid, text, system):
                    yield ev
                self._known.add(sid)
                return
            r.raise_for_status()
            async for ev in parse_sse(r.aiter_lines()):
                yield ev

    async def _stream_completions(self, sid: str, text: str, system: Optional[str]) -> AsyncIterator[Event]:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": text})
        body = {"model": self.model or "hermes-agent", "messages": messages, "stream": True}
        if self.reasoning:
            body["model_options"] = {"reasoning_effort": self.reasoning}
        headers = {"X-Hermes-Session-Id": sid}
        full: list[str] = []
        async with self._client.stream("POST", "/v1/chat/completions", json=body, headers=headers) as r:
            r.raise_for_status()
            async for ev in parse_sse(r.aiter_lines()):
                d = ev.data
                if "error" in d:
                    yield Event("error", {"message": str(d["error"].get("message", d["error"]))})
                    return
                for choice in d.get("choices", []):
                    delta = (choice.get("delta") or {}).get("content")
                    if delta:
                        full.append(delta)
                        yield Event("assistant.delta", {"delta": delta})
        yield Event("assistant.completed", {"content": "".join(full)})
        yield Event("done", {})


# -- pure helpers (tested without a server) -----------------------------------------------------


async def parse_sse(lines) -> AsyncIterator[Event]:
    """Minimal SSE reader: `event:` names the frame, `data:` lines carry JSON, blank line ends it."""
    name = "message"
    data_lines: list[str] = []
    async for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data_lines and data_lines != ["[DONE]"]:
                payload = "\n".join(data_lines)
                try:
                    yield Event(name, json.loads(payload))
                except json.JSONDecodeError:
                    log.debug("non-JSON SSE frame %r: %r", name, payload[:120])
            name, data_lines = "message", []
            continue
        if line.startswith(":"):
            continue  # keepalive comment
        if line.startswith("event:"):
            name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    if data_lines:  # stream ended without the trailing blank line
        try:
            yield Event(name, json.loads("\n".join(data_lines)))
        except json.JSONDecodeError:
            pass


# What Hermes writes as the assistant's turn when the model call itself failed — after its own
# retries and fallbacks — is the error text. On a phone that reads as June saying "HTTP 429:
# rate limited", which is not something she said. These are recognised and turned back into
# errors on the wire, and left out of the transcript.
_ERROR_SHAPES = (
    "HTTP 429", "HTTP 5", "rate limited", "Rate limited", "API call failed", "API failed after",
    "Error 524", "(No response generated)", "RateLimitError", "InternalServerError",
)


def looks_like_error(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    head = t[:200]
    return any(shape in head for shape in _ERROR_SHAPES)


def friendly_error(text: str) -> str:
    t = text or ""
    if "429" in t or "ate limit" in t:
        return "June's model is rate-limited right now. Try again in a moment."
    if "524" in t or "timeout" in t.lower() or "timed out" in t.lower():
        return "June's model timed out. Try again."
    return "June's model failed to answer. Try again."


def transcript(rows: list[dict]) -> list[dict]:
    """
    Hermes message rows → what the phone draws: user and assistant text only, oldest first.

    Tool calls, tool results and system rows are dropped — the phone shows a marker while a
    tool runs, never its output. Content arrives either as a string or as OpenAI-style parts.
    """
    out = []
    for m in rows:
        role = m.get("role")
        if role not in ("user", "assistant"):
            continue
        content = plain(m.get("content"))
        if not content.strip():
            continue
        if role == "assistant" and looks_like_error(content):
            continue
        out.append({"role": role, "content": content, "ts": m.get("created_at") or m.get("timestamp")})
    if len(out) >= 2 and _newest_first(out):
        out.reverse()
    return out


def plain(content) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, dict) and p.get("type") == "text":
                parts.append(str(p.get("text", "")))
            elif isinstance(p, str):
                parts.append(p)
        return "".join(parts)
    return str(content)


def _newest_first(rows: list[dict]) -> bool:
    ts = [r["ts"] for r in rows if r.get("ts") is not None]
    if len(ts) < 2:
        return True  # `order=latest` is documented newest-first; trust it when we cannot tell
    try:
        return float(ts[0]) > float(ts[-1])
    except (TypeError, ValueError):
        return True
