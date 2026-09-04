"""
Bots: anything with an OpenAI-compatible chat endpoint, sitting next to June.

June is a whole agent — tools, memory, cron — and gets `hermes.py`. A bot is smaller: a model
behind `/v1/chat/completions`, with no memory of its own, so this module keeps the transcript
for it (per phone, per bot, in the store) and replays the last few turns as context. That is
enough for a fast local model to answer "lights off" in under a second, or for a second agent
on another box to be one tap away, and it is the same shape LightChat's Agents use — a name,
a base URL, a key, a model.

Configured with `BOTS` as JSON:

    BOTS='[{"id":"z13","name":"Qwen","base_url":"http://192.168.68.73:1234/v1",
            "api_key":"","model":"qwen/qwen3.6-35b-a3b","system":"Answer in one line."}]'

The phone sees `{id, name}` for each and picks one per turn with `"bot": "<id>"` on the user
frame. No `bot`, or `"june"`, is June. Events come out as the same `Event`s `hermes.py` yields,
so `app.py` relays either without caring which it is.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import httpx

from hermes import Event, parse_sse
from store import Store

log = logging.getLogger("brighthermes.bots")

JUNE = "june"


@dataclass(frozen=True)
class Bot:
    id: str
    name: str
    base_url: str
    api_key: str = ""
    model: str = ""
    system: str = ""
    # How many past turns ride along as context. Small on purpose: these are the quick ones.
    context_turns: int = 12
    max_tokens: int = 400

    def public(self) -> dict:
        return {"id": self.id, "name": self.name}

    @staticmethod
    def parse_all(raw: str) -> list["Bot"]:
        if not raw.strip():
            return []
        try:
            items = json.loads(raw)
        except json.JSONDecodeError as e:
            log.error("BOTS is not valid JSON: %s", e)
            return []
        out: list[Bot] = []
        for it in items if isinstance(items, list) else []:
            if not isinstance(it, dict):
                continue
            bid = str(it.get("id", "")).strip()
            url = str(it.get("base_url", "")).strip().rstrip("/")
            if not bid or not url or bid == JUNE or not bid.isidentifier():
                log.warning("skipping bot %r: needs an identifier id (not 'june') and a base_url", bid)
                continue
            out.append(
                Bot(
                    id=bid,
                    name=str(it.get("name") or bid)[:24],
                    base_url=url,
                    api_key=str(it.get("api_key", "")),
                    model=str(it.get("model", "")),
                    system=str(it.get("system", "")),
                    context_turns=int(it.get("context_turns", 12)),
                    max_tokens=int(it.get("max_tokens", 400)),
                )
            )
        return out


class Bots:
    def __init__(self, bots: list[Bot], store: Store, june_name: str = "June"):
        self.by_id = {b.id: b for b in bots}
        self.store = store
        self.june_name = june_name
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=120.0, write=30.0, pool=5.0))

    async def aclose(self) -> None:
        await self.http.aclose()

    def roster(self) -> list[dict]:
        """June first, always; then the configured bots in order."""
        return [{"id": JUNE, "name": self.june_name}] + [b.public() for b in self.by_id.values()]

    def get(self, bot_id: Optional[str]) -> Optional[Bot]:
        return self.by_id.get(bot_id or "")

    def transcript(self, device: str, bot_id: str, limit: int) -> list[dict]:
        return self.store.bot_messages(device, bot_id, limit)

    async def stream(self, bot: Bot, device: str, text: str) -> AsyncIterator[Event]:
        history = self.store.bot_messages(device, bot.id, bot.context_turns * 2)
        messages: list[dict] = []
        if bot.system:
            messages.append({"role": "system", "content": bot.system})
        messages += [{"role": m["role"], "content": m["content"]} for m in history]
        messages.append({"role": "user", "content": text})
        self.store.append_bot_message(device, bot.id, "user", text)

        body = {"messages": messages, "stream": True, "max_tokens": bot.max_tokens}
        if bot.model:
            body["model"] = bot.model
        headers = {"Authorization": f"Bearer {bot.api_key}"} if bot.api_key else {}
        full: list[str] = []
        try:
            async with self.http.stream("POST", f"{bot.base_url}/chat/completions", json=body, headers=headers) as r:
                if r.status_code >= 400:
                    await r.aread()
                    yield Event("error", {"message": f"{bot.name} answered HTTP {r.status_code}"})
                    return
                async for ev in parse_sse(r.aiter_lines()):
                    d = ev.data
                    if "error" in d:
                        err = d["error"]
                        yield Event("error", {"message": str(err.get("message", err) if isinstance(err, dict) else err)})
                        return
                    for choice in d.get("choices", []):
                        delta = (choice.get("delta") or {}).get("content")
                        if delta:
                            full.append(delta)
                            yield Event("assistant.delta", {"delta": delta})
        except httpx.HTTPError as e:
            log.warning("bot %s: %s", bot.id, e)
            yield Event("error", {"message": f"{bot.name} is not reachable"})
            return
        content = "".join(full)
        if content.strip():
            self.store.append_bot_message(device, bot.id, "assistant", content)
        yield Event("assistant.completed", {"content": content})
        yield Event("done", {})


def bots_from_env() -> list[Bot]:
    return Bot.parse_all(os.environ.get("BOTS", ""))
