"""
Bots: other Hermes agents, sitting next to June.

A bot is a Hermes Agent with its API server on — another profile on June's own gateway
(`/p/<profile>` once `gateway.multiplex_profiles` is on), or a Hermes running somewhere else
entirely. Each one is a whole agent: its own sessions, memory, tools and cron, so the phone
gets exactly what it gets from June — streamed text, tool markers, a transcript that lives on
the server — and this module is nothing more than a roster of `hermes.Hermes` clients.

Configured with `BOTS` as JSON:

    BOTS='[{"id":"work","name":"Work","url":"http://172.17.0.1:8642/p/work","api_key":"…"},
           {"id":"lab","name":"Lab","url":"http://192.168.68.73:8642","api_key":"…","model":""}]'

`api_key` may be left out for a profile on June's gateway; it then reuses June's. The phone sees
`{id, name}` for each and picks one per turn with `"bot": "<id>"` on the user frame. No `bot`,
or `"june"`, is June.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

from hermes import Hermes

log = logging.getLogger("brighthermes.bots")

JUNE = "june"


@dataclass(frozen=True)
class BotSpec:
    id: str
    name: str
    url: str
    api_key: str = ""
    model: str = ""

    def public(self) -> dict:
        return {"id": self.id, "name": self.name}

    @staticmethod
    def parse_all(raw: str) -> list["BotSpec"]:
        if not raw.strip():
            return []
        try:
            items = json.loads(raw)
        except json.JSONDecodeError as e:
            log.error("BOTS is not valid JSON: %s", e)
            return []
        out: list[BotSpec] = []
        for it in items if isinstance(items, list) else []:
            if not isinstance(it, dict):
                continue
            bid = str(it.get("id", "")).strip()
            url = str(it.get("url") or it.get("base_url") or "").strip().rstrip("/")
            if not bid or not url or bid == JUNE or not bid.isidentifier():
                log.warning("skipping bot %r: needs an identifier id (not 'june') and a url", bid)
                continue
            out.append(
                BotSpec(
                    id=bid,
                    name=str(it.get("name") or bid)[:24],
                    url=url,
                    api_key=str(it.get("api_key", "")),
                    model=str(it.get("model", "")),
                )
            )
        return out


class Bots:
    """June plus every configured Hermes, each behind the same client class."""

    def __init__(self, specs: list[BotSpec], june: Hermes, june_name: str = "June"):
        self.june = june
        self.june_name = june_name
        self.specs = {s.id: s for s in specs}
        self.clients: dict[str, Hermes] = {
            s.id: Hermes(s.url, s.api_key or june.api_key, s.model) for s in specs
        }

    async def aclose(self) -> None:
        for c in self.clients.values():
            await c.aclose()

    def roster(self) -> list[dict]:
        """June first, always; then the configured bots in order."""
        return [{"id": JUNE, "name": self.june_name}] + [s.public() for s in self.specs.values()]

    def client(self, bot_id: Optional[str]) -> Optional[Hermes]:
        """The Hermes to talk to for `bot_id`; None means no such bot."""
        if not bot_id or bot_id == JUNE:
            return self.june
        return self.clients.get(bot_id)

    def name(self, bot_id: Optional[str]) -> str:
        if not bot_id or bot_id == JUNE:
            return self.june_name
        s = self.specs.get(bot_id)
        return s.name if s else bot_id


def bots_from_env() -> list[BotSpec]:
    return BotSpec.parse_all(os.environ.get("BOTS", ""))
