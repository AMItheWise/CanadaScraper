from __future__ import annotations

import random
import time
from dataclasses import dataclass


@dataclass(slots=True)
class DelayRule:
    min_delay_ms: int
    max_delay_ms: int


class DomainThrottler:
    def __init__(self, rules: dict[str, DelayRule]) -> None:
        self._rules = rules
        self._last_hit: dict[str, float] = {}

    def wait(self, domain: str) -> None:
        rule = self._rules.get(domain)
        if not rule:
            return

        last = self._last_hit.get(domain)
        delay = random.uniform(rule.min_delay_ms / 1000.0, rule.max_delay_ms / 1000.0)
        if last is None:
            self._last_hit[domain] = time.monotonic()
            return

        elapsed = time.monotonic() - last
        to_sleep = delay - elapsed
        if to_sleep > 0:
            time.sleep(to_sleep)

        self._last_hit[domain] = time.monotonic()
