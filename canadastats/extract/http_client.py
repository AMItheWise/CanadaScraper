from __future__ import annotations

import json
import logging
import random
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from canadastats.config import AppConfig
from canadastats.extract.robots import RobotsPolicy
from canadastats.extract.throttler import DelayRule, DomainThrottler
from canadastats.utils import stable_id

logger = logging.getLogger(__name__)


class HttpClient:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.timeout = config.http.timeout_seconds
        self.max_retries = config.http.max_retries
        self.user_agents = config.http.user_agents

        throttle_rules = {
            r.domain: DelayRule(min_delay_ms=r.min_delay_ms, max_delay_ms=r.max_delay_ms)
            for r in config.throttle_domain_rules
        }
        self.throttler = DomainThrottler(throttle_rules)
        self.robots = RobotsPolicy(timeout_seconds=self.timeout)

        self.session = requests.Session()
        self.cache_dir = Path("data") / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def purge_old_cache(self, days: int = 30) -> int:
        cutoff = time.time() - (days * 24 * 60 * 60)
        removed = 0
        for file in self.cache_dir.glob("*.*"):
            try:
                if file.stat().st_mtime < cutoff:
                    file.unlink(missing_ok=True)
                    removed += 1
            except OSError:
                continue
        return removed

    def _pick_user_agent(self) -> str:
        if not self.user_agents:
            return "canadastats-bot/0.1"
        return random.choice(self.user_agents)

    def _cache_response(self, url: str, body: str, ext: str = "html") -> None:
        key = stable_id(url)
        target = self.cache_dir / f"{key}.{ext}"
        meta = self.cache_dir / f"{key}.meta.json"
        target.write_text(body, encoding="utf-8", errors="ignore")
        meta.write_text(json.dumps({"url": url}, ensure_ascii=True), encoding="utf-8")

    def request_text(self, url: str, allow_disallowed: bool = False) -> str:
        user_agent = self._pick_user_agent()
        domain = urlparse(url).netloc

        if not allow_disallowed and not self.robots.can_fetch(user_agent, url):
            raise PermissionError(f"Blocked by robots.txt: {url}")

        headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            self.throttler.wait(domain)
            try:
                response = self.session.get(url, headers=headers, timeout=self.timeout)
                response.raise_for_status()
                text = response.text
                self._cache_response(url, text, ext="html")
                return text
            except requests.RequestException as exc:
                last_error = exc
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning("HTTP request failed (%s) attempt=%s url=%s", exc, attempt, url)
                time.sleep(backoff)

        assert last_error is not None
        raise RuntimeError(f"Failed after retries: {url}") from last_error

    def request_json(self, url: str, allow_disallowed: bool = False) -> dict:
        user_agent = self._pick_user_agent()
        domain = urlparse(url).netloc

        if not allow_disallowed and not self.robots.can_fetch(user_agent, url):
            raise PermissionError(f"Blocked by robots.txt: {url}")

        headers = {
            "User-Agent": user_agent,
            "Accept": "application/json,text/plain,*/*",
        }

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            self.throttler.wait(domain)
            try:
                response = self.session.get(url, headers=headers, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                self._cache_response(url, json.dumps(data, ensure_ascii=False), ext="json")
                return data
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning("JSON request failed (%s) attempt=%s url=%s", exc, attempt, url)
                time.sleep(backoff)

        assert last_error is not None
        raise RuntimeError(f"Failed after retries: {url}") from last_error

    def soup(self, url: str, allow_disallowed: bool = False) -> BeautifulSoup:
        return BeautifulSoup(self.request_text(url, allow_disallowed=allow_disallowed), "lxml")
