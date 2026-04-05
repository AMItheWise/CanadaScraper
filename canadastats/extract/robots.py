from __future__ import annotations

import logging
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests

logger = logging.getLogger(__name__)


class RobotsPolicy:
    def __init__(self, timeout_seconds: int = 20) -> None:
        self.timeout_seconds = timeout_seconds
        self._parsers: dict[str, RobotFileParser] = {}
        self._allow_all_domains: set[str] = set()

    def _load_parser(self, url: str) -> None:
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain in self._parsers or domain in self._allow_all_domains:
            return

        robots_url = urljoin(f"{parsed.scheme}://{domain}", "/robots.txt")
        try:
            response = requests.get(robots_url, timeout=self.timeout_seconds)
            if response.status_code != 200:
                self._allow_all_domains.add(domain)
                logger.debug("No robots.txt for %s (status=%s)", domain, response.status_code)
                return
            parser = RobotFileParser()
            parser.set_url(robots_url)
            parser.parse(response.text.splitlines())
            self._parsers[domain] = parser
        except requests.RequestException:
            self._allow_all_domains.add(domain)
            logger.debug("Robots lookup failed for %s; allowing by default", domain)

    def can_fetch(self, user_agent: str, url: str) -> bool:
        parsed = urlparse(url)
        domain = parsed.netloc
        self._load_parser(url)

        if domain in self._allow_all_domains:
            return True

        parser = self._parsers.get(domain)
        if not parser:
            return True

        return parser.can_fetch(user_agent, url)
