from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
]

DEFAULT_RSEQ_LEAGUE_URLS = [
    "https://www.rseq-stats.ca/scolaire/basketball-benj-f-d1",
    "https://www.rseq-stats.ca/scolaire/basketball-benj-f-d2",
    "https://www.rseq-stats.ca/scolaire/basketball-benj-m-d1",
    "https://www.rseq-stats.ca/scolaire/basketball-benj-m-d2",
    "https://www.rseq-stats.ca/scolaire/basketball-cad-f-d1",
    "https://www.rseq-stats.ca/scolaire/basketball-cad-f-d2",
    "https://www.rseq-stats.ca/scolaire/basketball-cad-m-d1",
    "https://www.rseq-stats.ca/scolaire/basketball-cad-m-d2",
    "https://www.rseq-stats.ca/scolaire/basketball-juv-f-d1",
    "https://www.rseq-stats.ca/scolaire/basketball-juv-f-d2",
    "https://www.rseq-stats.ca/scolaire/basketball-juv-m-d1",
    "https://www.rseq-stats.ca/scolaire/basketball-juv-m-d2",
    "https://www.rseq-stats.ca/scolaire/football-cad-d1",
    "https://www.rseq-stats.ca/scolaire/football-juv-d1",
    "https://www.rseq-stats.ca/scolaire/football-juv-d2",
    "https://www.rseq-stats.ca/collegial/basketball-f-d1",
    "https://www.rseq-stats.ca/collegial/basketball-m-d1",
    "https://www.rseq-stats.ca/collegial/football-d1",
    "https://www.rseq-stats.ca/collegial/football-d2",
    "https://www.rseq-stats.ca/collegial/football-d3",
    "https://www.rseq-stats.ca/universitaire/basketball-f",
    "https://www.rseq-stats.ca/universitaire/basketball-m",
    "https://www.rseq-stats.ca/universitaire/football",
]


@dataclass(slots=True)
class DomainThrottleRule:
    domain: str
    min_delay_ms: int
    max_delay_ms: int


@dataclass(slots=True)
class DatabaseConfig:
    url: str = "sqlite:///data/canada_stats.db"


@dataclass(slots=True)
class SyncConfig:
    manual_only: bool = True


@dataclass(slots=True)
class HttpConfig:
    timeout_seconds: int = 30
    max_retries: int = 3
    user_agents: list[str] = field(default_factory=lambda: list(DEFAULT_USER_AGENTS))


@dataclass(slots=True)
class SourcesConfig:
    enabled: dict[str, bool] = field(
        default_factory=lambda: {
            "rseq": True,
            "hmhshl": True,
            "bc_football": True,
            "bc_basketball": True,
        }
    )
    rseq_league_urls: list[str] = field(default_factory=lambda: list(DEFAULT_RSEQ_LEAGUE_URLS))
    hmhshl_allow_api: bool = False
    bc_football_seed_url: str = "https://www.bchighschoolfootball.com/leagues/front_pagePro.cfm?clientID=652&leagueID=6713"
    bc_basketball_seed_url: str = "https://www.bcboysbasketball.com/leagues/slider.cfm?leagueID=0&clientID=2192&link=Pro"


@dataclass(slots=True)
class AppConfig:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)
    http: HttpConfig = field(default_factory=HttpConfig)
    throttle_domain_rules: list[DomainThrottleRule] = field(
        default_factory=lambda: [
            DomainThrottleRule(domain="www.rseq-stats.ca", min_delay_ms=400, max_delay_ms=900),
            DomainThrottleRule(domain="s1.rseq.ca", min_delay_ms=400, max_delay_ms=900),
            DomainThrottleRule(domain="hmhshl.com", min_delay_ms=700, max_delay_ms=1400),
            DomainThrottleRule(domain="www.bchighschoolfootball.com", min_delay_ms=600, max_delay_ms=1300),
            DomainThrottleRule(domain="www.bcboysbasketball.com", min_delay_ms=600, max_delay_ms=1300),
        ]
    )
    sources: SourcesConfig = field(default_factory=SourcesConfig)


def _deep_update(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def default_config_dict() -> dict[str, Any]:
    cfg = AppConfig()
    return {
        "database": {"url": cfg.database.url},
        "sync": {"manual_only": cfg.sync.manual_only},
        "http": {
            "timeout_seconds": cfg.http.timeout_seconds,
            "max_retries": cfg.http.max_retries,
            "user_agents": cfg.http.user_agents,
        },
        "throttle": {
            "domain_rules": [
                {
                    "domain": r.domain,
                    "min_delay_ms": r.min_delay_ms,
                    "max_delay_ms": r.max_delay_ms,
                }
                for r in cfg.throttle_domain_rules
            ]
        },
        "sources": {
            "enabled": cfg.sources.enabled,
            "rseq": {"league_urls": cfg.sources.rseq_league_urls},
            "hmhshl": {"allow_api": cfg.sources.hmhshl_allow_api},
            "bc_football": {"seed_url": cfg.sources.bc_football_seed_url},
            "bc_basketball": {"seed_url": cfg.sources.bc_basketball_seed_url},
        },
    }


def load_config(path: str | Path = "config.yaml") -> AppConfig:
    cfg_path = Path(path)
    merged = default_config_dict()

    if cfg_path.exists():
        with cfg_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        merged = _deep_update(merged, raw)

    domain_rules = [
        DomainThrottleRule(
            domain=row["domain"],
            min_delay_ms=int(row["min_delay_ms"]),
            max_delay_ms=int(row["max_delay_ms"]),
        )
        for row in merged["throttle"]["domain_rules"]
    ]

    return AppConfig(
        database=DatabaseConfig(url=merged["database"]["url"]),
        sync=SyncConfig(manual_only=bool(merged["sync"]["manual_only"])),
        http=HttpConfig(
            timeout_seconds=int(merged["http"]["timeout_seconds"]),
            max_retries=int(merged["http"]["max_retries"]),
            user_agents=list(merged["http"]["user_agents"]),
        ),
        throttle_domain_rules=domain_rules,
        sources=SourcesConfig(
            enabled={k: bool(v) for k, v in merged["sources"]["enabled"].items()},
            rseq_league_urls=list(merged["sources"]["rseq"]["league_urls"]),
            hmhshl_allow_api=bool(merged["sources"]["hmhshl"]["allow_api"]),
            bc_football_seed_url=str(merged["sources"]["bc_football"]["seed_url"]),
            bc_basketball_seed_url=str(merged["sources"]["bc_basketball"]["seed_url"]),
        ),
    )


def write_default_config(path: str | Path = "config.yaml") -> None:
    cfg_path = Path(path)
    if cfg_path.exists():
        return
    with cfg_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(default_config_dict(), f, sort_keys=False, allow_unicode=False)
