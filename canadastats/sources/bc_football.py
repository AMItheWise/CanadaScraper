from __future__ import annotations

import logging
import re
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

from canadastats.config import AppConfig
from canadastats.extract.http_client import HttpClient
from canadastats.models import (
    GameRecord,
    IssueRecord,
    LeagueRecord,
    PlayerStatRecord,
    SourceSyncPayload,
    StandingRecord,
    TeamRecord,
)
from canadastats.sources.base import (
    SourceAdapter,
    extract_js_redirect_target,
    parse_html_tables,
    row_to_metrics,
)
from canadastats.transform.normalization import clean_text, pick_primary_metrics
from canadastats.utils import parse_int, stable_id

logger = logging.getLogger(__name__)

DATE_LINE_RE = re.compile(
    r"^(MON|TUE|WED|THU|FRI|SAT|SUN)\s+[A-Z]{3}\.\s+\d{1,2}\s+at\s+\d{1,2}:\d{2}(AM|PM)$",
    flags=re.IGNORECASE,
)


class BcFootballSource(SourceAdapter):
    name = "bc_football"

    def __init__(self, client: HttpClient, config: AppConfig) -> None:
        self.client = client
        self.config = config
        self.seed_url = config.sources.bc_football_seed_url
        self.base_url = "https://www.bchighschoolfootball.com/leagues/"

    def sync_all(self) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        league_ids = self._discover_active_leagues()
        if not league_ids:
            payload.issues.append(
                IssueRecord(
                    source_league_id=None,
                    severity="error",
                    message="No BC football leagues discovered",
                    raw_pointer=self.seed_url,
                )
            )
            return payload

        for league_id in league_ids:
            try:
                payload.merge(self.sync_league(league_id))
            except Exception as exc:  # noqa: BLE001
                logger.exception("BC football league failed: %s", league_id)
                payload.issues.append(
                    IssueRecord(
                        source_league_id=league_id,
                        severity="error",
                        message=f"League sync failed: {exc}",
                        raw_pointer=league_id,
                    )
                )
        return payload

    def sync_league(self, league_id: str) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        landing_url, landing_html = self._resolve_landing_page(league_id)
        soup = BeautifulSoup(landing_html, "lxml")

        title_text = clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")
        display_name = title_text.split("-")[0].strip() if title_text else f"BC Football League {league_id}"
        season_label = self._extract_recent_season_label(landing_html)

        payload.leagues.append(
            LeagueRecord(
                source_league_id=league_id,
                sport="football",
                province="BC",
                season_label=season_label,
                display_name=display_name,
                metadata={"landing_url": landing_url},
            )
        )

        endpoints = self._collect_endpoints(landing_html, league_id)
        standings_url = endpoints.get("standings")
        schedules_url = endpoints.get("schedules")
        stats_url = endpoints.get("stats")
        teams_url = endpoints.get("teams")

        if standings_url:
            payload.merge(self._parse_standings_page(league_id, standings_url))
        if teams_url:
            payload.merge(self._parse_teams_page(league_id, teams_url))
        if stats_url:
            payload.merge(self._parse_stats_page(league_id, stats_url))
        if schedules_url:
            payload.merge(self._parse_schedules_page(league_id, schedules_url))

        return payload

    def _discover_active_leagues(self) -> list[str]:
        html = self.client.request_text(self.seed_url)
        ids = re.findall(r"clear\.cfm\?clientid=652(?:&amp;|&)leagueid=(\d+)", html, flags=re.IGNORECASE)
        ordered: list[str] = []
        for league_id in ids:
            if league_id not in ordered:
                ordered.append(league_id)
        return ordered

    def _resolve_landing_page(self, league_id: str) -> tuple[str, str]:
        clear_url = f"{self.base_url}clear.cfm?leagueID={league_id}&clientID=652"
        clear_html = self.client.request_text(clear_url)
        target = extract_js_redirect_target(clear_html)
        if not target:
            return clear_url, clear_html

        frameset_url = urljoin(self.base_url, target)
        frameset_html = self.client.request_text(frameset_url)
        top_target = extract_js_redirect_target(frameset_html)
        if not top_target:
            return frameset_url, frameset_html

        landing_url = urljoin(self.base_url, top_target)
        landing_html = self.client.request_text(landing_url)
        return landing_url, landing_html

    def _collect_endpoints(self, html: str, league_id: str) -> dict[str, str]:
        endpoints: dict[str, str] = {}
        mapping = {
            "standings": r"href=\"([^\"]*standings\.cfm[^\"]*)\"",
            "schedules": r"href=\"([^\"]*schedules\.cfm[^\"]*)\"",
            "stats": r"href=\"([^\"]*stats_football\.cfm[^\"]*)\"",
            "teams": r"href=\"([^\"]*teams\.cfm[^\"]*)\"",
        }

        for key, pattern in mapping.items():
            match = re.search(pattern, html, flags=re.IGNORECASE)
            if match:
                endpoints[key] = urljoin(self.base_url, match.group(1).replace("&amp;", "&"))

        defaults = {
            "standings": f"{self.base_url}standings.cfm?leagueID={league_id}&clientID=652",
            "schedules": f"{self.base_url}schedules.cfm?leagueID={league_id}&clientID=652",
            "stats": f"{self.base_url}stats_football.cfm?leagueID={league_id}&clientID=652",
            "teams": f"{self.base_url}teams.cfm?leagueID={league_id}&clientID=652",
        }
        for key, fallback in defaults.items():
            endpoints.setdefault(key, fallback)
        return endpoints

    def _parse_standings_page(self, league_id: str, url: str) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        soup = self.client.soup(url)
        for table in parse_html_tables(soup):
            headers = {h.lower() for h in table.headers}
            if "team" not in headers or "gp" not in headers:
                continue

            for idx, row in enumerate(table.rows, start=1):
                team_name = clean_text(row.get("Team") or row.get("col_1") or "")
                if not team_name or team_name.lower() == "team":
                    continue

                source_team_id = stable_id("bc_football", league_id, team_name)[:16]
                payload.teams.append(
                    TeamRecord(
                        source_league_id=league_id,
                        source_team_id=source_team_id,
                        name=team_name,
                        short_code=None,
                    )
                )
                payload.standings.append(
                    StandingRecord(
                        source_league_id=league_id,
                        source_team_id=source_team_id,
                        team_name=team_name,
                        rank=parse_int(row.get("col_1")) or idx,
                        gp=parse_int(row.get("GP")),
                        w=parse_int(row.get("W")),
                        l=parse_int(row.get("L")),
                        pts=parse_int(row.get("PTS")),
                        custom=row,
                    )
                )

        return payload

    def _parse_teams_page(self, league_id: str, url: str) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        html = self.client.request_text(url)
        soup = BeautifulSoup(html, "lxml")

        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href")
            text = clean_text(anchor.get_text(" ", strip=True))
            if not href or not text:
                continue
            if "teamid=" not in href.lower() and "/team/" not in href.lower():
                continue

            parsed = urlparse(href)
            team_id = parse_qs(parsed.query).get("teamID", [""])[0]
            if not team_id:
                team_match = re.search(r"team/(\d+)", href, flags=re.IGNORECASE)
                team_id = team_match.group(1) if team_match else stable_id("bc_football", league_id, text)[:12]

            payload.teams.append(
                TeamRecord(
                    source_league_id=league_id,
                    source_team_id=team_id,
                    name=text,
                    short_code=None,
                )
            )

        return payload

    def _parse_stats_page(self, league_id: str, url: str) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        soup = self.client.soup(url)

        for table in parse_html_tables(soup):
            headers = {h.lower() for h in table.headers}
            if "player" not in headers:
                continue

            for idx, row in enumerate(table.rows, start=1):
                player_name = clean_text(row.get("Player") or row.get("col_1") or "")
                if not player_name or player_name.lower() == "player":
                    continue

                team_name = clean_text(row.get("Team") or "") or None
                metrics = row_to_metrics(row, skip_keys={"Player", "Team", "col_1", "col_2"})
                m1, m2, m3, m4, m5 = pick_primary_metrics(metrics, "football")
                payload.player_stats.append(
                    PlayerStatRecord(
                        source_league_id=league_id,
                        source_player_id=None,
                        player_name=player_name,
                        team_name=team_name,
                        stat_group="scoring_leaders",
                        rank=idx,
                        metric_1=m1,
                        metric_2=m2,
                        metric_3=m3,
                        metric_4=m4,
                        metric_5=m5,
                        metrics=metrics,
                    )
                )

        return payload

    def _parse_schedules_page(self, league_id: str, url: str) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        html = self.client.request_text(url)
        soup = BeautifulSoup(html, "lxml")

        added = 0
        for table in parse_html_tables(soup):
            headers = {h.lower() for h in table.headers}
            if not ({"away", "away team", "home", "home team"} & headers):
                continue
            for row in table.rows:
                away_team = clean_text(row.get("Away") or row.get("Away Team") or "")
                home_team = clean_text(row.get("Home") or row.get("Home Team") or "")
                if not away_team or not home_team:
                    continue
                payload.games.append(
                    GameRecord(
                        source_league_id=league_id,
                        source_game_id=clean_text(row.get("#") or row.get("Game #") or "") or None,
                        date_time=clean_text(row.get("Date") or "") or None,
                        home_team=home_team,
                        away_team=away_team,
                        home_score=parse_int(row.get("Home Score") or row.get("GF")),
                        away_score=parse_int(row.get("Away Score") or row.get("GF_1")),
                        status=clean_text(row.get("Type") or row.get("Result") or "") or None,
                        venue=clean_text(row.get("Location") or row.get("Venue") or "") or None,
                        custom=row,
                    )
                )
                added += 1

        if added == 0:
            payload.games.extend(self._parse_non_tabular_games(league_id, html))
            payload.issues.append(
                IssueRecord(
                    source_league_id=league_id,
                    severity="info",
                    message="Schedule parsed from non-tabular snippets (fallback)",
                    raw_pointer=url,
                )
            )

        return payload

    def _parse_non_tabular_games(self, league_id: str, html: str) -> list[GameRecord]:
        soup = BeautifulSoup(html, "lxml")
        lines = [clean_text(x) for x in soup.get_text("\n").splitlines()]
        lines = [x for x in lines if x]
        parsed: list[GameRecord] = []
        seen: set[tuple[str, str, str]] = set()

        for idx, line in enumerate(lines):
            if not DATE_LINE_RE.match(line):
                continue
            window = lines[idx : idx + 14]
            score_pos = [i for i, token in enumerate(window) if re.fullmatch(r"\d{1,3}", token)]
            if len(score_pos) < 2:
                continue

            away_team = self._nearest_team(window, score_pos[0])
            home_team = self._nearest_team(window, score_pos[1])
            if not away_team or not home_team:
                continue

            key = (line, away_team, home_team)
            if key in seen:
                continue
            seen.add(key)

            parsed.append(
                GameRecord(
                    source_league_id=league_id,
                    source_game_id=None,
                    date_time=line,
                    home_team=home_team,
                    away_team=away_team,
                    home_score=parse_int(window[score_pos[1]]),
                    away_score=parse_int(window[score_pos[0]]),
                    status="FINAL" if any("FINAL" in token.upper() for token in window) else "SCHEDULED",
                    venue=None,
                    custom={"fallback": True},
                )
            )

        return parsed

    @staticmethod
    def _nearest_team(window: list[str], before_index: int) -> str | None:
        for i in range(before_index - 1, -1, -1):
            token = clean_text(window[i])
            if not token:
                continue
            if DATE_LINE_RE.match(token):
                continue
            if token.upper() in {"FINAL", "TBA"}:
                continue
            if re.fullmatch(r"\d{1,3}", token):
                continue
            if len(token) < 3:
                continue
            return token
        return None

    @staticmethod
    def _extract_recent_season_label(html: str) -> str | None:
        years = re.findall(r"(20\d{2})", html)
        if not years:
            return None
        return str(max(int(y) for y in years))
