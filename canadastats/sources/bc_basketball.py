from __future__ import annotations

import logging
import re
from datetime import datetime
from urllib.parse import urljoin

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
from canadastats.utils import parse_int, slugify, stable_id

logger = logging.getLogger(__name__)


class BcBasketballSource(SourceAdapter):
    name = "bc_basketball"

    def __init__(self, client: HttpClient, config: AppConfig) -> None:
        self.client = client
        self.config = config
        self.seed_url = config.sources.bc_basketball_seed_url
        self.base_url = "https://www.bcboysbasketball.com/leagues/"
        self.current_year = datetime.utcnow().year

    def sync_all(self) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        league_ids = self._discover_active_leagues()

        if not league_ids:
            payload.issues.append(
                IssueRecord(
                    source_league_id=None,
                    severity="error",
                    message="No BC basketball leagues discovered",
                    raw_pointer=self.seed_url,
                )
            )
            return payload

        for league_id in league_ids:
            try:
                payload.merge(self.sync_league(league_id))
            except Exception as exc:  # noqa: BLE001
                logger.exception("BC basketball league failed: %s", league_id)
                payload.issues.append(
                    IssueRecord(
                        source_league_id=league_id,
                        severity="error",
                        message=f"League sync failed: {exc}",
                        raw_pointer=league_id,
                    )
                )

        payload.merge(self._parse_rankings_pages())
        return payload

    def sync_league(self, league_id: str) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        landing_url, landing_html = self._resolve_landing_page(league_id)
        if not self._looks_current_season(landing_html):
            payload.issues.append(
                IssueRecord(
                    source_league_id=league_id,
                    severity="info",
                    message="League skipped because it does not appear to be current season",
                    raw_pointer=landing_url,
                )
            )
            return payload

        soup = BeautifulSoup(landing_html, "lxml")
        title_text = clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")
        display_name = title_text.split("-")[0].strip() if title_text else f"BC Basketball League {league_id}"
        season_label = self._extract_recent_year(landing_html)

        payload.leagues.append(
            LeagueRecord(
                source_league_id=league_id,
                sport="basketball",
                province="BC",
                season_label=season_label,
                display_name=display_name,
                metadata={"landing_url": landing_url},
            )
        )

        endpoints = self._collect_endpoints(landing_html, league_id)
        if endpoints.get("standings"):
            payload.merge(self._parse_standings_page(league_id, endpoints["standings"]))
        if endpoints.get("teams"):
            payload.merge(self._parse_teams_page(league_id, endpoints["teams"]))
        if endpoints.get("stats"):
            payload.merge(self._parse_stats_page(league_id, endpoints["stats"]))
        if endpoints.get("schedules"):
            payload.merge(self._parse_schedules_page(league_id, endpoints["schedules"]))

        return payload

    def _discover_active_leagues(self) -> list[str]:
        seed_html = self.client.request_text(self.seed_url)
        league_ids: list[str] = []

        for league_id in re.findall(r"clear\.cfm\?clientid=2192(?:&amp;|&)leagueid=(\d+)", seed_html, flags=re.IGNORECASE):
            if league_id != "0" and league_id not in league_ids:
                league_ids.append(league_id)

        pick_links = sorted(set(re.findall(r"href=\"(pick_league\.cfm\?[^\"]+)\"", seed_html, flags=re.IGNORECASE)))
        for pick_link in pick_links:
            pick_url = urljoin(self.base_url, pick_link.replace("&amp;", "&"))
            html = self.client.request_text(pick_url)
            for league_id in re.findall(r"clear\.cfm\?clientid=2192(?:&amp;|&)leagueid=(\d+)", html, flags=re.IGNORECASE):
                if league_id == "0":
                    continue
                if league_id not in league_ids:
                    league_ids.append(league_id)

        return league_ids

    def _resolve_landing_page(self, league_id: str) -> tuple[str, str]:
        clear_url = f"{self.base_url}clear.cfm?leagueID={league_id}&clientID=2192"
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
            "stats": r"href=\"([^\"]*stats_basketball\.cfm[^\"]*)\"",
            "teams": r"href=\"([^\"]*teams\.cfm[^\"]*)\"",
        }

        for key, pattern in mapping.items():
            match = re.search(pattern, html, flags=re.IGNORECASE)
            if match:
                endpoints[key] = urljoin(self.base_url, match.group(1).replace("&amp;", "&"))

        defaults = {
            "standings": f"{self.base_url}standings.cfm?leagueID={league_id}&clientID=2192",
            "schedules": f"{self.base_url}schedules.cfm?leagueID={league_id}&clientID=2192",
            "stats": f"{self.base_url}stats_basketball.cfm?leagueID={league_id}&clientID=2192",
            "teams": f"{self.base_url}teams.cfm?leagueID={league_id}&clientID=2192",
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

                source_team_id = stable_id("bc_basketball", league_id, team_name)[:16]
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
            if "team" not in href.lower():
                continue

            team_match = re.search(r"teamid=(\d+)", href, flags=re.IGNORECASE)
            source_team_id = team_match.group(1) if team_match else stable_id("bc_basketball", league_id, text)[:12]
            payload.teams.append(
                TeamRecord(
                    source_league_id=league_id,
                    source_team_id=source_team_id,
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
                metrics = row_to_metrics(row, skip_keys={"Player", "Team", "Pos", "#", "col_1", "col_2", "col_3"})
                m1, m2, m3, m4, m5 = pick_primary_metrics(metrics, "basketball")
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
        soup = self.client.soup(url)

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
                        status=clean_text(row.get("Result") or row.get("Type") or "") or None,
                        venue=clean_text(row.get("Location") or row.get("Venue") or "") or None,
                        custom=row,
                    )
                )

        return payload

    def _parse_rankings_pages(self) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        html = self.client.request_text(self.seed_url)
        soup = BeautifulSoup(html, "lxml")

        ranking_links: list[tuple[str, str]] = []
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            text = clean_text(anchor.get_text(" ", strip=True))
            if "custom_page.cfm" not in href:
                continue
            if "rank" not in text.lower() and "ranking" not in href.lower():
                continue
            ranking_links.append((urljoin(self.base_url, href.replace("&amp;", "&")), text or "Rankings"))

        unique_links = []
        seen = set()
        for url, label in ranking_links:
            if url in seen:
                continue
            seen.add(url)
            unique_links.append((url, label))

        for ranking_url, label in unique_links:
            ranking_html = self.client.request_text(ranking_url)
            ranking_soup = BeautifulSoup(ranking_html, "lxml")

            page_id_match = re.search(r"pageid=(\d+)", ranking_url, flags=re.IGNORECASE)
            page_id = page_id_match.group(1) if page_id_match else slugify(label)

            tables = parse_html_tables(ranking_soup)
            rank_table_index = 0
            for table in tables:
                headers = {h.lower() for h in table.headers}
                if "rank" not in headers or "team" not in headers:
                    continue

                rank_table_index += 1
                section_title = table.title or f"Ranking {rank_table_index}"
                source_league_id = f"ranking-{page_id}-{slugify(section_title)}"
                display_name = clean_text(f"BC Rankings - {section_title}")

                payload.leagues.append(
                    LeagueRecord(
                        source_league_id=source_league_id,
                        sport="basketball",
                        province="BC",
                        season_label=str(self.current_year),
                        display_name=display_name,
                        metadata={"ranking_url": ranking_url, "section": section_title},
                    )
                )

                for row in table.rows:
                    team_name = clean_text(row.get("Team") or row.get("col_2") or "")
                    if not team_name:
                        continue
                    source_team_id = stable_id("bc_basketball_ranking", team_name)[:16]

                    payload.teams.append(
                        TeamRecord(
                            source_league_id=source_league_id,
                            source_team_id=source_team_id,
                            name=team_name,
                            short_code=None,
                        )
                    )
                    payload.standings.append(
                        StandingRecord(
                            source_league_id=source_league_id,
                            source_team_id=source_team_id,
                            team_name=team_name,
                            rank=parse_int(row.get("Rank") or row.get("col_1")),
                            gp=None,
                            w=None,
                            l=None,
                            pts=None,
                            custom=row,
                        )
                    )

        return payload

    def _looks_current_season(self, html: str) -> bool:
        year = self.current_year
        year_candidates = {str(year), str(year - 1), f"{year - 1}-{year}", f"{year}/{year + 1}"}
        lowered = html.lower()
        return any(token.lower() in lowered for token in year_candidates)

    @staticmethod
    def _extract_recent_year(html: str) -> str | None:
        years = re.findall(r"(20\d{2})", html)
        if not years:
            return None
        return str(max(int(y) for y in years))
