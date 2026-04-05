from __future__ import annotations

import logging
import re
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
from canadastats.sources.base import SourceAdapter, parse_html_tables
from canadastats.transform.normalization import clean_text, pick_primary_metrics
from canadastats.utils import parse_int, stable_id

logger = logging.getLogger(__name__)

DATE_LINE_RE = re.compile(
    r"^(MON|TUE|WED|THU|FRI|SAT|SUN)\s+[A-Z]{3}\.\s+\d{1,2}\s+at\s+\d{1,2}:\d{2}(AM|PM)$",
    flags=re.IGNORECASE,
)


class HmhshlSource(SourceAdapter):
    name = "hmhshl"

    def __init__(self, client: HttpClient, config: AppConfig) -> None:
        self.client = client
        self.config = config
        self.base_url = "https://hmhshl.com"
        self.main_league_id = "hmhshl-main"

    def sync_all(self) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        payload.leagues.append(
            LeagueRecord(
                source_league_id=self.main_league_id,
                sport="hockey",
                province="NS",
                season_label=None,
                display_name="Halifax Metro High School Hockey League",
                metadata={"source_url": self.base_url},
            )
        )

        try:
            payload.merge(self._scrape_standings())
            payload.merge(self._scrape_team_pages())
            payload.merge(self._scrape_games())
        except Exception as exc:  # noqa: BLE001
            logger.exception("HMHSHL sync failed")
            payload.issues.append(
                IssueRecord(
                    source_league_id=self.main_league_id,
                    severity="error",
                    message=f"HMHSHL sync failed: {exc}",
                    raw_pointer=self.base_url,
                )
            )

        return payload

    def sync_league(self, league_id: str) -> SourceSyncPayload:
        if league_id != self.main_league_id:
            payload = SourceSyncPayload(source=self.name)
            payload.issues.append(
                IssueRecord(
                    source_league_id=league_id,
                    severity="warning",
                    message="HMHSHL supports only league id hmhshl-main",
                    raw_pointer=None,
                )
            )
            return payload
        return self.sync_all()

    def _scrape_standings(self) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        for url in [f"{self.base_url}/standings/", f"{self.base_url}/stats/"]:
            html = self.client.request_text(url)
            soup = BeautifulSoup(html, "lxml")
            tables = parse_html_tables(soup)
            for table in tables:
                header_set = {h.lower() for h in table.headers}
                if "team" not in header_set or "gp" not in header_set:
                    continue

                section = table.title or "Standings"
                for row in table.rows:
                    team_name = clean_text(row.get("Team") or row.get("col_2") or "")
                    if not team_name or team_name.lower() == "team":
                        continue

                    source_team_id = stable_id("hmhshl", team_name)[:16]
                    payload.teams.append(
                        TeamRecord(
                            source_league_id=self.main_league_id,
                            source_team_id=source_team_id,
                            name=team_name,
                            short_code=None,
                        )
                    )
                    payload.standings.append(
                        StandingRecord(
                            source_league_id=self.main_league_id,
                            source_team_id=source_team_id,
                            team_name=team_name,
                            rank=parse_int(row.get("col_1")),
                            gp=parse_int(row.get("GP")),
                            w=parse_int(row.get("W")),
                            l=parse_int(row.get("L")),
                            pts=parse_int(row.get("PTS")),
                            custom={"section": section, **row},
                        )
                    )
        return payload

    def _scrape_team_pages(self) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        home_html = self.client.request_text(f"{self.base_url}/")
        team_links = sorted(set(re.findall(r'href="(/teams/\d+/[^\"]+/)"', home_html, flags=re.IGNORECASE)))

        if not team_links:
            payload.issues.append(
                IssueRecord(
                    source_league_id=self.main_league_id,
                    severity="warning",
                    message="No team links found on HMHSHL homepage",
                    raw_pointer=f"{self.base_url}/",
                )
            )
            return payload

        for path in team_links:
            url = urljoin(self.base_url, path)
            team_id_match = re.search(r"/teams/(\d+)/", path)
            source_team_id = team_id_match.group(1) if team_id_match else stable_id("hmhshl", path)[:12]
            try:
                soup = self.client.soup(url)
            except Exception as exc:  # noqa: BLE001
                payload.issues.append(
                    IssueRecord(
                        source_league_id=self.main_league_id,
                        severity="warning",
                        message=f"Failed to parse team page {url}: {exc}",
                        raw_pointer=url,
                    )
                )
                continue

            title = clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")
            team_name = clean_text(title.split("|")[0]) if title else source_team_id
            payload.teams.append(
                TeamRecord(
                    source_league_id=self.main_league_id,
                    source_team_id=source_team_id,
                    name=team_name,
                    short_code=None,
                )
            )

            tables = parse_html_tables(soup)
            for table in tables:
                headers = {h.lower() for h in table.headers}
                if "player" not in headers:
                    continue
                if not ({"g", "a", "pts"} & headers):
                    continue

                for idx, row in enumerate(table.rows, start=1):
                    player_name = clean_text(row.get("Player") or row.get("col_1") or "")
                    if not player_name or player_name.lower() == "player":
                        continue

                    metrics = {
                        "G": row.get("G"),
                        "A": row.get("A"),
                        "PTS": row.get("Pts") or row.get("PTS"),
                        "PIM": row.get("PIM"),
                    }
                    m1, m2, m3, m4, m5 = pick_primary_metrics(metrics, "hockey")
                    payload.player_stats.append(
                        PlayerStatRecord(
                            source_league_id=self.main_league_id,
                            source_player_id=None,
                            player_name=player_name,
                            team_name=team_name,
                            stat_group="team_skaters",
                            rank=idx,
                            metric_1=m1,
                            metric_2=m2,
                            metric_3=m3,
                            metric_4=m4,
                            metric_5=m5,
                            metrics={k: v for k, v in metrics.items() if v is not None},
                        )
                    )

        return payload

    def _scrape_games(self) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        for url in [f"{self.base_url}/schedule/", f"{self.base_url}/results/"]:
            html = self.client.request_text(url)
            soup = BeautifulSoup(html, "lxml")

            parsed_from_tables = 0
            for table in parse_html_tables(soup):
                headers = {h.lower() for h in table.headers}
                if not ({"away team", "away", "home team", "home"} & headers):
                    continue
                for row in table.rows:
                    away_team = clean_text(row.get("Away Team") or row.get("Away") or "")
                    home_team = clean_text(row.get("Home Team") or row.get("Home") or "")
                    if not away_team or not home_team:
                        continue
                    payload.games.append(
                        GameRecord(
                            source_league_id=self.main_league_id,
                            source_game_id=clean_text(row.get("Game #") or row.get("#") or "") or None,
                            date_time=clean_text(row.get("Date") or "") or None,
                            home_team=home_team,
                            away_team=away_team,
                            home_score=parse_int(row.get("GF") or row.get("Home Score")),
                            away_score=parse_int(row.get("GF_1") or row.get("Away Score")),
                            status=clean_text(row.get("Type") or row.get("Status") or "") or None,
                            venue=clean_text(row.get("Venue") or row.get("Location") or "") or None,
                            custom=row,
                        )
                    )
                    parsed_from_tables += 1

            if parsed_from_tables == 0:
                payload.games.extend(self._parse_text_snippets(url, html))
                payload.issues.append(
                    IssueRecord(
                        source_league_id=self.main_league_id,
                        severity="info",
                        message="No structured static game table found; captured any detectable snippets only",
                        raw_pointer=url,
                    )
                )

        return payload

    def _parse_text_snippets(self, url: str, html: str) -> list[GameRecord]:
        soup = BeautifulSoup(html, "lxml")
        lines = [clean_text(line) for line in soup.get_text("\n").splitlines()]
        lines = [line for line in lines if line]
        snippets: list[GameRecord] = []
        seen: set[tuple[str, str, str]] = set()

        for idx, line in enumerate(lines):
            if not DATE_LINE_RE.match(line):
                continue
            window = lines[idx : idx + 12]
            number_positions = [j for j, value in enumerate(window) if re.fullmatch(r"\d{1,3}", value)]
            if len(number_positions) < 2:
                continue

            away_score = parse_int(window[number_positions[0]])
            home_score = parse_int(window[number_positions[1]])
            away_team = self._nearest_team_name(window, number_positions[0])
            home_team = self._nearest_team_name(window, number_positions[1])
            if not away_team or not home_team:
                continue

            key = (line, away_team, home_team)
            if key in seen:
                continue
            seen.add(key)

            status = "FINAL" if any("FINAL" in token.upper() for token in window) else "SCHEDULED"
            snippets.append(
                GameRecord(
                    source_league_id=self.main_league_id,
                    source_game_id=None,
                    date_time=line,
                    home_team=home_team,
                    away_team=away_team,
                    home_score=home_score,
                    away_score=away_score,
                    status=status,
                    venue=None,
                    custom={"raw_pointer": url},
                )
            )

        return snippets

    @staticmethod
    def _nearest_team_name(window: list[str], score_pos: int) -> str | None:
        for i in range(score_pos - 1, -1, -1):
            token = clean_text(window[i])
            if not token:
                continue
            if DATE_LINE_RE.match(token):
                continue
            if token.upper() in {"FINAL", "TBA"}:
                continue
            if re.fullmatch(r"\d{1,3}", token):
                continue
            if len(token) <= 2:
                continue
            return token
        return None
