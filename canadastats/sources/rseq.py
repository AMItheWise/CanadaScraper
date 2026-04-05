from __future__ import annotations

import logging
import re
from typing import Any

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
from canadastats.sources.base import SourceAdapter, row_to_metrics
from canadastats.transform.normalization import clean_text, normalize_sport, pick_primary_metrics
from canadastats.utils import parse_float, parse_int

logger = logging.getLogger(__name__)

RSEQ_LEAGUE_ID_RE = re.compile(r"LeagueId=(?P<league>[a-f0-9\-]{36})", flags=re.IGNORECASE)


def extract_league_id_from_html(html: str) -> str | None:
    match = RSEQ_LEAGUE_ID_RE.search(html)
    if not match:
        return None
    return match.group("league")


def infer_sport_from_url(url: str) -> str:
    return normalize_sport(url)


class RseqSource(SourceAdapter):
    name = "rseq"

    def __init__(self, client: HttpClient, config: AppConfig) -> None:
        self.client = client
        self.config = config

    def sync_all(self) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        for league_url in self.config.sources.rseq_league_urls:
            try:
                payload.merge(self._sync_from_page(league_url))
            except Exception as exc:  # noqa: BLE001
                logger.exception("RSEQ failed for %s", league_url)
                payload.issues.append(
                    IssueRecord(
                        source_league_id=None,
                        severity="error",
                        message=f"Failed scraping {league_url}: {exc}",
                        raw_pointer=league_url,
                    )
                )
        return payload

    def sync_league(self, league_id: str) -> SourceSyncPayload:
        if league_id.startswith("http"):
            return self._sync_from_page(league_id)
        return self._sync_from_league_id(league_id=league_id, sport_hint="unknown", league_url=None)

    def _sync_from_page(self, league_url: str) -> SourceSyncPayload:
        html = self.client.request_text(league_url)
        league_id = extract_league_id_from_html(html)
        if not league_id:
            payload = SourceSyncPayload(source=self.name)
            payload.issues.append(
                IssueRecord(
                    source_league_id=None,
                    severity="warning",
                    message="Could not locate embedded LeagueId; page may not expose stats feed",
                    raw_pointer=league_url,
                )
            )
            return payload

        return self._sync_from_league_id(
            league_id=league_id,
            sport_hint=infer_sport_from_url(league_url),
            league_url=league_url,
        )

    def _sync_from_league_id(self, league_id: str, sport_hint: str, league_url: str | None) -> SourceSyncPayload:
        payload = SourceSyncPayload(source=self.name)
        api_url = f"https://s1.rseq.ca/api/LeagueApi/GetLeagueDiffusion/?leagueId={league_id}"
        data = self.client.request_json(api_url)

        sport = normalize_sport(str(data.get("SportName") or sport_hint))
        league_name = clean_text(str(data.get("LeagueName") or f"RSEQ {league_id}"))
        season_label = clean_text(str(data.get("SchoolYearYears") or "")) or None

        payload.leagues.append(
            LeagueRecord(
                source_league_id=league_id,
                sport=sport,
                province="QC",
                season_label=season_label,
                display_name=league_name,
                metadata={"league_url": league_url, "api_url": api_url},
            )
        )

        teams = data.get("Teams") or []
        for team in teams:
            team_id = str(team.get("TeamId") or "")
            team_name = clean_text(str(team.get("TeamName") or ""))
            if not team_name:
                continue
            payload.teams.append(
                TeamRecord(
                    source_league_id=league_id,
                    source_team_id=team_id or None,
                    name=team_name,
                    short_code=clean_text(str(team.get("TeamCode") or "")) or None,
                )
            )

        standings = data.get("Standings") or []
        for row in standings:
            team_name = clean_text(str(row.get("TeamName") or row.get("TeamNameDiffusionHtml") or ""))
            if not team_name:
                continue
            payload.standings.append(
                StandingRecord(
                    source_league_id=league_id,
                    source_team_id=str(row.get("TeamId") or "") or None,
                    team_name=team_name,
                    rank=parse_int(row.get("Position")),
                    gp=parse_int(row.get("GamesPlayed")),
                    w=parse_int(row.get("Wins")),
                    l=parse_int(row.get("Losses")),
                    pts=parse_float(row.get("TotalPoints") or row.get("LeaguePoints")),
                    custom={k: v for k, v in row.items() if k not in {"TeamNameDiffusionHtml"}},
                )
            )

        game_keys = [
            "PreSeasonGames",
            "RegularSeasonGames",
            "PostSeasonGames",
            "ChampionshipGames",
            "ConferenceChampionshipGames",
            "AllStarGames",
        ]
        for game_key in game_keys:
            for game in data.get(game_key) or []:
                away_team = clean_text(str(game.get("VisitingTeamName") or ""))
                home_team = clean_text(str(game.get("HomeTeamName") or ""))
                if not away_team or not home_team:
                    continue
                payload.games.append(
                    GameRecord(
                        source_league_id=league_id,
                        source_game_id=str(game.get("GameId") or "") or None,
                        date_time=clean_text(str(game.get("GameDate") or game.get("GameDateFormatted") or "")) or None,
                        home_team=home_team,
                        away_team=away_team,
                        home_score=parse_int(game.get("HomeTeamScore")),
                        away_score=parse_int(game.get("VisitingTeamScore")),
                        status=clean_text(str(game.get("GameResultFormatted") or "")) or None,
                        venue=clean_text(str(game.get("SportFacilityDescription") or "")) or None,
                        custom={
                            "season_type": game.get("SeasonType"),
                            "description": game.get("Description"),
                            "is_released": game.get("IsReleased"),
                        },
                    )
                )

        for stat_key, stat_rows in data.items():
            if not (stat_key.startswith("League") and stat_key.endswith("Stats")):
                continue
            if not isinstance(stat_rows, list) or not stat_rows:
                continue
            if not isinstance(stat_rows[0], dict):
                continue

            for idx, row in enumerate(stat_rows, start=1):
                player_name = clean_text(
                    str(
                        row.get("FullName")
                        or f"{row.get('AthleteFirstName', '')} {row.get('AthleteLastName', '')}"
                        or ""
                    )
                )
                if not player_name:
                    continue
                team_name = clean_text(str(row.get("TeamName") or "")) or None

                metrics = row_to_metrics(
                    {k: clean_text(str(v)) for k, v in row.items()},
                    skip_keys={
                        "LeagueId",
                        "TeamId",
                        "TeamName",
                        "TeamCode",
                        "AthleteId",
                        "AthleteLastName",
                        "AthleteFirstName",
                        "FullName",
                        "LastName",
                        "FirstName",
                    },
                )
                m1, m2, m3, m4, m5 = pick_primary_metrics(metrics, sport)

                payload.player_stats.append(
                    PlayerStatRecord(
                        source_league_id=league_id,
                        source_player_id=str(row.get("AthleteId") or "") or None,
                        player_name=player_name,
                        team_name=team_name,
                        stat_group=stat_key,
                        rank=parse_int(row.get("Position")) or idx,
                        metric_1=m1,
                        metric_2=m2,
                        metric_3=m3,
                        metric_4=m4,
                        metric_5=m5,
                        metrics=metrics,
                    )
                )

        return payload
