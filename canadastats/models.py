from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class LeagueRecord:
    source_league_id: str
    sport: str
    province: str
    season_label: str | None
    display_name: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TeamRecord:
    source_league_id: str
    source_team_id: str | None
    name: str
    short_code: str | None = None


@dataclass(slots=True)
class StandingRecord:
    source_league_id: str
    source_team_id: str | None
    team_name: str
    rank: int | None
    gp: int | None
    w: int | None
    l: int | None
    pts: float | int | None
    custom: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class GameRecord:
    source_league_id: str
    source_game_id: str | None
    date_time: str | None
    home_team: str
    away_team: str
    home_score: int | None
    away_score: int | None
    status: str | None
    venue: str | None
    custom: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PlayerStatRecord:
    source_league_id: str
    source_player_id: str | None
    player_name: str
    team_name: str | None
    stat_group: str
    rank: int | None
    metric_1: float | int | None = None
    metric_2: float | int | None = None
    metric_3: float | int | None = None
    metric_4: float | int | None = None
    metric_5: float | int | None = None
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class IssueRecord:
    source_league_id: str | None
    severity: str
    message: str
    raw_pointer: str | None = None


@dataclass(slots=True)
class SourceSyncPayload:
    source: str
    leagues: list[LeagueRecord] = field(default_factory=list)
    teams: list[TeamRecord] = field(default_factory=list)
    standings: list[StandingRecord] = field(default_factory=list)
    games: list[GameRecord] = field(default_factory=list)
    player_stats: list[PlayerStatRecord] = field(default_factory=list)
    issues: list[IssueRecord] = field(default_factory=list)

    def merge(self, other: "SourceSyncPayload") -> None:
        self.leagues.extend(other.leagues)
        self.teams.extend(other.teams)
        self.standings.extend(other.standings)
        self.games.extend(other.games)
        self.player_stats.extend(other.player_stats)
        self.issues.extend(other.issues)
