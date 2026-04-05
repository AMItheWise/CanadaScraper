from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from canadastats.utils import now_utc_iso


def _sqlite_path_from_url(url: str) -> Path:
    if not url.startswith("sqlite:///"):
        raise ValueError(f"Only sqlite:/// URLs are supported, got: {url}")
    return Path(url.replace("sqlite:///", "", 1))


class Repository:
    def __init__(self, database_url: str) -> None:
        self.db_path = _sqlite_path_from_url(database_url)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.init_db()

    def close(self) -> None:
        self.conn.close()

    def init_db(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS scrape_runs (
                run_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                summary_json TEXT
            );

            CREATE TABLE IF NOT EXISTS scrape_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                source TEXT NOT NULL,
                source_league_id TEXT,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                raw_pointer TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES scrape_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS leagues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                source_league_id TEXT NOT NULL,
                sport TEXT NOT NULL,
                province TEXT NOT NULL,
                season_label TEXT,
                display_name TEXT NOT NULL,
                metadata_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(source, source_league_id)
            );

            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                league_id INTEGER NOT NULL,
                source_team_id TEXT NOT NULL,
                name TEXT NOT NULL,
                short_code TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(league_id, source_team_id),
                FOREIGN KEY(league_id) REFERENCES leagues(id)
            );

            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                source_player_key TEXT NOT NULL,
                source_player_id TEXT,
                name TEXT NOT NULL,
                team_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(source, source_player_key),
                FOREIGN KEY(team_id) REFERENCES teams(id)
            );

            CREATE TABLE IF NOT EXISTS standing_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                league_id INTEGER NOT NULL,
                team_id INTEGER,
                rank INTEGER,
                gp INTEGER,
                w INTEGER,
                l INTEGER,
                pts REAL,
                custom_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES scrape_runs(run_id),
                FOREIGN KEY(league_id) REFERENCES leagues(id),
                FOREIGN KEY(team_id) REFERENCES teams(id)
            );

            CREATE TABLE IF NOT EXISTS game_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                league_id INTEGER NOT NULL,
                source_game_id TEXT,
                date_time TEXT,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                home_score INTEGER,
                away_score INTEGER,
                status TEXT,
                venue TEXT,
                custom_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES scrape_runs(run_id),
                FOREIGN KEY(league_id) REFERENCES leagues(id)
            );

            CREATE TABLE IF NOT EXISTS player_stat_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                league_id INTEGER NOT NULL,
                player_id INTEGER,
                player_name TEXT NOT NULL,
                team_name TEXT,
                stat_group TEXT NOT NULL,
                rank INTEGER,
                metric_1 REAL,
                metric_2 REAL,
                metric_3 REAL,
                metric_4 REAL,
                metric_5 REAL,
                metrics_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES scrape_runs(run_id),
                FOREIGN KEY(league_id) REFERENCES leagues(id),
                FOREIGN KEY(player_id) REFERENCES players(id)
            );

            CREATE INDEX IF NOT EXISTS idx_leagues_source ON leagues(source);
            CREATE INDEX IF NOT EXISTS idx_runs_source_status ON scrape_runs(source, status, started_at);
            CREATE INDEX IF NOT EXISTS idx_standing_run_league ON standing_snapshots(run_id, league_id);
            CREATE INDEX IF NOT EXISTS idx_games_run_league ON game_snapshots(run_id, league_id);
            CREATE INDEX IF NOT EXISTS idx_player_stats_run_league ON player_stat_snapshots(run_id, league_id);
            """
        )
        self._refresh_views(cur)
        self.conn.commit()

    def _refresh_views(self, cur: sqlite3.Cursor) -> None:
        cur.executescript(
            """
            DROP VIEW IF EXISTS current_standings;
            CREATE VIEW current_standings AS
            SELECT ss.*, l.source, l.source_league_id, l.sport, l.province, l.display_name AS league_name, t.name AS team_name
            FROM standing_snapshots ss
            JOIN scrape_runs sr ON sr.run_id = ss.run_id
            JOIN leagues l ON l.id = ss.league_id
            LEFT JOIN teams t ON t.id = ss.team_id
            WHERE sr.status = 'success'
              AND sr.started_at = (SELECT MAX(started_at) FROM scrape_runs WHERE status = 'success');

            DROP VIEW IF EXISTS current_games;
            CREATE VIEW current_games AS
            SELECT gs.*, l.source, l.source_league_id, l.sport, l.province, l.display_name AS league_name
            FROM game_snapshots gs
            JOIN scrape_runs sr ON sr.run_id = gs.run_id
            JOIN leagues l ON l.id = gs.league_id
            WHERE sr.status = 'success'
              AND sr.started_at = (SELECT MAX(started_at) FROM scrape_runs WHERE status = 'success');

            DROP VIEW IF EXISTS current_leaders;
            CREATE VIEW current_leaders AS
            SELECT ps.*, l.source, l.source_league_id, l.sport, l.province, l.display_name AS league_name
            FROM player_stat_snapshots ps
            JOIN scrape_runs sr ON sr.run_id = ps.run_id
            JOIN leagues l ON l.id = ps.league_id
            WHERE sr.status = 'success'
              AND sr.started_at = (SELECT MAX(started_at) FROM scrape_runs WHERE status = 'success');

            DROP VIEW IF EXISTS source_health;
            CREATE VIEW source_health AS
            SELECT source, MAX(started_at) AS last_started_at,
                   (SELECT status FROM scrape_runs s2 WHERE s2.source = s1.source ORDER BY started_at DESC LIMIT 1) AS latest_status
            FROM scrape_runs s1
            GROUP BY source;
            """
        )

    def start_run(self, run_id: str, source: str) -> None:
        now = now_utc_iso()
        self.conn.execute(
            """
            INSERT INTO scrape_runs(run_id, source, started_at, status)
            VALUES (?, ?, ?, ?)
            """,
            (run_id, source, now, "running"),
        )
        self.conn.commit()

    def finish_run(self, run_id: str, status: str, summary_json: str) -> None:
        now = now_utc_iso()
        self.conn.execute(
            """
            UPDATE scrape_runs
            SET finished_at = ?, status = ?, summary_json = ?
            WHERE run_id = ?
            """,
            (now, status, summary_json, run_id),
        )
        cur = self.conn.cursor()
        self._refresh_views(cur)
        self.conn.commit()

    def add_issue(
        self,
        run_id: str,
        source: str,
        source_league_id: str | None,
        severity: str,
        message: str,
        raw_pointer: str | None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO scrape_issues(run_id, source, source_league_id, severity, message, raw_pointer, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, source, source_league_id, severity, message, raw_pointer, now_utc_iso()),
        )

    def upsert_league(
        self,
        source: str,
        source_league_id: str,
        sport: str,
        province: str,
        season_label: str | None,
        display_name: str,
        metadata_json: str,
    ) -> int:
        now = now_utc_iso()
        self.conn.execute(
            """
            INSERT INTO leagues(
                source, source_league_id, sport, province, season_label, display_name,
                metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, source_league_id)
            DO UPDATE SET
              sport = excluded.sport,
              province = excluded.province,
              season_label = excluded.season_label,
              display_name = excluded.display_name,
              metadata_json = excluded.metadata_json,
              updated_at = excluded.updated_at
            """,
            (
                source,
                source_league_id,
                sport,
                province,
                season_label,
                display_name,
                metadata_json,
                now,
                now,
            ),
        )
        row = self.conn.execute(
            "SELECT id FROM leagues WHERE source = ? AND source_league_id = ?",
            (source, source_league_id),
        ).fetchone()
        assert row is not None
        return int(row["id"])

    def upsert_team(
        self,
        league_id: int,
        source_team_id: str,
        name: str,
        short_code: str | None,
    ) -> int:
        now = now_utc_iso()
        self.conn.execute(
            """
            INSERT INTO teams(league_id, source_team_id, name, short_code, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(league_id, source_team_id)
            DO UPDATE SET name = excluded.name, short_code = excluded.short_code, updated_at = excluded.updated_at
            """,
            (league_id, source_team_id, name, short_code, now, now),
        )
        row = self.conn.execute(
            "SELECT id FROM teams WHERE league_id = ? AND source_team_id = ?",
            (league_id, source_team_id),
        ).fetchone()
        assert row is not None
        return int(row["id"])

    def find_team_id(self, league_id: int, source_team_id: str | None, name: str | None) -> int | None:
        if source_team_id:
            row = self.conn.execute(
                "SELECT id FROM teams WHERE league_id = ? AND source_team_id = ?",
                (league_id, source_team_id),
            ).fetchone()
            if row:
                return int(row["id"])
        if name:
            row = self.conn.execute(
                "SELECT id FROM teams WHERE league_id = ? AND lower(name) = lower(?) LIMIT 1",
                (league_id, name),
            ).fetchone()
            if row:
                return int(row["id"])
        return None

    def upsert_player(
        self,
        source: str,
        source_player_key: str,
        source_player_id: str | None,
        name: str,
        team_id: int | None,
    ) -> int:
        now = now_utc_iso()
        self.conn.execute(
            """
            INSERT INTO players(source, source_player_key, source_player_id, name, team_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, source_player_key)
            DO UPDATE SET
              source_player_id = excluded.source_player_id,
              name = excluded.name,
              team_id = excluded.team_id,
              updated_at = excluded.updated_at
            """,
            (source, source_player_key, source_player_id, name, team_id, now, now),
        )
        row = self.conn.execute(
            "SELECT id FROM players WHERE source = ? AND source_player_key = ?",
            (source, source_player_key),
        ).fetchone()
        assert row is not None
        return int(row["id"])

    def insert_standing(
        self,
        run_id: str,
        league_id: int,
        team_id: int | None,
        rank: int | None,
        gp: int | None,
        w: int | None,
        l: int | None,
        pts: float | int | None,
        custom_json: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO standing_snapshots(
                run_id, league_id, team_id, rank, gp, w, l, pts, custom_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, league_id, team_id, rank, gp, w, l, pts, custom_json, now_utc_iso()),
        )

    def insert_game(
        self,
        run_id: str,
        league_id: int,
        source_game_id: str | None,
        date_time: str | None,
        home_team: str,
        away_team: str,
        home_score: int | None,
        away_score: int | None,
        status: str | None,
        venue: str | None,
        custom_json: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO game_snapshots(
                run_id, league_id, source_game_id, date_time, home_team, away_team,
                home_score, away_score, status, venue, custom_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                league_id,
                source_game_id,
                date_time,
                home_team,
                away_team,
                home_score,
                away_score,
                status,
                venue,
                custom_json,
                now_utc_iso(),
            ),
        )

    def insert_player_stat(
        self,
        run_id: str,
        league_id: int,
        player_id: int | None,
        player_name: str,
        team_name: str | None,
        stat_group: str,
        rank: int | None,
        metric_1: float | int | None,
        metric_2: float | int | None,
        metric_3: float | int | None,
        metric_4: float | int | None,
        metric_5: float | int | None,
        metrics_json: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO player_stat_snapshots(
                run_id, league_id, player_id, player_name, team_name, stat_group,
                rank, metric_1, metric_2, metric_3, metric_4, metric_5, metrics_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                league_id,
                player_id,
                player_name,
                team_name,
                stat_group,
                rank,
                metric_1,
                metric_2,
                metric_3,
                metric_4,
                metric_5,
                metrics_json,
                now_utc_iso(),
            ),
        )

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def query_rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        return list(self.conn.execute(sql, params).fetchall())
