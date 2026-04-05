from __future__ import annotations

import json
import logging
import re
import traceback
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from canadastats.config import AppConfig
from canadastats.extract.http_client import HttpClient
from canadastats.load.repository import Repository
from canadastats.models import SourceSyncPayload
from canadastats.sources.bc_basketball import BcBasketballSource
from canadastats.sources.bc_football import BcFootballSource
from canadastats.sources.hmhshl import HmhshlSource
from canadastats.sources.rseq import RseqSource
from canadastats.transform.normalization import to_json
from canadastats.utils import stable_id

logger = logging.getLogger(__name__)


class SyncService:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.repo = Repository(config.database.url)
        self.client = HttpClient(config)
        self.adapters = {
            "rseq": RseqSource(self.client, config),
            "hmhshl": HmhshlSource(self.client, config),
            "bc_football": BcFootballSource(self.client, config),
            "bc_basketball": BcBasketballSource(self.client, config),
        }

    def close(self) -> None:
        self.repo.close()

    def sync_all(self) -> dict[str, Any]:
        run_id = str(uuid.uuid4())
        self.repo.start_run(run_id, "all")
        aggregate_summary = {
            "leagues": 0,
            "teams": 0,
            "standings": 0,
            "games": 0,
            "player_stats": 0,
            "issues": 0,
        }

        try:
            for source_name, adapter in self.adapters.items():
                if not self.config.sources.enabled.get(source_name, True):
                    logger.info("Source disabled in config: %s", source_name)
                    continue
                logger.info("Syncing source=%s", source_name)
                payload = adapter.sync_all()
                source_summary = self._persist_payload(run_id, payload)
                for key in aggregate_summary:
                    aggregate_summary[key] += int(source_summary.get(key, 0))

            aggregate_summary["cache_purged_files"] = self.client.purge_old_cache(days=30)
            self.repo.finish_run(run_id, "success", json.dumps(aggregate_summary, ensure_ascii=False))
            return {"run_id": run_id, **aggregate_summary}
        except Exception as exc:  # noqa: BLE001
            logger.exception("sync_all failed")
            self.repo.rollback()
            self.repo.add_issue(
                run_id=run_id,
                source="all",
                source_league_id=None,
                severity="error",
                message=str(exc),
                raw_pointer=traceback.format_exc(),
            )
            self.repo.finish_run(run_id, "failed", json.dumps({"error": str(exc)}, ensure_ascii=False))
            raise

    def sync_source(self, source_name: str) -> dict[str, Any]:
        adapter = self.adapters.get(source_name)
        if not adapter:
            raise ValueError(f"Unknown source: {source_name}")

        run_id = str(uuid.uuid4())
        self.repo.start_run(run_id, source_name)

        try:
            payload = adapter.sync_all()
            summary = self._persist_payload(run_id, payload)
            summary["cache_purged_files"] = self.client.purge_old_cache(days=30)
            self.repo.finish_run(run_id, "success", json.dumps(summary, ensure_ascii=False))
            return {"run_id": run_id, **summary}
        except Exception as exc:  # noqa: BLE001
            logger.exception("sync_source failed")
            self.repo.rollback()
            self.repo.add_issue(
                run_id=run_id,
                source=source_name,
                source_league_id=None,
                severity="error",
                message=str(exc),
                raw_pointer=traceback.format_exc(),
            )
            self.repo.finish_run(run_id, "failed", json.dumps({"error": str(exc)}, ensure_ascii=False))
            raise

    def sync_league(self, source_name: str, league_id: str) -> dict[str, Any]:
        adapter = self.adapters.get(source_name)
        if not adapter:
            raise ValueError(f"Unknown source: {source_name}")

        run_id = str(uuid.uuid4())
        self.repo.start_run(run_id, f"{source_name}:{league_id}")

        try:
            payload = adapter.sync_league(league_id)
            summary = self._persist_payload(run_id, payload)
            summary["cache_purged_files"] = self.client.purge_old_cache(days=30)
            self.repo.finish_run(run_id, "success", json.dumps(summary, ensure_ascii=False))
            return {"run_id": run_id, **summary}
        except Exception as exc:  # noqa: BLE001
            logger.exception("sync_league failed")
            self.repo.rollback()
            self.repo.add_issue(
                run_id=run_id,
                source=source_name,
                source_league_id=league_id,
                severity="error",
                message=str(exc),
                raw_pointer=traceback.format_exc(),
            )
            self.repo.finish_run(run_id, "failed", json.dumps({"error": str(exc)}, ensure_ascii=False))
            raise

    def doctor(self) -> tuple[bool, str]:
        checks: list[tuple[str, bool, str]] = []

        try:
            _ = self.repo.query_rows("SELECT name FROM sqlite_master WHERE type='table' AND name='leagues'")
            checks.append(("database", True, f"Connected: {self.repo.db_path}"))
        except Exception as exc:  # noqa: BLE001
            checks.append(("database", False, str(exc)))

        try:
            for source_name, enabled in self.config.sources.enabled.items():
                checks.append((f"source_enabled.{source_name}", bool(enabled), f"enabled={enabled}"))
        except Exception as exc:  # noqa: BLE001
            checks.append(("config.sources.enabled", False, str(exc)))

        seed_urls = {
            "rseq": "https://www.rseq-stats.ca/",
            "hmhshl": "https://hmhshl.com/",
            "bc_football": "https://www.bchighschoolfootball.com/",
            "bc_basketball": "https://www.bcboysbasketball.com/",
        }
        for source_name, url in seed_urls.items():
            try:
                _ = self.client.request_text(url)
                checks.append((f"network.{source_name}", True, url))
            except Exception as exc:  # noqa: BLE001
                checks.append((f"network.{source_name}", False, f"{url} -> {exc}"))

        try:
            can_fetch = self.client.robots.can_fetch("canadastats/0.1", "https://hmhshl.com/api/league/standings")
            checks.append(("robots.hmhshl_api_disallowed_expected", can_fetch is False, f"can_fetch={can_fetch}"))
        except Exception as exc:  # noqa: BLE001
            checks.append(("robots", False, str(exc)))

        all_ok = all(ok for _, ok, _ in checks)
        report_lines = ["CanadaStats Doctor Report", "=" * 28]
        for name, ok, detail in checks:
            report_lines.append(f"[{ 'OK' if ok else 'FAIL' }] {name}: {detail}")

        report = "\n".join(report_lines)
        report_path = Path("logs") / "latest_report.txt"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report, encoding="utf-8")
        return all_ok, report

    def export_csv(self, sport: str, out_dir: str) -> dict[str, str]:
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        params = (sport,)
        standings = pd.read_sql_query(
            """
            SELECT source, league_name, team_name, rank, gp, w, l, pts
            FROM current_standings
            WHERE sport = ?
            ORDER BY source, league_name, rank
            """,
            self.repo.conn,
            params=params,
        )

        leaders = pd.read_sql_query(
            """
            SELECT source, league_name, player_name, team_name, stat_group, rank,
                   metric_1, metric_2, metric_3, metric_4, metric_5
            FROM current_leaders
            WHERE sport = ?
            ORDER BY source, league_name, stat_group, rank
            """,
            self.repo.conn,
            params=params,
        )

        games = pd.read_sql_query(
            """
            SELECT source, league_name, date_time, away_team, home_team,
                   away_score, home_score, status, venue
            FROM current_games
            WHERE sport = ?
            ORDER BY source, league_name, date_time
            """,
            self.repo.conn,
            params=params,
        )

        files = {
            "standings": str(Path(out_dir) / f"{sport}_standings.csv"),
            "leaders": str(Path(out_dir) / f"{sport}_leaders.csv"),
            "games": str(Path(out_dir) / f"{sport}_games.csv"),
        }
        standings.to_csv(files["standings"], index=False)
        leaders.to_csv(files["leaders"], index=False)
        games.to_csv(files["games"], index=False)
        return files

    def _persist_payload(self, run_id: str, payload: SourceSyncPayload) -> dict[str, Any]:
        league_db_id: dict[str, int] = {}

        for league in payload.leagues:
            league_db_id[league.source_league_id] = self.repo.upsert_league(
                source=payload.source if payload.source != "all" else self._infer_source_from_league_id(league.source_league_id),
                source_league_id=league.source_league_id,
                sport=league.sport,
                province=league.province,
                season_label=league.season_label,
                display_name=league.display_name,
                metadata_json=to_json(league.metadata),
            )

        def ensure_league(source_league_id: str) -> int:
            if source_league_id in league_db_id:
                return league_db_id[source_league_id]

            placeholder_source = payload.source if payload.source != "all" else self._infer_source_from_league_id(source_league_id)
            new_id = self.repo.upsert_league(
                source=placeholder_source,
                source_league_id=source_league_id,
                sport="unknown",
                province="unknown",
                season_label=None,
                display_name=source_league_id,
                metadata_json="{}",
            )
            league_db_id[source_league_id] = new_id
            return new_id

        for team in payload.teams:
            lid = ensure_league(team.source_league_id)
            team_key = team.source_team_id or stable_id(team.source_league_id, team.name)[:16]
            self.repo.upsert_team(lid, team_key, team.name, team.short_code)

        for standing in payload.standings:
            lid = ensure_league(standing.source_league_id)
            team_id = self.repo.find_team_id(lid, standing.source_team_id, standing.team_name)
            if team_id is None:
                team_key = standing.source_team_id or stable_id(standing.source_league_id, standing.team_name)[:16]
                team_id = self.repo.upsert_team(lid, team_key, standing.team_name, None)
            self.repo.insert_standing(
                run_id=run_id,
                league_id=lid,
                team_id=team_id,
                rank=standing.rank,
                gp=standing.gp,
                w=standing.w,
                l=standing.l,
                pts=standing.pts,
                custom_json=to_json(standing.custom),
            )

        for game in payload.games:
            lid = ensure_league(game.source_league_id)
            self.repo.insert_game(
                run_id=run_id,
                league_id=lid,
                source_game_id=game.source_game_id,
                date_time=game.date_time,
                home_team=game.home_team,
                away_team=game.away_team,
                home_score=game.home_score,
                away_score=game.away_score,
                status=game.status,
                venue=game.venue,
                custom_json=to_json(game.custom),
            )

        for stat in payload.player_stats:
            lid = ensure_league(stat.source_league_id)
            team_id = self.repo.find_team_id(lid, None, stat.team_name)
            source = payload.source if payload.source != "all" else self._infer_source_from_league_id(stat.source_league_id)
            player_key = stat.source_player_id or stable_id(stat.source_league_id, stat.team_name or "", stat.player_name)
            player_id = self.repo.upsert_player(
                source=source,
                source_player_key=player_key,
                source_player_id=stat.source_player_id,
                name=stat.player_name,
                team_id=team_id,
            )
            self.repo.insert_player_stat(
                run_id=run_id,
                league_id=lid,
                player_id=player_id,
                player_name=stat.player_name,
                team_name=stat.team_name,
                stat_group=stat.stat_group,
                rank=stat.rank,
                metric_1=stat.metric_1,
                metric_2=stat.metric_2,
                metric_3=stat.metric_3,
                metric_4=stat.metric_4,
                metric_5=stat.metric_5,
                metrics_json=to_json(stat.metrics),
            )

        for issue in payload.issues:
            self.repo.add_issue(
                run_id=run_id,
                source=payload.source,
                source_league_id=issue.source_league_id,
                severity=issue.severity,
                message=issue.message,
                raw_pointer=issue.raw_pointer,
            )

        self.repo.commit()
        return {
            "leagues": len(payload.leagues),
            "teams": len(payload.teams),
            "standings": len(payload.standings),
            "games": len(payload.games),
            "player_stats": len(payload.player_stats),
            "issues": len(payload.issues),
        }

    @staticmethod
    def _infer_source_from_league_id(source_league_id: str) -> str:
        if source_league_id.startswith("hmhshl"):
            return "hmhshl"
        if source_league_id.startswith("ranking-"):
            return "bc_basketball"
        if re.fullmatch(r"[a-f0-9\-]{36}", source_league_id, flags=re.IGNORECASE):
            return "rseq"
        if source_league_id.isdigit():
            return "bc"
        return "unknown"
