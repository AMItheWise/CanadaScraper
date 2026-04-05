import json
from pathlib import Path

from canadastats.load.repository import Repository


def test_repository_current_views(tmp_path: Path) -> None:
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    repo = Repository(db_url)

    run_id = "run-1"
    repo.start_run(run_id, "rseq")

    league_id = repo.upsert_league(
        source="rseq",
        source_league_id="league-1",
        sport="basketball",
        province="QC",
        season_label="2026",
        display_name="Test League",
        metadata_json=json.dumps({"x": 1}),
    )
    team_id = repo.upsert_team(league_id, "team-1", "Test Team", "TT")
    player_id = repo.upsert_player("rseq", "player-key-1", "player-1", "Player One", team_id)

    repo.insert_standing(run_id, league_id, team_id, 1, 10, 8, 2, 16, "{}")
    repo.insert_game(run_id, league_id, "game-1", "2026-01-01", "Home", "Away", 70, 65, "FINAL", "Gym", "{}")
    repo.insert_player_stat(run_id, league_id, player_id, "Player One", "Test Team", "scoring", 1, 30, 0, 0, 0, 0, "{}")
    repo.commit()
    repo.finish_run(run_id, "success", json.dumps({"ok": True}))

    standings = repo.query_rows("SELECT * FROM current_standings")
    games = repo.query_rows("SELECT * FROM current_games")
    leaders = repo.query_rows("SELECT * FROM current_leaders")

    assert len(standings) == 1
    assert len(games) == 1
    assert len(leaders) == 1

    repo.close()
