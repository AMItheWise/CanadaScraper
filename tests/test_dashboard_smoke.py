from pathlib import Path

from canadastats.app.streamlit_app import load_dataframes


def test_dashboard_smoke(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "database:\n  url: sqlite:///{0}\n".format((tmp_path / "dashboard.db").as_posix()),
        encoding="utf-8",
    )
    data = load_dataframes(str(config_path))
    assert "standings" in data
    assert "games" in data
    assert "leaders" in data
