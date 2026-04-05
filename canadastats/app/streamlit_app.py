from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

from canadastats.config import load_config, write_default_config


@st.cache_resource
def get_connection(config_path: str) -> sqlite3.Connection:
    write_default_config(config_path)
    config = load_config(config_path)
    db_path = config.database.url.replace("sqlite:///", "", 1)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


@st.cache_data(ttl=120)
def load_dataframes(config_path: str) -> dict[str, pd.DataFrame]:
    conn = get_connection(config_path)

    tables = {}
    for name, sql in {
        "runs": "SELECT * FROM scrape_runs ORDER BY started_at DESC LIMIT 50",
        "health": "SELECT * FROM source_health",
        "standings": "SELECT * FROM current_standings",
        "games": "SELECT * FROM current_games",
        "leaders": "SELECT * FROM current_leaders",
        "issues": "SELECT * FROM scrape_issues ORDER BY id DESC LIMIT 300",
    }.items():
        try:
            tables[name] = pd.read_sql_query(sql, conn)
        except Exception:
            tables[name] = pd.DataFrame()

    return tables


def apply_common_filters(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df.copy()
    st.sidebar.markdown("### Filters")

    for column, label in [
        ("sport", "Sport"),
        ("province", "Province"),
        ("source", "Source"),
        ("league_name", "League"),
        ("team_name", "Team"),
    ]:
        if column in filtered.columns:
            values = sorted(v for v in filtered[column].dropna().astype(str).unique() if v)
            selected = st.sidebar.multiselect(f"{label}", values, key=f"{prefix}_{column}")
            if selected:
                filtered = filtered[filtered[column].astype(str).isin(selected)]

    player_search = st.sidebar.text_input("Player search", key=f"{prefix}_player")
    if player_search and "player_name" in filtered.columns:
        filtered = filtered[
            filtered["player_name"].fillna("").str.contains(player_search, case=False, regex=False)
        ]

    return filtered


def render_status_badge(label: str, status: str) -> None:
    colors = {
        "success": "#2E8B57",
        "running": "#C98C00",
        "failed": "#B22222",
    }
    color = colors.get(status.lower(), "#555")
    st.markdown(
        f"<div style='display:inline-block;padding:4px 10px;border-radius:10px;background:{color};color:white;'>"
        f"{label}: {status.upper()}</div>",
        unsafe_allow_html=True,
    )


def render_overview(data: dict[str, pd.DataFrame]) -> None:
    st.header("Overview")

    runs = data["runs"]
    if runs.empty:
        st.warning("No runs yet. Run `python -m canadastats sync all` first.")
        return

    latest = runs.iloc[0]
    st.write(f"Last run at: **{latest.get('started_at', 'n/a')}**")
    render_status_badge("Latest run", str(latest.get("status", "unknown")))

    cols = st.columns(4)
    cols[0].metric("Standings rows", len(data["standings"]))
    cols[1].metric("Games rows", len(data["games"]))
    cols[2].metric("Leaders rows", len(data["leaders"]))
    cols[3].metric("Recent issues", len(data["issues"]))

    st.subheader("Source Health")
    if data["health"].empty:
        st.info("No source health records yet.")
    else:
        st.dataframe(data["health"], use_container_width=True)

    st.subheader("Recent Issues")
    st.dataframe(data["issues"].head(50), use_container_width=True)


def render_leaderboards(data: dict[str, pd.DataFrame]) -> None:
    st.header("Leaderboards")
    df = apply_common_filters(data["leaders"], "leaders")

    if df.empty:
        st.info("No leaderboard data for current filters.")
        return

    if "metric_1" in df.columns:
        df = df.sort_values(by=["sport", "metric_1"], ascending=[True, False], na_position="last")

    show_cols = [
        c
        for c in [
            "sport",
            "source",
            "league_name",
            "player_name",
            "team_name",
            "stat_group",
            "rank",
            "metric_1",
            "metric_2",
            "metric_3",
        ]
        if c in df.columns
    ]

    st.dataframe(df[show_cols], use_container_width=True)
    csv = df[show_cols].to_csv(index=False).encode("utf-8")
    st.download_button("Download Leaderboards CSV", data=csv, file_name="leaderboards.csv", mime="text/csv")


def render_standings(data: dict[str, pd.DataFrame]) -> None:
    st.header("Standings")
    df = apply_common_filters(data["standings"], "standings")
    if df.empty:
        st.info("No standings data for current filters.")
        return

    order_cols = [c for c in ["sport", "league_name", "rank"] if c in df.columns]
    if order_cols:
        df = df.sort_values(by=order_cols)

    show_cols = [
        c
        for c in ["sport", "province", "source", "league_name", "team_name", "rank", "gp", "w", "l", "pts"]
        if c in df.columns
    ]
    st.dataframe(df[show_cols], use_container_width=True)

    csv = df[show_cols].to_csv(index=False).encode("utf-8")
    st.download_button("Download Standings CSV", data=csv, file_name="standings.csv", mime="text/csv")


def render_games(data: dict[str, pd.DataFrame]) -> None:
    st.header("Games")
    df = apply_common_filters(data["games"], "games")

    if df.empty:
        st.info("No games data for current filters.")
        return

    order_cols = [c for c in ["sport", "league_name", "date_time"] if c in df.columns]
    if order_cols:
        df = df.sort_values(by=order_cols, ascending=True)

    show_cols = [
        c
        for c in [
            "sport",
            "source",
            "league_name",
            "date_time",
            "away_team",
            "away_score",
            "home_team",
            "home_score",
            "status",
            "venue",
        ]
        if c in df.columns
    ]
    st.dataframe(df[show_cols], use_container_width=True)

    csv = df[show_cols].to_csv(index=False).encode("utf-8")
    st.download_button("Download Games CSV", data=csv, file_name="games.csv", mime="text/csv")


def render_source_detail(data: dict[str, pd.DataFrame]) -> None:
    st.header("Source Detail")
    st.markdown("This page highlights freshness and known source caveats.")

    if not data["health"].empty:
        st.subheader("Freshness")
        st.dataframe(data["health"], use_container_width=True)

    st.subheader("Known Caveats")
    caveats = pd.DataFrame(
        [
            {
                "source": "rseq",
                "status": "Partial by sport",
                "notes": "Basketball and football ingested from LeagueDiffusion API; hockey excluded in Phase 1.",
            },
            {
                "source": "hmhshl",
                "status": "Partial",
                "notes": "API endpoints intentionally excluded due to robots policy; static HTML only.",
            },
            {
                "source": "bc_football",
                "status": "Available",
                "notes": "Schedules may require non-tabular fallback parsing.",
            },
            {
                "source": "bc_basketball",
                "status": "Available",
                "notes": "Current-season filtering inferred from page year markers.",
            },
        ]
    )
    st.dataframe(caveats, use_container_width=True)

    st.subheader("Recent Source Issues")
    st.dataframe(data["issues"].head(100), use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="Canada HS Sports Dashboard", layout="wide")
    st.title("Canada HS Sports Aggregator")

    config_path = st.sidebar.text_input("Config path", value="config.yaml")
    data = load_dataframes(config_path)

    last_updated = None
    if not data["runs"].empty:
        last_updated = data["runs"].iloc[0].get("started_at")
    st.caption(f"Last updated: {last_updated or 'No successful run yet'}")

    page = st.sidebar.radio(
        "Page",
        ["Overview", "Leaderboards", "Standings", "Games", "Source Detail"],
        index=0,
    )

    if page == "Overview":
        render_overview(data)
    elif page == "Leaderboards":
        render_leaderboards(data)
    elif page == "Standings":
        render_standings(data)
    elif page == "Games":
        render_games(data)
    else:
        render_source_detail(data)


if __name__ == "__main__":
    main()
