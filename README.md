# CanadaStats Scraper

Portfolio-grade Python scraping system for Canadian school sports data.

This project demonstrates practical, production-minded scraping:
- multi-source extraction adapters
- robots-aware request policy
- per-domain throttling and retry behavior
- normalized snapshot storage in SQLite
- operational CLI and Streamlit analytics UI

## What This Project Scrapes

Current source adapters:
- `rseq-stats.ca` (basketball + football leagues configured in `config.yaml`)
- `hmhshl.com` (HTML-visible data only, API access intentionally disabled)
- `bchighschoolfootball.com`
- `bcboysbasketball.com`

Data is normalized and written into snapshot tables, with current-state SQL views for analytics and exports.

## Why This Is A Good Scraping Showcase

- Structured ETL layering (`extract -> transform -> load`)
- Defensive ingestion (errors captured into `scrape_issues`, run-level status tracking)
- Config-driven source control and domain-specific throttling
- Repeatable local runtime with CLI entrypoints and tests
- Clear operational docs and Windows scheduler scripts

## Architecture

```text
Source Adapters (canadastats/sources/*.py)
    -> HttpClient + RobotsPolicy + Throttler
    -> Normalized payload models
    -> Repository upserts + snapshot inserts (SQLite)
    -> Current views (standings, games, leaders, source health)
    -> Streamlit dashboard / CSV export
```

Core modules:
- `canadastats/extract/` request client, robots checks, throttling
- `canadastats/sources/` source-specific parsing logic
- `canadastats/load/repository.py` schema, upserts, views
- `canadastats/sync_service.py` orchestration and persistence
- `canadastats/cli.py` command surface
- `canadastats/app/streamlit_app.py` local dashboard

## Tech Stack

- Python 3.11+
- Requests + BeautifulSoup + lxml
- SQLite
- Pandas
- Streamlit
- Pytest

## Quick Start

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .[dev]
Copy-Item config.example.yaml config.yaml
python -m canadastats sync all
streamlit run canadastats/app/streamlit_app.py
```

## CLI Commands

```powershell
# full sync
python -m canadastats sync all

# single source
python -m canadastats sync source --name rseq

# single league in source
python -m canadastats sync league --source bc_football --league-id 6713

# diagnostics
python -m canadastats doctor

# export current snapshots
python -m canadastats export csv --sport basketball --out .\exports
```

## Data Model Highlights

- `scrape_runs`: run-level status and summary
- `scrape_issues`: non-fatal and fatal extraction issues
- `leagues`, `teams`, `players`: normalized entities
- `standing_snapshots`, `game_snapshots`, `player_stat_snapshots`: historical snapshots
- `current_standings`, `current_games`, `current_leaders`, `source_health`: analytics views

## Responsible Scraping Practices

- Targets only public, non-login pages
- Enforces robots policy checks before requests
- Uses domain-level delays and retry limits
- Leaves HMHSHL API disabled (`sources.hmhshl.allow_api: false`)

Review source and platform terms before enabling additional coverage.

## Testing

```powershell
python -m pip install -e .[dev]
pytest
```

Existing tests validate parser behavior, robots policy logic, repository integration, and dashboard smoke flow.

## Project Structure

```text
canadastats/
  app/            # Streamlit UI
  extract/        # HTTP client, robots, throttle
  load/           # SQLite repository + current views
  sources/        # source adapters and parsers
  transform/      # normalization helpers
docs/             # operations + setup notes
scripts/          # Windows scheduler automation
tests/            # pytest suite
```

## Public Repo Checklist

This repository includes:
- `LICENSE` (MIT)
- `CONTRIBUTING.md`
- `SECURITY.md`
- GitHub issue and PR templates
- GitHub Actions CI for tests

## Roadmap

- Add schema migration strategy for long-lived deployments
- Add richer source-level metrics and alerting
- Expand league coverage with per-source feature flags

## Disclaimer

This project is for educational and portfolio demonstration purposes. Respect robots directives, source terms, and legal boundaries in your jurisdiction.
