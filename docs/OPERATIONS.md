# Operations Guide

## Daily Workflow

1. Activate environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

2. Sync data:

```powershell
python -m canadastats sync all
```

3. Launch dashboard:

```powershell
streamlit run canadastats/app/streamlit_app.py
```

## Commands

- Full sync: `python -m canadastats sync all`
- Source sync: `python -m canadastats sync source --name <source>`
- League sync: `python -m canadastats sync league --source <source> --league-id <id>`
- Health check: `python -m canadastats doctor`
- Export CSV: `python -m canadastats export csv --sport basketball --out .\exports`

## Database & Backups

- SQLite DB path: `data/canada_stats.db`
- Backup command:

```powershell
Copy-Item data\canada_stats.db data\canada_stats_backup_$(Get-Date -Format yyyyMMdd_HHmmss).db
```

## Logs

- Main log: `logs/canadastats.log`
- Latest doctor report: `logs/latest_report.txt`

## Cache Retention

- Raw HTTP cache is in `data/cache/`.
- Sync operations purge cached files older than 30 days.

## Restore / Reset

- Stop dashboard and sync jobs.
- Restore backup DB file to `data/canada_stats.db`.
- Run doctor and dashboard again.

## Failure Handling

1. Run `python -m canadastats doctor`
2. Review latest issues in dashboard "Source Detail" page
3. Review logs
4. Retry source-specific sync:

```powershell
python -m canadastats sync source --name rseq
```
