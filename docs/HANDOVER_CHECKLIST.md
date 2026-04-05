# Handover Checklist (1-2 Hours)

## Session Goals

- Run a manual update
- Open and use dashboard filters
- Export CSV
- Understand where data/logs/config live
- Optional scheduler setup

## Agenda

1. Environment check (10 min)
- Activate `.venv`
- Verify `python -m canadastats doctor`

2. Manual sync walk-through (15 min)
- Run `python -m canadastats sync all`
- Explain run summary JSON

3. Dashboard usage (20 min)
- Overview page
- Leaderboards filters
- Standings comparisons
- Games table
- Source Detail caveats

4. Exports and files (10 min)
- Run `python -m canadastats export csv --sport basketball --out .\exports`
- Show generated CSV files

5. Troubleshooting basics (10 min)
- Read `logs/canadastats.log`
- Run `doctor`
- Retry source-specific sync

6. Optional automation (10 min)
- Setup Task Scheduler script
- Remove/update task scripts

## User Takeaway Commands

```powershell
python -m canadastats sync all
streamlit run canadastats/app/streamlit_app.py
python -m canadastats doctor
python -m canadastats export csv --sport basketball --out .\exports
```

## Follow-up Items

- Confirm preferred daily scheduler time
- Decide whether to enable cloud deployment in Phase 2
- Define next extra feature priorities (alerts, PDF export, richer mobile UX)
