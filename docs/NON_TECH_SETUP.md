# Non-Technical Setup Guide

## Goal

Get the stats tool running on your Windows PC with minimal technical steps.

## 1) Install Python

- Install Python 3.11 or newer from python.org.
- During install, enable "Add Python to PATH".

## 2) Open Project Folder

- Open PowerShell in the project folder (where `README.md` is located).

## 3) Create Virtual Environment

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

## 4) Install App

```powershell
python -m pip install --upgrade pip
python -m pip install -e .
```

## 5) Get Data (Manual Update)

```powershell
python -m canadastats sync all
```

Wait for the command to finish. It writes data to `data/canada_stats.db`.

## 6) Open Dashboard

```powershell
streamlit run canadastats/app/streamlit_app.py
```

A browser tab opens automatically. If not, copy the local URL shown in PowerShell.

## 7) Update Data Later

Run this anytime:

```powershell
python -m canadastats sync all
```

Then refresh the dashboard page.

## 8) Optional: Automatic Daily Updates

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup_windows_scheduler.ps1
```

To remove automation:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/remove_windows_scheduler.ps1
```

## 9) Basic Troubleshooting

- Run health check:

```powershell
python -m canadastats doctor
```

- Check logs:

- `logs/canadastats.log`
- `logs/latest_report.txt`

- If needed, re-run install:

```powershell
python -m pip install -e .
```
