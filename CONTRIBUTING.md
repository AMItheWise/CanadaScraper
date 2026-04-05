# Contributing

Thanks for your interest in improving CanadaStats.

## Development Setup

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .[dev]
Copy-Item config.example.yaml config.yaml
```

## Local Validation

Run before opening a PR:

```powershell
pytest
python -m canadastats doctor
```

If your changes touch source adapters, also run a source-limited sync and confirm no regressions in logs.

## Pull Request Guidelines

- Keep PRs focused and small.
- Include test coverage for parser/repository behavior when possible.
- Document source-specific assumptions and fallbacks.
- Do not commit runtime artifacts (`data/cache`, DB files, logs, exports).

## Source Adapter Rules

- Respect robots and public page boundaries.
- Avoid scraping private or authenticated endpoints.
- Prefer resilient selectors over brittle index-based parsing.
- Capture parsing anomalies in `scrape_issues` instead of failing silently.
