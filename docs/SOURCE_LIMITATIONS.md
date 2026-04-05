# Source Limitations and Policy Notes

## General Policy

- Public pages only
- No login-protected content
- Respect robots.txt
- Low request rate with delays and retries

## RSEQ (`rseq-stats.ca`)

- Included: basketball + football leagues from configured URLs
- Excluded: hockey pages in Phase 1 (no direct feed detected from those league pages)
- Data path: embedded `LeagueId` -> `s1.rseq.ca/api/LeagueApi/GetLeagueDiffusion`

## HMHSHL (`hmhshl.com`)

- Robots currently disallow `/api/`
- Phase 1 intentionally avoids all `/api/` usage
- Included: static HTML-visible standings + team-level player snippets + any visible game snippets
- Result: partial coverage by design

## BC Football (`bchighschoolfootball.com`)

- Uses JS redirect chain (`clear.cfm` -> `frameset.cfm` -> landing page)
- Schedules may appear in non-tabular card-like content
- Fallback parser captures visible snippets when structured rows are absent

## BC Boys Basketball (`bcboysbasketball.com`)

- Active leagues discovered from slider/pick pages + clear links
- Current-season filtering inferred from page year markers
- Rankings ingested from linked ranking custom pages where available

## Known Data Caveats

- Cross-sport comparisons are not semantically equivalent; dashboard keeps leaderboards per sport.
- Schedule completeness differs by source and page structure.
- Site layout changes may require parser updates.
