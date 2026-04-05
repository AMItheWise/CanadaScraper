from canadastats.sources.rseq import extract_league_id_from_html


def test_extract_league_id_from_html() -> None:
    html = '<iframe src="https://diffusion.rseq.ca/?Type=League&LeagueId=969a2f20-08c5-4be6-ba54-13b60f2a8300"></iframe>'
    assert extract_league_id_from_html(html) == "969a2f20-08c5-4be6-ba54-13b60f2a8300"


def test_extract_league_id_missing() -> None:
    assert extract_league_id_from_html("<html><body>No league</body></html>") is None
