import requests

from canadastats.extract.robots import RobotsPolicy


class DummyResponse:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text


def test_robots_disallow_api(monkeypatch) -> None:
    def fake_get(url: str, timeout: int):  # noqa: ANN001
        assert url.endswith("/robots.txt")
        return DummyResponse(200, "User-agent: *\nDisallow: /api/\n")

    monkeypatch.setattr(requests, "get", fake_get)

    policy = RobotsPolicy(timeout_seconds=2)
    assert policy.can_fetch("canadastats/0.1", "https://hmhshl.com/standings/") is True
    assert policy.can_fetch("canadastats/0.1", "https://hmhshl.com/api/league/standings") is False


def test_robots_missing_defaults_allow(monkeypatch) -> None:
    def fake_get(url: str, timeout: int):  # noqa: ANN001
        return DummyResponse(404, "")

    monkeypatch.setattr(requests, "get", fake_get)

    policy = RobotsPolicy(timeout_seconds=2)
    assert policy.can_fetch("canadastats/0.1", "https://example.com/anything") is True
