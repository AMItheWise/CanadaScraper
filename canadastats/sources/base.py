from __future__ import annotations

import abc
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from canadastats.models import SourceSyncPayload
from canadastats.transform.normalization import clean_text
from canadastats.utils import parse_int


@dataclass(slots=True)
class ParsedTable:
    title: str
    headers: list[str]
    rows: list[dict[str, str]]


class SourceAdapter(abc.ABC):
    name: str

    @abc.abstractmethod
    def sync_all(self) -> SourceSyncPayload:
        raise NotImplementedError

    @abc.abstractmethod
    def sync_league(self, league_id: str) -> SourceSyncPayload:
        raise NotImplementedError


def absolutize(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


def sanitize_headers(headers: list[str]) -> list[str]:
    cleaned = [clean_text(h).strip("#") for h in headers]
    return [h if h else f"col_{idx + 1}" for idx, h in enumerate(cleaned)]


def nearest_heading(table: Tag) -> str:
    cursor = table
    for _ in range(8):
        cursor = cursor.find_previous()
        if cursor is None:
            break
        if isinstance(cursor, Tag) and cursor.name in {"h1", "h2", "h3", "h4", "strong", "caption"}:
            text = clean_text(cursor.get_text(" ", strip=True))
            if text:
                return text
    return ""


def parse_html_tables(soup: BeautifulSoup) -> list[ParsedTable]:
    parsed: list[ParsedTable] = []
    for table in soup.find_all("table"):
        headers = [clean_text(th.get_text(" ", strip=True)) for th in table.find_all("th")]
        headers = sanitize_headers(headers)
        rows: list[dict[str, str]] = []
        for tr in table.find_all("tr"):
            # Skip pure header rows (thead) that use only <th> cells.
            if tr.find_all("th") and not tr.find_all("td"):
                continue
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue
            values = [clean_text(td.get_text(" ", strip=True)) for td in cells]
            if not any(values):
                continue
            if headers and values == headers[: len(values)]:
                continue
            row: dict[str, str] = {}
            if headers:
                for idx, value in enumerate(values):
                    key = headers[idx] if idx < len(headers) else f"col_{idx + 1}"
                    row[key] = value
            else:
                for idx, value in enumerate(values):
                    row[f"col_{idx + 1}"] = value
            rows.append(row)

        if rows:
            parsed.append(
                ParsedTable(
                    title=nearest_heading(table),
                    headers=headers,
                    rows=rows,
                )
            )
    return parsed


def extract_js_redirect_target(html: str) -> str | None:
    patterns = [
        r"this\.location\s*=\s*\"([^\"]+)\"",
        r"top\.location\s*=\s*\"([^\"]+)\"",
        r"window\.location\s*=\s*\"([^\"]+)\"",
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def first_int_from_any(row: dict[str, str], keys: list[str]) -> int | None:
    for key in keys:
        if key in row:
            value = parse_int(row[key])
            if value is not None:
                return value
    return None


def row_to_metrics(row: dict[str, str], skip_keys: set[str]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for key, value in row.items():
        if key in skip_keys:
            continue
        if value == "":
            continue
        metrics[key] = value
    return metrics
