from __future__ import annotations

import json
import re
from typing import Any

from canadastats.utils import parse_float


def normalize_sport(value: str | None) -> str:
    if not value:
        return "unknown"
    text = value.lower()
    if "basket" in text:
        return "basketball"
    if "hockey" in text:
        return "hockey"
    if "football" in text:
        return "football"
    return "unknown"


def clean_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def to_json(data: dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def pick_primary_metrics(metrics: dict[str, Any], sport: str) -> tuple[float | None, float | None, float | None, float | None, float | None]:
    normalized = {k: parse_float(v) for k, v in metrics.items() if parse_float(v) is not None}

    priority: list[str]
    if sport == "basketball":
        priority = [
            "TotalPoints",
            "PTS",
            "Points",
            "PointsPerGame",
            "PPG",
            "Rebounds",
            "TotalRebounds",
            "Assists",
            "A",
            "G",
        ]
    elif sport == "hockey":
        priority = [
            "PTS",
            "Pts",
            "Points",
            "G",
            "Goals",
            "A",
            "Assists",
            "PIM",
            "SV%",
            "GAA",
        ]
    elif sport == "football":
        priority = [
            "Points",
            "PTS",
            "Touchdowns",
            "TD",
            "QbPassYards",
            "WrYards",
            "RbYards",
            "YDS",
            "CMP",
            "ATT",
        ]
    else:
        priority = list(normalized.keys())

    selected: list[float | None] = []
    used: set[str] = set()

    for key in priority:
        if key in normalized and key not in used:
            selected.append(normalized[key])
            used.add(key)
        if len(selected) >= 5:
            break

    if len(selected) < 5:
        for key, value in normalized.items():
            if key in used:
                continue
            selected.append(value)
            if len(selected) >= 5:
                break

    while len(selected) < 5:
        selected.append(None)

    return tuple(selected[:5])  # type: ignore[return-value]
