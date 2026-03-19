from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class NormalizedQuery:
    raw_input: str
    digits: str
    nsn: str | None
    fsc: str | None
    niin: str
    is_full_nsn: bool


@dataclass(slots=True)
class LookupResult:
    query: dict[str, Any]
    identification: dict[str, Any] | None
    part_numbers: list[dict[str, Any]] = field(default_factory=list)
    manufacturers: list[dict[str, Any]] = field(default_factory=list)
    packaging_profiles: list[dict[str, Any]] = field(default_factory=list)
    freight: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "identification": self.identification,
            "part_numbers": self.part_numbers,
            "manufacturers": self.manufacturers,
            "packaging_profiles": self.packaging_profiles,
            "freight": self.freight,
            "warnings": self.warnings,
            "summary": self.summary,
            "raw": self.raw,
        }
