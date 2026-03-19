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
class LookupStatus:
    found_in_identification: bool
    reference_rows_found: int
    reference_rows_after_cage_join: int
    packaging_rows_found: int
    freight_rows_found: int
    cage_rows_found: int
    ui_rows_shown: int
    exported_part_rows: int
    exported_packaging_rows: int
    exported_freight_rows: int


@dataclass(slots=True)
class LookupResult:
    query_id: str
    query: dict[str, Any]
    status: LookupStatus
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
            "query_id": self.query_id,
            "query": self.query,
            "status": {
                "found_in_identification": self.status.found_in_identification,
                "reference_rows_found": self.status.reference_rows_found,
                "reference_rows_after_cage_join": self.status.reference_rows_after_cage_join,
                "packaging_rows_found": self.status.packaging_rows_found,
                "freight_rows_found": self.status.freight_rows_found,
                "cage_rows_found": self.status.cage_rows_found,
                "ui_rows_shown": self.status.ui_rows_shown,
                "exported_part_rows": self.status.exported_part_rows,
                "exported_packaging_rows": self.status.exported_packaging_rows,
                "exported_freight_rows": self.status.exported_freight_rows,
            },
            "identification": self.identification,
            "part_numbers": self.part_numbers,
            "manufacturers": self.manufacturers,
            "packaging_profiles": self.packaging_profiles,
            "freight": self.freight,
            "warnings": self.warnings,
            "summary": self.summary,
            "raw": self.raw,
        }
