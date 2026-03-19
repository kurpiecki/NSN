from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from models import LookupResult
from utils import normalize_nsn

logger = logging.getLogger(__name__)


class NsnLookupService:
    def __init__(self, db_path: str | Path = "data/nsn.duckdb") -> None:
        self.db_path = Path(db_path)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if not self.db_path.exists():
            raise FileNotFoundError(
                f"Brak bazy indeksu: {self.db_path}. Uruchom najpierw build_local_index()."
            )
        return duckdb.connect(str(self.db_path), read_only=True)

    @staticmethod
    def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
        lowered = {c.lower(): c for c in df.columns}
        for c in candidates:
            if c.lower() in lowered:
                return lowered[c.lower()]
        for col in df.columns:
            lc = col.lower()
            if any(c in lc for c in candidates):
                return col
        return None

    def _with_niin(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if out.empty:
            out["__niin_guess"] = pd.Series(dtype="string")
            return out
        joined = out.astype(str).agg("|".join, axis=1)
        out["__niin_guess"] = joined.str.extract(r"(\d{9})", expand=False)
        return out

    def _get_domain_df(
        self,
        prefix: str,
        include_substring: str | None = None,
        exclude_substring: str | None = None,
    ) -> pd.DataFrame:
        con = self._connect()
        table_rows = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_type = 'BASE TABLE'
              AND table_name LIKE ?
            ORDER BY table_name
            """,
            [f"{prefix}%"],
        ).fetchall()

        frames: list[pd.DataFrame] = []
        for (table_name,) in table_rows:
            if include_substring and include_substring not in table_name:
                continue
            if exclude_substring and exclude_substring in table_name:
                continue
            df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
            if not df.empty:
                df.insert(0, "table_name", table_name)
                frames.append(df)
        con.close()
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True, sort=False)

    def get_identification(self, niin: str) -> dict[str, Any] | None:
        df = self._get_domain_df("identification__")
        df = self._with_niin(df)
        hit = df[df["__niin_guess"] == niin]
        if hit.empty:
            return None
        return hit.iloc[0].dropna().to_dict()

    def get_reference_rows(self, niin: str) -> list[dict[str, Any]]:
        df = self._get_domain_df("reference__")
        df = self._with_niin(df)
        hit = df[df["__niin_guess"] == niin]
        return hit.fillna("").to_dict(orient="records")

    def get_packaging_rows(self, niin: str) -> list[dict[str, Any]]:
        df = self._get_domain_df("freight_packaging__", exclude_substring="freight")
        df = self._with_niin(df)
        hit = df[df["__niin_guess"] == niin]
        return hit.fillna("").to_dict(orient="records")

    def get_freight_rows(self, niin: str) -> list[dict[str, Any]]:
        df = self._get_domain_df("freight_packaging__", include_substring="freight")
        df = self._with_niin(df)
        hit = df[df["__niin_guess"] == niin]
        return hit.fillna("").to_dict(orient="records")

    def get_cage_details(self, cage_codes: set[str]) -> list[dict[str, Any]]:
        if not cage_codes:
            return []
        df = self._get_domain_df("cage__")
        if df.empty:
            return []
        code_col = self._pick_column(df, ["cage", "cage_code", "cagecd"])
        if not code_col:
            return []
        dff = df.copy()
        dff["__cage_clean"] = dff[code_col].astype(str).str.strip().str.upper()
        hit = dff[dff["__cage_clean"].isin({c.upper() for c in cage_codes})]
        return hit.fillna("").to_dict(orient="records")

    def build_user_friendly_result(
        self,
        query: dict[str, Any],
        identification: dict[str, Any] | None,
        reference_rows: list[dict[str, Any]],
        cage_rows: list[dict[str, Any]],
        packaging_rows: list[dict[str, Any]],
        freight_rows: list[dict[str, Any]],
    ) -> LookupResult:
        warnings: list[str] = []

        if identification is None:
            warnings.append("Brak rekordu IDENTIFICATION dla podanego NIIN.")
        if not reference_rows:
            warnings.append("Brak rekordów REFERENCE dla podanego NIIN.")
        if not packaging_rows:
            warnings.append("Brak rekordów PACKAGING/FREIGHT_PACKAGING dla podanego NIIN.")
        if reference_rows and not cage_rows:
            warnings.append("Brak mapowania CAGE -> dane producenta.")
        if packaging_rows:
            warnings.append(
                "Profile opakowania są mapowane do NIIN/NSN. Brak pewnego mapowania PN -> packaging w danych źródłowych."
            )

        ref_df = pd.DataFrame(reference_rows)
        cage_df = pd.DataFrame(cage_rows)

        part_number_col = self._pick_column(ref_df, ["part", "part_number", "pni", "pin"]) if not ref_df.empty else None
        ref_cage_col = self._pick_column(ref_df, ["cage", "cc"]) if not ref_df.empty else None
        cage_code_col = self._pick_column(cage_df, ["cage", "cage_code", "cagecd"]) if not cage_df.empty else None
        mfr_name_col = self._pick_column(cage_df, ["name", "company", "vendor", "activity"]) if not cage_df.empty else None
        country_col = self._pick_column(cage_df, ["country", "cntry"]) if not cage_df.empty else None
        city_col = self._pick_column(cage_df, ["city", "town"]) if not cage_df.empty else None

        part_numbers: list[dict[str, Any]] = []

        cage_map: dict[str, dict[str, Any]] = {}
        if not cage_df.empty and cage_code_col:
            for _, row in cage_df.iterrows():
                code = str(row.get(cage_code_col, "")).strip().upper()
                if not code:
                    continue
                cage_map[code] = row.to_dict()

        if not ref_df.empty:
            for _, row in ref_df.iterrows():
                part_no = str(row.get(part_number_col, "")).strip() if part_number_col else ""
                cage_code = str(row.get(ref_cage_col, "")).strip().upper() if ref_cage_col else ""
                mfr = cage_map.get(cage_code, {})
                item = {
                    "part_number": part_no,
                    "cage_code": cage_code,
                    "manufacturer_name": mfr.get(mfr_name_col, "") if mfr_name_col else "",
                    "country": mfr.get(country_col, "") if country_col else "",
                    "city": mfr.get(city_col, "") if city_col else "",
                    "status_reference": {
                        k: v
                        for k, v in row.to_dict().items()
                        if str(v).strip() and k not in {part_number_col, ref_cage_col}
                    },
                    "notes": "Powiązanie z NIIN/NSN z REFERENCE.",
                }
                part_numbers.append(item)

        mfr_set = {
            (p.get("cage_code"), p.get("manufacturer_name"), p.get("country"), p.get("city"))
            for p in part_numbers
            if p.get("cage_code") or p.get("manufacturer_name")
        }
        manufacturer_rows = [
            {"cage_code": c, "manufacturer_name": n, "country": co, "city": ci}
            for (c, n, co, ci) in mfr_set
        ]

        p_df = pd.DataFrame(packaging_rows)
        units: list[str] = []
        if not p_df.empty:
            possible_unit_cols = [c for c in p_df.columns if "unit" in c.lower() or "uoi" in c.lower()]
            for col in possible_unit_cols:
                vals = p_df[col].dropna().astype(str).str.strip()
                units.extend([v for v in vals if v])

        summary = {
            "unique_part_numbers": len({p["part_number"] for p in part_numbers if p["part_number"]}),
            "unique_manufacturers": len({m["cage_code"] for m in manufacturer_rows if m["cage_code"]}),
            "unique_packaging_profiles": len(packaging_rows),
            "detected_units": sorted(set(units))[:50],
        }

        return LookupResult(
            query=query,
            identification=identification,
            part_numbers=part_numbers,
            manufacturers=manufacturer_rows,
            packaging_profiles=packaging_rows,
            freight=freight_rows,
            warnings=warnings,
            summary=summary,
            raw={
                "identification": identification,
                "reference_rows": reference_rows,
                "packaging_rows": packaging_rows,
                "freight_rows": freight_rows,
                "cage_rows": cage_rows,
            },
        )

    def lookup_nsn(self, nsn_or_niin: str) -> dict[str, Any]:
        query = normalize_nsn(nsn_or_niin)
        niin = query["niin"]

        identification = self.get_identification(niin)
        reference_rows = self.get_reference_rows(niin)
        packaging_rows = self.get_packaging_rows(niin)
        freight_rows = self.get_freight_rows(niin)

        ref_df = pd.DataFrame(reference_rows)
        cage_col = self._pick_column(ref_df, ["cage", "cage_code", "cc"]) if not ref_df.empty else None
        cage_codes = set()
        if cage_col:
            cage_codes = set(ref_df[cage_col].fillna("").astype(str).str.strip().str.upper())
            cage_codes.discard("")

        cage_rows = self.get_cage_details(cage_codes)

        result = self.build_user_friendly_result(
            query=query,
            identification=identification,
            reference_rows=reference_rows,
            cage_rows=cage_rows,
            packaging_rows=packaging_rows,
            freight_rows=freight_rows,
        )
        return result.to_dict()


def result_to_csv_bytes(result: dict[str, Any]) -> bytes:
    frames = []
    for key in ["part_numbers", "packaging_profiles", "freight", "manufacturers"]:
        rows = result.get(key, [])
        if rows:
            df = pd.DataFrame(rows)
            df.insert(0, "section", key)
            frames.append(df)

    if not frames:
        return b""

    merged = pd.concat(frames, ignore_index=True)
    buf = io.StringIO()
    merged.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")
