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
        # read_only powodował konflikt konfiguracji z połączeniami zapisującymi w Streamlit.
        return duckdb.connect(str(self.db_path))

    @staticmethod
    def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
        if df.empty:
            return None
        normalized = {c.upper(): c for c in df.columns}
        for c in candidates:
            if c.upper() in normalized:
                return normalized[c.upper()]
        for col in df.columns:
            u = col.upper()
            if any(c.upper() in u for c in candidates):
                return col
        return None

    @staticmethod
    def _clean_codes(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip().str.upper()

    def _list_tables(self) -> list[str]:
        con = self._connect()
        rows = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='main' AND table_type='BASE TABLE'
            ORDER BY table_name
            """
        ).fetchall()
        con.close()
        return [r[0] for r in rows]

    def _table_df(self, table_name: str) -> pd.DataFrame:
        con = self._connect()
        df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
        con.close()
        if not df.empty:
            df.insert(0, "table_name", table_name)
        return df

    def _find_table(self, patterns: list[str]) -> str | None:
        tables = self._list_tables()
        for p in patterns:
            p_up = p.upper()
            for t in tables:
                if p_up in t.upper():
                    return t
        return None

    def _filter_by_niin(self, df: pd.DataFrame, niin: str) -> pd.DataFrame:
        if df.empty:
            return df
        niin_col = self._pick_column(df, ["NIIN"])
        if niin_col:
            return df[self._clean_codes(df[niin_col]) == niin]
        # fallback defensywny
        joined = df.astype(str).agg("|".join, axis=1)
        mask = joined.str.contains(rf"\b{niin}\b", regex=True, na=False)
        return df[mask]

    def get_identification(self, niin: str, fsc: str | None = None) -> dict[str, Any] | None:
        p_nsn_tbl = self._find_table(["IDENTIFICATION__P_FLIS_NSN"])
        v_ident_tbl = self._find_table(["IDENTIFICATION__V_FLIS_IDENTIFICATION"])

        base = pd.DataFrame()
        ext = pd.DataFrame()

        if p_nsn_tbl:
            base_df = self._table_df(p_nsn_tbl)
            base_df = self._filter_by_niin(base_df, niin)
            if fsc:
                fsc_col = self._pick_column(base_df, ["FSC"])
                if fsc_col:
                    base_df = base_df[self._clean_codes(base_df[fsc_col]) == fsc]
            base = base_df

        if v_ident_tbl:
            ext_df = self._table_df(v_ident_tbl)
            ext = self._filter_by_niin(ext_df, niin)

        if base.empty and ext.empty:
            return None

        result: dict[str, Any] = {}
        if not base.empty:
            result.update(base.iloc[0].dropna().to_dict())
        if not ext.empty:
            for k, v in ext.iloc[0].dropna().to_dict().items():
                if k not in result or not str(result.get(k, "")).strip():
                    result[k] = v
        return result

    def get_reference_rows(self, niin: str) -> list[dict[str, Any]]:
        tbl = self._find_table(["REFERENCE__V_FLIS_PART"])
        if not tbl:
            return []
        df = self._filter_by_niin(self._table_df(tbl), niin)
        return df.fillna("").to_dict(orient="records")

    def get_cage_details(self, cage_codes: set[str]) -> list[dict[str, Any]]:
        if not cage_codes:
            return []

        tables: list[str] = []
        for p in ["CAGE__P_CAGE", "CAGE__V_CAGE_ADDRESS", "CAGE__V_CAGE_STATUS_AND_TYPE"]:
            t = self._find_table([p])
            if t:
                tables.append(t)

        if not tables:
            return []

        frames = []
        for t in tables:
            df = self._table_df(t)
            code_col = self._pick_column(df, ["CAGE_CODE", "CAGE"])
            if not code_col:
                continue
            hit = df[self._clean_codes(df[code_col]).isin({c.upper() for c in cage_codes})]
            if not hit.empty:
                frames.append(hit)

        if not frames:
            return []

        merged = pd.concat(frames, ignore_index=True, sort=False).fillna("")
        code_col = self._pick_column(merged, ["CAGE_CODE", "CAGE"])
        if not code_col:
            return merged.to_dict(orient="records")

        output: list[dict[str, Any]] = []
        for code, group in merged.groupby(self._clean_codes(merged[code_col]), dropna=False):
            row: dict[str, Any] = {"CAGE_CODE": code}
            for _, r in group.iterrows():
                for k, v in r.to_dict().items():
                    if str(v).strip() and (k not in row or not str(row[k]).strip()):
                        row[k] = v
            output.append(row)
        return output

    def get_packaging_rows(self, niin: str) -> list[dict[str, Any]]:
        p1_tbl = self._find_table(["FREIGHT_PACKAGING__V_FLIS_PACKAGING_1"])
        p2_tbl = self._find_table(["FREIGHT_PACKAGING__V_FLIS_PACKAGING_2"])
        p3_tbl = self._find_table(["FREIGHT_PACKAGING__V_FLIS_PACKAGING_3"])

        p1 = self._filter_by_niin(self._table_df(p1_tbl), niin) if p1_tbl else pd.DataFrame()
        p2 = self._filter_by_niin(self._table_df(p2_tbl), niin) if p2_tbl else pd.DataFrame()
        p3 = self._filter_by_niin(self._table_df(p3_tbl), niin) if p3_tbl else pd.DataFrame()

        if p1.empty and p2.empty and p3.empty:
            return []

        # Łączenie po NIIN + PICA_SICA, gdy dostępne.
        def prep(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
            if df.empty:
                return df
            out = df.copy()
            niin_col = self._pick_column(out, ["NIIN"])
            pica_col = self._pick_column(out, ["PICA_SICA"])
            if niin_col:
                out["__niin"] = self._clean_codes(out[niin_col])
            if pica_col:
                out["__pica_sica"] = self._clean_codes(out[pica_col])
            rename_map = {c: f"{c}{suffix}" for c in out.columns if c not in {"__niin", "__pica_sica"}}
            return out.rename(columns=rename_map)

        a = prep(p1, "_P1")
        b = prep(p2, "_P2")
        c = prep(p3, "_P3")

        joined = a
        if not b.empty:
            keys = [k for k in ["__niin", "__pica_sica"] if k in joined.columns and k in b.columns]
            joined = joined.merge(b, on=keys, how="outer") if keys else pd.concat([joined, b], ignore_index=True, sort=False)
        if not c.empty:
            keys = [k for k in ["__niin", "__pica_sica"] if k in joined.columns and k in c.columns]
            joined = joined.merge(c, on=keys, how="outer") if keys else pd.concat([joined, c], ignore_index=True, sort=False)

        joined = joined.fillna("")
        return joined.to_dict(orient="records")

    def get_freight_rows(self, niin: str) -> list[dict[str, Any]]:
        freight_tbl = self._find_table(["FREIGHT_PACKAGING__V_FREIGHT"])
        dss_tbl = self._find_table(["FREIGHT_PACKAGING__V_DSS_WEIGHT_AND_CUBE"])

        f = self._filter_by_niin(self._table_df(freight_tbl), niin) if freight_tbl else pd.DataFrame()
        d = self._filter_by_niin(self._table_df(dss_tbl), niin) if dss_tbl else pd.DataFrame()

        if f.empty and d.empty:
            return []
        if f.empty:
            return d.fillna("").to_dict(orient="records")
        if d.empty:
            return f.fillna("").to_dict(orient="records")

        f_niin = self._pick_column(f, ["NIIN"])
        d_niin = self._pick_column(d, ["NIIN"])
        if f_niin and d_niin:
            f2 = f.copy()
            d2 = d.copy()
            f2["__niin"] = self._clean_codes(f2[f_niin])
            d2["__niin"] = self._clean_codes(d2[d_niin])
            m = f2.merge(d2, on="__niin", how="outer", suffixes=("_FREIGHT", "_DSS"))
            return m.fillna("").to_dict(orient="records")

        return pd.concat([f, d], ignore_index=True, sort=False).fillna("").to_dict(orient="records")

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
            warnings.append("Brak rekordu IDENTIFICATION dla podanego NIIN/NSN.")
        if not reference_rows:
            warnings.append("Brak rekordów REFERENCE (V_FLIS_PART) dla NIIN.")
        if not packaging_rows:
            warnings.append("Brak rekordów PACKAGING (V_FLIS_PACKAGING_1/2/3) dla NIIN.")
        if not freight_rows:
            warnings.append("Brak rekordów FREIGHT/DSS dla NIIN.")
        if reference_rows and not cage_rows:
            warnings.append("Brak mapowania CAGE_CODE -> dane producenta w katalogu CAGE.")
        if packaging_rows:
            warnings.append(
                "Packaging jest prezentowany jako profile dla NIIN/NSN (i PICA_SICA), bez automatycznego mapowania do konkretnego PART_NUMBER."
            )

        ref_df = pd.DataFrame(reference_rows)
        cage_df = pd.DataFrame(cage_rows)

        part_number_col = self._pick_column(ref_df, ["PART_NUMBER"])
        ref_cage_col = self._pick_column(ref_df, ["CAGE_CODE", "CAGE"])
        cage_code_col = self._pick_column(cage_df, ["CAGE_CODE", "CAGE"])
        mfr_name_col = self._pick_column(cage_df, ["COMPANY_NAME", "COMPANY", "NAME"])
        country_col = self._pick_column(cage_df, ["COUNTRY"])
        city_col = self._pick_column(cage_df, ["CITY"])

        part_numbers: list[dict[str, Any]] = []
        cage_map: dict[str, dict[str, Any]] = {}

        if not cage_df.empty and cage_code_col:
            for _, row in cage_df.iterrows():
                code = str(row.get(cage_code_col, "")).strip().upper()
                if code:
                    cage_map[code] = row.to_dict()

        if not ref_df.empty:
            for _, row in ref_df.iterrows():
                part_no = str(row.get(part_number_col, "")).strip() if part_number_col else ""
                cage_code = str(row.get(ref_cage_col, "")).strip().upper() if ref_cage_col else ""
                mfr = cage_map.get(cage_code, {})
                part_numbers.append(
                    {
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
                        "notes": "Kwalifikująca referencja z V_FLIS_PART (po NIIN).",
                    }
                )

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
            possible_unit_cols = [c for c in p_df.columns if "UI" == c.upper() or "UNIT" in c.upper()]
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
                "tables_detected": self._list_tables(),
            },
        )

    def lookup_nsn(self, nsn_or_niin: str) -> dict[str, Any]:
        query = normalize_nsn(nsn_or_niin)
        niin = query["niin"]
        fsc = query.get("fsc")

        identification = self.get_identification(niin, fsc=fsc)
        reference_rows = self.get_reference_rows(niin)
        packaging_rows = self.get_packaging_rows(niin)
        freight_rows = self.get_freight_rows(niin)

        ref_df = pd.DataFrame(reference_rows)
        cage_col = self._pick_column(ref_df, ["CAGE_CODE", "CAGE"])
        cage_codes = set()
        if cage_col:
            cage_codes = set(self._clean_codes(ref_df[cage_col]))
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
