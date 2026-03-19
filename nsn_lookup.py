from __future__ import annotations

import io
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from models import LookupResult, LookupStatus
from utils import normalize_nsn

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class QueryContext:
    query_id: str
    raw_input: str
    normalized_nsn: str | None
    fsc: str | None
    niin: str


REFERENCE_TECH_COLUMNS = ["RNCC", "RNVC", "DAC", "RNAAC", "RNFC", "RNSC", "RNJC", "CAGE_STATUS"]


class NsnLookupService:
    def __init__(self, db_path: str | Path = "data/nsn.duckdb") -> None:
        self.db_path = Path(db_path)
        self.log_dir = Path("logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.trace_logger = self._build_file_logger("nsn_trace", self.log_dir / "search_trace.log")
        self.error_logger = self._build_file_logger("nsn_error", self.log_dir / "errors.log")
        self._con = self._connect()
        self._tables = self._load_tables()
        self._table_columns: dict[str, list[str]] = {}

    @staticmethod
    def _build_file_logger(name: str, path: Path) -> logging.Logger:
        log = logging.getLogger(name)
        if not any(isinstance(h, logging.FileHandler) and Path(getattr(h, "baseFilename", "")) == path.resolve() for h in log.handlers):
            handler = logging.FileHandler(path, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            log.addHandler(handler)
            log.setLevel(logging.INFO)
            log.propagate = False
        return log

    def _trace(self, ctx: QueryContext | None, message: str) -> None:
        prefix = f"[query_id={ctx.query_id}] " if ctx else ""
        self.trace_logger.info(f"{prefix}{message}")

    def _error(self, ctx: QueryContext | None, message: str) -> None:
        prefix = f"[query_id={ctx.query_id}] " if ctx else ""
        self.error_logger.error(f"{prefix}{message}")

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if not self.db_path.exists():
            raise FileNotFoundError(f"Brak bazy indeksu: {self.db_path}. Uruchom najpierw build_local_index().")
        return duckdb.connect(str(self.db_path), read_only=True)

    def _load_tables(self) -> list[str]:
        rows = self._con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='main' AND table_type='BASE TABLE'
            ORDER BY table_name
            """
        ).fetchall()
        tables = [r[0] for r in rows]
        self._trace(None, f"Detected tables ({len(tables)}): {tables}")
        return tables

    def _columns(self, table_name: str) -> list[str]:
        if table_name not in self._table_columns:
            rows = self._con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            self._table_columns[table_name] = [str(r[1]) for r in rows]
        return self._table_columns[table_name]

    @staticmethod
    def _pick_column_from_names(columns: list[str], candidates: list[str]) -> str | None:
        normalized = {c.upper(): c for c in columns}
        for c in candidates:
            if c.upper() in normalized:
                return normalized[c.upper()]
        for col in columns:
            u = col.upper()
            if any(c.upper() in u for c in candidates):
                return col
        return None

    def _find_table(self, patterns: list[str], *, ctx: QueryContext) -> str | None:
        for p in patterns:
            p_up = p.upper()
            for t in self._tables:
                if p_up in t.upper():
                    self._trace(ctx, f"table selected pattern='{p}' table={t}")
                    return t
        self._trace(ctx, f"table not found patterns={patterns}")
        return None

    def _query_rows_by_niin(self, *, table_name: str, niin: str, ctx: QueryContext, fsc: str | None = None) -> list[dict[str, Any]]:
        cols = self._columns(table_name)
        niin_col = self._pick_column_from_names(cols, ["NIIN"])
        if not niin_col:
            self._trace(ctx, f"skip table={table_name}: NIIN column missing")
            return []

        sql = f"SELECT * FROM \"{table_name}\" WHERE UPPER(TRIM(COALESCE(\"{niin_col}\", ''))) = ?"
        params: list[str] = [niin]

        fsc_col = self._pick_column_from_names(cols, ["FSC"])
        if fsc and fsc_col:
            sql += f" AND UPPER(TRIM(COALESCE(\"{fsc_col}\", ''))) = ?"
            params.append(fsc)

        self._trace(ctx, f"SQL/filter executed table={table_name} niin={niin} fsc={fsc if fsc else '-'}")
        df = self._con.execute(sql, params).fetchdf()
        self._trace(ctx, f"table={table_name} matched rows={len(df)}")
        if df.empty:
            return []
        df.insert(0, "table_name", table_name)
        return df.fillna("").to_dict(orient="records")

    def get_identification(self, *, ctx: QueryContext) -> dict[str, Any] | None:
        base_tbl = self._find_table(["IDENTIFICATION__P_FLIS_NSN"], ctx=ctx)
        ext_tbl = self._find_table(["IDENTIFICATION__V_FLIS_IDENTIFICATION"], ctx=ctx)

        base_rows = self._query_rows_by_niin(table_name=base_tbl, niin=ctx.niin, ctx=ctx, fsc=ctx.fsc) if base_tbl else []
        ext_rows = self._query_rows_by_niin(table_name=ext_tbl, niin=ctx.niin, ctx=ctx) if ext_tbl else []

        if not base_rows and not ext_rows:
            self._trace(ctx, "identification rows matched=0")
            return None

        result: dict[str, Any] = {}
        for row in (base_rows[:1] + ext_rows[:1]):
            for k, v in row.items():
                if str(v).strip() and (k not in result or not str(result[k]).strip()):
                    result[k] = v
        self._trace(ctx, f"identification rows matched={1 if result else 0}")
        return result

    def get_reference_rows(self, *, ctx: QueryContext) -> list[dict[str, Any]]:
        tbl = self._find_table(["REFERENCE__V_FLIS_PART"], ctx=ctx)
        if not tbl:
            return []
        rows = self._query_rows_by_niin(table_name=tbl, niin=ctx.niin, ctx=ctx)
        self._trace(ctx, f"reference rows matched={len(rows)}")
        return rows

    def get_packaging_rows(self, *, ctx: QueryContext) -> list[dict[str, Any]]:
        collected: list[tuple[int, list[dict[str, Any]]]] = []
        profile_counts: dict[str, int] = {}
        for idx, pat in enumerate(
            [
            "FREIGHT_PACKAGING__V_FLIS_PACKAGING_1",
            "FREIGHT_PACKAGING__V_FLIS_PACKAGING_2",
            "FREIGHT_PACKAGING__V_FLIS_PACKAGING_3",
            ],
            start=1,
        ):
            tbl = self._find_table([pat], ctx=ctx)
            if not tbl:
                continue
            rows = self._query_rows_by_niin(table_name=tbl, niin=ctx.niin, ctx=ctx)
            collected.append((idx, rows))
            profile_counts[f"packaging_{idx}_rows"] = len(rows)

        merged_profiles: dict[tuple[str, str], dict[str, Any]] = {}
        for profile_idx, rows in collected:
            for row in rows:
                niin = str(row.get("NIIN", ctx.niin)).strip() or ctx.niin
                pica_sica = str(row.get("PICA_SICA", "")).strip()
                key = (niin, pica_sica)
                profile = merged_profiles.setdefault(
                    key,
                    {
                        "NIIN": niin,
                        "PICA_SICA": pica_sica,
                    },
                )
                for k, v in row.items():
                    if k == "table_name":
                        continue
                    if str(v).strip() and (k not in profile or not str(profile[k]).strip()):
                        profile[k] = v
                profile[f"has_packaging_{profile_idx}"] = True
        out = list(merged_profiles.values())
        self._trace(
            ctx,
            "packaging rows matched="
            f"{len(out)} details={profile_counts if profile_counts else {'packaging_1_rows': 0, 'packaging_2_rows': 0, 'packaging_3_rows': 0}}",
        )
        return out

    def get_freight_rows(self, *, ctx: QueryContext) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for pat in ["FREIGHT_PACKAGING__V_FREIGHT", "FREIGHT_PACKAGING__V_DSS_WEIGHT_AND_CUBE"]:
            tbl = self._find_table([pat], ctx=ctx)
            if not tbl:
                continue
            out.extend(self._query_rows_by_niin(table_name=tbl, niin=ctx.niin, ctx=ctx))
        self._trace(ctx, f"freight rows matched={len(out)}")
        return out

    def get_cage_details(self, *, ctx: QueryContext, cage_codes: set[str]) -> list[dict[str, Any]]:
        clean_codes = sorted({c.strip().upper() for c in cage_codes if c and c.strip()})
        if not clean_codes:
            self._trace(ctx, "unique cage codes=0")
            return []

        merged: dict[str, dict[str, Any]] = {}
        for pat in ["CAGE__P_CAGE", "CAGE__V_CAGE_ADDRESS", "CAGE__V_CAGE_STATUS_AND_TYPE"]:
            tbl = self._find_table([pat], ctx=ctx)
            if not tbl:
                continue
            cols = self._columns(tbl)
            code_col = self._pick_column_from_names(cols, ["CAGE_CODE", "CAGE"])
            if not code_col:
                continue
            placeholders = ",".join(["?" for _ in clean_codes])
            sql = f"SELECT * FROM \"{tbl}\" WHERE UPPER(TRIM(COALESCE(\"{code_col}\", ''))) IN ({placeholders})"
            rows = self._con.execute(sql, clean_codes).fetchdf().fillna("").to_dict(orient="records")
            for row in rows:
                code = str(row.get(code_col, "")).strip().upper()
                if not code:
                    continue
                merged.setdefault(code, {"CAGE_CODE": code})
                for k, v in row.items():
                    if str(v).strip() and (k not in merged[code] or not str(merged[code][k]).strip()):
                        merged[code][k] = v
        result = list(merged.values())
        self._trace(ctx, f"unique cage codes={len(clean_codes)}")
        self._trace(ctx, f"cage rows matched={len(result)}")
        return result

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

    def build_user_friendly_result(
        self,
        *,
        ctx: QueryContext,
        query: dict[str, Any],
        identification: dict[str, Any] | None,
        reference_rows: list[dict[str, Any]],
        cage_rows: list[dict[str, Any]],
        packaging_rows: list[dict[str, Any]],
        freight_rows: list[dict[str, Any]],
    ) -> LookupResult:
        warnings: list[str] = []
        found_in_identification = identification is not None
        if found_in_identification:
            warnings.append("NSN znaleziony w Identification.")
        else:
            warnings.append("Brak rekordu IDENTIFICATION dla podanego NIIN/NSN.")
        if not reference_rows:
            warnings.append("Brak reference rows.")
        if not packaging_rows:
            warnings.append("Brak packaging rows.")
        if not freight_rows:
            warnings.append("Brak freight rows.")
        if reference_rows and not cage_rows:
            warnings.append("Brak danych CAGE.")
        if len(reference_rows) > 50:
            warnings.append(
                "Liczba referencji jest nietypowo duża; sprawdź, czy dane nie wymagają dodatkowego filtrowania."
            )

        ref_df = pd.DataFrame(reference_rows)
        cage_df = pd.DataFrame(cage_rows)

        part_number_col = self._pick_column(ref_df, ["PART_NUMBER"])
        ref_cage_col = self._pick_column(ref_df, ["CAGE_CODE", "CAGE"])
        cage_code_col = self._pick_column(cage_df, ["CAGE_CODE", "CAGE"])
        mfr_name_col = self._pick_column(cage_df, ["COMPANY_NAME", "COMPANY", "NAME"])
        country_col = self._pick_column(cage_df, ["COUNTRY"])
        city_col = self._pick_column(cage_df, ["CITY"])

        cage_map: dict[str, dict[str, Any]] = {}
        if cage_code_col:
            for _, row in cage_df.iterrows():
                code = str(row.get(cage_code_col, "")).strip().upper()
                if code:
                    cage_map[code] = row.to_dict()

        part_numbers: list[dict[str, Any]] = []
        for _, row in ref_df.iterrows():
            cage_code = str(row.get(ref_cage_col, "")).strip().upper() if ref_cage_col else ""
            mfr = cage_map.get(cage_code, {})
            source_fields = {
                col.lower(): str(row.get(col, "")).strip()
                for col in REFERENCE_TECH_COLUMNS
                if col in ref_df.columns and str(row.get(col, "")).strip()
            }
            part_number_value = str(row.get(part_number_col, "")).strip() if part_number_col else ""
            reference_type = "commercial_part_number"
            upper_pn = part_number_value.upper()
            if upper_pn.startswith(("MIL-", "MIL ", "MILPRF", "MIL-PRF", "AMS")):
                reference_type = "specification_reference"
            elif "/" in part_number_value or "," in part_number_value:
                reference_type = "compound_reference"
            part_numbers.append(
                {
                    "part_number": part_number_value,
                    "cage_code": cage_code,
                    "manufacturer_name": mfr.get(mfr_name_col, "") if mfr_name_col else "",
                    "country": mfr.get(country_col, "") if country_col else "",
                    "city": mfr.get(city_col, "") if city_col else "",
                    "reference_type": reference_type,
                    "notes": "Kwalifikująca referencja z V_FLIS_PART (po NIIN).",
                    **source_fields,
                }
            )

        summary = {
            "unique_part_numbers": len({p["part_number"] for p in part_numbers if p["part_number"]}),
            "unique_manufacturers": len({p["cage_code"] for p in part_numbers if p["cage_code"]}),
            "unique_packaging_profiles": len(packaging_rows),
        }

        status = LookupStatus(
            found_in_identification=found_in_identification,
            reference_rows_found=len(reference_rows),
            reference_rows_after_cage_join=len(part_numbers),
            packaging_rows_found=len(packaging_rows),
            freight_rows_found=len(freight_rows),
            cage_rows_found=len(cage_rows),
            ui_rows_shown=len(part_numbers),
            exported_part_rows=len(part_numbers),
            exported_packaging_rows=len(packaging_rows),
            exported_freight_rows=len(freight_rows),
        )

        return LookupResult(
            query_id=ctx.query_id,
            query=query,
            status=status,
            identification=identification,
            part_numbers=part_numbers,
            manufacturers=list(cage_map.values()),
            packaging_profiles=packaging_rows,
            freight=freight_rows,
            warnings=warnings,
            summary=summary,
            raw={
                "query_context": {
                    "query_id": ctx.query_id,
                    "raw_input": ctx.raw_input,
                    "normalized_nsn": ctx.normalized_nsn,
                    "fsc": ctx.fsc,
                    "niin": ctx.niin,
                },
                "identification": identification,
                "reference_rows": reference_rows,
                "packaging_rows": packaging_rows,
                "freight_rows": freight_rows,
                "cage_rows": cage_rows,
                "diagnostics": {
                    "reference_rows_raw": len(reference_rows),
                    "reference_rows_after_cage_join": len(part_numbers),
                    "ui_part_rows": len(part_numbers),
                    "export_json_part_rows": len(part_numbers),
                    "export_json_packaging_rows": len(packaging_rows),
                    "export_json_freight_rows": len(freight_rows),
                },
            },
        )

    def suggest_known_nsn(self) -> str | None:
        ctx = QueryContext(query_id="sample", raw_input="sample", normalized_nsn=None, fsc=None, niin="")
        tbl = self._find_table(["IDENTIFICATION__P_FLIS_NSN"], ctx=ctx)
        if not tbl:
            return None
        df = self._con.execute(f'SELECT * FROM "{tbl}" LIMIT 200').fetchdf()
        if df.empty:
            return None
        fsc_col = self._pick_column(df, ["FSC"])
        niin_col = self._pick_column(df, ["NIIN"])
        if not fsc_col or not niin_col:
            return None
        sample = df[(df[fsc_col].astype(str).str.strip() != "") & (df[niin_col].astype(str).str.strip() != "")]
        if sample.empty:
            return None
        row = sample.iloc[0]
        fsc = str(row[fsc_col]).strip()
        niin = str(row[niin_col]).strip()
        return f"{fsc}{niin}" if len(fsc) == 4 and len(niin) == 9 else None

    def lookup_nsn(self, nsn_or_niin: str) -> dict[str, Any]:
        query = normalize_nsn(nsn_or_niin)
        ctx = QueryContext(
            query_id=str(uuid.uuid4()),
            raw_input=nsn_or_niin,
            normalized_nsn=query.get("nsn"),
            fsc=query.get("fsc"),
            niin=query["niin"],
        )
        try:
            self._trace(ctx, f"lookup start input={ctx.raw_input}")
            self._trace(ctx, f"normalized fsc={ctx.fsc} niin={ctx.niin} nsn={ctx.normalized_nsn}")

            identification = self.get_identification(ctx=ctx)
            reference_rows = self.get_reference_rows(ctx=ctx)

            ref_df = pd.DataFrame(reference_rows)
            cage_col = self._pick_column(ref_df, ["CAGE_CODE", "CAGE"])
            cage_codes = set(ref_df[cage_col].astype(str).tolist()) if cage_col else set()
            cage_rows = self.get_cage_details(ctx=ctx, cage_codes=cage_codes)

            packaging_rows = self.get_packaging_rows(ctx=ctx)
            freight_rows = self.get_freight_rows(ctx=ctx)

            result = self.build_user_friendly_result(
                ctx=ctx,
                query=query,
                identification=identification,
                reference_rows=reference_rows,
                cage_rows=cage_rows,
                packaging_rows=packaging_rows,
                freight_rows=freight_rows,
            )
            self._trace(
                ctx,
                "lookup completed "
                f"status=identification:{result.status.found_in_identification} "
                f"reference:{result.status.reference_rows_found} "
                f"packaging:{result.status.packaging_rows_found} "
                f"freight:{result.status.freight_rows_found} "
                f"cage:{result.status.cage_rows_found}",
            )
            return result.to_dict()
        except Exception as exc:  # noqa: BLE001
            self._error(ctx, f"Lookup error input={nsn_or_niin}: {exc}")
            self._trace(ctx, f"lookup failed: {exc}")
            raise


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
