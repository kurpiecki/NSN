from __future__ import annotations

import io
import logging
import re
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
        self._characteristics_loader_initialized = self._is_characteristics_loader_initialized()
        self._characteristics_table_name = self._find_characteristics_table()
        self._trace(None, f"characteristics loader initialized={self._characteristics_loader_initialized}")
        self._trace(None, f"V_CHARACTERISTICS.CSV found={bool(self._characteristics_table_name)} table={self._characteristics_table_name or '-'}")

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

    def _is_characteristics_loader_initialized(self) -> bool:
        return any(t.upper().startswith("CHARACTERISTICS__") for t in self._tables)

    def _find_characteristics_table(self) -> str | None:
        preferred = "characteristics__v_characteristics"
        for table_name in self._tables:
            if table_name.lower() == preferred:
                return table_name
        for table_name in self._tables:
            if "CHARACTERISTICS" in table_name.upper():
                return table_name
        return None

    @staticmethod
    def _normalized_column_name(value: str) -> str:
        return "".join(ch for ch in value.upper() if ch.isalnum())

    def _resolve_characteristics_columns(self, table_name: str, *, ctx: QueryContext) -> dict[str, str] | None:
        columns = self._columns(table_name)
        expected_map = {
            "niin": "NIIN",
            "mrc": "MRC",
            "requirements_statement": "REQUIREMENTS_STATEMENT",
            "clear_text_reply": "CLEAR_TEXT_REPLY",
        }
        normalized = {self._normalized_column_name(col): col for col in columns}
        resolved: dict[str, str] = {}
        for field_name, expected_name in expected_map.items():
            target = self._normalized_column_name(expected_name)
            direct = normalized.get(target)
            if direct:
                resolved[field_name] = direct
                continue
            for normalized_name, original in normalized.items():
                if target in normalized_name:
                    resolved[field_name] = original
                    break
        if "niin" not in resolved:
            self._trace(ctx, f"characteristics column detection failed table={table_name}: NIIN missing")
            return None
        missing = [name for name in ["mrc", "requirements_statement", "clear_text_reply"] if name not in resolved]
        if missing:
            self._trace(ctx, f"characteristics column fallback unresolved table={table_name} missing={missing}")
        return resolved

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

    def get_characteristics_rows(self, niin: str, *, ctx: QueryContext) -> tuple[list[dict[str, Any]], list[str]]:
        warnings: list[str] = []
        if not self._characteristics_loader_initialized:
            warning = "folder CHARACTERISTICS niedostępny"
            warnings.append(warning)
            self._trace(ctx, f"characteristics loader unavailable: {warning}")
            self._trace(ctx, f"characteristics_rows_found for NIIN={niin}: 0")
            return [], warnings

        if not self._characteristics_table_name:
            warning = "brak pliku V_CHARACTERISTICS.CSV"
            warnings.append(warning)
            self._trace(ctx, f"characteristics file unavailable: {warning}")
            self._trace(ctx, f"characteristics_rows_found for NIIN={niin}: 0")
            return [], warnings

        table_name = self._characteristics_table_name
        resolved = self._resolve_characteristics_columns(table_name, ctx=ctx)
        if not resolved:
            warning = "Nie udało się dopasować kolumn CHARACTERISTICS (oczekiwane: NIIN, MRC, REQUIREMENTS_STATEMENT, CLEAR_TEXT_REPLY)."
            warnings.append(warning)
            self._trace(ctx, warning)
            self._trace(ctx, f"characteristics_rows_found for NIIN={niin}: 0")
            return [], warnings

        niin_col = resolved["niin"]
        sql = f'SELECT * FROM "{table_name}" WHERE UPPER(TRIM(COALESCE("{niin_col}", ''))) = ?'
        df = self._con.execute(sql, [niin]).fetchdf()
        self._trace(ctx, f"characteristics_rows_found for NIIN={niin}: {len(df)}")
        if df.empty:
            warnings.append("brak danych CHARACTERISTICS dla tego NIIN")
            return [], warnings

        rows: list[dict[str, Any]] = []
        for row in df.fillna("").to_dict(orient="records"):
            rows.append(
                {
                    "niin": str(row.get(resolved["niin"], "")).strip(),
                    "mrc": str(row.get(resolved.get("mrc", ""), "")).strip() if resolved.get("mrc") else "",
                    "requirements_statement": str(row.get(resolved.get("requirements_statement", ""), "")).strip()
                    if resolved.get("requirements_statement")
                    else "",
                    "clear_text_reply": str(row.get(resolved.get("clear_text_reply", ""), "")).strip()
                    if resolved.get("clear_text_reply")
                    else "",
                }
            )
        return rows, warnings

    @staticmethod
    def extract_quantity_and_unit(text: str | None) -> tuple[float | None, str | None]:
        if not text:
            return None, None
        candidate = str(text).strip()
        if not candidate:
            return None, None
        match = re.match(r"^\s*(\d+(?:\.\d+)?)\s+([A-Za-z][A-Za-z0-9\-/ ]*)\s*$", candidate)
        if not match:
            return None, None
        value_raw = match.group(1)
        unit_raw = match.group(2).strip()
        if not unit_raw:
            return None, None
        try:
            value = float(value_raw)
        except ValueError:
            return None, None
        return value, unit_raw

    @staticmethod
    def _is_statement_match(statement: str, target: str) -> bool:
        return target.lower() in statement.lower()

    def detect_physical_form(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        physical_form_raw = None
        physical_form_normalized = None
        for row in rows:
            statement = str(row.get("requirements_statement", "")).strip()
            if statement and self._is_statement_match(statement, "Physical Form"):
                reply = str(row.get("clear_text_reply", "")).strip()
                if reply:
                    physical_form_raw = reply
                    if reply.lower() in {"liquid", "solid", "powder", "gas", "paste", "gel"}:
                        physical_form_normalized = reply.lower()
                break
        return {
            "physical_form_raw": physical_form_raw,
            "physical_form_normalized": physical_form_normalized,
        }

    def summarize_characteristics(self, rows: list[dict[str, Any]], *, ctx: QueryContext) -> dict[str, Any]:
        physical = self.detect_physical_form(rows)
        quantity_raw = None
        for row in rows:
            statement = str(row.get("requirements_statement", "")).strip()
            if statement and self._is_statement_match(statement, "Quantity Within Each Unit Package"):
                reply = str(row.get("clear_text_reply", "")).strip()
                if reply:
                    quantity_raw = reply
                break
        quantity_value, quantity_unit = self.extract_quantity_and_unit(quantity_raw)
        summary = {
            "physical_form_raw": physical.get("physical_form_raw"),
            "physical_form_normalized": physical.get("physical_form_normalized"),
            "quantity_within_each_unit_package_raw": quantity_raw,
            "quantity_value": quantity_value,
            "quantity_unit": quantity_unit,
        }
        self._trace(ctx, f"extracted_physical_form={summary.get('physical_form_raw')}")
        self._trace(ctx, f"extracted_quantity_raw={summary.get('quantity_within_each_unit_package_raw')}")
        self._trace(ctx, f"extracted_quantity_value={summary.get('quantity_value')}")
        self._trace(ctx, f"extracted_quantity_unit={summary.get('quantity_unit')}")
        return summary

    @staticmethod
    def _normalize_free_text(value: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", str(value).upper())

    @staticmethod
    def _format_nsn_from_parts(fsc: str | None, niin: str) -> str | None:
        clean_fsc = re.sub(r"\D", "", str(fsc or ""))
        clean_niin = re.sub(r"\D", "", str(niin or ""))
        if len(clean_fsc) == 4 and len(clean_niin) == 9:
            return f"{clean_fsc}-{clean_niin[0:2]}-{clean_niin[2:5]}-{clean_niin[5:9]}"
        return None

    def _extract_niin_and_fsc_from_identification(
        self,
        identification: dict[str, Any] | None,
        fallback_niin: str,
        fallback_fsc: str | None,
    ) -> tuple[str, str | None]:
        if not identification:
            return fallback_niin, fallback_fsc
        niin_col = self._pick_column_from_names(list(identification.keys()), ["NIIN"])
        fsc_col = self._pick_column_from_names(list(identification.keys()), ["FSC"])
        nii = str(identification.get(niin_col or "", "")).strip() if niin_col else ""
        fsc = str(identification.get(fsc_col or "", "")).strip() if fsc_col else ""
        return (nii or fallback_niin, fsc or fallback_fsc)

    def _query_reference_rows_by_part_number(self, *, part_number: str, ctx: QueryContext) -> list[dict[str, Any]]:
        tbl = self._find_table(["REFERENCE__V_FLIS_PART"], ctx=ctx)
        if not tbl:
            return []
        cols = self._columns(tbl)
        pn_col = self._pick_column_from_names(cols, ["PART_NUMBER", "PARTNO", "PN"])
        if not pn_col:
            self._trace(ctx, f"skip table={tbl}: PART_NUMBER column missing")
            return []

        raw = part_number.strip()
        normalized = self._normalize_free_text(raw)
        if not raw:
            return []

        sql = (
            f'SELECT * FROM "{tbl}" '
            f'WHERE UPPER(TRIM(COALESCE("{pn_col}", \'\'))) = ? '
            f'OR REGEXP_REPLACE(UPPER(COALESCE("{pn_col}", \'\')), \'[^A-Z0-9]\', \'\', \'g\') = ?'
        )
        df = self._con.execute(sql, [raw.upper(), normalized]).fetchdf()
        self._trace(ctx, f"reference rows matched by part_number={len(df)}")
        if df.empty:
            return []
        df.insert(0, "table_name", tbl)
        return df.fillna("").to_dict(orient="records")

    def _pick_first_non_empty(self, rows: list[dict[str, Any]], candidates: list[str]) -> Any:
        for row in rows:
            col = self._pick_column_from_names(list(row.keys()), candidates)
            if not col:
                continue
            value = row.get(col)
            if str(value).strip():
                return value
        return ""

    def _build_freight_packaging_summary(
        self,
        *,
        characteristics_summary: dict[str, Any],
        packaging_rows: list[dict[str, Any]],
        freight_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        unit_container = self._pick_first_non_empty(packaging_rows, ["UNIT_CONTAINER", "UNIT_PACK", "PACKAGE_TYPE"])
        intermediate_container = self._pick_first_non_empty(
            packaging_rows, ["INTERMEDIATE_CONTAINER", "INTERMEDIATE_PACK", "PICA_SICA"]
        )
        ui = self._pick_first_non_empty(packaging_rows, ["UI", "UNIT_OF_ISSUE"])
        icq = self._pick_first_non_empty(packaging_rows, ["ICQ", "INTERMEDIATE_CONTAINER_QUANTITY", "PACKAGE_QTY"])
        unit_pack_weight = self._pick_first_non_empty(
            packaging_rows, ["UNIT_PACK_WEIGHT", "PACK_WEIGHT", "UNIT_WEIGHT", "WEIGHT"]
        )
        unit_pack_dimensions = self._pick_first_non_empty(
            packaging_rows, ["UNIT_PACK_DIMENSIONS", "DIMENSIONS", "UNIT_DIMENSIONS"]
        )
        unit_pack_cube = self._pick_first_non_empty(packaging_rows, ["UNIT_PACK_CUBE", "CUBE"])
        dss_weight = self._pick_first_non_empty(freight_rows, ["DSS_WEIGHT", "WEIGHT"])
        dss_cube = self._pick_first_non_empty(freight_rows, ["DSS_CUBE", "CUBE"])
        unpackaged_item_weight = self._pick_first_non_empty(
            freight_rows, ["UNPACKAGED_ITEM_WEIGHT", "ITEM_WEIGHT", "NET_WEIGHT"]
        )
        unpackaged_item_dimensions = self._pick_first_non_empty(
            freight_rows, ["UNPACKAGED_ITEM_DIMENSIONS", "ITEM_DIMENSIONS", "DIMENSIONS"]
        )
        supplemental_instructions = self._pick_first_non_empty(
            packaging_rows + freight_rows, ["SUPPLEMENTAL_INSTRUCTIONS", "SPECIAL_HANDLING", "REMARKS"]
        )
        return {
            "physical_form_raw": characteristics_summary.get("physical_form_raw"),
            "quantity_within_each_unit_package_raw": characteristics_summary.get("quantity_within_each_unit_package_raw"),
            "quantity_value": characteristics_summary.get("quantity_value"),
            "quantity_unit": characteristics_summary.get("quantity_unit"),
            "ui": ui,
            "icq": icq,
            "unit_container": unit_container,
            "intermediate_container": intermediate_container,
            "unpackaged_item_weight": unpackaged_item_weight,
            "unpackaged_item_dimensions": unpackaged_item_dimensions,
            "unit_pack_weight": unit_pack_weight,
            "unit_pack_dimensions": unit_pack_dimensions,
            "unit_pack_cube": unit_pack_cube,
            "dss_weight": dss_weight,
            "dss_cube": dss_cube,
            "supplemental_instructions": supplemental_instructions,
        }

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
        characteristics_rows: list[dict[str, Any]],
        characteristics_summary: dict[str, Any],
        characteristics_warnings: list[str],
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
        warnings.extend(characteristics_warnings)
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
            "characteristics_rows": len(characteristics_rows),
        }

        status = LookupStatus(
            found_in_identification=found_in_identification,
            reference_rows_found=len(reference_rows),
            reference_rows_after_cage_join=len(part_numbers),
            packaging_rows_found=len(packaging_rows),
            freight_rows_found=len(freight_rows),
            cage_rows_found=len(cage_rows),
            ui_rows_shown=len(part_numbers),
            characteristics_rows_found=len(characteristics_rows),
            exported_part_rows=len(part_numbers),
            exported_packaging_rows=len(packaging_rows),
            exported_freight_rows=len(freight_rows),
            exported_characteristics_rows=len(characteristics_rows),
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
            characteristics={
                "summary": characteristics_summary,
                "rows": characteristics_rows,
            },
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
                "characteristics_rows": characteristics_rows,
                "cage_rows": cage_rows,
                "diagnostics": {
                    "reference_rows_raw": len(reference_rows),
                    "reference_rows_after_cage_join": len(part_numbers),
                    "ui_part_rows": len(part_numbers),
                    "export_json_part_rows": len(part_numbers),
                    "export_json_packaging_rows": len(packaging_rows),
                    "export_json_freight_rows": len(freight_rows),
                    "export_json_characteristics_rows": len(characteristics_rows),
                },
            },
        )

    def build_infoproduct_result(
        self,
        *,
        ctx: QueryContext,
        query_type: str,
        query_raw: str,
        niin: str,
        fsc: str | None,
        identification: dict[str, Any] | None,
        reference_rows: list[dict[str, Any]],
        cage_rows: list[dict[str, Any]],
        characteristics_rows: list[dict[str, Any]],
        characteristics_summary: dict[str, Any],
        characteristics_warnings: list[str],
        packaging_rows: list[dict[str, Any]],
        freight_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        nsn_value = self._format_nsn_from_parts(fsc, niin)
        if not nsn_value and identification:
            ni2, fsc2 = self._extract_niin_and_fsc_from_identification(identification, niin, fsc)
            nsn_value = self._format_nsn_from_parts(fsc2, ni2)
        summary = self._build_freight_packaging_summary(
            characteristics_summary=characteristics_summary,
            packaging_rows=packaging_rows,
            freight_rows=freight_rows,
        )
        summary["sources"] = {
            "physical_form_and_quantity": "CHARACTERISTICS",
            "ui_container_weight_dimensions_cube": "FREIGHT_PACKAGING",
        }

        ref_df = pd.DataFrame(reference_rows)
        part_col = self._pick_column(ref_df, ["PART_NUMBER"])
        cage_col = self._pick_column(ref_df, ["CAGE_CODE", "CAGE"])
        rncc_col = self._pick_column(ref_df, ["RNCC", "REFERENCE_TYPE"])

        cage_df = pd.DataFrame(cage_rows)
        cage_code_col = self._pick_column(cage_df, ["CAGE_CODE", "CAGE"])
        manufacturer_col = self._pick_column(cage_df, ["COMPANY_NAME", "COMPANY", "NAME"])
        cage_map: dict[str, str] = {}
        if cage_code_col and manufacturer_col:
            for _, c_row in cage_df.iterrows():
                code = str(c_row.get(cage_code_col, "")).strip().upper()
                if code and str(c_row.get(manufacturer_col, "")).strip():
                    cage_map[code] = str(c_row.get(manufacturer_col, "")).strip()

        part_numbers: list[dict[str, Any]] = []
        for _, row in ref_df.iterrows():
            pn = str(row.get(part_col, "")).strip() if part_col else ""
            cage_code = str(row.get(cage_col, "")).strip().upper() if cage_col else ""
            part_numbers.append(
                {
                    "part_number": pn,
                    "cage_code": cage_code,
                    "manufacturer_name": cage_map.get(cage_code, ""),
                    "reference_type": str(row.get(rncc_col, "")).strip() if rncc_col else "",
                    "nsn_level_info_note": "shared NSN-level product info",
                }
            )

        part_specific_info = [
            {
                "part_number": item["part_number"],
                "cage_code": item["cage_code"],
                "manufacturer_name": item["manufacturer_name"],
                "info_scope": "shared_nsn_level",
                "product_info": summary,
            }
            for item in part_numbers
        ]

        debug = {
            "query_type": query_type,
            "normalized_query": {"fsc": fsc, "niin": niin, "nsn": nsn_value},
            "found_niin_count": 1,
            "found_part_number_count": len(part_numbers),
            "found_characteristics_row_count": len(characteristics_rows),
            "found_packaging_profile_count": len(packaging_rows),
            "info_scope_per_part_number": [{"part_number": p["part_number"], "info_scope": p["info_scope"]} for p in part_specific_info],
        }

        warnings: list[str] = list(characteristics_warnings)
        if not characteristics_rows:
            warnings.append("Brak characteristics rows dla NIIN.")
        if not packaging_rows and not freight_rows:
            warnings.append("Brak packaging/freight rows dla NIIN.")
        if not reference_rows:
            warnings.append("Brak part numbers dla NIIN.")

        return {
            "query_type": query_type,
            "query_raw": query_raw,
            "matches": [
                {
                    "nsn": nsn_value or "",
                    "niin": niin,
                    "part_numbers": part_numbers,
                    "shared_product_info": summary,
                    "part_specific_info": part_specific_info,
                    "characteristics_rows": characteristics_rows,
                    "packaging_profiles": packaging_rows,
                }
            ],
            "warnings": warnings,
            "debug": debug,
        }

    def lookup_infoproduct(self, query: str) -> dict[str, Any]:
        raw_query = str(query or "").strip()
        if not raw_query:
            raise ValueError("Puste zapytanie InfoProduct.")

        normalized_nsn: dict[str, Any] | None = None
        query_type = "part_number"
        try:
            normalized_nsn = normalize_nsn(raw_query)
            query_type = "nsn"
        except ValueError:
            normalized_nsn = None

        ctx = QueryContext(
            query_id=str(uuid.uuid4()),
            raw_input=raw_query,
            normalized_nsn=normalized_nsn.get("nsn") if normalized_nsn else None,
            fsc=normalized_nsn.get("fsc") if normalized_nsn else None,
            niin=normalized_nsn.get("niin") if normalized_nsn else "",
        )
        if query_type == "nsn":
            identification = self.get_identification(ctx=ctx)
            reference_rows = self.get_reference_rows(ctx=ctx)
            ref_df = pd.DataFrame(reference_rows)
            cage_col = self._pick_column(ref_df, ["CAGE_CODE", "CAGE"])
            cage_codes = set(ref_df[cage_col].astype(str).tolist()) if cage_col else set()
            cage_rows = self.get_cage_details(ctx=ctx, cage_codes=cage_codes)
            packaging_rows = self.get_packaging_rows(ctx=ctx)
            freight_rows = self.get_freight_rows(ctx=ctx)
            characteristics_rows, characteristics_warnings = self.get_characteristics_rows(ctx.niin, ctx=ctx)
            characteristics_summary = self.summarize_characteristics(characteristics_rows, ctx=ctx)
            return self.build_infoproduct_result(
                ctx=ctx,
                query_type="nsn",
                query_raw=raw_query,
                niin=ctx.niin,
                fsc=ctx.fsc,
                identification=identification,
                reference_rows=reference_rows,
                cage_rows=cage_rows,
                characteristics_rows=characteristics_rows,
                characteristics_summary=characteristics_summary,
                characteristics_warnings=characteristics_warnings,
                packaging_rows=packaging_rows,
                freight_rows=freight_rows,
            )

        reference_rows_by_pn = self._query_reference_rows_by_part_number(part_number=raw_query, ctx=ctx)
        ref_df = pd.DataFrame(reference_rows_by_pn)
        niin_col = self._pick_column(ref_df, ["NIIN"])
        fsc_col = self._pick_column(ref_df, ["FSC"])
        matched_pairs: list[tuple[str, str | None]] = []
        if niin_col:
            seen: set[tuple[str, str | None]] = set()
            for _, row in ref_df.iterrows():
                niin = str(row.get(niin_col, "")).strip()
                if not niin:
                    continue
                fsc_val = str(row.get(fsc_col, "")).strip() if fsc_col else None
                key = (niin, fsc_val or None)
                if key not in seen:
                    seen.add(key)
                    matched_pairs.append(key)

        matches: list[dict[str, Any]] = []
        warnings: list[str] = []
        if not matched_pairs:
            warnings.append("Nie znaleziono NIIN dla podanego part number.")

        for niin, fsc in matched_pairs:
            local_ctx = QueryContext(
                query_id=ctx.query_id,
                raw_input=ctx.raw_input,
                normalized_nsn=None,
                fsc=fsc,
                niin=niin,
            )
            identification = self.get_identification(ctx=local_ctx)
            ref_rows = self.get_reference_rows(ctx=local_ctx)
            local_ref_df = pd.DataFrame(ref_rows)
            cage_col = self._pick_column(local_ref_df, ["CAGE_CODE", "CAGE"])
            cage_codes = set(local_ref_df[cage_col].astype(str).tolist()) if cage_col else set()
            cage_rows = self.get_cage_details(ctx=local_ctx, cage_codes=cage_codes)
            packaging_rows = self.get_packaging_rows(ctx=local_ctx)
            freight_rows = self.get_freight_rows(ctx=local_ctx)
            characteristics_rows, characteristics_warnings = self.get_characteristics_rows(niin, ctx=local_ctx)
            characteristics_summary = self.summarize_characteristics(characteristics_rows, ctx=local_ctx)
            built = self.build_infoproduct_result(
                ctx=local_ctx,
                query_type="part_number",
                query_raw=raw_query,
                niin=niin,
                fsc=fsc,
                identification=identification,
                reference_rows=ref_rows,
                cage_rows=cage_rows,
                characteristics_rows=characteristics_rows,
                characteristics_summary=characteristics_summary,
                characteristics_warnings=characteristics_warnings,
                packaging_rows=packaging_rows,
                freight_rows=freight_rows,
            )
            matches.extend(built["matches"])
            warnings.extend(built.get("warnings", []))

        return {
            "query_type": "part_number",
            "query_raw": raw_query,
            "matches": matches,
            "warnings": sorted({w for w in warnings if w}),
            "debug": {
                "query_type": "part_number",
                "normalized_query": self._normalize_free_text(raw_query),
                "found_niin_count": len(matched_pairs),
                "found_part_number_count": len(reference_rows_by_pn),
                "found_characteristics_row_count": sum(len(m.get("characteristics_rows", [])) for m in matches),
                "found_packaging_profile_count": sum(len(m.get("packaging_profiles", [])) for m in matches),
                "info_scope_per_part_number": [
                    {"part_number": p.get("part_number", ""), "info_scope": p.get("info_scope", "")}
                    for m in matches
                    for p in m.get("part_specific_info", [])
                ],
            },
        }

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
            characteristics_rows, characteristics_warnings = self.get_characteristics_rows(ctx.niin, ctx=ctx)
            characteristics_summary = self.summarize_characteristics(characteristics_rows, ctx=ctx)

            result = self.build_user_friendly_result(
                ctx=ctx,
                query=query,
                identification=identification,
                reference_rows=reference_rows,
                cage_rows=cage_rows,
                packaging_rows=packaging_rows,
                freight_rows=freight_rows,
                characteristics_rows=characteristics_rows,
                characteristics_summary=characteristics_summary,
                characteristics_warnings=characteristics_warnings,
            )
            self._trace(
                ctx,
                "lookup completed "
                f"status=identification:{result.status.found_in_identification} "
                f"reference:{result.status.reference_rows_found} "
                f"packaging:{result.status.packaging_rows_found} "
                f"freight:{result.status.freight_rows_found} "
                f"characteristics:{result.status.characteristics_rows_found} "
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

    characteristics = result.get("characteristics", {})
    characteristics_rows = characteristics.get("rows", []) if isinstance(characteristics, dict) else []
    if characteristics_rows:
        df = pd.DataFrame(characteristics_rows)
        df.insert(0, "section", "characteristics_rows")
        frames.append(df)

    characteristics_summary = characteristics.get("summary", {}) if isinstance(characteristics, dict) else {}
    if characteristics_summary:
        df = pd.DataFrame([characteristics_summary])
        df.insert(0, "section", "characteristics_summary")
        frames.append(df)

    if not frames:
        return b""

    merged = pd.concat(frames, ignore_index=True)
    buf = io.StringIO()
    merged.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")
