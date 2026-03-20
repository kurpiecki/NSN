from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from nsn_lookup import NsnLookupService
from perplexity_client import PerplexityClient

INPUT_COLUMNS = ["row_no", "number", "specification", "measure", "quantity"]
FIRST_STAGE_COLUMNS = [
    "row_no",
    "offer_item_name",
    "manufacturer_name",
    "brand_name",
    "listed_price",
    "offer_url",
    "supplier_country",
    "part_number",
    "cage_code",
    "nsn",
]
FINAL_COLUMNS = FIRST_STAGE_COLUMNS + ["listed_price_pln", "oznaczenie_oferowanego_produktu_nr_nsn_pn"]


@dataclass(slots=True)
class FxRates:
    eur_pln: float = 4.0
    usd_pln: float = 4.0
    gbp_pln: float = 5.0


def extract_nsn(specification: str) -> str:
    digits = re.sub(r"\D", "", specification or "")
    if len(digits) >= 13:
        return digits[:13]
    return "BRAK"


def parse_price_to_pln(raw_price: str, rates: FxRates) -> float | None:
    if not raw_price:
        return None
    text = raw_price.upper().replace(",", ".").strip()
    match = re.search(r"(-?\d+(?:\.\d+)?)", text)
    if not match:
        return None
    amount = float(match.group(1))
    if "PLN" in text or "ZŁ" in text:
        return amount
    if "EUR" in text or "€" in text:
        return amount * rates.eur_pln
    if "USD" in text or "$" in text:
        return amount * rates.usd_pln
    if "GBP" in text or "£" in text:
        return amount * rates.gbp_pln
    return amount


def load_prompt(path: str | Path) -> str:
    p = Path(path)
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8", errors="replace").strip()


def parse_json_rows(api_text: str) -> list[dict[str, Any]]:
    stripped = api_text.strip()
    if not stripped:
        return []
    parsed: Any
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [x for x in parsed if isinstance(x, dict)]
    return []


def build_decode_table(df_in: pd.DataFrame, lookup_service: NsnLookupService) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in df_in.iterrows():
        row_no = int(row.get("row_no"))
        nsn = extract_nsn(str(row.get("specification", "")))
        if nsn == "BRAK":
            rows.append(
                {
                    "row_no": row_no,
                    "input_specification": row.get("specification", ""),
                    "nsn": "BRAK",
                    "part_number": "",
                    "manufacturer_name": "",
                    "supplier_country": "",
                    "cage_code": "",
                }
            )
            continue

        lookup = lookup_service.lookup_nsn(nsn)
        parts = lookup.get("part_numbers", [])
        if not parts:
            rows.append(
                {
                    "row_no": row_no,
                    "input_specification": row.get("specification", ""),
                    "nsn": nsn,
                    "part_number": "",
                    "manufacturer_name": "",
                    "supplier_country": "",
                    "cage_code": "",
                }
            )
            continue

        for part in parts:
            rows.append(
                {
                    "row_no": row_no,
                    "input_specification": row.get("specification", ""),
                    "nsn": nsn,
                    "part_number": part.get("part_number", ""),
                    "manufacturer_name": part.get("manufacturer_name", ""),
                    "supplier_country": part.get("country", ""),
                    "cage_code": part.get("cage_code", ""),
                }
            )
    return pd.DataFrame(rows)


def run_perplexity_pipeline(
    decode_df: pd.DataFrame,
    *,
    client: PerplexityClient,
    model: str,
    prompt1: str,
    prompt2: str,
    max_steps: int,
    max_output_tokens: int,
    fx_rates: FxRates,
    progress_cb: callable | None = None,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    offer_rows: list[dict[str, Any]] = []
    logs: list[dict[str, str]] = []

    active_df = decode_df[decode_df["nsn"] != "BRAK"].copy()
    for idx, (_, row) in enumerate(active_df.iterrows(), start=1):
        row_no = int(row["row_no"])
        if progress_cb:
            progress_cb(idx, len(active_df), row_no)

        spec = str(row.get("input_specification", ""))
        context = (
            f"row_no={row_no}\nNSN={row.get('nsn', '')}\n"
            f"part_number={row.get('part_number', '')}\nmanufacturer={row.get('manufacturer_name', '')}\n"
            f"supplier_country={row.get('supplier_country', '')}\ncage_code={row.get('cage_code', '')}\n"
            f"specification={spec}"
        )
        first_input = f"{prompt1}\n\n{context}".strip()
        first_text = client.create_response_text(
            model=model,
            input_text=first_input,
            max_steps=max_steps,
            max_output_tokens=max_output_tokens,
            tools=[{"type": "web_search"}],
        )
        logs.append({"stage": "prompt1", "row_no": str(row_no), "request": first_input[:3000], "response": first_text[:3000]})
        first_rows = parse_json_rows(first_text)

        for item in first_rows:
            normalized = {k: item.get(k, "") for k in FIRST_STAGE_COLUMNS}
            normalized["row_no"] = row_no
            normalized["part_number"] = normalized["part_number"] or row.get("part_number", "")
            normalized["cage_code"] = normalized["cage_code"] or row.get("cage_code", "")
            normalized["nsn"] = normalized["nsn"] or row.get("nsn", "")

            second_input = f"{prompt2}\n\n{json.dumps(normalized, ensure_ascii=False)}"
            second_text = client.create_response_text(
                model=model,
                input_text=second_input,
                max_steps=max_steps,
                max_output_tokens=max_output_tokens,
                tools=[{"type": "web_search"}],
            )
            logs.append({"stage": "prompt2", "row_no": str(row_no), "request": second_input[:3000], "response": second_text[:3000]})
            second_rows = parse_json_rows(second_text)
            if not second_rows:
                second_rows = [normalized]
            for out in second_rows:
                final = {k: out.get(k, normalized.get(k, "")) for k in FIRST_STAGE_COLUMNS}
                final["row_no"] = row_no
                price_pln = parse_price_to_pln(str(final.get("listed_price", "")), fx_rates)
                final["listed_price_pln"] = "" if price_pln is None else round(price_pln, 2)
                final["oznaczenie_oferowanego_produktu_nr_nsn_pn"] = f"NSN={final.get('nsn', '')} / PN={final.get('part_number', '')}"
                offer_rows.append(final)

    out_df = pd.DataFrame(offer_rows)
    if out_df.empty:
        out_df = pd.DataFrame(columns=FINAL_COLUMNS)
    else:
        out_df = out_df.reindex(columns=FINAL_COLUMNS)
    return out_df, logs


def archive_and_clean(paths: list[Path], archive_root: Path) -> list[Path]:
    archive_root.mkdir(parents=True, exist_ok=True)
    archived: list[Path] = []
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    for path in paths:
        if not path.exists():
            continue
        target = archive_root / f"{stamp}_{path.name}"
        path.replace(target)
        archived.append(target)
    return archived
