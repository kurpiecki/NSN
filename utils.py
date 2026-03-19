from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format=LOG_FORMAT)


def normalize_nsn(input_str: str) -> dict[str, Any]:
    if not input_str or not input_str.strip():
        raise ValueError("Puste wejście. Wpisz NSN (13 cyfr) lub NIIN (9 cyfr).")

    digits = re.sub(r"\D", "", input_str)
    if len(digits) == 13:
        return {
            "raw_input": input_str,
            "digits": digits,
            "nsn": f"{digits[0:4]}-{digits[4:6]}-{digits[6:9]}-{digits[9:13]}",
            "fsc": digits[0:4],
            "niin": digits[4:],
            "is_full_nsn": True,
        }
    if len(digits) == 9:
        return {
            "raw_input": input_str,
            "digits": digits,
            "nsn": None,
            "fsc": None,
            "niin": digits,
            "is_full_nsn": False,
        }
    raise ValueError(
        f"Niepoprawny format '{input_str}'. Oczekiwane 13 cyfr (NSN) albo 9 cyfr (NIIN)."
    )


def export_result_to_json(result: dict[str, Any], output_path: str | Path) -> Path:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p
