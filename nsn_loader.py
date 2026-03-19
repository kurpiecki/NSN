from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

DATA_DIRS = ["CAGE", "IDENTIFICATION", "REFERENCE", "FREIGHT_PACKAGING", "CHARACTERISTICS"]
ENCODINGS = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]


def _csv_delimiter_for(path: Path, encoding: str) -> str:
    sample = path.read_text(encoding=encoding, errors="replace")[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except csv.Error:
        return ","


def discover_source_files(base_dir: str | Path) -> dict[str, list[Path]]:
    base = Path(base_dir)
    output: dict[str, list[Path]] = {}
    for folder in DATA_DIRS:
        folder_path = base / folder
        if not folder_path.exists():
            output[folder] = []
            continue
        files = sorted(
            [
                p
                for p in folder_path.iterdir()
                if p.is_file() and p.suffix.lower() in {".csv", ".txt"}
            ]
        )
        output[folder] = files
    return output


def scan_columns(files: Iterable[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for file_path in files:
        used_encoding = None
        header = []
        delimiter = ","
        for enc in ENCODINGS:
            try:
                delimiter = _csv_delimiter_for(file_path, enc)
                with file_path.open("r", encoding=enc, errors="replace", newline="") as f:
                    reader = csv.reader(f, delimiter=delimiter)
                    header = next(reader, [])
                used_encoding = enc
                break
            except Exception:
                continue

        if used_encoding is None:
            logger.warning("Nie udało się odczytać nagłówka: %s", file_path)
            continue

        rows.append(
            {
                "file": str(file_path),
                "encoding": used_encoding,
                "delimiter": delimiter,
                "columns": "|".join([c.strip() for c in header if c is not None]),
            }
        )
    return rows
