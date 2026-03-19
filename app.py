from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from nsn_index import NsnIndexBuilder
from nsn_lookup import NsnLookupService
from utils import export_result_to_json, setup_logging


def build_local_index(base_dir: str = ".", db_path: str = "data/nsn.duckdb", rebuild: bool = False) -> dict[str, int]:
    builder = NsnIndexBuilder(base_dir=base_dir, db_path=db_path)
    return builder.build_local_index(rebuild=rebuild)


def lookup_nsn(nsn_or_niin: str, db_path: str = "data/nsn.duckdb") -> dict:
    service = NsnLookupService(db_path=db_path)
    return service.lookup_nsn(nsn_or_niin)


def launch_ui() -> int:
    cmd = [sys.executable, "-m", "streamlit", "run", "streamlit_app.py"]
    return subprocess.call(cmd)


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description="Lokalna wyszukiwarka NSN (PUB LOG)")
    parser.add_argument("query", nargs="?", help="NSN (13 cyfr) lub NIIN (9 cyfr)")
    parser.add_argument("--base-dir", default=".", help="Katalog z folderami CAGE/IDENTIFICATION/REFERENCE/FREIGHT_PACKAGING")
    parser.add_argument("--db-path", default="data/nsn.duckdb", help="Ścieżka bazy DuckDB")
    parser.add_argument("--build-index", action="store_true", help="Buduj indeks")
    parser.add_argument("--rebuild", action="store_true", help="Usuń i zbuduj indeks od nowa")
    parser.add_argument("--out-json", help="Eksport wyniku do JSON")
    parser.add_argument("--ui", action="store_true", help="Uruchom lokalny interfejs Streamlit")

    args = parser.parse_args()

    if args.ui or (not args.query and not args.build_index):
        raise SystemExit(launch_ui())

    if args.build_index:
        loaded = build_local_index(base_dir=args.base_dir, db_path=args.db_path, rebuild=args.rebuild)
        print("Załadowane rekordy:")
        print(json.dumps(loaded, ensure_ascii=False, indent=2))

    if args.query:
        result = lookup_nsn(args.query, db_path=args.db_path)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if args.out_json:
            path = export_result_to_json(result, args.out_json)
            print(f"Zapisano JSON: {path}")


if __name__ == "__main__":
    main()
