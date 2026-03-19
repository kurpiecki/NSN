from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from nsn_loader import discover_source_files, scan_columns

logger = logging.getLogger(__name__)


class NsnIndexBuilder:
    def __init__(self, base_dir: str | Path = ".", db_path: str | Path = "data/nsn.duckdb") -> None:
        self.base_dir = Path(base_dir)
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _ensure_meta_table(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS source_files_meta (
                folder VARCHAR,
                file_path VARCHAR,
                encoding VARCHAR,
                delimiter VARCHAR,
                columns VARCHAR,
                loaded_at TIMESTAMP DEFAULT now()
            )
            """
        )

    def _load_csv_as_text_table(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        file_path: Path,
        encoding: str,
        delimiter: str,
    ) -> None:
        view_name = "tmp_csv"
        con.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT *
            FROM read_csv_auto(
                '{file_path.as_posix()}',
                delim='{delimiter}',
                header=true,
                all_varchar=true,
                ignore_errors=true,
                auto_detect=true,
                quote='"',
                escape='"',
                encoding='{encoding}'
            )
            """
        )

        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM {view_name} WHERE 1=0
            """
        )
        con.execute(f"INSERT INTO {table_name} SELECT * FROM {view_name}")
        con.execute(f"DROP VIEW {view_name}")

    def build_local_index(self, rebuild: bool = False) -> dict[str, int]:
        discovered = discover_source_files(self.base_dir)
        con = duckdb.connect(str(self.db_path))
        self._ensure_meta_table(con)

        loaded_counts: dict[str, int] = {}

        if rebuild:
            self._drop_managed_objects(con)
            self._ensure_meta_table(con)

        for folder, files in discovered.items():
            if not files:
                logger.warning("Brak katalogu lub plików źródłowych: %s", folder)
                loaded_counts[folder] = 0
                continue

            specs = scan_columns(files)
            loaded_counts[folder] = 0
            for spec in specs:
                fp = Path(spec["file"])
                table_name = f"{folder.lower()}__{fp.stem.lower()}"
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
                self._load_csv_as_text_table(
                    con,
                    table_name=table_name,
                    file_path=fp,
                    encoding=spec["encoding"],
                    delimiter=spec["delimiter"],
                )
                con.execute(
                    """
                    INSERT INTO source_files_meta(folder, file_path, encoding, delimiter, columns)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [folder, spec["file"], spec["encoding"], spec["delimiter"], spec["columns"]],
                )
                rowcount = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                logger.info("Załadowano %s (%s wierszy)", table_name, rowcount)
                loaded_counts[folder] += rowcount

        self._create_normalized_views(con)
        con.close()
        return loaded_counts

    def _drop_managed_objects(self, con: duckdb.DuckDBPyConnection) -> None:
        managed_prefixes = (
            "identification__",
            "reference__",
            "cage__",
            "freight_packaging__",
            "characteristics__",
        )
        tables = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_type = 'BASE TABLE'
            """
        ).fetchall()
        for (table_name,) in tables:
            if table_name == "source_files_meta" or table_name.startswith(managed_prefixes):
                con.execute(f"DROP TABLE IF EXISTS {table_name}")

        for view_name in ["v_identification", "v_reference", "v_cage", "v_packaging", "v_freight"]:
            con.execute(f"DROP VIEW IF EXISTS {view_name}")

    def _create_normalized_views(self, con: duckdb.DuckDBPyConnection) -> None:
        # Widoki są opcjonalne; lookup działa bezpośrednio na tabelach bazowych.
        # Tworzymy je tylko wtedy, gdy da się zbudować bezpieczne UNION ALL.
        for view_name in ["v_identification", "v_reference", "v_cage", "v_packaging", "v_freight"]:
            con.execute(f"DROP VIEW IF EXISTS {view_name}")
