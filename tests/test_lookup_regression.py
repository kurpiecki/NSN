from __future__ import annotations

from pathlib import Path

import duckdb

from nsn_lookup import NsnLookupService
from utils import normalize_nsn


def _create_test_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE identification__p_flis_nsn (FSC VARCHAR, NIIN VARCHAR, ITEM_NAME VARCHAR)")
    con.execute("CREATE TABLE identification__v_flis_identification (NIIN VARCHAR, EXTRA VARCHAR)")
    con.execute("CREATE TABLE reference__v_flis_part (NIIN VARCHAR, PART_NUMBER VARCHAR, CAGE_CODE VARCHAR)")
    con.execute("CREATE TABLE cage__p_cage (CAGE_CODE VARCHAR, COMPANY_NAME VARCHAR)")
    con.execute("CREATE TABLE freight_packaging__v_flis_packaging_1 (NIIN VARCHAR, PICA_SICA VARCHAR)")
    con.execute("CREATE TABLE freight_packaging__v_freight (NIIN VARCHAR, FREIGHT_CLASS VARCHAR)")

    con.execute("INSERT INTO identification__p_flis_nsn VALUES ('4935','000000012','A')")
    con.execute("INSERT INTO identification__p_flis_nsn VALUES ('6850','010445034','B')")
    con.execute("INSERT INTO identification__v_flis_identification VALUES ('000000012','X')")
    con.execute("INSERT INTO identification__v_flis_identification VALUES ('010445034','Y')")

    con.execute("INSERT INTO reference__v_flis_part VALUES ('000000012','PN-1','C001')")
    con.execute("INSERT INTO reference__v_flis_part VALUES ('010445034','PN-2','C002')")
    con.execute("INSERT INTO cage__p_cage VALUES ('C001','MFR1')")
    con.execute("INSERT INTO cage__p_cage VALUES ('C002','MFR2')")

    con.execute("INSERT INTO freight_packaging__v_flis_packaging_1 VALUES ('000000012','P1')")
    con.execute("INSERT INTO freight_packaging__v_freight VALUES ('010445034','F2')")
    con.close()


def test_normalize_nsn_accepts_full_and_hyphenated() -> None:
    assert normalize_nsn("4935000000012")["niin"] == "000000012"
    assert normalize_nsn("4935-00-000-0012")["niin"] == "000000012"


def test_two_sequential_lookups_use_own_niin(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    service = NsnLookupService(db_path=db_path)

    first = service.lookup_nsn("4935000000012")
    second = service.lookup_nsn("6850010445034")

    assert first["query"]["niin"] == "000000012"
    assert second["query"]["niin"] == "010445034"
    assert first["query"]["niin"] != second["query"]["niin"]


def test_identification_match_does_not_report_global_not_found(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    service = NsnLookupService(db_path=db_path)

    result = service.lookup_nsn("4935000000012")

    assert result["status"]["found_in_identification"] is True
    assert "Brak rekordu IDENTIFICATION" not in " ".join(result["warnings"])


def test_missing_reference_does_not_remove_identification(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    con = duckdb.connect(str(db_path))
    con.execute("DELETE FROM reference__v_flis_part WHERE NIIN='000000012'")
    con.close()

    service = NsnLookupService(db_path=db_path)
    result = service.lookup_nsn("4935000000012")

    assert result["status"]["found_in_identification"] is True
    assert result["status"]["reference_rows_found"] == 0
    assert result["identification"] is not None
