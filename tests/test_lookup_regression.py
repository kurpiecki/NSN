from __future__ import annotations

from pathlib import Path

import duckdb

from nsn_lookup import NsnLookupService
from utils import normalize_nsn


def _create_test_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE identification__p_flis_nsn (FSC VARCHAR, NIIN VARCHAR, ITEM_NAME VARCHAR)")
    con.execute("CREATE TABLE identification__v_flis_identification (NIIN VARCHAR, EXTRA VARCHAR)")
    con.execute(
        "CREATE TABLE reference__v_flis_part (NIIN VARCHAR, PART_NUMBER VARCHAR, CAGE_CODE VARCHAR, RNCC VARCHAR, RNVC VARCHAR, DAC VARCHAR, RNAAC VARCHAR, RNFC VARCHAR, RNSC VARCHAR, RNJC VARCHAR, CAGE_STATUS VARCHAR)"
    )
    con.execute("CREATE TABLE cage__p_cage (CAGE_CODE VARCHAR, COMPANY_NAME VARCHAR)")
    con.execute("CREATE TABLE cage__v_cage_address (CAGE_CODE VARCHAR, CITY VARCHAR, COUNTRY VARCHAR)")
    con.execute("CREATE TABLE freight_packaging__v_flis_packaging_1 (NIIN VARCHAR, PICA_SICA VARCHAR)")
    con.execute("CREATE TABLE freight_packaging__v_flis_packaging_2 (NIIN VARCHAR, PICA_SICA VARCHAR, PACKAGE_QTY VARCHAR)")
    con.execute("CREATE TABLE freight_packaging__v_flis_packaging_3 (NIIN VARCHAR, PICA_SICA VARCHAR, PACKAGE_TYPE VARCHAR)")
    con.execute("CREATE TABLE freight_packaging__v_freight (NIIN VARCHAR, FREIGHT_CLASS VARCHAR)")
    con.execute(
        "CREATE TABLE characteristics__v_characteristics (NIIN VARCHAR, MRC VARCHAR, REQUIREMENTS_STATEMENT VARCHAR, CLEAR_TEXT_REPLY VARCHAR)"
    )

    con.execute("INSERT INTO identification__p_flis_nsn VALUES ('4935','000000012','A')")
    con.execute("INSERT INTO identification__p_flis_nsn VALUES ('6850','010445034','B')")
    con.execute("INSERT INTO identification__v_flis_identification VALUES ('000000012','X')")
    con.execute("INSERT INTO identification__v_flis_identification VALUES ('010445034','Y')")

    con.execute("INSERT INTO reference__v_flis_part VALUES ('000000012','PN-1','C001','A','1','','','','','','A')")
    con.execute("INSERT INTO reference__v_flis_part VALUES ('000000012','PN-1','C003','A','1','','','','','','A')")
    con.execute("INSERT INTO reference__v_flis_part VALUES ('000000012','MIL-PRF-9999','C004','A','1','','','','','','A')")
    con.execute("INSERT INTO reference__v_flis_part VALUES ('010445034','PN-2','C002','A','1','','','','','','A')")
    con.execute("INSERT INTO cage__p_cage VALUES ('C001','MFR1')")
    con.execute("INSERT INTO cage__p_cage VALUES ('C002','MFR2')")
    con.execute("INSERT INTO cage__v_cage_address VALUES ('C001','Austin','US')")
    con.execute("INSERT INTO cage__v_cage_address VALUES ('C002','Boston','US')")

    con.execute("INSERT INTO freight_packaging__v_flis_packaging_1 VALUES ('000000012','P1')")
    con.execute("INSERT INTO freight_packaging__v_flis_packaging_2 VALUES ('000000012','P1','10')")
    con.execute("INSERT INTO freight_packaging__v_flis_packaging_3 VALUES ('000000012','P1','BOX')")
    con.execute("INSERT INTO freight_packaging__v_flis_packaging_1 VALUES ('000000012','P2')")
    con.execute("INSERT INTO freight_packaging__v_flis_packaging_2 VALUES ('000000012','P2','5')")
    con.execute("INSERT INTO freight_packaging__v_freight VALUES ('010445034','F2')")
    con.execute(
        """
        INSERT INTO characteristics__v_characteristics VALUES
        ('000000012','ABCD','Physical Form','LIQUID'),
        ('000000012','EFGH','Quantity Within Each Unit Package','4.0 OUNCES'),
        ('010445034','IJKL','Color','CLEAR')
        """
    )
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


def test_reference_rows_are_not_deduplicated_by_part_number(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    service = NsnLookupService(db_path=db_path)

    result = service.lookup_nsn("4935000000012")

    assert result["status"]["reference_rows_found"] == 3
    assert len(result["part_numbers"]) == 3
    assert result["status"]["reference_rows_after_cage_join"] == 3
    assert {row["cage_code"] for row in result["part_numbers"] if row["part_number"] == "PN-1"} == {"C001", "C003"}


def test_cage_left_join_behavior_keeps_rows_without_cage_data(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    service = NsnLookupService(db_path=db_path)

    result = service.lookup_nsn("4935000000012")

    row_c003 = next(row for row in result["part_numbers"] if row["cage_code"] == "C003")
    assert row_c003["manufacturer_name"] == ""
    assert result["status"]["reference_rows_after_cage_join"] == result["status"]["reference_rows_found"]


def test_packaging_profiles_include_all_pica_sica(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    service = NsnLookupService(db_path=db_path)

    result = service.lookup_nsn("4935000000012")

    assert result["status"]["packaging_rows_found"] == 2
    assert {row["PICA_SICA"] for row in result["packaging_profiles"]} == {"P1", "P2"}


def test_export_contains_all_rows_returned_by_lookup(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    service = NsnLookupService(db_path=db_path)

    result = service.lookup_nsn("4935000000012")

    assert result["status"]["exported_part_rows"] == len(result["part_numbers"])
    assert result["status"]["exported_packaging_rows"] == len(result["packaging_profiles"])
    assert result["status"]["ui_rows_shown"] == len(result["part_numbers"])
    assert result["status"]["exported_characteristics_rows"] == len(result["characteristics"]["rows"])


def test_characteristics_rows_and_summary_are_exposed(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    service = NsnLookupService(db_path=db_path)

    result = service.lookup_nsn("4935000000012")

    assert result["status"]["characteristics_rows_found"] == 2
    assert len(result["characteristics"]["rows"]) == 2
    assert result["characteristics"]["summary"]["physical_form_raw"] == "LIQUID"
    assert result["characteristics"]["summary"]["quantity_within_each_unit_package_raw"] == "4.0 OUNCES"
    assert result["characteristics"]["summary"]["quantity_value"] == 4.0
    assert result["characteristics"]["summary"]["quantity_unit"] == "OUNCES"


def test_characteristics_missing_table_does_not_break_lookup(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    con = duckdb.connect(str(db_path))
    con.execute("DROP TABLE characteristics__v_characteristics")
    con.close()
    service = NsnLookupService(db_path=db_path)

    result = service.lookup_nsn("4935000000012")

    assert result["characteristics"]["rows"] == []
    assert any("Brak załadowanych tabel CHARACTERISTICS w bazie indeksu" in w for w in result["warnings"])


def test_infoproduct_lookup_by_nsn_returns_shared_scope(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    service = NsnLookupService(db_path=db_path)

    result = service.lookup_infoproduct("4935000000012")

    assert result["query_type"] == "nsn"
    assert len(result["matches"]) == 1
    match = result["matches"][0]
    assert match["niin"] == "000000012"
    assert match["shared_product_info"]["physical_form_raw"] == "LIQUID"
    assert match["shared_product_info"]["quantity_value"] == 4.0
    assert all(row["info_scope"] == "shared_nsn_level" for row in match["part_specific_info"])


def test_infoproduct_lookup_by_part_number_supports_multiple_niin(tmp_path: Path) -> None:
    db_path = tmp_path / "nsn.duckdb"
    _create_test_db(db_path)
    con = duckdb.connect(str(db_path))
    con.execute("INSERT INTO reference__v_flis_part VALUES ('010445034','PN-1','C002','A','1','','','','','','A')")
    con.close()

    service = NsnLookupService(db_path=db_path)
    result = service.lookup_infoproduct("PN-1")

    assert result["query_type"] == "part_number"
    assert len(result["matches"]) == 2
    assert {row["niin"] for row in result["matches"]} == {"000000012", "010445034"}
