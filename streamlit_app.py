from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from app import build_local_index
from nsn_lookup import NsnLookupService, result_to_csv_bytes


@st.cache_resource
def get_lookup_service(db_path: str) -> NsnLookupService:
    return NsnLookupService(db_path=db_path)


def _tail_text_file(path: Path, lines: int = 80) -> str:
    if not path.exists():
        return f"(brak pliku: {path})"
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:]) if content else "(plik pusty)"


def _init_state() -> None:
    st.session_state.setdefault("last_result", None)
    st.session_state.setdefault("last_error", None)
    st.session_state.setdefault("lookup_in_progress", False)


st.set_page_config(page_title="NSN Lookup (PUB LOG)", layout="wide")
_init_state()

st.title("NSN Lookup (lokalne dane PUB LOG)")

with st.sidebar:
    st.header("Konfiguracja")
    base_dir = st.text_input("Katalog źródłowy", value=".")
    db_path = st.text_input("Plik bazy DuckDB", value="data/nsn.duckdb")
    if st.button("Zbuduj / odśwież indeks"):
        with st.spinner("Budowanie indeksu..."):
            try:
                loaded = build_local_index(base_dir=base_dir, db_path=db_path, rebuild=True)
            except Exception as exc:  # noqa: BLE001
                st.session_state["last_error"] = str(exc)
                st.error(f"Błąd budowy indeksu: {exc}")
            else:
                get_lookup_service.clear()
                st.success("Indeks gotowy")
                st.json(loaded)

with st.form("lookup_form"):
    query = st.text_input("Wpisz NSN lub NIIN", value="")
    submitted = st.form_submit_button(
        "Szukaj",
        type="primary",
        disabled=st.session_state.get("lookup_in_progress", False),
    )

if submitted:
    if not query.strip():
        st.warning("Wpisz NSN lub NIIN.")
    else:
        st.session_state["lookup_in_progress"] = True
        try:
            with st.status("Szukanie...", expanded=False):
                service = get_lookup_service(db_path)
                result = service.lookup_nsn(query)
            st.session_state["last_result"] = result
            st.session_state["last_error"] = None
        except Exception as exc:  # noqa: BLE001
            st.session_state["last_error"] = str(exc)
            st.error(f"Błąd lookup: {exc}")
        finally:
            st.session_state["lookup_in_progress"] = False

if st.button("Podaj NSN testowy"):
    try:
        service = get_lookup_service(db_path)
        sample_nsn = service.suggest_known_nsn()
    except Exception as exc:  # noqa: BLE001
        st.session_state["last_error"] = str(exc)
        st.error(f"Błąd pobrania NSN testowego: {exc}")
    else:
        if sample_nsn:
            st.success(f"NSN testowy znaleziony lokalnie: {sample_nsn}")
        else:
            st.warning("Nie udało się pobrać testowego NSN z lokalnej bazy.")



def _render_result(result: dict[str, Any]) -> None:
    query = result.get("query", {})
    status = result.get("status", {})
    summary = result.get("summary", {})

    st.subheader("SEKCJA 1: Podsumowanie")
    cols = st.columns(4)
    cols[0].metric("Query ID", result.get("query_id", "-"))
    cols[1].metric("NSN", query.get("nsn") or "(NIIN input)")
    cols[2].metric("FSC", query.get("fsc") or "-")
    cols[3].metric("NIIN", query.get("niin"))

    cols = st.columns(6)
    cols[0].metric("ID found", "TAK" if status.get("found_in_identification") else "NIE")
    cols[1].metric("Reference", status.get("reference_rows_found", 0))
    cols[2].metric("Packaging", status.get("packaging_rows_found", 0))
    cols[3].metric("Freight", status.get("freight_rows_found", 0))
    cols[4].metric("CAGE", status.get("cage_rows_found", 0))
    cols[5].metric("Characteristics", status.get("characteristics_rows_found", 0))

    cols = st.columns(3)
    cols[0].metric("Part numbers", summary.get("unique_part_numbers", 0))
    cols[1].metric("Producenci", summary.get("unique_manufacturers", 0))
    cols[2].metric("Packaging profiles", summary.get("unique_packaging_profiles", 0))

    if result.get("identification"):
        st.success("NSN znaleziony w Identification")
        st.json(result["identification"])
    else:
        st.warning("Brak rekordu w Identification")

    st.subheader("SEKCJA 2: Part numbers / kwalifikujące się produkty")
    part_rows = result.get("part_numbers", [])
    if len(part_rows) > 50:
        st.warning(
            f"Liczba referencji jest nietypowo duża ({len(part_rows)}). Sprawdź, czy dane nie wymagają dodatkowego filtrowania."
        )
    default_show_all = len(part_rows) <= 50
    show_all = st.checkbox(
        f"Pokaż wszystkie referencje ({len(part_rows)})",
        value=default_show_all,
        key=f"show_all_part_rows_{result.get('query_id')}",
    )
    if show_all:
        shown_rows = part_rows
    else:
        shown_rows = part_rows[:20]
        st.info(f"Wyświetlono {len(shown_rows)} z {len(part_rows)} rekordów. Zaznacz 'Pokaż wszystkie referencje'.")
    st.dataframe(pd.DataFrame(shown_rows), use_container_width=True)

    st.subheader("SEKCJA 3: Packaging / opakowania")
    st.info("Profile dotyczą NIIN/NSN. Brak automatycznego mapowania PN -> packaging bez jawnego klucza.")
    st.dataframe(pd.DataFrame(result.get("packaging_profiles", [])), use_container_width=True)

    st.subheader("SEKCJA 4: Freight / transport")
    st.dataframe(pd.DataFrame(result.get("freight", [])), use_container_width=True)

    st.subheader("SEKCJA 5: Characteristics / cechy itemu")
    characteristics = result.get("characteristics", {})
    characteristics_summary = characteristics.get("summary", {}) if isinstance(characteristics, dict) else {}
    characteristics_rows = characteristics.get("rows", []) if isinstance(characteristics, dict) else []

    cols = st.columns(3)
    cols[0].metric("Forma fizyczna", characteristics_summary.get("physical_form_raw") or "-")
    quantity_raw = characteristics_summary.get("quantity_within_each_unit_package_raw")
    cols[1].metric("Ilość w opakowaniu jednostkowym", quantity_raw or "-")
    cols[2].metric("Jednostka", characteristics_summary.get("quantity_unit") or "-")

    if characteristics_rows:
        st.dataframe(
            pd.DataFrame(characteristics_rows)[["mrc", "requirements_statement", "clear_text_reply"]],
            use_container_width=True,
        )
    else:
        st.warning("brak danych CHARACTERISTICS dla tego NIIN")

    st.subheader("SEKCJA 6: Debug")
    debug = {
        "query_id": result.get("query_id"),
        "normalized_nsn": query.get("nsn"),
        "fsc": query.get("fsc"),
        "niin": query.get("niin"),
        "status": status,
        "warnings": result.get("warnings", []),
    }
    st.json(debug)
    with st.expander("Surowe rekordy"):
        st.json(result.get("raw", {}))

    json_bytes = json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8")
    csv_bytes = result_to_csv_bytes(result)
    st.download_button("Eksport JSON", data=json_bytes, file_name="nsn_lookup_result.json", mime="application/json")
    st.download_button("Eksport CSV", data=csv_bytes, file_name="nsn_lookup_result.csv", mime="text/csv")


last_error = st.session_state.get("last_error")
if last_error:
    st.error(f"Ostatni błąd: {last_error}")

with st.expander("Logi plikowe (debug)", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**logs/search_trace.log**")
        st.code(_tail_text_file(Path("logs/search_trace.log")), language="text")
    with col2:
        st.markdown("**logs/errors.log**")
        st.code(_tail_text_file(Path("logs/errors.log")), language="text")

last_result = st.session_state.get("last_result")
if last_result:
    _render_result(last_result)
else:
    st.info("Brak wyniku lookup. Wpisz NSN/NIIN i kliknij 'Szukaj'.")
