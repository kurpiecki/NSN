from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from app import build_local_index
from nsn_lookup import NsnLookupService, result_to_csv_bytes

st.set_page_config(page_title="NSN Lookup (PUB LOG)", layout="wide")
st.title("NSN Lookup (lokalne dane PUB LOG)")

with st.sidebar:
    st.header("Konfiguracja")
    base_dir = st.text_input("Katalog źródłowy", value=".")
    db_path = st.text_input("Plik bazy DuckDB", value="data/nsn.duckdb")
    if st.button("Zbuduj / odśwież indeks"):
        with st.spinner("Budowanie indeksu..."):
            loaded = build_local_index(base_dir=base_dir, db_path=db_path, rebuild=True)
            st.success("Indeks gotowy")
            st.json(loaded)

query = st.text_input("Wpisz NSN lub NIIN", value="")
if st.button("Szukaj"):
    if not query.strip():
        st.warning("Wpisz NSN lub NIIN.")
    else:
        service = NsnLookupService(db_path=db_path)
        try:
            result = service.lookup_nsn(query)
        except Exception as e:
            st.error(f"Błąd: {e}")
        else:
            summary = result.get("summary", {})
            st.subheader("SEKCJA 1: Podsumowanie")
            cols = st.columns(3)
            cols[0].metric("NSN", result["query"].get("nsn") or "(NIIN input)")
            cols[1].metric("FSC", result["query"].get("fsc") or "-")
            cols[2].metric("NIIN", result["query"].get("niin"))
            cols = st.columns(3)
            cols[0].metric("Part numbers", summary.get("unique_part_numbers", 0))
            cols[1].metric("Producenci", summary.get("unique_manufacturers", 0))
            cols[2].metric("Packaging profiles", summary.get("unique_packaging_profiles", 0))
            st.write("Wykryte jednostki:", ", ".join(summary.get("detected_units", [])) or "-")

            if result.get("identification"):
                st.json(result["identification"])

            st.subheader("SEKCJA 2: Part numbers / kwalifikujące się produkty")
            st.dataframe(pd.DataFrame(result.get("part_numbers", [])), use_container_width=True)

            st.subheader("SEKCJA 3: Packaging / opakowania")
            st.info("Profile dotyczą NIIN/NSN. Brak automatycznego mapowania PN -> packaging bez jawnego klucza.")
            st.dataframe(pd.DataFrame(result.get("packaging_profiles", [])), use_container_width=True)

            st.subheader("SEKCJA 4: Freight / transport")
            st.dataframe(pd.DataFrame(result.get("freight", [])), use_container_width=True)

            st.subheader("SEKCJA 5: Surowe dane / debug")
            with st.expander("Pokaż RAW"):
                st.json(result.get("raw", {}))

            if result.get("warnings"):
                for w in result["warnings"]:
                    st.warning(w)

            json_bytes = json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8")
            csv_bytes = result_to_csv_bytes(result)

            st.download_button("Eksport JSON", data=json_bytes, file_name="nsn_lookup_result.json", mime="application/json")
            st.download_button("Eksport CSV", data=csv_bytes, file_name="nsn_lookup_result.csv", mime="text/csv")
