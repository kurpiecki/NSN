from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from app import build_local_index
from nsn_lookup import NsnLookupService, result_to_csv_bytes

REFRESH_SECONDS = 30


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _add_log(message: str) -> None:
    logs: list[str] = st.session_state.setdefault("logs", [])
    logs.append(f"[{_now_iso()}] {message}")
    st.session_state["logs"] = logs[-200:]


def _tail_text_file(path: Path, lines: int = 60) -> str:
    if not path.exists():
        return f"(brak pliku: {path})"
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:]) if content else "(plik pusty)"


def _append_error_log(message: str) -> None:
    path = Path("logs/errors.log")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{_now_iso()} | ERROR | {message}\n")


def _init_state() -> None:
    st.session_state.setdefault("monitor_running", True)
    st.session_state.setdefault("logs", [])
    st.session_state.setdefault("last_tick", _now_iso())
    st.session_state.setdefault("last_result", None)
    st.session_state.setdefault("last_error", None)


st.set_page_config(page_title="NSN Lookup (PUB LOG)", layout="wide")
_init_state()

if st.session_state["monitor_running"]:
    st_autorefresh(interval=REFRESH_SECONDS * 1000, key="monitor_refresh")
    st.session_state["last_tick"] = _now_iso()
    _add_log("Heartbeat: UI aktywne, oczekuję na działania użytkownika.")

st.title("NSN Lookup (lokalne dane PUB LOG)")
st.caption(
    "Panel kontrolny: krótkie logi + ręczne wyszukiwanie NSN/NIIN. "
    f"Auto-odświeżanie co {REFRESH_SECONDS} sekund."
)

control_col1, control_col2, control_col3 = st.columns([1, 1, 3])
if control_col1.button("Start", use_container_width=True):
    st.session_state["monitor_running"] = True
    _add_log("Uruchomiono auto-odświeżanie (monitoring).")
if control_col2.button("Stop", use_container_width=True):
    st.session_state["monitor_running"] = False
    _add_log("Zatrzymano auto-odświeżanie.")
status = "RUNNING" if st.session_state["monitor_running"] else "STOPPED"
control_col3.info(f"Status monitora: **{status}** | Ostatni tick: {st.session_state['last_tick']}")

with st.expander("Logi działania (krótkie)", expanded=True):
    logs = st.session_state.get("logs", [])
    if logs:
        st.code("\n".join(logs[-40:]), language="text")
    else:
        st.write("Brak logów jeszcze.")

with st.expander("Logi plikowe (debug)", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**logs/search_trace.log**")
        st.code(_tail_text_file(Path("logs/search_trace.log")), language="text")
    with col2:
        st.markdown("**logs/errors.log**")
        st.code(_tail_text_file(Path("logs/errors.log")), language="text")

with st.sidebar:
    st.header("Konfiguracja")
    base_dir = st.text_input("Katalog źródłowy", value=".")
    db_path = st.text_input("Plik bazy DuckDB", value="data/nsn.duckdb")
    if st.button("Zbuduj / odśwież indeks"):
        _add_log(f"Start budowy indeksu dla base_dir={base_dir}, db_path={db_path}")
        with st.spinner("Budowanie indeksu..."):
            try:
                loaded = build_local_index(base_dir=base_dir, db_path=db_path, rebuild=True)
            except Exception as exc:  # noqa: BLE001
                st.session_state["last_error"] = str(exc)
                _add_log(f"Błąd budowy indeksu: {exc}")
                _append_error_log(f"Błąd budowy indeksu: {exc}")
                st.error(f"Błąd budowy indeksu: {exc}")
            else:
                _add_log("Indeks gotowy.")
                st.success("Indeks gotowy")
                st.json(loaded)

query = st.text_input("Wpisz NSN lub NIIN", value="")
if st.button("Szukaj"):
    if not query.strip():
        _add_log("Wyszukiwanie pominięte: puste wejście.")
        st.warning("Wpisz NSN lub NIIN.")
    else:
        _add_log(f"Start lookup dla zapytania: {query}")
        service = NsnLookupService(db_path=db_path)
        try:
            result = service.lookup_nsn(query)
        except Exception as exc:  # noqa: BLE001
            st.session_state["last_error"] = str(exc)
            _add_log(f"Błąd lookup: {exc}")
            _append_error_log(f"Błąd lookup: {exc}")
            st.error(f"Błąd: {exc}")
        else:
            st.session_state["last_result"] = result
            st.session_state["last_error"] = None
            _add_log("Lookup zakończony poprawnie.")

if st.button("Podaj NSN testowy"):
    service = NsnLookupService(db_path=db_path)
    try:
        sample_nsn = service.suggest_known_nsn()
    except Exception as exc:  # noqa: BLE001
        st.session_state["last_error"] = str(exc)
        _add_log(f"Błąd pobrania NSN testowego: {exc}")
        _append_error_log(f"Błąd pobrania NSN testowego: {exc}")
    else:
        if sample_nsn:
            _add_log(f"NSN testowy z lokalnej bazy: {sample_nsn}")
            st.success(f"NSN testowy znaleziony lokalnie: {sample_nsn}")
        else:
            st.warning("Nie udało się pobrać testowego NSN z lokalnej bazy.")


def _render_result(result: dict[str, Any]) -> None:
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


last_error = st.session_state.get("last_error")
if last_error:
    st.error(f"Ostatni błąd: {last_error}")

last_result = st.session_state.get("last_result")
if last_result:
    _render_result(last_result)
else:
    st.info("Brak wyniku lookup. Wpisz NSN/NIIN i kliknij 'Szukaj'.")
