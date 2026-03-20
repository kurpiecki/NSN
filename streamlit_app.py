from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from app import build_local_index
from nsn_lookup import NsnLookupService
from offer_pipeline import FxRates, archive_and_clean, build_decode_table, load_prompt, run_perplexity_pipeline
from perplexity_client import PerplexityAPIError, PerplexityClient

st.set_page_config(page_title="NSN + Perplexity Pipeline", layout="wide")
st.title("NSN → PN → Perplexity Pipeline")

work_dir = Path("workspace_data")
work_dir.mkdir(parents=True, exist_ok=True)
input_path = work_dir / "input.csv"
decode_path = work_dir / "decoded_nsn_parts.csv"
output_path = work_dir / "output.csv"
log_path = work_dir / "api_log.csv"
archive_dir = work_dir / "archive"

with st.sidebar:
    st.header("Konfiguracja danych")
    base_dir = st.text_input("Katalog źródłowy NSN", value=".")
    db_path = st.text_input("Baza DuckDB", value="data/nsn.duckdb")
    if st.button("Zbuduj / odśwież indeks NSN"):
        loaded = build_local_index(base_dir=base_dir, db_path=db_path, rebuild=True)
        st.success("Indeks gotowy")
        st.json(loaded)

    st.header("Ustawienia Perplexity")
    model = st.text_input("Model", value="sonar-pro")
    max_output_tokens = st.slider("MAX OUTPUT TOKENS", 1, 1200, 600)
    max_steps = st.slider("MAX STEPS", 1, 10, 4)
    eur_pln = st.number_input("EUR/PLN", value=4.0, step=0.01)
    usd_pln = st.number_input("USD/PLN", value=4.0, step=0.01)
    gbp_pln = st.number_input("GBP/PLN", value=5.0, step=0.01)

st.subheader("1) Plik wejściowy")
uploaded = st.file_uploader("Wgraj input CSV", type=["csv"])
if uploaded is not None:
    df_raw = pd.read_csv(uploaded)
    if "row_no" not in df_raw.columns:
        df_raw.insert(0, "row_no", range(1, len(df_raw) + 1))
    df_raw.to_csv(input_path, index=False)
    st.success(f"Zapisano: {input_path}")

if input_path.exists():
    df_input = pd.read_csv(input_path)
    st.dataframe(df_input.head(20), use_container_width=True)
    min_row = int(df_input["row_no"].min())
    max_row = int(df_input["row_no"].max())
    c1, c2 = st.columns(2)
    with c1:
        from_row = st.number_input("Od wiersza row_no", min_value=min_row, max_value=max_row, value=min_row)
    with c2:
        to_row = st.number_input("Do wiersza row_no", min_value=min_row, max_value=max_row, value=max_row)

    if st.button("2) Dekoduj NSN -> PN / producent"):
        service = NsnLookupService(db_path=db_path)
        scope_df = df_input[(df_input["row_no"] >= from_row) & (df_input["row_no"] <= to_row)].copy()
        decoded = build_decode_table(scope_df, service)
        decoded.to_csv(decode_path, index=False)
        st.success(f"Zapisano: {decode_path}")
        st.dataframe(decoded.head(50), use_container_width=True)

if decode_path.exists():
    st.subheader("2) Dane po dekodowaniu NSN")
    decoded_df = pd.read_csv(decode_path)
    st.dataframe(decoded_df.head(50), use_container_width=True)

    prompt1 = load_prompt("prompt1.csv")
    prompt2 = load_prompt("prompt2.csv")
    st.caption("Podgląd prompt1.csv")
    st.code(prompt1[:2000] or "(pusty prompt1.csv)")
    st.caption("Podgląd prompt2.csv")
    st.code(prompt2[:2000] or "(pusty prompt2.csv)")

    if st.button("3) Uruchom Perplexity (prompt1 -> prompt2)"):
        try:
            client = PerplexityClient()
        except PerplexityAPIError as exc:
            st.error(str(exc))
        else:
            progress = st.progress(0.0)
            info = st.empty()

            def on_progress(current: int, total: int, row_no: int) -> None:
                progress.progress(current / max(total, 1))
                info.info(f"Obecnie obrabiany row_no={row_no} ({current}/{total})")

            out_df, logs = run_perplexity_pipeline(
                decoded_df,
                client=client,
                model=model,
                prompt1=prompt1,
                prompt2=prompt2,
                max_steps=max_steps,
                max_output_tokens=max_output_tokens,
                fx_rates=FxRates(eur_pln=eur_pln, usd_pln=usd_pln, gbp_pln=gbp_pln),
                progress_cb=on_progress,
            )
            out_df.to_csv(output_path, index=False)
            pd.DataFrame(logs).to_csv(log_path, index=False)
            st.success(f"Gotowe. Wynik: {output_path}")
            st.dataframe(out_df.head(100), use_container_width=True)

if output_path.exists():
    st.subheader("3) Wynik output.csv")
    out_df = pd.read_csv(output_path)
    st.dataframe(out_df, use_container_width=True)
    st.download_button("Pobierz output.csv", output_path.read_bytes(), file_name="output.csv")

if log_path.exists():
    st.subheader("Log request/response API")
    st.dataframe(pd.read_csv(log_path).tail(30), use_container_width=True)

st.subheader("Cleaner")
if st.button("Cleaner: archiwizuj i wyczyść pliki robocze"):
    archived = archive_and_clean([input_path, decode_path, output_path, log_path], archive_dir)
    if archived:
        st.success("Zarchiwizowano pliki:")
        for p in archived:
            st.write(f"- {p}")
    else:
        st.info("Brak plików do czyszczenia")
