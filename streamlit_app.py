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
    default_models = {
        "ChatGPT (OpenAI GPT-5.2)": "openai/gpt-5.2",
        "ChatGPT mini (OpenAI GPT-5 mini)": "openai/gpt-5-mini",
        "Sonar": "perplexity/sonar",
        "Sonar Pro": "perplexity/sonar-pro",
        "Sonar Deep Research": "perplexity/sonar-deep-research",
        "R1 1776": "perplexity/r1-1776",
    }
    model_label = st.selectbox("Model domyślny (start: ChatGPT)", options=list(default_models.keys()), index=0)
    model_from_list = default_models[model_label]
    custom_model = st.text_input("Lub wpisz własny identyfikator modelu", value=model_from_list)
    selected_model = custom_model.strip() or model_from_list

    if selected_model.lower() == "chatgpt":
        st.warning("Alias 'chatgpt' jest mapowany na 'openai/gpt-5.2', bo API odrzuca samą nazwę 'chatgpt'.")

    is_chatgpt_family = selected_model.lower().startswith("openai/gpt")
    if is_chatgpt_family:
        max_output_tokens = st.slider("MAX OUTPUT TOKENS (ChatGPT)", 1, 1200, 600)
        max_steps = st.slider("MAX STEPS (ChatGPT)", 1, 10, 4)
    else:
        st.info("Wybrany model nie jest ChatGPT — parametry mogą mieć inne limity.")
        max_output_tokens = st.slider("MAX OUTPUT TOKENS", 1, 1200, 400)
        max_steps = st.slider("MAX STEPS", 1, 10, 2)

    api_timeout_s = st.number_input("Timeout API (sekundy)", min_value=10, max_value=600, value=90, step=5)
    batch_size = st.number_input("Ile wierszy wysłać do API w jednym uruchomieniu", min_value=1, max_value=500, value=10, step=1)
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

st.subheader("1b) Wgraj gotowy plik po obróbce NSN")
uploaded_decoded = st.file_uploader("Wgraj decoded_nsn_parts.csv", type=["csv"], key="decoded_uploader")
if uploaded_decoded is not None:
    decoded_uploaded_df = pd.read_csv(uploaded_decoded)
    decoded_uploaded_df.to_csv(decode_path, index=False)
    st.success(f"Wgrano i zapisano plik obrobiony: {decode_path}")

if decode_path.exists():
    st.download_button(
        "Pobierz aktualny decoded_nsn_parts.csv",
        decode_path.read_bytes(),
        file_name="decoded_nsn_parts.csv",
    )
else:
    decoded_template = pd.DataFrame(
        columns=[
            "row_no",
            "input_specification",
            "nsn",
            "part_number",
            "manufacturer_name",
            "supplier_country",
            "cage_code",
        ]
    )
    st.download_button(
        "Pobierz szablon decoded_nsn_parts.csv",
        decoded_template.to_csv(index=False).encode("utf-8"),
        file_name="decoded_nsn_parts_template.csv",
    )

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

    active_groups_total = int(decoded_df.loc[decoded_df["nsn"] != "BRAK", "row_no"].nunique())
    st.caption(f"Rekordy row_no gotowe do wysłania do API (nsn != BRAK): {active_groups_total}")
    active_preview_df = decoded_df[decoded_df["nsn"] != "BRAK"].copy()
    if not active_preview_df.empty:
        first_row_no = int(active_preview_df.iloc[0]["row_no"])
        preview_group = active_preview_df[active_preview_df["row_no"] == first_row_no].copy()
        sample = preview_group.iloc[0]
        preview_parts = preview_group[["part_number", "manufacturer_name", "supplier_country", "cage_code"]].fillna("").to_dict(orient="records")
        st.caption("Podgląd przykładowego kontekstu wysyłanego do API")
        st.code(
            (
                f"row_no={int(sample['row_no'])}\nNSN={sample.get('nsn', '')}\n"
                f"specification={sample.get('input_specification', '')}\n"
                f"candidate_parts_for_row_no={preview_parts}"
            )[:2000]
        )

    if "api_cursor" not in st.session_state:
        st.session_state.api_cursor = 0
    if "api_paused" not in st.session_state:
        st.session_state.api_paused = False
    if "api_out_rows" not in st.session_state:
        st.session_state.api_out_rows = []
    if "api_logs" not in st.session_state:
        st.session_state.api_logs = []

    c_run, c_pause, c_stop = st.columns(3)
    run_clicked = c_run.button("3) Start / Wznów Perplexity")
    if c_pause.button("Pauza"):
        st.session_state.api_paused = True
        st.info("Pipeline zapauzowany. Kliknij Start / Wznów, aby kontynuować.")
    if c_stop.button("Stop + reset"):
        st.session_state.api_paused = False
        st.session_state.api_cursor = 0
        st.session_state.api_out_rows = []
        st.session_state.api_logs = []
        st.warning("Pipeline zatrzymany i zresetowany.")

    if run_clicked:
        st.session_state.api_paused = False
        try:
            client = PerplexityClient(timeout_s=int(api_timeout_s))
        except PerplexityAPIError as exc:
            st.error(str(exc))
        else:
            progress = st.progress(0.0)
            info = st.empty()

            live_log_box = st.empty()

            def on_progress(current: int, total: int, row_no: int) -> None:
                progress.progress(current / max(total, 1))
                info.info(f"Obecnie obrabiany row_no={row_no} ({current}/{total})")

            def on_log(item: dict[str, str]) -> None:
                live_log_box.code(
                    f"[{item.get('stage')}] row_no={item.get('row_no')}\n"
                    f"REQUEST:\n{item.get('request', '')[:700]}\n\n"
                    f"RESPONSE:\n{item.get('response', '')[:700]}"
                )

            out_df, logs = run_perplexity_pipeline(
                decoded_df,
                client=client,
                model=selected_model,
                prompt1=prompt1,
                prompt2=prompt2,
                max_steps=max_steps,
                max_output_tokens=max_output_tokens,
                fx_rates=FxRates(eur_pln=eur_pln, usd_pln=usd_pln, gbp_pln=gbp_pln),
                progress_cb=on_progress,
                log_cb=on_log,
                start_index=int(st.session_state.api_cursor),
                row_limit=int(batch_size),
            )
            st.session_state.api_out_rows.extend(out_df.to_dict(orient="records"))
            st.session_state.api_logs.extend(logs)
            st.session_state.api_cursor += int(batch_size)

            total_active = int(decoded_df.loc[decoded_df["nsn"] != "BRAK", "row_no"].nunique())
            if st.session_state.api_cursor >= total_active:
                st.success("Gotowe. Przetworzono wszystkie wybrane wiersze.")
            else:
                st.info(
                    f"Przetworzono paczkę {batch_size} wierszy. "
                    f"Pozycja: {st.session_state.api_cursor}/{total_active}. "
                    "Kliknij Start / Wznów dla kolejnej paczki."
                )

            full_out_df = pd.DataFrame(st.session_state.api_out_rows)
            if full_out_df.empty:
                full_out_df = out_df
            full_logs_df = pd.DataFrame(st.session_state.api_logs)
            full_out_df.to_csv(output_path, index=False)
            full_logs_df.to_csv(log_path, index=False)
            st.dataframe(full_out_df.head(100), use_container_width=True)

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
