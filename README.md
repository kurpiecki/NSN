# NSN + Perplexity Pipeline

Aplikacja Streamlit do przetwarzania pliku wejściowego (`row_no`, `number`, `specification`, `measure`, `quantity`) w pipeline:
1. ekstrakcja NSN ze `specification` (gdy brak numeru → `BRAK`),
2. dekodowanie NSN do `part_number`/producent/CAGE przez lokalną bazę NSN,
3. wyszukiwanie ofert przez API Perplexity (prompt1),
4. weryfikacja wyników przez API Perplexity (prompt2),
5. zapis `output.csv` z ceną w PLN i pełnym `row_no` na każdym etapie.

## Kluczowe funkcje
- zakres obróbki po `row_no` (od/do),
- możliwość wgrania gotowego `decoded_nsn_parts.csv` (bez ponownej analizy NSN),
- możliwość pobrania aktualnego pliku `decoded_nsn_parts.csv` lub szablonu tego formatu,
- tryb testowy: zapis `prompt1_output.csv` i osobne uruchamianie prompt2 na wybranym zakresie wierszy,
- stały numer `row_no` w każdym rekordzie wynikowym,
- ustawiany w UI model Perplexity (program go nie nadpisuje),
- domyślny model ustawiony na ChatGPT (`openai/gpt-5.2`) + lista pozostałych modeli w menu,
- ustawienia `MAX OUTPUT TOKENS` (1-1200) i `MAX STEPS` (1-10),
- ustawiany timeout API w sekundach (przerwanie requestu po przekroczeniu limitu),
- kontrola paczek wysyłanych do API (ile wierszy w jednym uruchomieniu),
- do promptu trafia jeden rekord `row_no` naraz wraz z pełną listą kandydatów PN/producent dla tego samego `row_no`,
- przyciski sterujące przebiegiem: Start/Wznów, Pauza, Stop+reset,
- podgląd request/response API,
- przeliczanie cen do PLN po kursach z UI (EUR/USD/GBP),
- Cleaner: archiwizacja i czyszczenie plików roboczych.

## Pliki konfiguracyjne
- `secrets.py`:
  - `PERPLEXITY_API_KEY`
  - `PERPLEXITY_BASE_URL`
- `prompt1.csv` – treść promptu etapu 1,
- `prompt2.csv` – treść promptu etapu 2.

## Uruchomienie
```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Testy
```bash
pytest
```
