# NSN Local Lookup (PUB LOG)

Lokalna aplikacja (offline) do wyszukiwania informacji o NSN/NIIN na podstawie rozpakowanych plików PUB LOG.

## Co robi

- automatycznie wykrywa pliki `CSV/TXT` w katalogach:
  - `CAGE`
  - `IDENTIFICATION`
  - `REFERENCE`
  - `FREIGHT_PACKAGING`
- buduje lokalny indeks w DuckDB przy pierwszym uruchomieniu,
- wyszukuje dane dla NSN (13 cyfr) lub NIIN (9 cyfr),
- pokazuje:
  - dane identyfikacyjne,
  - listę kwalifikujących się PN/CAGE,
  - profile packaging dla NIIN/NSN,
  - dane freight/transport,
  - ostrzeżenia i raw/debug,
- ma panel kontrolny UI z krótkimi logami i heartbeat co 30 sekund.

## Ograniczenia interpretacyjne (ważne)

1. Program pokazuje kwalifikujące się PN/CAGE pod NSN na podstawie lokalnych danych PUB LOG.
2. Program pokazuje dostępne dane packaging dla NSN/NIIN.
3. Program **nie** zakłada automatycznie mapowania `PN -> packaging profile`, jeśli dane źródłowe tego jednoznacznie nie potwierdzają.
4. Program nie wyszukuje ofert internetowych i nie sprawdza dostępności rynkowej.
5. Program działa offline i nie korzysta z zewnętrznych API.

## Instalacja

Wymagania: Python 3.11+

```bash
python -m venv .venv
. .venv/Scripts/activate   # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Szybki start (UI)

Jeśli uruchomisz bez argumentów, program automatycznie otworzy interfejs Streamlit:

```bash
python app.py
```

Równoważnie:

```bash
python app.py --ui
```

## Budowa indeksu

```bash
python app.py --build-index --base-dir . --db-path data/nsn.duckdb
```

Wymuszenie przebudowy:

```bash
python app.py --build-index --rebuild --base-dir . --db-path data/nsn.duckdb
```

## CLI

Wyszukiwanie i wypisanie JSON:

```bash
python app.py 8030-01-031-6840 --db-path data/nsn.duckdb
```

Eksport do JSON:

```bash
python app.py 8030010316840 --db-path data/nsn.duckdb --out-json out/result.json
```

## Streamlit UI (tryb ręczny)

```bash
streamlit run streamlit_app.py
```

UI zawiera:
- przycisk `Start` (włącza auto-refresh co 30 s),
- przycisk `Stop` (zatrzymuje auto-refresh),
- status monitora i ostatni heartbeat,
- krótkie logi działania (budowa indeksu, lookup, błędy, heartbeat),
- pole NSN/NIIN + przycisk `Szukaj`,
- sekcje: podsumowanie, PN/producenci, packaging, freight, raw/debug,
- eksport: JSON + CSV.

## Architektura

- `app.py` – CLI + funkcje programistyczne `build_local_index()` i `lookup_nsn()` + launcher UI.
- `streamlit_app.py` – lokalny interfejs testowy + panel kontrolny (start/stop, logi, heartbeat).
- `nsn_loader.py` – wykrywanie plików i nagłówków, encodings/delimitery.
- `nsn_index.py` – budowa indeksu DuckDB i widoków logicznych.
- `nsn_lookup.py` – logika lookup i składanie wyniku użytkowego.
- `models.py` – modele danych zwracanych.
- `utils.py` – normalizacja NSN, export JSON, logowanie.

## Jakie pliki/tabele są używane i jak łączone

- `IDENTIFICATION/*` -> tabele `identification__*` (rekord bazowy NSN/NIIN),
- `REFERENCE/*` -> tabele `reference__*` (PN/CAGE i kody referencyjne),
- `CAGE/*` -> tabele `cage__*` (dane producenta po kodzie CAGE),
- `FREIGHT_PACKAGING/*` -> tabele `freight_packaging__*` (packaging i freight, rozdzielane po nazwie tabeli).

Klucz wyszukiwania: **NIIN** (9 cyfr).

- NSN wejściowy normalizuje się do FSC + NIIN,
- rekordy są wiązane defensywnie po wykrytym NIIN,
- CAGE mapowany do danych producenta po kodzie CAGE,
- packaging i freight raportowane jako dane dla NIIN/NSN (bez wymuszania mapowania do konkretnego PN).

## Struktura PUB LOG (DLA) i diagnostyka

Na podstawie dokumentu cross-reference DLA, typowe pliki dla Twojego scenariusza to m.in.:
- `Identification.zip`: `V_FLIS_IDENTIFICATION`, `V_FLIS_STANDARDIZATION`, `V_FLIS_CANCELLED_NIIN`,
- `REFERENCE`: `V_FLIS_PART`,
- `Freight_Packaging.zip`: `V_FREIGHT`, `V_FLIS_PACKAGING_1/2/3`,
- `CAGE.zip`: `V_CAGE_ADDRESS`, `V_CAGE_STATUS_AND_TYPE`.

W tej wersji lookup czyta **bezpośrednio z tabel z prefiksami**:
- `identification__*`
- `reference__*`
- `cage__*`
- `freight_packaging__*`

To eliminuje problem z brakiem widoków typu `v_identification`, jeśli baza była budowana częściowo albo na innej wersji DuckDB.

## Funkcje API

- `normalize_nsn(input_str)`
- `build_local_index()`
- `lookup_nsn(nsn_or_niin)`
- `get_identification(niin)`
- `get_reference_rows(niin)`
- `get_cage_details(cage_codes)`
- `get_packaging_rows(niin)`
- `get_freight_rows(niin)`
- `build_user_friendly_result(...)`
- `export_result_to_json(...)`
