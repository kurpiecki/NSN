from __future__ import annotations

import pandas as pd

from offer_pipeline import FxRates, extract_nsn, parse_price_to_pln


def test_extract_nsn_returns_digits_or_brak() -> None:
    assert extract_nsn("NSN 8030-01-031-6840") == "8030010316840"
    assert extract_nsn("brak numeru") == "BRAK"


def test_parse_price_to_pln_converts_known_currencies() -> None:
    rates = FxRates(eur_pln=4.1, usd_pln=3.9, gbp_pln=5.0)
    assert parse_price_to_pln("10 EUR", rates) == 41.0
    assert parse_price_to_pln("10 USD", rates) == 39.0
    assert parse_price_to_pln("10 GBP", rates) == 50.0
    assert parse_price_to_pln("10 PLN", rates) == 10.0


def test_row_no_is_preserved_in_dataframes() -> None:
    df = pd.DataFrame([
        {"row_no": 7, "number": "1", "specification": "x", "measure": "szt", "quantity": 1},
    ])
    assert int(df.iloc[0]["row_no"]) == 7
