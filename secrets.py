"""Aplikacyjne sekrety + kompatybilność ze standardowym modułem `secrets`.

Ten projekt historycznie używał pliku `secrets.py` do trzymania konfiguracji API.
Niestety nazwa koliduje ze standardowym modułem Pythona `secrets`, którego
używają zależności (np. NumPy/Pandas) podczas startu aplikacji.

Aby nie psuć istniejącej konfiguracji użytkowników, eksportujemy zarówno stałe
aplikacyjne, jak i podstawowe funkcje API kompatybilne z `secrets` stdlib.
"""

from __future__ import annotations

import base64
import binascii
import hmac
import os
import random

# Konfiguracja aplikacji.
PERPLEXITY_API_KEY = "WSTAW_TUTAJ"
PERPLEXITY_BASE_URL = "https://api.perplexity.ai"


# Kompatybilne API modułu `secrets`.
_sysrand = random.SystemRandom()


def choice(sequence):
    return _sysrand.choice(sequence)


def randbelow(exclusive_upper_bound: int) -> int:
    if exclusive_upper_bound <= 0:
        raise ValueError("Upper bound must be positive")
    return _sysrand.randrange(exclusive_upper_bound)


def randbits(k: int) -> int:
    if k < 0:
        raise ValueError("number of bits must be non-negative")
    return _sysrand.getrandbits(k)


def token_bytes(nbytes: int | None = None) -> bytes:
    if nbytes is None:
        nbytes = 32
    return os.urandom(nbytes)


def token_hex(nbytes: int | None = None) -> str:
    return binascii.hexlify(token_bytes(nbytes)).decode("ascii")


def token_urlsafe(nbytes: int | None = None) -> str:
    tok = token_bytes(nbytes)
    return base64.urlsafe_b64encode(tok).rstrip(b"=").decode("ascii")


def compare_digest(a, b) -> bool:
    return hmac.compare_digest(a, b)
