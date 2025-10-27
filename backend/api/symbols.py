"""Helpers for working with GPW symbol aliases used across the project."""
from __future__ import annotations
from typing import Dict

from .company_ingestion import _normalize_gpw_symbol


# Dodawaj wg potrzeb.
ALIASES_RAW_TO_WA: Dict[str, str] = {
    "ALIOR": "ALR.WA",
    "ALLEGRO": "ALE.WA",
    "ASSECOPOL": "ACP.WA",
    "CCC": "CCC.WA",
    "CDPROJEKT": "CDR.WA",
    "CYFRPLSAT": "CPS.WA",
    "DINOPL": "DNP.WA",
    "JSW": "JSW.WA",
    "KGHM": "KGH.WA",
    "KRUK": "KRU.WA",
    "LPP": "LPP.WA",
    "MBANK": "MBK.WA",
    "MERCATOR": "MRC.WA",
    "ORANGEPL": "OPL.WA",
    "PEKAO": "PEO.WA",
    "PEPCO": "PCO.WA",
    "PGE": "PGE.WA",
    "PKNORLEN": "PKN.WA",
    "PKOBP": "PKO.WA",
    "SANPL": "SPL.WA",
    "TAURONPE": "TPE.WA",
    # ...
}


def _build_canonical_lookup() -> Dict[str, str]:
    """Build mapping used to resolve various aliases to canonical raw symbol."""

    lookup: Dict[str, str] = {}

    for raw_symbol, wa_symbol in ALIASES_RAW_TO_WA.items():
        raw_upper = raw_symbol.upper()
        lookup.setdefault(raw_upper, raw_upper)

        wa_upper = wa_symbol.upper()
        lookup.setdefault(wa_upper, raw_upper)

        if "." in wa_upper:
            base = wa_upper.split(".", 1)[0].strip()
            if base:
                lookup.setdefault(base, raw_upper)

    return lookup


_ALIASES_CANONICAL_LOOKUP = _build_canonical_lookup()

DEFAULT_OHLC_SYNC_SYMBOLS = (
    "ALR",
    "ALE",
    "ACP",
    "CCC",
    "CDR",
    "CPS",
    "DNP",
    "JSW",
    "KGH",
    "KRU",
    "LPP",
    "MBK",
    "MRC",
    "OPL",
    "PEO",
    "PCO",
    "PGE",
    "PKN",
    "PKO",
    "SPL",
    "TPE",
)

def pretty_symbol(raw: str) -> str:
    """Zwraca 'ładny' ticker z sufiksem .WA jeśli znamy alias; w p.p. zwraca raw."""

    return ALIASES_RAW_TO_WA.get(raw, raw)


def normalize_input_symbol(s: str) -> str:
    """
    Dla wejścia użytkownika zwraca bazowy ticker używany przy pobieraniu
    notowań (np. ``CDR`` zamiast ``CDPROJEKT``).

    W praktyce użytkownicy często wpisują tickery małymi literami, z
    sufiksem ``.WA`` albo korzystają z historycznych oznaczeń z GPW.
    Funkcja stara się więc:
    - zamienić znane aliasy na odpowiadający im ticker GPW,
    - w przypadku wejścia zakończonego ``.WA`` zwrócić fragment przed
      sufiksem,
    - w ostateczności zwrócić wejście w postaci UPPERCASE.
    """

    cleaned = s.strip()
    if not cleaned:
        return ""

    candidate = cleaned.upper()

    canonical = _ALIASES_CANONICAL_LOOKUP.get(candidate)
    if canonical:
        candidate = canonical

    try:
        return _normalize_gpw_symbol(candidate)
    except RuntimeError:
        pass

    if "." in candidate:
        base = candidate.split(".", 1)[0].strip()
        if base:
            return base

    return candidate


def to_stooq_symbol(value: str) -> str:
    """Zwraca ticker używany w zapytaniach do Stooq dla danego symbolu GPW."""

    normalized = _normalize_gpw_symbol(value)
    alias = ALIASES_RAW_TO_WA.get(normalized)
    if alias:
        base = alias.split(".", 1)[0].strip()
        if base:
            return base.upper()
    return normalized


__all__ = [
    "ALIASES_RAW_TO_WA",
    "DEFAULT_OHLC_SYNC_SYMBOLS",
    "normalize_input_symbol",
    "pretty_symbol",
    "to_stooq_symbol",
]
