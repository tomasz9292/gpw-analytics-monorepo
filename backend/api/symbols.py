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

DEFAULT_OHLC_SYNC_SYMBOLS = (
    "ALIOR",
    "ALLEGRO",
    "ASSECOPOL",
    "CCC",
    "CDPROJEKT",
    "CYFRPLSAT",
    "DINOPL",
    "JSW",
    "KGHM",
    "KRUK",
    "LPP",
    "MBANK",
    "MERCATOR",
    "ORANGEPL",
    "PEKAO",
    "PEPCO",
    "PGE",
    "PKNORLEN",
    "PKOBP",
    "SANPL",
    "TAURONPE",
)

ALIASES_WA_TO_RAW: Dict[str, str] = {wa.lower(): raw for raw, wa in ALIASES_RAW_TO_WA.items()}
ALIASES_TICKER_TO_RAW: Dict[str, str] = {
    wa.split(".", 1)[0].lower(): raw
    for raw, wa in ALIASES_RAW_TO_WA.items()
    if wa
}


def pretty_symbol(raw: str) -> str:
    """Zwraca 'ładny' ticker z sufiksem .WA jeśli znamy alias; w p.p. zwraca raw."""

    return ALIASES_RAW_TO_WA.get(raw, raw)


def normalize_input_symbol(s: str) -> str:
    """
    Dla wejścia użytkownika zwraca surowy symbol (RAW) używany w bazie.
    Obsługuje zarówno 'CDR.WA' jak i 'CDPROJEKT'.

    W praktyce użytkownicy często wpisują tickery małymi literami albo z
    sufiksem .WA dla spółek z GPW.  Funkcja stara się więc:
    - przywrócić RAW z mapy aliasów, jeśli go znamy,
    - w przeciwnym razie, gdy ticker wygląda jak "XYZ.WA", uciąć sufiks i
      zwrócić bazowy symbol,
    - w ostateczności zwrócić wejście spójne wielkościowo (UPPER).
    """

    cleaned = s.strip()
    if not cleaned:
        return ""

    lower = cleaned.lower()

    maybe = ALIASES_WA_TO_RAW.get(lower)
    if maybe:
        return maybe

    ticker = ALIASES_TICKER_TO_RAW.get(lower)
    if ticker:
        return ticker

    if "." in cleaned:
        base = cleaned.split(".", 1)[0].strip()
        if base:
            return base.upper()

    return cleaned.upper()


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
    "ALIASES_WA_TO_RAW",
    "ALIASES_TICKER_TO_RAW",
    "DEFAULT_OHLC_SYNC_SYMBOLS",
    "normalize_input_symbol",
    "pretty_symbol",
    "to_stooq_symbol",
]
