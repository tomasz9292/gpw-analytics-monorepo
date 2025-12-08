"""Helpers for working with GPW symbol aliases used across the project."""
from __future__ import annotations

import re
from typing import Dict

# Add aliases as needed.
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

# Generic ticker pattern used by API/ClickHouse code to guard against malformed inputs.
TICKER_LIKE_PATTERN = re.compile(r"^[0-9A-Z]{1,8}(?:[._-][0-9A-Z]{1,8})?$")


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


def to_base_symbol(raw: str) -> str:
    """Return the traded base ticker for a canonical symbol or alias."""

    cleaned = raw.strip().upper()
    alias = ALIASES_RAW_TO_WA.get(cleaned)
    if alias:
        base = alias.split(".", 1)[0].strip()
        if base:
            return base.upper()
    if "." in cleaned:
        base = cleaned.split(".", 1)[0].strip()
        if base:
            return base.upper()
    return cleaned


def normalize_ticker(value: str) -> str:
    """
    Normalize any GPW-like ticker to the base form used across the project.

    Rules:
    - trim whitespace and collapse separators,
    - drop common suffixes like .WA / .PL,
    - map long aliases (e.g. PKNORLEN, DINOPL) to their traded base (PKN, DNP),
    - enforce upper-case alphanumeric ticker.
    """

    cleaned = value.strip().upper()
    if not cleaned:
        raise RuntimeError("Empty ticker")

    cleaned = re.sub(r"[\s_\-]+", "", cleaned)
    for suffix in (".WA", ".PL"):
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)]
            break

    base = to_base_symbol(cleaned)

    if not base or not TICKER_LIKE_PATTERN.fullmatch(base):
        raise RuntimeError(f"Invalid ticker: {value}")

    return base


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
    """Return a display-friendly ticker with the .WA suffix when available."""

    return ALIASES_RAW_TO_WA.get(raw, raw)


def normalize_input_symbol(s: str) -> str:
    """
    Normalize user input to the base ticker used for OHLC imports/queries.

    Users may type lower-case symbols, add suffixes (.WA/.PL) or use legacy
    aliases from GPW. The function converts them to the traded base symbol.
    """

    cleaned = s.strip()
    if not cleaned:
        return ""

    candidate = cleaned.upper()

    canonical = _ALIASES_CANONICAL_LOOKUP.get(candidate)
    if canonical:
        candidate = canonical

    try:
        return normalize_ticker(candidate)
    except RuntimeError:
        pass

    if "." in candidate:
        base = candidate.split(".", 1)[0].strip()
        if base:
            try:
                return normalize_ticker(base)
            except RuntimeError:
                return base

    return candidate


def to_stooq_symbol(value: str) -> str:
    """Return the ticker understood by Stooq for a given GPW symbol."""

    normalized = normalize_ticker(value)
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
    "normalize_ticker",
    "pretty_symbol",
    "to_base_symbol",
    "to_stooq_symbol",
]
