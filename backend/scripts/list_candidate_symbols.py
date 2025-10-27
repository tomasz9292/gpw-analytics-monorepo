#!/usr/bin/env python3
"""List symbols returned by `_list_candidate_symbols` for a given universe."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.main import (
    _build_filters_from_universe,
    _collect_candidate_metadata,
    _list_candidate_symbols,
    get_ch,
)
from api.symbols import normalize_input_symbol


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "List tickers selected by backend.api.main._list_candidate_symbols "
            "for the provided universe filters."
        )
    )
    parser.add_argument(
        "--universe",
        metavar="FILTER",
        nargs="+",
        required=True,
        help=(
            "Universe definition exactly as it is sent from the frontend. "
            "Examples: index:WIG40, isin:PLLOTOS00025. Multiple filters can be "
            "passed one after another."
        ),
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Print one ticker per line instead of a JSON array.",
    )
    parser.add_argument(
        "--with-company-info",
        action="store_true",
        help=(
            "Augment the output with basic metadata fetched from the companies table "
            "(name, ISIN, sector, industry)."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)

    filters = _build_filters_from_universe(args.universe)
    ch = get_ch()
    symbols = _list_candidate_symbols(ch, filters)

    metadata_lookup: dict[str, dict[str, str]] = {}
    if args.with_company_info:
        metadata_lookup = _collect_candidate_metadata(ch, symbols)
        if not metadata_lookup:
            print(
                "[warn] Nie udało się pobrać dodatkowych danych spółek z tabeli companies.",
                file=sys.stderr,
            )

    if args.pretty:
        if args.with_company_info and metadata_lookup:
            header = ["#", "Symbol", "Nazwa", "ISIN", "Sektor", "Branża"]
            rows = [header]
            for idx, symbol in enumerate(symbols, start=1):
                normalized = normalize_input_symbol(symbol) or symbol
                key = normalized.upper()
                entry = metadata_lookup.get(key) or metadata_lookup.get(symbol.upper(), {})
                rows.append(
                    [
                        f"{idx}",
                        symbol,
                        entry.get("name", "-"),
                        entry.get("isin", "-"),
                        entry.get("sector", "-"),
                        entry.get("industry", "-"),
                    ]
                )

            widths = [max(len(row[col]) for row in rows) for col in range(len(header))]
            for row in rows:
                formatted = "  ".join(
                    cell.ljust(width) if idx else cell for idx, (cell, width) in enumerate(zip(row, widths))
                )
                print(formatted)
        else:
            for idx, symbol in enumerate(symbols, start=1):
                print(f"{idx:2d}. {symbol}")
    else:
        if args.with_company_info and metadata_lookup:
            payload = []
            for symbol in symbols:
                normalized = normalize_input_symbol(symbol) or symbol
                key = normalized.upper()
                entry = metadata_lookup.get(key) or metadata_lookup.get(symbol.upper(), {})
                payload.append({"symbol": symbol, **entry})
            print(json.dumps(payload, ensure_ascii=False))
        else:
            print(json.dumps(symbols, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
