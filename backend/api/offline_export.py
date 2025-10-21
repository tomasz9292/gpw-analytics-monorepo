"""CLI tool for exporting GPW OHLC data from Stooq to a local file."""
from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, List, Sequence

from .company_ingestion import HttpRequestLog, SimpleHttpSession
from .stooq_ohlc import OhlcRow, StooqOhlcHarvester
from .symbols import DEFAULT_OHLC_SYNC_SYMBOLS, normalize_input_symbol, pretty_symbol


@dataclass
class ExportStats:
    symbols: int = 0
    successful: int = 0
    failed: int = 0
    rows: int = 0
    skipped: int = 0
    duration_seconds: float = 0.0


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0 Safari/537.36",
]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Pobiera dzienne notowania OHLC dla wskazanych spółek GPW ze Stooq "
            "i zapisuje je do pliku CSV."
        )
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        help=(
            "Lista symboli GPW (np. CDR.WA, PKO) do pobrania. Jeśli pominięto, "
            "użyta zostanie domyślna lista z panelu synchronizacji."
        ),
    )
    parser.add_argument(
        "--symbols-file",
        type=Path,
        default=None,
        help="Ścieżka do pliku tekstowego z symbolami (jeden wiersz = jeden ticker).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("ohlc_export.csv"),
        help="Ścieżka do pliku wyjściowego (domyślnie ohlc_export.csv).",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=None,
        help="Najwcześniejsza data notowań w formacie YYYY-MM-DD.",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=1.0,
        help="Minimalna pauza (w sekundach) pomiędzy zapytaniami do Stooq.",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=3.0,
        help="Maksymalna pauza (w sekundach) pomiędzy zapytaniami do Stooq.",
    )
    parser.add_argument(
        "--request-log",
        type=Path,
        default=None,
        help="Opcjonalny plik JSON z logiem zapytań HTTP (do diagnostyki).",
    )
    return parser


def _load_symbols(arguments: argparse.Namespace) -> List[str]:
    symbols: List[str] = []
    if arguments.symbols_file:
        try:
            file_content = arguments.symbols_file.read_text(encoding="utf-8")
        except Exception as exc:  # pragma: no cover - zależy od systemu plików
            raise RuntimeError(f"Nie można odczytać pliku z symbolami: {exc}") from exc
        symbols.extend(
            line.strip()
            for line in file_content.splitlines()
            if line.strip() and not line.strip().startswith("#")
        )
    if arguments.symbols:
        symbols.extend(arguments.symbols)
    if not symbols:
        symbols = list(DEFAULT_OHLC_SYNC_SYMBOLS)
    normalized: List[str] = []
    seen: set[str] = set()
    for raw in symbols:
        cleaned = normalize_input_symbol(str(raw))
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    if not normalized:
        raise RuntimeError("Brak poprawnych symboli do pobrania.")
    return normalized


def _write_csv(path: Path, rows: Iterable[OhlcRow]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["symbol", "date", "open", "high", "low", "close", "volume"])
        count = 0
        for row in rows:
            writer.writerow(
                [
                    row.symbol,
                    row.date.isoformat(),
                    f"{row.open:.6f}" if row.open is not None else "",
                    f"{row.high:.6f}" if row.high is not None else "",
                    f"{row.low:.6f}" if row.low is not None else "",
                    f"{row.close:.6f}" if row.close is not None else "",
                    "" if row.volume is None else f"{row.volume:.6f}",
                ]
            )
            count += 1
    return count


def _format_summary(stats: ExportStats) -> str:
    parts = [
        f"Symbole: {stats.successful}/{stats.symbols}",
        f"Wiersze: {stats.rows}",
    ]
    if stats.failed:
        parts.append(f"Niepowodzenia: {stats.failed}")
    if stats.skipped:
        parts.append(f"Pominięte (po filtrach): {stats.skipped}")
    if stats.duration_seconds:
        parts.append(f"Czas: {stats.duration_seconds:.1f}s")
    return ", ".join(parts)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.max_delay < args.min_delay:
        parser.error("--max-delay musi być większy lub równy --min-delay")

    try:
        symbols = _load_symbols(args)
    except RuntimeError as exc:
        parser.error(str(exc))

    user_agent = random.choice(USER_AGENTS)
    session = SimpleHttpSession(
        headers={
            "User-Agent": user_agent,
            "Accept": "text/csv, text/plain, */*;q=0.8",
            "Referer": "https://stooq.pl/",
            "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.7,en;q=0.6",
        }
    )
    if hasattr(session, "headers"):
        session.headers.pop("X-Requested-With", None)

    harvester = StooqOhlcHarvester(session=session)

    collected: List[OhlcRow] = []
    stats = ExportStats(symbols=len(symbols))
    start_time = time.monotonic()

    print(
        f"Pobieranie notowań dla {len(symbols)} symboli (zapis do {args.output})...",
        file=sys.stderr,
    )

    for index, symbol in enumerate(symbols, start=1):
        label = pretty_symbol(symbol)
        try:
            history = harvester.fetch_history(symbol)
        except Exception as exc:
            stats.failed += 1
            print(f"[{index}/{len(symbols)}] {label}: błąd pobierania ({exc})", file=sys.stderr)
            continue

        filtered = (
            row for row in history if args.start_date is None or row.date >= args.start_date
        )
        filtered_rows = list(filtered)
        if not filtered_rows:
            stats.skipped += 1
            print(
                f"[{index}/{len(symbols)}] {label}: brak wierszy po zastosowaniu filtrów",
                file=sys.stderr,
            )
            continue

        collected.extend(filtered_rows)
        stats.successful += 1
        stats.rows += len(filtered_rows)
        print(
            f"[{index}/{len(symbols)}] {label}: {len(filtered_rows)} wierszy",
            file=sys.stderr,
        )
        if index < len(symbols) and args.max_delay > 0:
            delay = random.uniform(args.min_delay, args.max_delay)
            time.sleep(delay)

    stats.duration_seconds = time.monotonic() - start_time

    if not collected:
        print("Brak danych do zapisania.", file=sys.stderr)
        return 1

    stats.rows = _write_csv(args.output, collected)

    print(_format_summary(stats), file=sys.stderr)

    if args.request_log and hasattr(session, "get_history"):
        history: Iterable[HttpRequestLog] = session.get_history()
        serialized = [log.model_dump() for log in history]
        try:
            args.request_log.write_text(json.dumps(serialized, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:  # pragma: no cover - zależy od systemu plików
            print(f"Nie udało się zapisać logu zapytań: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":  # pragma: no cover - punkt wejścia CLI
    raise SystemExit(main())
