#!/usr/bin/env python3
"""Automatyczne pobieranie i uruchamianie skryptu przygotowującego środowisko LLM.

Skrypt pobiera odpowiedni plik bootstrap (bash lub PowerShell) z repozytorium
`gpw-analytics-monorepo`, zapisuje go lokalnie i – po uzyskaniu zgody użytkownika –
uruchamia instalację środowiska lokalnego modelu LLM.
"""

from __future__ import annotations

import argparse
import platform
import shutil
import subprocess
import sys
import tempfile
import textwrap
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, Tuple

DEFAULT_BASE_URL = "https://raw.githubusercontent.com/gpw-tools/gpw-analytics-monorepo/main/scripts"

SCRIPT_VARIANTS: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "bash": ("bootstrap_local_llm.sh", ("bash",)),
    "powershell": (
        "bootstrap_local_llm.ps1",
        ("powershell", "-ExecutionPolicy", "Bypass", "-File"),
    ),
}

CONFIRM_CHOICES = {"t", "tak", "y", "yes"}


def detect_variant() -> str:
    system = platform.system().lower()
    if system == "windows":
        return "powershell"
    if system in {"linux", "darwin"}:
        return "bash"
    raise RuntimeError(
        f"Nieobsługiwany system operacyjny: {system!r}. Wybierz wariant ręcznie opcją --variant."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pobierz i uruchom przygotowanie środowiska lokalnego LLM",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Adres bazowy, z którego pobierany jest skrypt bootstrap (domyślnie oficjalne repozytorium)",
    )
    parser.add_argument(
        "--variant",
        choices=sorted(SCRIPT_VARIANTS.keys()),
        help="Wymuś wariant skryptu (bash/powershell). Domyślnie wykrywany na podstawie systemu",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Ścieżka zapisu pobranego pliku instalacyjnego. Domyślnie tymczasowy katalog systemowy",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Nie zadawaj pytania o instalację – załóż zgodę użytkownika",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Tylko pobierz plik, bez uruchamiania instalacji",
    )
    parser.add_argument(
        "--keep-file",
        action="store_true",
        help="Nie usuwaj pobranego pliku z katalogu tymczasowego po zakończeniu",
    )
    return parser.parse_args()


def confirm_installation() -> bool:
    try:
        answer = input("Czy chcesz zainstalować środowisko lokalnego LLM? [t/N]: ")
    except EOFError:
        return False
    return answer.strip().lower() in CONFIRM_CHOICES


def download_script(url: str, destination: Path) -> None:
    try:
        with urllib.request.urlopen(url) as response, destination.open("wb") as file:
            shutil.copyfileobj(response, file)
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Nie udało się pobrać skryptu z {url}: {exc.reason if hasattr(exc, 'reason') else exc}"
        ) from exc


def ensure_executable(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode | 0o111)


def run_installer(command: Tuple[str, ...], script_path: Path) -> int:
    process = subprocess.run((*command, str(script_path)), check=False)
    return process.returncode


def main() -> int:
    args = parse_args()

    variant = args.variant or detect_variant()
    script_name, command = SCRIPT_VARIANTS[variant]
    script_url = f"{args.base_url.rstrip('/')}/{script_name}"

    cleanup_required = False
    if args.output:
        script_path = args.output.expanduser().resolve()
        script_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        temp_dir = Path(tempfile.mkdtemp(prefix="gpw-llm-setup-"))
        script_path = temp_dir / script_name
        cleanup_required = not args.keep_file

    print(
        textwrap.dedent(
            f"""
            >>> Pobieranie skryptu instalacyjnego ({script_name})
            >>> Źródło: {script_url}
            >>> Lokalizacja pliku: {script_path}
            """
        ).strip()
    )

    try:
        download_script(script_url, script_path)
    except RuntimeError as error:
        print(error, file=sys.stderr)
        return 1

    if variant == "bash":
        ensure_executable(script_path)

    if args.download_only:
        print("Pobrano skrypt. Uruchom go ręcznie, aby zainstalować środowisko LLM.")
        return 0

    if not args.yes and not confirm_installation():
        print("Anulowano instalację na życzenie użytkownika.")
        return 0

    print(">>> Uruchamianie instalatora – proszę czekać…")
    return_code = run_installer(command, script_path)

    if return_code == 0:
        print(">>> Środowisko LLM zostało przygotowane pomyślnie.")
    else:
        print(
            "Instalator zakończył się z kodem błędu",
            return_code,
            "– sprawdź komunikaty powyżej.",
            file=sys.stderr,
        )

    if cleanup_required:
        shutil.rmtree(script_path.parent, ignore_errors=True)

    return return_code


if __name__ == "__main__":
    sys.exit(main())
