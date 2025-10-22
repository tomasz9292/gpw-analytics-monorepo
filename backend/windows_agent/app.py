"""Tkinter application for downloading GPW data and exporting it to ClickHouse."""

from __future__ import annotations

import base64
import binascii
import csv
import json
import os
import random
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import clickhouse_connect
import keyring
import requests
from clickhouse_connect.driver import Client as ClickHouseClient
from clickhouse_connect.driver.exceptions import OperationalError
from tkinter import (
    BooleanVar,
    END,
    IntVar,
    StringVar,
    Tk,
    filedialog,
    messagebox,
)
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

from api.company_ingestion import CompanyDataHarvester
from api.stooq_news import StooqCompanyNewsHarvester
from api.stooq_ohlc import OhlcRow, StooqOhlcHarvester
from api.symbols import DEFAULT_OHLC_SYNC_SYMBOLS, normalize_input_symbol

def _get_base_path() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent


BASE_PATH = _get_base_path()
RESOURCES_DIR = BASE_PATH / "resources"
ICON_B64_PATH = RESOURCES_DIR / "gpw_agent_icon.b64"

if os.name == "nt":
    appdata = os.getenv("APPDATA")
    CONFIG_DIR = Path(appdata) / "GPWAnalyticsAgent" if appdata else Path.home() / "GPWAnalyticsAgent"
else:
    CONFIG_DIR = Path.home() / ".gpw_analytics_agent"

ICON_CACHE_PATH = CONFIG_DIR / "gpw-agent.ico"
CONFIG_FILE = CONFIG_DIR / "config.json"
KEYRING_SERVICE = "GPWAnalyticsAgent"
DEFAULT_OUTPUT_DIR = Path.home() / "Documents" / "GPW Analytics"
DEFAULT_NEWS_LIMIT = 30


@dataclass
class DbConfig:
    host: str = "localhost"
    port: int = 8123
    database: str = "default"
    username: str = "default"
    use_https: bool = False
    table_ohlc: str = "ohlc"
    table_companies: str = "companies"
    table_news: str = "company_news"

    def to_json(self) -> Dict[str, object]:
        data = asdict(self)
        data.pop("use_https", None)
        data["scheme"] = "https" if self.use_https else "http"
        return data

    @classmethod
    def from_json(cls, payload: Dict[str, object]) -> "DbConfig":
        scheme = payload.get("scheme", "http")
        use_https = str(scheme).lower() == "https"
        return cls(
            host=str(payload.get("host", "localhost")),
            port=int(payload.get("port", 8123)),
            database=str(payload.get("database", "default")),
            username=str(payload.get("username", "default")),
            use_https=use_https,
            table_ohlc=str(payload.get("table_ohlc", "ohlc")),
            table_companies=str(payload.get("table_companies", "companies")),
            table_news=str(payload.get("table_news", "company_news")),
        )


@dataclass
class DownloadResults:
    ohlc: List[OhlcRow] = field(default_factory=list)
    companies: List[Dict[str, object]] = field(default_factory=list)
    news: List[Dict[str, object]] = field(default_factory=list)
    output_files: List[Path] = field(default_factory=list)

    def clear(self) -> None:
        self.ohlc.clear()
        self.companies.clear()
        self.news.clear()
        self.output_files.clear()


class DownloadError(RuntimeError):
    """Raised when download fails."""


class App:
    def __init__(self) -> None:
        self.root = Tk()
        self.root.title("GPW – Agent pobierania danych")
        self.root.geometry("980x720")
        self.root.minsize(860, 640)
        self._configure_styles()
        self._icon_path = self._prepare_icon()
        if self._icon_path:
            try:
                self.root.iconbitmap(default=str(self._icon_path))
            except Exception:
                pass

        self.agent_id = f"agent-{uuid.uuid4().hex[:6]}"
        self.download_thread: Optional[threading.Thread] = None
        self.export_thread: Optional[threading.Thread] = None
        self.running = False

        self.output_dir_var = StringVar(value=str(DEFAULT_OUTPUT_DIR))
        self.symbols_var = StringVar(value=", ".join(DEFAULT_OHLC_SYNC_SYMBOLS))
        self.start_date_var = StringVar(value="")
        self.end_date_var = StringVar(value="")
        self.fetch_history_var = BooleanVar(value=True)
        self.fetch_companies_var = BooleanVar(value=True)
        self.fetch_news_var = BooleanVar(value=False)
        self.news_limit_var = IntVar(value=DEFAULT_NEWS_LIMIT)
        self.random_delay_var = BooleanVar(value=True)

        self.db_host_var = StringVar()
        self.db_port_var = IntVar(value=8123)
        self.db_database_var = StringVar(value="default")
        self.db_username_var = StringVar(value="default")
        self.db_https_var = BooleanVar(value=False)
        self.db_table_ohlc_var = StringVar(value="ohlc")
        self.db_table_companies_var = StringVar(value="companies")
        self.db_table_news_var = StringVar(value="company_news")
        self.db_password_cache: Optional[str] = None

        self.results = DownloadResults()

        self._load_config()
        self._build_ui()
        self._log("Agent gotowy. Wybierz zakres danych i kliknij 'Pobierz dane'.")

    def _prepare_icon(self) -> Optional[Path]:
        if os.name != "nt":
            return None
        icon_bytes = self._load_icon_bytes()
        if not icon_bytes:
            return None
        cache_path = ICON_CACHE_PATH
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        try:
            if cache_path.exists():
                try:
                    if cache_path.read_bytes() == icon_bytes:
                        return cache_path
                except OSError:
                    pass
            cache_path.write_bytes(icon_bytes)
            return cache_path
        except OSError:
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".ico") as temp_icon:
                    temp_icon.write(icon_bytes)
                    return Path(temp_icon.name)
            except Exception:
                return None
        return None

    def _load_icon_bytes(self) -> Optional[bytes]:
        if not ICON_B64_PATH.exists():
            return None
        try:
            encoded = ICON_B64_PATH.read_text(encoding="utf-8")
        except OSError:
            return None
        payload = "".join(encoded.split())
        if not payload:
            return None
        try:
            return base64.b64decode(payload)
        except (binascii.Error, ValueError):
            return None

    def _configure_styles(self) -> None:
        accent = "#2563eb"
        neutral_bg = "#f3f4f6"
        surface_bg = "#ffffff"
        text_color = "#111827"

        self.root.configure(background=neutral_bg)
        self.root.option_add("*Font", "Segoe UI 10")
        self.root.option_add("*TButton.padding", 6)
        self.root.option_add("*TButton.relief", "flat")
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("TFrame", background=neutral_bg)
        style.configure("Content.TFrame", background=neutral_bg, padding=8)
        style.configure("Card.TLabelframe", background=surface_bg, borderwidth=1, relief="solid", padding=16)
        style.configure("Card.TLabelframe.Label", background=surface_bg, foreground=text_color, font=("Segoe UI Semibold", 10))
        style.configure("TLabel", background=surface_bg, foreground=text_color)
        style.configure("Card.TFrame", background=surface_bg, padding=8)
        style.configure("TNotebook", background=neutral_bg, borderwidth=0, padding=4)
        style.configure("TNotebook.Tab", padding=(16, 10), font=("Segoe UI Semibold", 10))
        style.map(
            "TNotebook.Tab",
            background=[("selected", surface_bg), ("!selected", neutral_bg)],
            foreground=[("selected", text_color)],
        )
        style.configure("TButton", background=surface_bg, foreground=text_color, borderwidth=0, focuscolor=neutral_bg)
        style.map(
            "TButton",
            background=[("active", "#e5e7eb"), ("pressed", "#d1d5db")],
            relief=[("pressed", "sunken"), ("!pressed", "flat")],
        )
        style.configure(
            "Accent.TButton",
            background=accent,
            foreground="#ffffff",
            borderwidth=0,
            focusthickness=3,
            focuscolor="#bfdbfe",
            padding=(18, 10),
        )
        style.map(
            "Accent.TButton",
            background=[("active", "#1d4ed8"), ("pressed", "#1e3a8a")],
        )
        style.configure("TCheckbutton", background=surface_bg, foreground=text_color, padding=6)
        style.map("TCheckbutton", background=[("active", "#e5e7eb")])
        style.configure("TEntry", fieldbackground="#ffffff", padding=8)
        style.configure("TSpinbox", fieldbackground="#ffffff", padding=8)
        style.configure("TLabelframe", background=surface_bg)

    # ------------------------------------------------------------------
    # UI
    def _build_ui(self) -> None:
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True, padx=16, pady=16)

        downloads_frame = ttk.Frame(notebook, style="Content.TFrame")
        notebook.add(downloads_frame, text="Pobieranie danych")
        self._build_download_tab(downloads_frame)

        db_frame = ttk.Frame(notebook, style="Content.TFrame")
        notebook.add(db_frame, text="Połączenie z bazą danych")
        self._build_db_tab(db_frame)

        log_frame = ttk.LabelFrame(self.root, text="Log zdarzeń", style="Card.TLabelframe")
        log_frame.pack(fill="both", expand=True, padx=16, pady=(0, 16))
        self.log_text = ScrolledText(log_frame, height=12, wrap="word", state="disabled", font=("Consolas", 10))
        self.log_text.pack(fill="both", expand=True, padx=8, pady=8)
        self.log_text.configure(background="#ffffff", foreground="#111827", insertbackground="#111827")

    def _build_download_tab(self, parent: ttk.Frame) -> None:
        container = ttk.Frame(parent, style="Content.TFrame")
        container.pack(fill="both", expand=True)

        options = ttk.LabelFrame(container, text="Zakres pobierania", style="Card.TLabelframe")
        options.pack(fill="x", padx=4, pady=8)

        ttk.Label(options, text="Lista spółek (oddzielone przecinkami)").grid(row=0, column=0, sticky="w")
        ttk.Entry(options, textvariable=self.symbols_var, width=80).grid(row=1, column=0, columnspan=3, sticky="ew", pady=4)

        ttk.Checkbutton(options, text="Notowania historyczne", variable=self.fetch_history_var).grid(row=2, column=0, sticky="w", pady=2)
        ttk.Checkbutton(options, text="Profile spółek", variable=self.fetch_companies_var).grid(row=2, column=1, sticky="w", pady=2)
        ttk.Checkbutton(options, text="Wiadomości", variable=self.fetch_news_var).grid(row=2, column=2, sticky="w", pady=2)

        ttk.Label(options, text="Data od (RRRR-MM-DD)").grid(row=3, column=0, sticky="w", pady=(8, 2))
        ttk.Label(options, text="Data do (RRRR-MM-DD)").grid(row=3, column=1, sticky="w", pady=(8, 2))
        ttk.Label(options, text="Limit wiadomości").grid(row=3, column=2, sticky="w", pady=(8, 2))

        ttk.Entry(options, textvariable=self.start_date_var, width=18).grid(row=4, column=0, sticky="w")
        ttk.Entry(options, textvariable=self.end_date_var, width=18).grid(row=4, column=1, sticky="w")
        ttk.Spinbox(options, from_=5, to=500, textvariable=self.news_limit_var, width=8).grid(row=4, column=2, sticky="w")

        ttk.Checkbutton(options, text="Losowe opóźnienia między zapytaniami", variable=self.random_delay_var).grid(row=5, column=0, columnspan=2, sticky="w", pady=(8, 0))

        options.columnconfigure(0, weight=1)
        options.columnconfigure(1, weight=1)
        options.columnconfigure(2, weight=1)

        destination = ttk.LabelFrame(container, text="Katalog wynikowy", style="Card.TLabelframe")
        destination.pack(fill="x", padx=4, pady=8)
        ttk.Entry(destination, textvariable=self.output_dir_var, width=80).grid(row=0, column=0, sticky="ew", padx=(0, 8), pady=4)
        ttk.Button(destination, text="Wybierz...", command=self._choose_output_dir).grid(row=0, column=1, pady=4)
        destination.columnconfigure(0, weight=1)

        actions = ttk.Frame(container, style="Card.TFrame")
        actions.pack(fill="x", padx=4, pady=(0, 8))
        ttk.Button(actions, text="Pobierz dane", command=self._start_download, style="Accent.TButton").pack(side="left")
        ttk.Button(actions, text="Otwórz katalog", command=self._open_output_dir).pack(side="left", padx=(8, 0))

    def _build_db_tab(self, parent: ttk.Frame) -> None:
        container = ttk.Frame(parent, style="Content.TFrame")
        container.pack(fill="both", expand=True)

        connection = ttk.LabelFrame(container, text="Parametry połączenia", style="Card.TLabelframe")
        connection.pack(fill="x", padx=4, pady=8)

        labels = [
            ("Adres hosta", self.db_host_var),
            ("Port", self.db_port_var),
            ("Baza danych", self.db_database_var),
            ("Użytkownik", self.db_username_var),
            ("Tabela notowań", self.db_table_ohlc_var),
            ("Tabela profili", self.db_table_companies_var),
            ("Tabela wiadomości", self.db_table_news_var),
        ]

        for idx, (label, var) in enumerate(labels):
            ttk.Label(connection, text=label).grid(row=idx, column=0, sticky="w", pady=3)
            entry_kwargs = {"textvariable": var, "width": 30}
            ttk.Entry(connection, **entry_kwargs).grid(row=idx, column=1, sticky="ew", pady=3)

        ttk.Checkbutton(connection, text="Szyfrowane połączenie (HTTPS)", variable=self.db_https_var).grid(row=len(labels), column=0, columnspan=2, sticky="w", pady=(8, 0))

        ttk.Button(connection, text="Wprowadź hasło", command=self._prompt_password).grid(row=len(labels) + 1, column=0, pady=12, sticky="w")
        ttk.Button(connection, text="Zapisz konfigurację", command=self._save_config, style="Accent.TButton").grid(row=len(labels) + 1, column=1, pady=12, sticky="e")

        actions = ttk.Frame(container, style="Card.TFrame")
        actions.pack(fill="x", padx=4, pady=(0, 8))
        ttk.Button(actions, text="Przetestuj połączenie", command=self._test_connection).pack(side="left")
        ttk.Button(actions, text="Eksportuj ostatnie dane", command=self._start_export).pack(side="left", padx=(8, 0))

    # ------------------------------------------------------------------
    # Helpers
    def _choose_output_dir(self) -> None:
        directory = filedialog.askdirectory(initialdir=self.output_dir_var.get())
        if directory:
            self.output_dir_var.set(directory)
            self._log(f"Zmieniono katalog na: {directory}")

    def _open_output_dir(self) -> None:
        path = Path(self.output_dir_var.get())
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        try:
            if os.name == "nt":
                os.startfile(str(path))  # type: ignore[attr-defined]
            else:
                import subprocess

                subprocess.Popen(["xdg-open", str(path)])
        except Exception as exc:  # pragma: no cover - platform specific
            messagebox.showerror("Błąd", f"Nie udało się otworzyć katalogu: {exc}")

    def _log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        self.log_text.insert(END, f"[{timestamp}] {message}\n")
        self.log_text.configure(state="disabled")
        self.log_text.see(END)

    def _collect_symbols(self) -> List[str]:
        symbols = []
        for chunk in self.symbols_var.get().replace(";", ",").split(","):
            symbol = normalize_input_symbol(chunk.strip())
            if symbol:
                symbols.append(symbol)
        if not symbols:
            symbols = list(DEFAULT_OHLC_SYNC_SYMBOLS)
        return sorted(dict.fromkeys(symbols))

    # ------------------------------------------------------------------
    # Configuration
    def _load_config(self) -> None:
        if CONFIG_FILE.exists():
            try:
                payload = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
                config = DbConfig.from_json(payload)
                self.db_host_var.set(config.host)
                self.db_port_var.set(config.port)
                self.db_database_var.set(config.database)
                self.db_username_var.set(config.username)
                self.db_https_var.set(config.use_https)
                self.db_table_ohlc_var.set(config.table_ohlc)
                self.db_table_companies_var.set(config.table_companies)
                self.db_table_news_var.set(config.table_news)
                self.db_password_cache = keyring.get_password(KEYRING_SERVICE, config.username)
            except Exception as exc:
                self._log(f"Nie udało się wczytać konfiguracji: {exc}")
        else:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    def _save_config(self) -> None:
        config = DbConfig(
            host=self.db_host_var.get().strip() or "localhost",
            port=int(self.db_port_var.get() or 8123),
            database=self.db_database_var.get().strip() or "default",
            username=self.db_username_var.get().strip() or "default",
            use_https=bool(self.db_https_var.get()),
            table_ohlc=self.db_table_ohlc_var.get().strip() or "ohlc",
            table_companies=self.db_table_companies_var.get().strip() or "companies",
            table_news=self.db_table_news_var.get().strip() or "company_news",
        )
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        CONFIG_FILE.write_text(json.dumps(config.to_json(), indent=2), encoding="utf-8")
        self._log("Zapisano konfigurację połączenia.")
        if self.db_password_cache:
            keyring.set_password(KEYRING_SERVICE, config.username, self.db_password_cache)

    def _prompt_password(self) -> None:
        from tkinter.simpledialog import askstring

        current = self.db_password_cache or ""
        password = askstring("Hasło", "Podaj hasło użytkownika", show="*", initialvalue=current)
        if password is not None:
            self.db_password_cache = password
            keyring.set_password(KEYRING_SERVICE, self.db_username_var.get().strip(), password)
            self._log("Zapisano hasło w Menedżerze poświadczeń systemu.")

    # ------------------------------------------------------------------
    # Downloading
    def _start_download(self) -> None:
        if self.download_thread and self.download_thread.is_alive():
            messagebox.showinfo("Pobieranie", "Proces pobierania już trwa.")
            return
        self.download_thread = threading.Thread(target=self._download_worker, daemon=True)
        self.download_thread.start()

    def _download_worker(self) -> None:
        self.results.clear()
        symbols = self._collect_symbols()
        self._log(f"Rozpoczynam pobieranie dla: {', '.join(symbols)}")
        start_date = self._parse_date(self.start_date_var.get())
        end_date = self._parse_date(self.end_date_var.get())
        output_dir = Path(self.output_dir_var.get())
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            if self.fetch_history_var.get():
                self._download_ohlc(symbols, start_date, end_date, output_dir)
            if self.fetch_companies_var.get():
                self._download_companies(symbols, output_dir)
            if self.fetch_news_var.get():
                self._download_news(symbols, output_dir)
            self._log("Pobieranie zakończone powodzeniem.")
        except DownloadError as exc:
            self._log(str(exc))
            messagebox.showerror("Błąd", str(exc))

    def _download_ohlc(
        self, symbols: List[str], start: Optional[date], end: Optional[date], output_dir: Path
    ) -> None:
        harvester = StooqOhlcHarvester()
        rows: List[OhlcRow] = []
        for symbol in symbols:
            self._log(f"Pobieram notowania {symbol}")
            try:
                history = harvester.fetch_history(symbol)
            except requests.HTTPError as exc:
                raise DownloadError(f"Błąd pobierania notowań {symbol}: {exc}") from exc
            filtered = [row for row in history if self._is_in_range(row.date, start, end)]
            rows.extend(filtered)
            self._maybe_sleep()
        if not rows:
            raise DownloadError("Brak danych notowań w podanym zakresie.")
        rows.sort(key=lambda row: (row.symbol, row.date))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = output_dir / f"ohlc_{timestamp}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["symbol", "date", "open", "high", "low", "close", "volume"])
            for row in rows:
                writer.writerow(
                    [
                        row.symbol,
                        row.date.isoformat(),
                        row.open or "",
                        row.high or "",
                        row.low or "",
                        row.close or "",
                        row.volume or "",
                    ]
                )
        self._log(f"Zapisano notowania do pliku {csv_path}")
        self.results.ohlc = rows
        self.results.output_files.append(csv_path)

    def _download_companies(self, symbols: List[str], output_dir: Path) -> None:
        harvester = CompanyDataHarvester()
        rows: List[Dict[str, object]] = []
        for symbol in symbols:
            self._log(f"Pobieram profil spółki {symbol}")
            try:
                profile = harvester.fetch_stooq_profile(symbol)
            except Exception as exc:
                raise DownloadError(f"Błąd pobierania profilu {symbol}: {exc}") from exc
            founded = profile.get("founded")
            employees = profile.get("employees")
            rows.append(
                {
                    "symbol": symbol,
                    "company_name": profile.get("companyName"),
                    "short_name": profile.get("shortName"),
                    "isin": profile.get("isin"),
                    "website": profile.get("website"),
                    "listing_date": profile.get("listing_date"),
                    "founded": self._to_int(founded),
                    "employees": self._to_int(employees),
                    "profile": profile.get("profile"),
                    "source_url": profile.get("url"),
                    "retrieved_at": datetime.utcnow().isoformat(),
                }
            )
            self._maybe_sleep()
        if not rows:
            raise DownloadError("Nie udało się pobrać profili spółek.")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = output_dir / f"companies_{timestamp}.json"
        json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        self._log(f"Zapisano profile spółek do pliku {json_path}")
        self.results.companies = rows
        self.results.output_files.append(json_path)

    def _download_news(self, symbols: List[str], output_dir: Path) -> None:
        harvester = StooqCompanyNewsHarvester()
        rows: List[Dict[str, object]] = []
        limit = max(1, int(self.news_limit_var.get()))
        for symbol in symbols:
            self._log(f"Pobieram wiadomości {symbol}")
            try:
                news_items = harvester.fetch_news(symbol, limit=limit)
            except Exception as exc:
                raise DownloadError(f"Błąd pobierania wiadomości {symbol}: {exc}") from exc
            for item in news_items:
                rows.append(
                    {
                        "symbol": symbol,
                        "title": item.title,
                        "url": item.url,
                        "published_at": item.published_at,
                        "source": "Stooq",
                    }
                )
            self._maybe_sleep()
        if not rows:
            raise DownloadError("Nie znaleziono wiadomości dla wybranych spółek.")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = output_dir / f"news_{timestamp}.json"
        json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        self._log(f"Zapisano wiadomości do pliku {json_path}")
        self.results.news = rows
        self.results.output_files.append(json_path)

    def _maybe_sleep(self) -> None:
        if self.random_delay_var.get():
            delay = random.uniform(1.0, 3.5)
            self._log(f"Oczekiwanie {delay:.1f}s aby nie przeciążać serwisu...")
            time.sleep(delay)

    # ------------------------------------------------------------------
    # Export
    def _start_export(self) -> None:
        if self.export_thread and self.export_thread.is_alive():
            messagebox.showinfo("Eksport", "Trwa poprzedni eksport danych.")
            return
        if not (self.results.ohlc or self.results.companies or self.results.news):
            messagebox.showwarning("Eksport", "Najpierw pobierz dane, aby mieć co wysłać do bazy.")
            return
        self.export_thread = threading.Thread(target=self._export_worker, daemon=True)
        self.export_thread.start()

    def _export_worker(self) -> None:
        try:
            client = self._create_clickhouse_client()
        except OperationalError as exc:
            self._log(f"Błąd połączenia z ClickHouse: {exc}")
            messagebox.showerror("ClickHouse", f"Nie udało się połączyć: {exc}")
            return
        try:
            self._export_ohlc(client)
            self._export_companies(client)
            self._export_news(client)
            self._log("Eksport zakończony powodzeniem.")
            messagebox.showinfo("Eksport", "Dane wysłano do bazy ClickHouse.")
        except Exception as exc:
            self._log(f"Błąd eksportu: {exc}")
            messagebox.showerror("Eksport", f"Wystąpił błąd: {exc}")
        finally:
            client.close()

    def _export_ohlc(self, client: ClickHouseClient) -> None:
        if not self.results.ohlc:
            return
        table = self._sanitize_identifier(self.db_table_ohlc_var.get().strip() or "ohlc")
        rows = [
            [
                row.symbol,
                row.date,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
            ]
            for row in self.results.ohlc
        ]
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                symbol String,
                date Date,
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                close Nullable(Float64),
                volume Nullable(Float64)
            )
            ENGINE = MergeTree()
            ORDER BY (symbol, date)
            """
        )
        client.insert(table, rows, column_names=["symbol", "date", "open", "high", "low", "close", "volume"])
        self._log(f"Wysłano {len(rows)} rekordów notowań do tabeli {table}")

    def _export_companies(self, client: ClickHouseClient) -> None:
        if not self.results.companies:
            return
        table = self._sanitize_identifier(self.db_table_companies_var.get().strip() or "companies")
        columns = [
            "symbol",
            "company_name",
            "short_name",
            "isin",
            "website",
            "listing_date",
            "founded",
            "employees",
            "profile",
            "source_url",
            "retrieved_at",
        ]
        rows = [[row.get(column) for column in columns] for row in self.results.companies]
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                symbol String,
                company_name Nullable(String),
                short_name Nullable(String),
                isin Nullable(String),
                website Nullable(String),
                listing_date Nullable(String),
                founded Nullable(Int32),
                employees Nullable(Int32),
                profile Nullable(String),
                source_url Nullable(String),
                retrieved_at Nullable(String)
            )
            ENGINE = MergeTree()
            ORDER BY symbol
            """
        )
        client.insert(table, rows, column_names=columns)
        self._log(f"Wysłano {len(rows)} rekordów profili do tabeli {table}")

    def _export_news(self, client: ClickHouseClient) -> None:
        if not self.results.news:
            return
        table = self._sanitize_identifier(self.db_table_news_var.get().strip() or "company_news")
        columns = ["symbol", "title", "url", "published_at", "source"]
        rows = [[row.get(column) for column in columns] for row in self.results.news]
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                symbol String,
                title String,
                url String,
                published_at Nullable(String),
                source Nullable(String)
            )
            ENGINE = MergeTree()
            ORDER BY (symbol, published_at)
            """
        )
        client.insert(table, rows, column_names=columns)
        self._log(f"Wysłano {len(rows)} wiadomości do tabeli {table}")

    def _test_connection(self) -> None:
        try:
            client = self._create_clickhouse_client()
        except OperationalError as exc:
            messagebox.showerror("ClickHouse", f"Nie udało się połączyć: {exc}")
            self._log(f"Test połączenia nieudany: {exc}")
            return
        try:
            version = client.command("SELECT version()")
            messagebox.showinfo("ClickHouse", f"Połączono. Wersja serwera: {version}")
            self._log(f"Połączenie działa (wersja {version}).")
        finally:
            client.close()

    def _create_clickhouse_client(self) -> ClickHouseClient:
        host = self.db_host_var.get().strip() or "localhost"
        port = int(self.db_port_var.get() or 8123)
        database = self.db_database_var.get().strip() or "default"
        username = self.db_username_var.get().strip() or "default"
        scheme = "https" if self.db_https_var.get() else "http"
        password = self.db_password_cache or keyring.get_password(KEYRING_SERVICE, username) or ""
        if not password:
            raise OperationalError("Brak hasła w magazynie poświadczeń. Wybierz 'Wprowadź hasło'.")
        return clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            interface="https" if scheme == "https" else "http",
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_date(value: str) -> Optional[date]:
        value = (value or "").strip()
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _is_in_range(current: date, start: Optional[date], end: Optional[date]) -> bool:
        if start and current < start:
            return False
        if end and current > end:
            return False
        return True

    @staticmethod
    def _to_int(value: object) -> Optional[int]:
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip().replace(" ", "")
        try:
            return int(float(text))
        except ValueError:
            return None

    @staticmethod
    def _sanitize_identifier(value: str) -> str:
        cleaned = "".join(ch for ch in value if ch.isalnum() or ch == "_")
        return cleaned or "data"


def main() -> None:
    app = App()
    app.root.mainloop()


if __name__ == "__main__":
    main()
