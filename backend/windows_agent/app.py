"""Tkinter desktop agent for orchestrating GPW data downloads on Windows."""

from __future__ import annotations

import csv
import json
import queue
import random
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from tkinter import (  # noqa: F401 - exported for typing of Tk widgets
    BooleanVar,
    DISABLED,
    END,
    IntVar,
    NORMAL,
    StringVar,
    Tk,
    filedialog,
    messagebox,
)
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

from api.company_ingestion import (
    CompanyDataHarvester,
    GPW_COMPANY_PROFILES_FALLBACK_URL,
    GPW_COMPANY_PROFILES_URL,
    STOOQ_COMPANY_CATALOG_URL,
    STOOQ_COMPANY_PROFILE_URL,
    YAHOO_QUOTE_SUMMARY_URL,
)
from api.stooq_news import NewsItem, StooqCompanyNewsHarvester
from api.stooq_ohlc import OhlcRow, StooqOhlcHarvester
from api.symbols import DEFAULT_OHLC_SYNC_SYMBOLS, normalize_input_symbol


DEFAULT_API_URL = "http://localhost:8000/api/admin"
DEFAULT_OUTPUT_DIR = Path.home() / "Documents" / "gpw-agent-output"
USER_AGENT_CANDIDATES = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Edge/120.0 Chrome/120.0 Safari/537.36",
)


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _parse_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _normalize_symbols(values: Iterable[str]) -> List[str]:
    cleaned: List[str] = []
    for item in values:
        symbol = normalize_input_symbol(item)
        if symbol:
            cleaned.append(symbol)
    return sorted(dict.fromkeys(cleaned))


def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


@dataclass
class TaskResult:
    kind: str
    files: List[Path]
    metadata: Dict[str, Any]


class ApiClient:
    """Simple HTTP client talking to the FastAPI admin endpoints."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        url = f"{self.base_url}{path}"
        response = requests.request(method, url, timeout=20, **kwargs)
        return response

    def fetch_next_job(self, agent_id: str) -> Optional[Dict[str, Any]]:
        response = self._request("GET", "/windows-agent/jobs/next", params={"agent_id": agent_id})
        if response.status_code == 204:
            return None
        response.raise_for_status()
        return response.json()

    def update_job_status(
        self,
        job_id: str,
        *,
        agent_id: str,
        status: str,
        progress: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"status": status, "agent_id": agent_id}
        if progress:
            payload["progress"] = progress
        if details is not None:
            payload["details"] = details
        response = self._request("POST", f"/windows-agent/jobs/{job_id}/status", json=payload)
        response.raise_for_status()
        return response.json()


class WindowsAgentApp:
    def __init__(self) -> None:
        self.root = Tk()
        self.root.title("GPW Analytics – Agent Windows")
        self.root.geometry("980x760")

        self.api_url_var = StringVar(value=DEFAULT_API_URL)
        self.agent_id_var = StringVar(value=f"agent-{uuid.uuid4().hex[:6]}")
        self.poll_interval_var = IntVar(value=45)
        self.output_dir_var = StringVar(value=str(DEFAULT_OUTPUT_DIR))

        self.manual_history_var = BooleanVar(value=True)
        self.manual_company_var = BooleanVar(value=False)
        self.manual_news_var = BooleanVar(value=False)
        self.manual_symbols_var = StringVar(value=", ".join(DEFAULT_OHLC_SYNC_SYMBOLS))
        self.manual_start_date_var = StringVar(value="")
        self.manual_end_date_var = StringVar(value="")
        self.manual_news_limit_var = IntVar(value=20)

        self.status_var = StringVar(value="Tryb ręczny – gotowy")
        self.queue: "queue.Queue[str]" = queue.Queue()
        self.stop_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None
        self.running = False

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(200, self._poll_queue)

    # ---------------------------
    # UI helpers
    # ---------------------------

    def _build_ui(self) -> None:
        main_frame = ttk.Frame(self.root, padding=12)
        main_frame.pack(fill="both", expand=True)

        connection = ttk.LabelFrame(main_frame, text="Połączenie z panelem synchronizacji", padding=12)
        connection.pack(fill="x", expand=False)

        self._add_labeled_entry(
            connection,
            label="Adres API backendu",
            variable=self.api_url_var,
            row=0,
            columnspan=3,
        )

        self._add_labeled_entry(
            connection,
            label="Identyfikator agenta",
            variable=self.agent_id_var,
            row=1,
        )

        poll_entry = self._add_labeled_entry(
            connection,
            label="Odstęp między próbami (sekundy)",
            variable=self.poll_interval_var,
            row=1,
            column=1,
        )
        poll_entry.configure(width=12)

        output_frame = ttk.Frame(connection)
        output_frame.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(8, 0))
        output_frame.columnconfigure(0, weight=1)

        ttk.Label(output_frame, text="Katalog wyników").grid(row=0, column=0, sticky="w")
        output_entry = ttk.Entry(output_frame, textvariable=self.output_dir_var)
        output_entry.grid(row=1, column=0, sticky="ew", pady=4)
        ttk.Button(output_frame, text="Wybierz...", command=self._choose_directory).grid(
            row=1, column=1, padx=(8, 0)
        )

        buttons = ttk.Frame(connection)
        buttons.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(12, 0))

        self.start_button = ttk.Button(buttons, text="Start automatyczny", command=self.start)
        self.start_button.grid(row=0, column=0)
        self.stop_button = ttk.Button(buttons, text="Stop", command=self.stop, state=DISABLED)
        self.stop_button.grid(row=0, column=1, padx=(8, 0))

        ttk.Label(connection, textvariable=self.status_var).grid(
            row=4, column=0, columnspan=3, sticky="w", pady=(8, 0)
        )

        manual = ttk.LabelFrame(main_frame, text="Ręczne pobieranie danych", padding=12)
        manual.pack(fill="x", expand=False, pady=(12, 0))

        self._build_manual_section(manual)

        log_frame = ttk.LabelFrame(main_frame, text="Logi", padding=12)
        log_frame.pack(fill="both", expand=True, pady=(12, 0))
        self.log_widget = ScrolledText(log_frame, height=18, state=DISABLED)
        self.log_widget.pack(fill="both", expand=True)

    def _build_manual_section(self, container: ttk.LabelFrame) -> None:
        toggles = ttk.Frame(container)
        toggles.grid(row=0, column=0, columnspan=4, sticky="w")

        ttk.Checkbutton(
            toggles,
            text="Notowania historyczne (Stooq)",
            variable=self.manual_history_var,
        ).pack(anchor="w")
        ttk.Checkbutton(
            toggles,
            text="Profile spółek (GPW + Stooq)",
            variable=self.manual_company_var,
        ).pack(anchor="w")
        ttk.Checkbutton(
            toggles,
            text="Wiadomości o spółkach (Stooq)",
            variable=self.manual_news_var,
        ).pack(anchor="w")

        ttk.Label(container, text="Lista tickerów (oddziel przecinkiem lub nową linią)").grid(
            row=1, column=0, columnspan=4, sticky="w", pady=(8, 0)
        )
        self.symbol_entry = ttk.Entry(container, textvariable=self.manual_symbols_var)
        self.symbol_entry.grid(row=2, column=0, columnspan=4, sticky="ew")

        dates_frame = ttk.Frame(container)
        dates_frame.grid(row=3, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        dates_frame.columnconfigure(0, weight=1)
        dates_frame.columnconfigure(1, weight=1)

        ttk.Label(dates_frame, text="Data początkowa (opcjonalnie)").grid(row=0, column=0, sticky="w")
        ttk.Entry(dates_frame, textvariable=self.manual_start_date_var).grid(row=1, column=0, sticky="ew")
        ttk.Label(dates_frame, text="Data końcowa (opcjonalnie)").grid(row=0, column=1, sticky="w")
        ttk.Entry(dates_frame, textvariable=self.manual_end_date_var).grid(row=1, column=1, sticky="ew")

        limit_frame = ttk.Frame(container)
        limit_frame.grid(row=4, column=0, columnspan=4, sticky="w", pady=(8, 0))
        ttk.Label(limit_frame, text="Limit wiadomości na spółkę").pack(side="left")
        ttk.Entry(limit_frame, width=6, textvariable=self.manual_news_limit_var).pack(side="left", padx=(6, 0))

        button_frame = ttk.Frame(container)
        button_frame.grid(row=5, column=0, columnspan=4, sticky="w", pady=(12, 0))
        ttk.Button(button_frame, text="Uruchom ręcznie", command=self.run_manual).pack(side="left")
        ttk.Button(button_frame, text="Przywróć domyślne tickery", command=self._reset_symbols).pack(
            side="left", padx=(8, 0)
        )

    def _add_labeled_entry(
        self,
        container: ttk.LabelFrame,
        *,
        label: str,
        variable: Any,
        row: int,
        column: int = 0,
        columnspan: int = 1,
    ) -> ttk.Entry:
        ttk.Label(container, text=label).grid(row=row, column=column, sticky="w", padx=(0, 8))
        entry = ttk.Entry(container, textvariable=variable)
        entry.grid(row=row, column=column, columnspan=columnspan, sticky="ew", pady=4)
        if columnspan > 1:
            container.columnconfigure(column, weight=1)
        return entry

    # ---------------------------
    # Worker control
    # ---------------------------

    def start(self) -> None:
        if self.running:
            return
        base_url = self.api_url_var.get().strip()
        if not base_url:
            messagebox.showerror("Błąd", "Podaj adres API backendu")
            return
        self.running = True
        self.status_var.set("Tryb automatyczny – oczekiwanie na zlecenia")
        self._log("Aktywacja trybu automatycznego")
        self.stop_event.clear()
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        self.start_button.configure(state=DISABLED)
        self.stop_button.configure(state=NORMAL)

    def stop(self) -> None:
        if not self.running:
            return
        self._log("Zatrzymywanie agenta...")
        self.status_var.set("Zatrzymywanie...")
        self.running = False
        self.stop_event.set()
        thread = self.worker_thread
        if thread and thread.is_alive():
            thread.join(timeout=2.5)
        self.worker_thread = None
        self.start_button.configure(state=NORMAL)
        self.stop_button.configure(state=DISABLED)
        self.status_var.set("Tryb ręczny – gotowy")
        self._log("Agent zatrzymany")

    def _worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                client = ApiClient(self.api_url_var.get())
                job = client.fetch_next_job(self.agent_id_var.get())
            except Exception as exc:  # pragma: no cover - network specific
                self._log(f"[{_now()}] ❌ Błąd połączenia: {exc}")
                if self.stop_event.wait(self.poll_interval_var.get()):
                    break
                continue

            if job is None:
                if self.stop_event.wait(self.poll_interval_var.get()):
                    break
                continue

            job_id = job.get("id", "")
            name = job.get("name") or job_id
            self._log(f"[{_now()}] ▶️ Zlecenie {name}")
            try:
                client.update_job_status(
                    job_id,
                    agent_id=self.agent_id_var.get(),
                    status="running",
                    progress="Zadania rozpoczęte",
                )
            except Exception as exc:  # pragma: no cover - network specific
                self._log(f"[{_now()}] ❌ Nie udało się potwierdzić startu: {exc}")

            try:
                results = self._execute_tasks(job_id, job.get("tasks", []))
            except Exception as exc:  # pragma: no cover - runtime safety
                self._log(f"[{_now()}] ❌ Zlecenie zakończone błędem: {exc}")
                try:
                    client.update_job_status(
                        job_id,
                        agent_id=self.agent_id_var.get(),
                        status="failed",
                        progress=str(exc),
                    )
                except Exception as status_exc:  # pragma: no cover - safety
                    self._log(f"[{_now()}] ⚠️ Nie udało się wysłać statusu błędu: {status_exc}")
            else:
                summary = {
                    key: {
                        "files": [str(path) for path in value.files],
                        "metadata": value.metadata,
                    }
                    for key, value in results.items()
                }
                try:
                    client.update_job_status(
                        job_id,
                        agent_id=self.agent_id_var.get(),
                        status="completed",
                        progress="Zadania zakończone",
                        details=summary,
                    )
                except Exception as exc:  # pragma: no cover - network specific
                    self._log(f"[{_now()}] ⚠️ Nie udało się wysłać podsumowania: {exc}")
                self._log(f"[{_now()}] ✅ Zlecenie {name} zakończone")

        self.stop_event.clear()

    # ---------------------------
    # Manual execution
    # ---------------------------

    def run_manual(self) -> None:
        symbols = self._read_symbol_input()
        if not symbols:
            messagebox.showerror("Błąd", "Podaj przynajmniej jeden ticker")
            return
        tasks: List[Dict[str, Any]] = []
        start_date = _parse_date(self.manual_start_date_var.get())
        end_date = _parse_date(self.manual_end_date_var.get())

        if self.manual_history_var.get():
            tasks.append(
                {
                    "kind": "ohlc_history",
                    "symbols": symbols,
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                }
            )
        if self.manual_company_var.get():
            tasks.append({"kind": "company_profiles", "symbols": symbols})
        if self.manual_news_var.get():
            tasks.append(
                {
                    "kind": "company_news",
                    "symbols": symbols,
                    "limit": max(1, int(self.manual_news_limit_var.get() or 20)),
                }
            )

        if not tasks:
            messagebox.showinfo("Brak zadań", "Zaznacz przynajmniej jeden typ danych")
            return

        self._log(f"[{_now()}] ▶️ Ręczne uruchomienie dla {len(symbols)} spółek")
        try:
            results = self._execute_tasks(f"manual-{uuid.uuid4().hex[:6]}", tasks)
        except Exception as exc:
            self._log(f"[{_now()}] ❌ Ręczne pobranie nie powiodło się: {exc}")
            messagebox.showerror("Błąd", str(exc))
            return

        messages = []
        for key, result in results.items():
            files = "\n".join(str(path) for path in result.files)
            messages.append(f"{key}: {result.metadata.get('items', 0)} rekordów\n{files}")
        messagebox.showinfo("Zakończono", "\n\n".join(messages))
        self._log(f"[{_now()}] ✅ Ręczne pobranie zakończone")

    def _read_symbol_input(self) -> List[str]:
        raw_value = self.manual_symbols_var.get()
        tokens = [token.strip() for token in raw_value.replace("\n", ",").split(",")]
        return _normalize_symbols(token for token in tokens if token)

    # ---------------------------
    # Task execution
    # ---------------------------

    def _execute_tasks(self, job_id: str, tasks: Iterable[Dict[str, Any]]) -> Dict[str, TaskResult]:
        output_dir = Path(self.output_dir_var.get()).expanduser()
        _ensure_directory(output_dir)
        results: Dict[str, TaskResult] = {}
        for task in tasks:
            kind = str(task.get("kind"))
            if kind == "ohlc_history":
                results[kind] = self._run_ohlc_task(job_id, task, output_dir)
            elif kind == "company_profiles":
                results[kind] = self._run_company_task(job_id, task, output_dir)
            elif kind == "company_news":
                results[kind] = self._run_news_task(job_id, task, output_dir)
            else:
                raise RuntimeError(f"Nieznane zadanie: {kind}")
        return results

    def _run_ohlc_task(self, job_id: str, task: Dict[str, Any], output_dir: Path) -> TaskResult:
        symbols = task.get("symbols") or DEFAULT_OHLC_SYNC_SYMBOLS
        symbols_list = _normalize_symbols(symbols)
        start_date = _parse_date(task.get("start_date"))
        end_date = _parse_date(task.get("end_date"))

        session_headers = {
            "User-Agent": random.choice(USER_AGENT_CANDIDATES),
            "Accept": "text/csv, text/plain, */*;q=0.8",
            "Referer": "https://stooq.pl/",
        }
        harvester = StooqOhlcHarvester()
        if hasattr(harvester.session, "headers"):
            harvester.session.headers.update(session_headers)

        rows: List[Dict[str, Any]] = []
        for symbol in symbols_list:
            self._log(f"[{_now()}] Pobieranie notowań {symbol}")
            history = harvester.fetch_history(symbol)
            filtered = self._filter_ohlc_rows(history, start_date, end_date)
            for row in filtered:
                record = {
                    "symbol": row.symbol,
                    "date": row.date.isoformat(),
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                }
                rows.append(record)
            self._polite_pause()

        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        filename = output_dir / f"ohlc_{job_id}_{timestamp}.csv"
        with filename.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["symbol", "date", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            writer.writerows(rows)
        self._log(f"[{_now()}] Zapisano {len(rows)} wierszy notowań do {filename}")
        return TaskResult(
            kind="ohlc_history",
            files=[filename],
            metadata={"items": len(rows), "symbols": symbols_list},
        )

    def _run_company_task(self, job_id: str, task: Dict[str, Any], output_dir: Path) -> TaskResult:
        symbols = task.get("symbols")
        symbols_list = _normalize_symbols(symbols or []) if symbols else None
        harvester = CompanyDataHarvester(
            gpw_url=GPW_COMPANY_PROFILES_URL,
            gpw_fallback_url=GPW_COMPANY_PROFILES_FALLBACK_URL,
            gpw_stooq_url=STOOQ_COMPANY_CATALOG_URL,
            stooq_profile_url_template=STOOQ_COMPANY_PROFILE_URL,
            yahoo_url_template=YAHOO_QUOTE_SUMMARY_URL,
        )
        rows = harvester.fetch_gpw_profiles()
        collected: List[Dict[str, Any]] = []
        for base in rows:
            symbol = harvester._extract_symbol(base)
            if symbols_list and symbol not in symbols_list:
                continue
            stooq_payload = None
            try:
                stooq_payload = harvester.fetch_stooq_profile(symbol)
            except Exception as exc:  # pragma: no cover - remote errors
                self._log(f"[{_now()}] ⚠️ Stooq profil {symbol}: {exc}")
            fundamentals = None
            try:
                fundamentals = harvester.fetch_yahoo_summary(symbol)
            except Exception as exc:  # pragma: no cover - remote errors
                self._log(f"[{_now()}] ⚠️ Yahoo {symbol}: {exc}")
            row = harvester.build_row(base, fundamentals, stooq=stooq_payload)
            row.pop("raw_payload", None)
            row["symbol"] = symbol
            collected.append(row)
            self._polite_pause()

        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        filename = output_dir / f"companies_{job_id}_{timestamp}.json"
        with filename.open("w", encoding="utf-8") as handle:
            json.dump(collected, handle, ensure_ascii=False, indent=2)
        self._log(f"[{_now()}] Zapisano {len(collected)} profili spółek do {filename}")
        return TaskResult(
            kind="company_profiles",
            files=[filename],
            metadata={"items": len(collected)},
        )

    def _run_news_task(self, job_id: str, task: Dict[str, Any], output_dir: Path) -> TaskResult:
        symbols = task.get("symbols") or DEFAULT_OHLC_SYNC_SYMBOLS
        symbols_list = _normalize_symbols(symbols)
        limit = int(task.get("limit") or 20)
        harvester = StooqCompanyNewsHarvester(min_delay=0.6, max_delay=1.8)
        collected: List[Dict[str, Any]] = []
        for symbol in symbols_list:
            self._log(f"[{_now()}] Pobieranie wiadomości {symbol}")
            try:
                news_rows = harvester.fetch_news(symbol, limit=limit)
            except Exception as exc:  # pragma: no cover - remote errors
                self._log(f"[{_now()}] ⚠️ Wiadomości {symbol}: {exc}")
                continue
            for item in news_rows:
                collected.append(self._serialize_news(item))

        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        filename = output_dir / f"news_{job_id}_{timestamp}.json"
        with filename.open("w", encoding="utf-8") as handle:
            json.dump(collected, handle, ensure_ascii=False, indent=2)
        self._log(f"[{_now()}] Zapisano {len(collected)} wiadomości do {filename}")
        return TaskResult(
            kind="company_news",
            files=[filename],
            metadata={"items": len(collected), "limit": limit},
        )

    @staticmethod
    def _filter_ohlc_rows(
        rows: Iterable[OhlcRow],
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> List[OhlcRow]:
        filtered: List[OhlcRow] = []
        for row in rows:
            if start_date and row.date < start_date:
                continue
            if end_date and row.date > end_date:
                continue
            filtered.append(row)
        return filtered

    @staticmethod
    def _serialize_news(item: NewsItem) -> Dict[str, Any]:
        return {
            "symbol": item.symbol,
            "title": item.title,
            "url": item.url,
            "published_at": item.published_at,
        }

    # ---------------------------
    # Misc helpers
    # ---------------------------

    def _choose_directory(self) -> None:
        current = self.output_dir_var.get()
        directory = filedialog.askdirectory(initialdir=current or str(DEFAULT_OUTPUT_DIR))
        if directory:
            self.output_dir_var.set(directory)

    def _reset_symbols(self) -> None:
        self.manual_symbols_var.set(", ".join(DEFAULT_OHLC_SYNC_SYMBOLS))

    def _polite_pause(self) -> None:
        time.sleep(random.uniform(0.8, 2.4))

    def _log(self, message: str) -> None:
        timestamped = message if message.startswith("[") else f"[{_now()}] {message}"
        self.queue.put(timestamped)

    def _poll_queue(self) -> None:
        while not self.queue.empty():
            message = self.queue.get_nowait()
            self.log_widget.configure(state=NORMAL)
            self.log_widget.insert(END, message + "\n")
            self.log_widget.configure(state=DISABLED)
            self.log_widget.see(END)
        self.root.after(250, self._poll_queue)

    def _on_close(self) -> None:
        self.stop()
        self.root.destroy()

    # ---------------------------
    # Public API
    # ---------------------------

    def run(self) -> None:
        self.root.mainloop()


def main() -> None:
    app = WindowsAgentApp()
    app.run()


if __name__ == "__main__":
    main()

