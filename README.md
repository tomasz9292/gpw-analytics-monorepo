# gpw-analytics-monorepo

## Development

### Lokalny start w Dockerze ("one click")

Minimalne wymagania to działający Docker Desktop (lub Docker Engine + `docker compose`).
Po świeżym `git clone` wykonaj:

1. `cd gpw-analytics-monorepo`
2. Jedno polecenie dla swojego systemu:
   - Linux/macOS: `./scripts/run_local_env.sh`
   - Windows: `powershell -ExecutionPolicy Bypass -File .\\scripts\\run_local_env.ps1`

Skrypty automatycznie kopiują `.env.local.example` → `.env.local` przy pierwszym uruchomieniu
i startują `docker compose -f docker-compose.local-dev.yml up --build`.
Po kilku chwilach usługi będą dostępne pod adresami:

| Komponent  | Adres lokalny                                |
| ---------- | -------------------------------------------- |
| Frontend   | http://localhost:3000                        |
| Backend API| http://localhost:8000/api/admin/ping         |
| ClickHouse | http://localhost:8123 (login `default` / hasło `clickhouse`) |

Jeśli potrzebujesz innych portów lub poświadczeń, zaktualizuj `.env.local`
(wartości domyślne znajdują się w `.env.local.example`).

### Cloning the repository locally

If the codebase is currently only on GitHub and you need to create a working copy on your computer, you can use the helper script provided in `scripts/setup_local_repo.py`:

```bash
python scripts/setup_local_repo.py https://github.com/ORG/REPO.git C:\\sciezka\\do\\folderu
```

The script verifies that Git is installed, clones the repository into the destination directory (defaults to `gpw-analytics-monorepo` in the current working directory), and can optionally configure your Git identity:

```bash
python scripts/setup_local_repo.py https://github.com/ORG/REPO.git --user-name "Twoje Imię" --user-email "twoj.email@example.com"
```

You can pass `--branch nazwa-galezi` if you want to check out a non-default branch immediately after cloning.  To avoid mistakes such as running the environment creation and activation commands on the same PowerShell line (e.g. typing `py -3.12 -m venv .venv → .venv\Scripts\activate`), the helper can now set up a virtual environment for you:

```bash
python scripts/setup_local_repo.py https://github.com/ORG/REPO.git C:\\sciezka\\do\\folderu --create-venv --python "C:\\Users\\tomas\\AppData\\Local\\Programs\\Python\\Python312\\python.exe"
```

The `--create-venv` flag prepares a virtual environment (default name `.venv`) and prints the exact activation command for your platform.  Run the printed activation command as a **separate** step.  After activation install backend dependencies with:

```powershell
pip install -r backend/requirements.txt
```

### Local ClickHouse Sync

Use `./scripts/local-sync.sh` to start ClickHouse, build backend images, and expose the FastAPI server locally. The script waits until `http://localhost:8000/api/admin/ping` responds before streaming backend logs. Stop the services with `Ctrl+C`.

> **Windows tip:** PowerShell nie uruchamia plików z bieżącego katalogu bez prefiksu.
> Wpisz `cd scripts` i uruchom skrypt w powłoce bash (`Git Bash`, `WSL`).
> Jeśli po wpisaniu `bash` otrzymujesz komunikat *«execvpe(/bin/bash) failed»*,
> doinstaluj powłokę: zainstaluj [Git for Windows](https://gitforwindows.org/) i otwórz
> menu Start → **Git Bash**, albo włącz WSL poleceniem `wsl --install` (wymagany restart).
> W obu przypadkach przejdź do katalogu repozytorium i wykonaj `./scripts/local-sync.sh`.

For details on how the backend orchestrates historical OHLC imports or how frontend synchronization statuses are surfaced, look at the backend `api/offline_export.py` module and the frontend `AnalyticsDashboard` component.

### Windows desktop agent

The repository ships with a Tkinter desktop application that you can pin to the Windows taskbar or place on the desktop as a shortcut. It bundles three tasks in a single window:

1. **Download data** – choose GPW tickers, decide whether to fetch historical quotes, company profiles or news, and pick the output directory for the generated CSV/JSON files. Random pauses are added automatically so public data sources treat the traffic as human-like.
2. **Secure database connection** – the “Połącz z bazą danych” tab lets you enter ClickHouse host, port, database and table names. Passwords are stored safely in the Windows Credential Manager via the `keyring` library; the remaining fields live in `%APPDATA%\GPWAnalyticsAgent\config.json`.
3. **Export to ClickHouse** – after downloading data you can test the connection and push the last batch of files to your ClickHouse Cloud (or local) instance. Tables are created automatically if they are missing.

Launch the desktop agent with an activated virtual environment:

```powershell
python backend\windows_agent\app.py
```

The application remembers the last configuration, so once you drag the script to your desktop and create a shortcut, starting the workflow becomes a two-click operation.
