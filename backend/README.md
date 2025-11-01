# GPW Analytics backend

## Szybki start (środowisko lokalne)

### Wariant automatyczny (Docker Compose)

> Wymagania: Docker + Docker Compose.

1. Uruchom z katalogu głównego repozytorium:

   ```bash
   ./scripts/local-sync.sh
   ```

   Skrypt zbuduje obrazy, wystartuje ClickHouse i backend oraz poczeka aż
   `http://localhost:8000/api/admin/ping` zacznie odpowiadać. W trakcie działania
   pokaże logi backendu. Zakończ pracę kombinacją <kbd>Ctrl</kbd> + <kbd>C</kbd> —
   środowisko zostanie automatycznie zatrzymane i wyczyszczone.

2. W panelu administracyjnym kliknij **„Uruchom lokalnie”**, aby rozpocząć
   synchronizację danych historycznych.

> Domyślnie dane trafiają do lokalnej instancji ClickHouse uruchomionej w
> kontenerze. Jeżeli chcesz zapisywać rekordy bezpośrednio w istniejącym
> ClickHouse Cloud, ustaw zmienne połączeniowe i uruchom skrypt w trybie cloud:

```bash
export LOCAL_SYNC_CLICKHOUSE_URL="https://<twoja-instancja>.aws.clickhouse.cloud:8443/default?secure=1"
export CLICKHOUSE_USER="<user>"
export CLICKHOUSE_PASSWORD="<haslo>"
./scripts/local-sync.sh --cloud
```

> Możesz użyć także standardowych zmiennych środowiskowych backendu (`CLICKHOUSE_URL`,
> `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, itd.). Skrypt zweryfikuje, czy adres
> ClickHouse został podany przed startem. W trybie cloud kontener z ClickHouse nie jest
> uruchamiany — działa tylko backend FastAPI zapisujący dane do chmury.

### Wariant manualny (bez Dockera)

1. **Zainstaluj zależności Pythona** (najlepiej wirtualne środowisko):

   ```bash
   cd backend
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Uruchom ClickHouse** – backend zapisuje dane do tej bazy. Najprościej
   skorzystać z Dockera:

   ```bash
   docker run --rm \
     --name clickhouse \
     -p 8123:8123 -p 9000:9000 \
     clickhouse/clickhouse-server:23
   ```

   Po uruchomieniu endpoint HTTP ClickHouse będzie dostępny pod
   `http://localhost:8123`.

3. **Start backendu FastAPI na porcie 8000**. Przekaż adres ClickHouse poprzez
   zmienną środowiskową `CLICKHOUSE_URL` i włącz serwer:

   ```bash
   export CLICKHOUSE_URL="http://localhost:8123/default"
   uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
   ```

   Serwer nasłuchuje teraz pod `http://localhost:8000`. Możesz sprawdzić zdrowie
   aplikacji odwiedzając `http://localhost:8000/ping` (lub `/api/admin/ping`).

## Integracja z frontendem

- Panel Next.js domyślnie rozmawia z produkcyjnym API. Aby przełączyć go na
  lokalny backend ustaw zmienną środowiskową przed uruchomieniem frontendu:

  ```bash
  export NEXT_PUBLIC_API_BASE="http://localhost:8000"
  ```

- W widoku synchronizacji przycisk **„Uruchom lokalnie”** wysyła żądania na
  `http://localhost:8000/api/admin/...`. Backend udostępnia teraz wszystkie
  endpointy również pod tym prefiksem, więc wystarczy uruchomić go jak wyżej i
  zalogować się w panelu jako administrator.
- Panel udostępnia kartę **„Konfiguracja lokalnego ClickHouse”**, w której
  można podać adres URL lub ręczne parametry połączenia z ClickHouse Cloud.
  Formularz wysyła dane do endpointu `POST /api/admin/config/clickhouse`, a
  po zapisaniu kolejne kliknięcia „Uruchom lokalnie” automatycznie korzystają z
  przekazanej konfiguracji. Aktualne ustawienia można podejrzeć i zresetować
  do wartości środowiskowych tym samym panelem.

## Narzędzia pomocnicze

### Lista symboli z `_list_candidate_symbols`

Do szybkiego wypisania kandydatów dla danego uniwersum możesz użyć skryptu
`scripts/list_candidate_symbols.py`. Skrypt wymaga poprawnie ustawionych
zmiennych połączeniowych do ClickHouse, tak jak backend.

#### Linux / macOS (powłoka bash)

```bash
cd backend
python scripts/list_candidate_symbols.py --universe index:WIG40 --pretty
```

Jeżeli chcesz od razu zobaczyć podstawowe informacje z tabeli `companies`
(nazwa, ISIN, sektor, branża), dodaj przełącznik `--with-company-info`:

```bash
cd backend
python scripts/list_candidate_symbols.py --universe index:WIG40 --pretty --with-company-info
```

#### Windows (PowerShell)

```powershell
cd backend
python .\scripts\list_candidate_symbols.py --universe index:WIG40 --pretty
```

Opcjonalnie możesz wzbogacić wynik o dane z tabeli `companies`:

```powershell
cd backend
python .\scripts\list_candidate_symbols.py --universe index:WIG40 --pretty --with-company-info
```

Parametr `--universe` możesz podać wielokrotnie, np. `--universe index:WIG40
--universe isin:PLLOTOS00025`. Domyślnie skrypt drukuje JSON z listą tickerów;
flaga `--pretty` wypisze jeden symbol na linię z numeracją.

Skrypt korzysta z historycznych składów indeksów (`index:`) tak jak backend.
Jeżeli chcesz ograniczyć się wyłącznie do najnowszego składu, dodaj
`--no-include-index-history` (analogiczny przełącznik działa również w bashu).



## Ranking portfela i optymalizacja

Szczegółowy opis API portfelowego znajduje się w [docs/portfolio-optimisation.md](docs/portfolio-optimisation.md).
