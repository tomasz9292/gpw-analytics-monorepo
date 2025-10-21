# GPW Analytics backend

## Szybki start (środowisko lokalne)

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

