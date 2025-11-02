# Portfolio ranking and optimisation API

Nowa funkcjonalność backendu pozwala budować ranking spółek na bazie danych OHLC i
zasymulować portfel równoważony według najlepszych wyników. Moduł współpracuje z
lokalnymi modelami LLM (`llama.cpp`), dzięki czemu parametry rankingu mogą być
optymalizowane przy użyciu mocy karty graficznej.

## Endpoint

```
POST /api/admin/portfolio/optimise
```

> Endpoint jest dostępny również bez prefiksu `/api/admin`.

### Parametry żądania (`application/json`)

| Pole | Typ | Opis |
| ---- | --- | ---- |
| `symbols` | `List[str]` | Lista symboli do analizy. |
| `start_date` | `YYYY-MM-DD` | Początek okna czasowego dla danych OHLC. |
| `end_date` | `YYYY-MM-DD` | Koniec okna czasowego. |
| `top_n` | `int` | Liczba spółek wybieranych do portfela (domyślnie 5). |
| `initial_cash` | `float` | Wartość początkowa portfela (domyślnie 100 000). |
| `features` | `[{"name": str, "weight": float}]` | Opcjonalne wagi kompatybilnościowe dla prostego rankingu. |
| `score_components` | `[{...}]` | Lista komponentów score (pola: `feature`, `metric`, `lookback_days`, `weight`, `direction`, `min_value`, `max_value`, `normalize`, `scoring`). |
| `score_filters` | `object` | Filtry wszechświata (jak w `/backtest/portfolio`). |
| `score_weighting` | `"equal"/"score"` | Sposób ważenia portfela opartego o ranking. |
| `score_direction` | `"asc"/"desc"` | Kierunek sortowania score (domyślnie `desc`). |
| `score_min_score` | `float` | Minimalny score (opcjonalnie). |
| `score_max_score` | `float` | Maksymalny score (opcjonalnie). |
| `score_universe_fallback` | `List[str]` | Lista symboli zapasowych dla wszechświata (opcjonalnie). |
| `rebalance` | `str` | Częstotliwość rebalansingu (`none`, `monthly`, `quarterly`, `yearly`). |
| `fee_pct` | `float` | Koszt transakcyjny (np. 0.001 = 0,1%). |
| `threshold_pct` | `float` | Próg rebalansingu (np. 0.05 = 5%). |
| `benchmark` | `str` | Opcjonalny symbol benchmarku. |
| `enable_llm` | `bool` | Włączenie optymalizacji LLM. |
| `llm_model_path` | `str` | Ścieżka do lokalnego pliku modelu GGML/GGUF dla `llama.cpp`. |
| `llm_iterations` | `int` | Liczba iteracji optymalizacji (domyślnie 3). |
| `llm_temperature` | `float` | Temperatura próbkowania modelu. |
| `llm_max_tokens` | `int` | Limit tokenów na odpowiedź. |
| `llm_gpu_layers` | `int` | Liczba warstw trzymanych na GPU (opcjonalnie). |

### Odpowiedź

Zwracany obiekt zawiera:

- `top_symbols` – listę symboli wybranych do portfela.
- `ranking` – uporządkowane pozycje wraz z wartościami cech oraz wynikami
  znormalizowanymi.
- `simulation` – metryki portfela (stopa zwrotu, drawdown, przebieg dzienny).
- `optimisation` – historię iteracji LLM (jeżeli optymalizacja była włączona).

## Integracja z lokalnym LLM

Moduł korzysta z biblioteki `llama-cpp-python`. Aby włączyć optymalizację:

1. Zainstaluj pakiet w środowisku backendu, np. `pip install llama-cpp-python`.
2. Pobierz model w formacie GGUF/GGML kompatybilny z `llama.cpp`.
3. Uruchom endpoint z `enable_llm=true` oraz `llm_model_path` wskazującym na model.
4. Opcjonalnie ustaw `llm_gpu_layers`, aby część obliczeń wykonywała się na GPU.

LLM przegląda historię dotychczasowych wyników i sugeruje nowe zestawy wag cech.
Każda iteracja kończy się ponowną symulacją portfela i aktualizacją najlepszego
znalezionego rozwiązania.

## Dostępne cechy rankingu

Ranking korzysta z tych samych komponentów, które udostępnia moduł score w
panelu (`/backtest/portfolio`). Można łączyć metryki (np. `total_return`,
`max_drawdown`, `volatility`, `roc`, `price_change`) z różnymi oknami czasowymi,
wagami, normalizacją (`none` lub `percentile`) oraz skalowaniem typu
`linear_clamped`. Dzięki temu jeden endpoint obsługuje zarówno klasyczny
ranking oparty na kilku wskaźnikach, jak i złożone konfiguracje budowane w
konfiguratorze score.
