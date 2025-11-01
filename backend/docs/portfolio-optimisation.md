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
| `features` | `[{"name": str, "weight": float}]` | Lista cech rankingu wraz z wagami (opcjonalnie). |
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

- `momentum` – całkowita stopa zwrotu w analizowanym oknie.
- `volatility` – odwrotność zmienności dziennych stóp zwrotu (im mniejsza
  zmienność, tym większa wartość).
- `average_volume` – średni wolumen obrotu.

Wagi są normalizowane i muszą sumować się do 1.0. W przypadku braku jawnie
podanych wag system rozdziela je po równo.
