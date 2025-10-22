# GPW Analytics Windows Agent

Lokalna aplikacja okienkowa umożliwiająca pobieranie danych z GPW i eksport do ClickHouse. Poniższe kroki pozwalają uruchomić ją jednym kliknięciem w systemie Windows 11.

## Szybkie uruchomienie (instalacja skrótu)

1. Otwórz **PowerShell** i przejdź do katalogu `backend/windows_agent` repozytorium.
2. Uruchom skrypt budujący aplikację:

   ```powershell
   ./build.ps1
   ```

   Skrypt:

   - tworzy izolowane środowisko wirtualne,
   - instaluje zależności (wraz z `pyinstaller`),
   - buduje aplikację graficzną do katalogu `dist/GPWAnalyticsAgent`,
   - odtwarza plik ikony na podstawie zakodowanego zasobu tekstowego,
   - dodaje skrót **„GPW Analytics Agent”** do menu Start z dedykowaną ikoną.

3. Po zakończeniu w menu Start pojawi się wpis „GPW Analytics Agent”. Kliknięcie skrótu uruchomi program bez potrzeby korzystania z terminala.

> Możesz przekazać parametr `-OutputDir`, aby skopiować gotowy katalog aplikacji w inne miejsce, np. `./build.ps1 -OutputDir "C:\\Program Files\\GPW Agent"`.

## Aktualizacja wyglądu okna

Interfejs wykorzystuje motyw w stylu Windows 11: nowa typografia (Segoe UI), zaokrąglone karty i przyciski akcentowe. Wersja okna dostępna po zbudowaniu aplikacji od razu korzysta z odświeżonego motywu.

## Ręczne uruchomienie (bez budowania)

Jeśli chcesz tylko uruchomić aplikację podczas pracy nad kodem:

```bash
python backend/windows_agent/app.py
```

Program zapisuje konfigurację w katalogu `%APPDATA%\GPWAnalyticsAgent` (Windows) lub `~/.gpw_analytics_agent` (inne systemy).
