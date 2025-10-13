## GPW Analytics frontend

Interfejs panelu analitycznego wykorzystującego Next.js 15 (App Router) zintegrowany z backendem FastAPI. Aplikacja udostępnia analizę techniczną, konfigurator rankingów oraz symulator portfela. Stan użytkownika (lista obserwowanych, szablony score, ostatnie konfiguracje) może być zapisywany w koncie Google.

## Szybki start

```bash
npm install
npm run dev
```

Domyślnie aplikacja startuje pod adresem [http://localhost:3000](http://localhost:3000).

## Logowanie przez Google

1. Utwórz identyfikator OAuth 2.0 typu "Web" w konsoli Google Cloud.
2. Skonfiguruj zmienne środowiskowe (np. w pliku `.env.local`):

   ```bash
   NEXT_PUBLIC_GOOGLE_CLIENT_ID="twoj-client-id.apps.googleusercontent.com"
   AUTH_SECRET="dowolny_silny_klucz_sesji"
   ```

   `AUTH_SECRET` służy do podpisywania ciasteczek sesji – w środowisku produkcyjnym ustaw długi, losowy ciąg.

3. W konfiguracji identyfikatora OAuth w konsoli Google uzupełnij pola:

   | Pole w konsoli Google                     | Wartość dla środowiska lokalnego                     | Wartość produkcyjna (Vercel)                |
   | ---------------------------------------- | ---------------------------------------------------- | ------------------------------------------- |
   | Autoryzowane źródła JavaScript           | `http://localhost:3000`                              | `https://gpw-frontend.vercel.app`*          |
   | Autoryzowane identyfikatory URI przekierowania | *(pozostaw puste – przepływ tokenowy GIS nie używa przekierowania)* | *(pozostaw puste)* |

   \* Zastąp docelową domeną produkcyjną, jeżeli aplikacja działa pod innym adresem.

4. Po zalogowaniu preferencje użytkownika są zapisywane w pliku `data/users.json` (ścieżka ignorowana przez Gita). Dane synchronizują się automatycznie przy zmianach w konfiguratorach.

Jeżeli `NEXT_PUBLIC_GOOGLE_CLIENT_ID` nie jest ustawiony, przycisk logowania zostanie dezaktywowany.

## Struktura API

- `app/api/auth/google` – obsługa logowania z tokenem Google i tworzenie sesji HTTP-only.
- `app/api/auth/session` – szybki podgląd bieżącej sesji (używany przez panel).
- `app/api/auth/logout` – wylogowanie i czyszczenie sesji.
- `app/api/account/profile` – odczyt i zapis preferencji użytkownika (lista obserwowanych, szablony score, konfiguracje portfela).

Logowanie korzysta z usług Google Identity Services ładowanych po stronie klienta (`https://accounts.google.com/gsi/client`).

## Dane lokalne

Preferencje użytkownika są odkładane w `data/users.json`. Plik tworzony jest automatycznie i ignorowany przez repozytorium. Możesz go ręcznie usunąć, aby "wyczyścić" konta testowe.

## Budowanie i lint

```bash
npm run build    # produkcyjny build Next.js
npm run lint     # statyczna analiza kodu
```
