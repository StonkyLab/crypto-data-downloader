# Oprava parametru `--since`

Datum kontroly a opravy: 2026-08-11
Výchozí implementace: commit `cc29af2` (`Add --since to floor how far back a fresh symbol reaches`)

## Výsledný kontrakt

Parametr `--since` nyní bezpečně podporuje workflow, při kterém se staré aktivní
CSV přesune nebo zkomprimuje a downloader má vytvořit novou živou část historie:

- chybějící, prázdné nebo header-only CSV používá `since` jako **inkluzivní**
  spodní hranici;
- první dostupný záznam s timestampem přesně rovným `since` se zachová;
- CSV obsahující alespoň jeden validní záznam vždy pokračuje **exkluzivně za
  svým skutečným tailem**;
- pozdější `since` proto nikdy nesmí přeskočit úsek za existujícím tailem a
  vytvořit mezeru;
- nezarovnaný candle timestamp se tam, kde to API vyžaduje, posune na první
  platný candle open, který není starší než `since`.

## Provedené opravy

### Striktní CLI parser

Původní parser přijímal neplatné hodnoty jako `2026-02-31`,
`2026-01-01junk` nebo `1700000000000junk`. Nový testovatelný parser:

- přijímá pouze přesné `YYYY-MM-DD` v UTC nebo celé nezáporné epoch
  milliseconds;
- kontroluje platnost kalendářního dne i úplné spotřebování vstupu;
- odmítá whitespace, znaménka, suffixy, overflow, epoch/pre-epoch a budoucí
  timestampy;
- nepoužívá normalizaci neplatného data přes `tm`.

Implementace je v `include/stonky/since_parser.h`, zapojení v `main.cpp`.

### Fresh hranice versus existující tail

Byla přidána společná resume politika v `include/stonky/download_resume.h`,
která zachovává informaci, zda timestamp pochází ze skutečného CSV řádku, nebo
jde pouze o fresh fallback.

Tím byla opravena hranice pro:

- Bybit candles a funding;
- Hyperliquid candles a funding;
- Lighter candles a funding;
- Binance Futures funding.

Binance candles již pracovaly správně, protože API filtruje podle open time,
zatímco první CSV sloupec obsahuje close time. OKX candles i funding již
správně zachovávaly `TailCheck::foundValid`; jejich hraniční logika zůstala
beze změny.

Hyperliquid nyní aplikuje přibližné retention okno 5 000 barů pouze na fresh
download a používá `max(since, retentionStart)`. Existující CSV vždy pokračuje
ze svého tailu. Lighter daily listing probe se u raw millisecond timestampu
zaokrouhlí dolů pouze pro účely probe; skutečný download zůstává omezen přesným
`since`, takže se nepřeskočí zbytek prvního dne.

### MEXC candles a recovery markery

Spot i Futures nyní:

- zarovnávají fresh raw-ms floor na první candle open `>= since`, včetně
  týdenních a kalendářních měsíčních intervalů;
- při `since` novějším než poslední uzavřená svíčka provedou bezpečný fresh
  no-op místo chyby;
- po archivaci odstraní orphan `<symbol>.csv.prefix.pending` marker;
- pokud zůstal živý suffix s markerem, marker se atomicky odstraní, když suffix
  již floor pokrývá, nebo se přebázuje na nový floor;
- marker proto nemůže obnovit úmyslně archivovanou historii před `since` ani
  přeskočit požadovaný interval.

### MEXC funding

Funding cesta již nepoužívá nový floor přes existující lokální tail:

- fresh soubor používá inkluzivní `max(exchangeDefault, since)`;
- existující soubor používá hranici nejvýše rovnou svému skutečnému tailu;
- záznam přesně na fresh cutoff se zachová;
- prázdný filtrovaný výsledek je no-op pouze pro existující data, zatímco fresh
  běh bez dat nehlásí falešné dokončení;
- newest-first pagination se po validaci metadat a pořadí zastaví, jakmile
  nejstarší řádek stránky klesne pod cutoff. Equality se před zastavením uloží.

Tím je odstraněna původní možnost trvalé mezery mezi starším CSV tailem a
pozdějším `since`.

### Archivované OKX symboly

OKX režim `all` nyní umí z názvu individuálně komprimovaného souboru znovu
objevit delistovaný symbol. Podporované názvy jsou:

- `<symbol>.csv.gz`
- `<symbol>.csv.xz`
- `<symbol>.csv.bz2`
- `<symbol>.csv.zst`

Kontejnerové archivy jako `tar.gz` nebo ZIP se neprohledávají, protože samotný
název bezpečně neurčuje jejich obsah. Pro ně je potřeba dodat symboly přes `-s`
nebo `-a`. Downloader komprimované archivy při `--delete-delisted` automaticky
nemaže.

### Další hardening

Process-wide history floor používá atomické úložiště. Produkční kontrakt zůstává
„nastavit jednou před spuštěním workerů“, ale veřejný getter/setter již nemůže
způsobit datový race.

README a ChangeLog byly aktualizovány o nový kontrakt, archivní příklad,
podporované OKX názvy a chování MEXC markerů.

## Regresní testy

Byly přidány nebo rozšířeny deterministické testy pro:

- striktní parsing platných a neplatných hodnot `since`;
- inkluzivní fresh a exkluzivní persisted hranici;
- ochranu skutečného tailu před pozdějším floorem;
- Hyperliquid retention a Lighter day-floor helper;
- názvy individuálně komprimovaných CSV;
- MEXC fixed/week/calendar zarovnání;
- orphan, covered a rebased MEXC prefix marker;
- fresh/existing MEXC funding cutoff, equality, prázdný výsledek a ukončení
  pagination pod cutoffem.

## Ověření

- kompletní Release build: **PASS**;
- CTest: **13/13 PASS**;
- `git diff --check`: **PASS**;
- manuální CLI ověření:
  - `2026-02-31`: odmítnuto;
  - `2026-01-01junk`: odmítnuto;
  - `1700000000000junk`: odmítnuto;
  - platné datum a platné epoch milliseconds: přijato;
- nezávislá adversariální kontrola výsledného diffu: **CLEAN**, bez nalezeného
  blockeru nebo regresní mezery.

Testy jsou offline a deterministické; v rámci finálního ověření nebyly volány
živé burzovní endpointy.

## Provozní použití

Příklad:

```bash
crypto_data_downloader -e okx -o /data/okx --since 2024-01-01
```

`since` se začne uplatňovat poté, co je aktivní CSV přesunuto, odstraněno,
vyprázdněno nebo ponecháno pouze s hlavičkou. Pokud aktivní CSV stále obsahuje
validní data, downloader záměrně pokračuje z jeho tailu a `since` jej neposune
dopředu.

Změny jsou připravené ve workspace; tento report sám neprovádí commit.
