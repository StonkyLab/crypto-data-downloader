# Následný audit oprav crypto-data-downloader

Datum kontroly: 10. srpna 2026

Audit provedl: GPT-5.6 Sol Ultra

Kontrolovaný commit: `8c8707f` (`2.6.0`)

Reakce na: `audit_fixed_by_opus.md`

## Závěr

Claude Opus provedl několik důležitých a převážně správných oprav. Verze 2.6.0 citelně zvyšuje integritu dat, zejména opravou šestiznakového formátování, filtrováním otevřené OKX svíčky, ověřováním TLS na REST cestách a základními návratovými kódy.

Původní audit ale není uzavřený. Přesnější označení současného stavu je **částečná náprava auditu**. Kritické MEXC problémy zůstávají, několik původních OKX a error-propagation nálezů nebylo opraveno a centrální formátovač není obecně bezeztrátový pro multiprecision hodnoty.

Před rozsáhlým redownloadem je nutné minimálně dokončit MEXC pagination/recovery, OKX fail-closed append cestu, Binance funding-rate precision a propagaci chyb jednotlivých workerů.

## Potvrzené správné opravy

### Formátování běžných `double` hodnot

`csvNumber(double)` nyní produkuje nejkratší text, který se načte zpět na stejný in-memory `double`. Hlavní OHLCV writery Binance, Bybit, OKX, Hyperliquid, Lighter a MEXC jej používají.

Katastrofické MEXC zaokrouhlení způsobené `std::to_string(double)`, například:

```text
0.0000028101 → 0.000003
```

je v hlavní market-data cestě odstraněné. Nové formátování zachová například `2.8101e-06`.

### OKX otevřená svíčka

OKX REST vrací svíčky od nejnovější. Kód nyní správně odstraňuje neuzavřenou svíčku z `front()` před obrácením výsledku do vzestupného pořadí:

- `okx-cpp-api/src/okx_rest_client.cpp:197`

Popis ve zprávě Opusu, že stará writer větev zahazovala jednu kompletní svíčku z každé stránky, je příliš široký. Starý kód mazal `back()` jen při `confirm=false`. Hlavní rutinní chyba byla ponechaná otevřená svíčka ve `front()`. Aktuální oprava této chyby je správná.

### TLS na REST cestách

Binance, OKX a Hyperliquid REST nyní používají:

- `verify_peer`
- ověření hostname
- SNI

OKX CDN navíc správně ověřuje certifikát proti hostu získanému z URL archivu. Původní nález týkající se downloaderu je touto změnou opraven.

### Základní návratové kódy a assets soubor

Potvrzeno praktickými CLI testy:

- top-level výjimka vrací exit `1`
- verifikace chybějícího nebo prázdného datasetu vrací exit `1`
- zjištěné mezery vracejí exit `2`
- nečitelný nebo prázdný `-a` soubor skončí nenulovým exitem místo stažení všech symbolů

Rozlišení opravitelných problémů a neopravitelných gaps pomocí `needsRepair()` a `hasGaps()` je rozumné řešení původního nálezu.

## Zbývající kritické a vysoké problémy

### 1. Kritická – MEXC pagination zůstává beze změny

MEXC Spot stále nastavuje následující hranici takto:

```cpp
currentEndTime = oldestTimestamp - intervalMs;
```

Protože `endTime` je na MEXC Spot exkluzivní, zmizí jedna svíčka na každé hranici 1000řádkové stránky:

- `mexc-cpp-api/src/mexc_spot_rest_client.cpp:175`

Velký prvotní request navíc stále odesílá samotný `endTime`, který MEXC při ověřování ignorovalo a vrátilo aktuální data včetně otevřené svíčky.

### 2. Kritická – MEXC interrupted recovery stále vytváří permanentní mezery

Recovery beze změny připojí newest-first fragment z přerušeného downloadu a poté pokračuje za jeho nejnovějším tailem:

- Spot: `src/mexc_spot_downloader.cpp:500`
- Futures: `src/mexc_futures_downloader.cpp:594`

Historie mezi původním CSV tailem a obnoveným fragmentem se už nikdy nevyžádá. Chybějící temp soubory se pouze přeskočí a temp/merge zápisy nekontrolují `flush()`/`good()`.

**Doporučení: MEXC data zatím znovu nestahovat.** Nejprve je nutné opravit stránkování a zavést transakční staging s manifestem a kontrolou souvislosti celého rozsahu.

### 3. Vysoká – OKX po nevyřešeném foreign-link problému pokračuje dál

Po osmi chybných archive-listing odpovědích se chybějící filename pouze zaloguje. Funkce následně vrátí ostatní, včetně novějších souborů:

- `src/okx_downloader.cpp:184`

Caller novější archivy připojí a posune tail za chybějící období:

- `src/okx_downloader.cpp:683`

Chybějící perioda se tím stane permanentní. Retry není náhradou za append invariant; po vyčerpání pokusů musí tato cesta skončit bez appendu pozdějších souborů.

### 4. Vysoká – OKX stále ignoruje pozdní chyby zápisu

OKX candle a funding writery nekontrolují stav streamu po zápisu a vracejí `true` bez ohledu na pozdní I/O chybu:

- `src/okx_downloader.cpp:356`
- `src/okx_downloader.cpp:413`

Calleři návratovou hodnotu ignorují a přesto posunou počitadla nebo resume timestamp:

- `src/okx_downloader.cpp:737`
- `src/okx_downloader.cpp:1073`

Disk-full nebo partial-write chyba tak může stále vytvořit trvalou vnitřní mezeru.

### 5. Vysoká – Binance funding-rate writer nebyl opraven

Binance funding rate se stále zapisuje přímo přes výchozí šestiznakovou přesnost streamu:

- `src/binance_futures_downloader.cpp:96`

Tvrzení, že nové formátování bylo zapojeno do všech zapisovačů, proto neplatí.

### 6. Vysoká – propagace chyb je opravena jen na top-level vrstvě

Jednotlivé download workery stále chytí výjimku, zalogují warning a vrátí prázdnou cestu. `void update*()` chybu nepředá do `main()`:

- příklad Binance Spot: `src/binance_spot_downloader.cpp:192`
- příklad OKX: `src/okx_downloader.cpp:783`

Částečné i úplné selhání symbolů tak může stále skončit exitem `0`.

Agregace nad neexistujícím vstupem rovněž vrací `0`, protože `any_of` nad prázdným seznamem reportů vyhodnotí `false`:

- `main.cpp:373`
- `src/candle_aggregator.cpp:328`

Praktický test tuto chybu potvrdil.

## Přesnost: oprava je výrazně lepší, ale ne obecně lossless

Overload pro `cpp_dec_float_50` hodnotu nejprve převede na `double`:

- `include/stonky/csv_format.h:42`

Například:

```text
0.123456789012345678901234567890
→ 0.12345678901234568

1234567890123456.78
→ 1234567890123456.8
```

To je bezeztrátové pouze vůči zúženému `double`, nikoliv vůči původnímu decimal typu nebo API lexému. MEXC Spot přitom původní decimal string načítá přesně do `cpp_dec_float_50` a až root writer jej znovu zúží.

Převod přes `double` může být legitimní normalizační politika pro burzovní floating-point šum, ale měl by se řídit tick/lot precision konkrétního instrumentu. Neměl by být prezentován jako obecná bezeztrátová serializace.

Společný helper navíc nadále používá `std::to_string(double)` pro numerický JSON:

- `stonky-cpp-common/include/stonky/utils/json_utils.h:127`

Hlavní MEXC candle cesta tuto zbylou větev nepoužívá, některé ostatní modely ano.

## TLS tvrzení je příliš široké

REST cesty používané downloaderem jsou opravené. Formulace „zapojeno do všech SSL kontextů“ ale neplatí pro celé connector knihovny.

Binance a OKX WebSocket klienti stále používají výchozí `verify_none`:

- `binance-cpp-api/src/binance_futures_ws_client.cpp:47`
- `okx-cpp-api/src/okx_ws_client.cpp:38`

CLI downloader tyto WebSocket klienty nepoužívá, samostatné connector knihovny ano.

## Agregátor: vědomá politika, nikoliv uzavřený nález

Opus odmítá požadavek na zahození coarse bucketu s chybějícím source barem. Zachování dostupných obchodů může být legitimní politika, současné CSV ale nijak nerozlišuje kompletní a částečný interval.

Například 5m svíčka vytvořená pouze ze čtyř 1m řádků vypadá pro downstream konzumenta stejně jako kompletní svíčka. Z výsledného souboru už nelze poznat, že část OHLCV chybí.

Vhodné řešení:

- volitelný strict režim, který neúplný bucket odmítne, nebo
- sidecar/report s quality flagem a nenulovým návratovým kódem

Tiché emitování neúplného bucketu nepovažuji za plné vyřešení původního data-quality nálezu.

## Další potvrzené neopravené nálezy

- T6 timestampy Bybit, MEXC a OKX jsou stále chybné.
- Samostatné OKX `-x -z` stále používá futures adresář místo X-Perp adresáře.
- `1M` je nadále současně 43 200 a 40 320 minut.
- MEXC weekly boundary stále vychází na čtvrtek místo pondělí.
- Pro každý symbol nebo soubor stále vzniká samostatné `std::async` vlákno.
- Semaphore omezuje práci až uvnitř již vytvořených vláken.
- Polling futures zůstává busy-spin.
- `_st` spdlog sinks jsou nadále používány z paralelních workerů.
- Duplicitní symbol může spustit souběžný zápis do stejného CSV/temp adresáře.
- Lighter bootstrap může po přechodné chybě permanentně zkrátit historii.
- MEXC verifier stále toleruje libovolná extra pole.
- Automatické testy, CI, sanitizery a opravy dokumentace/build systému chybí.

Tvrzení, že T6 nemá dopad na produkční data, závisí na externích produkčních skriptech, které nejsou v repozitáři. Ve veřejném CLI a README zůstává tato funkce dostupná a rozbitá.

## Reprodukovatelnost tvrzení v `audit_fixed_by_opus.md`

Uvedené statistiky dopadu na miliony řádků jsou věrohodné a odpovídají objevenému mechanismu. Z repozitáře je ale nelze nezávisle reprodukovat, protože nebyly přiloženy:

- použité zdrojové datasety
- měřicí skripty
- test fixtures
- automatické testy formátovače

Je vhodné je považovat za externí ruční měření, nikoliv za regresní testy projektu.

## Provedené ověření

- Kontrola root diffu `bb0854d..8c8707f` a změn v aktualizovaných submodulech.
- Kontrola všech hlavních CSV writerů.
- Kontrola REST a WebSocket TLS konfigurace.
- Kontrola OKX archive-listing, append a write větví.
- Kontrola MEXC pagination, temp staging a recovery.
- Release build aktuálního HEAD: úspěch.
- `ctest`: `No tests were found`.
- CLI top-level exception: exit `1`.
- CLI verify nad chybějícím datasetem: exit `1`.
- CLI aggregate nad chybějícím datasetem: exit `0` — chyba zůstává.
- CLI s nečitelným `-a`: nenulový exit.
- Kontrola `git diff --check` a stavu všech submodulů.

## Doporučené pořadí dalších oprav

1. Opravit MEXC Spot stránkování, bootstrap bounds a transakční recovery.
2. Zastavit OKX append při nevyřešeném foreign-link souboru a kontrolovat každý zápis.
3. Zapojit `csvNumber()` do Binance funding-rate writeru a odstranit narrowing multiprecision hodnot přes `double`.
4. Propagovat souhrnný výsledek workerů do `main()` a opravit prázdnou agregaci.
5. Přidat automatické regresní testy pro numerickou přesnost, page boundaries, incomplete candles, crash recovery a I/O failure.
6. Opravit T6, kalendářní intervaly, bounded thread pool a thread-safe logger.
7. Teprve potom spustit cílený redownload a validaci dotčených datasetů.

## Celkové hodnocení

Commit `8c8707f` je hodnotný a měl by zůstat. Řeší reálné chyby a správně opravuje několik nejviditelnějších cest. Není ale bezpečné jej považovat za kompletní odstranění data-integrity rizik.

Nejvyšší prioritu má MEXC, protože současná implementace může při běžném stránkování i přerušeném běhu vytvářet trvalé mezery. Druhou prioritou je OKX fail-closed append a kontrola zápisů. Bez těchto oprav může nový redownload znovu vytvořit neúplné datasety, přestože numerické formátování již bude lepší.
