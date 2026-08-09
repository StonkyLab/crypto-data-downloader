# Audit crypto-data-downloader

Datum auditu: 6. srpna 2026  
Audit provedl: GPT-5.6 Sol Ultra

## Výsledek auditu

Projekt se úspěšně sestaví, ale v současném stavu bych vygenerovaným datasetům bez dodatečné kontroly nevěřil. Několik chyb vytváří věrohodně vypadající CSV, která však mohou obsahovat zaokrouhlené hodnoty, trvalé mezery nebo neuzavřené svíčky.

## Nejdůležitější nálezy

### 1. Kritická – nevratná ztráta numerické přesnosti

Binance, Bybit, OKX, Hyperliquid a Lighter zapisují desetinná čísla s výchozí C++ přesností šesti platných číslic. Například `12345.678901` skončí jako `12345.7`. Týká se OHLCV i funding rates:

- `src/binance_common.cpp:111`
- `src/bybit_downloader.cpp:183`
- `src/okx_downloader.cpp:384`
- `src/hyperliquid_downloader.cpp:181`
- `src/lighter_downloader.cpp:232`

MEXC Futures navíc převádí JSON přes `double` a `std::to_string`; například cena `0.0000028101` se změní na `0.000003`:

- `mexc-cpp-api/src/mexc_models.cpp:490`

Oprava musí zachovat původní decimal string nebo použít přesnou decimal reprezentaci. Již zapsané hodnoty nelze lokálně rekonstruovat.

### 2. Kritická – MEXC Spot deterministicky vynechává svíčky

Při backward pagination se další `endTime` nastaví na `oldestTimestamp - interval`. Endpoint ale používá exkluzivní hranici, takže zmizí jedna svíčka na každé hranici 1000řádkové stránky:

- `mexc-cpp-api/src/mexc_spot_rest_client.cpp:187`

Toto chování bylo ověřeno proti veřejnému MEXC API. Správná další hranice je přímo `oldestTimestamp`.

### 3. Kritická – MEXC recovery může vytvořit trvalou prostřední mezeru

MEXC stahuje od nejnovějších dat dozadu a průběžně ukládá temp dávky. Pokud download spadne dřív, než se dostane ke starému CSV tailu, příští běh novější fragment připojí a začne pokračovat až za ním. Celý prostřední rozsah se už nikdy nevyžádá:

- Spot recovery: `src/mexc_spot_downloader.cpp:325`
- Futures recovery: `src/mexc_futures_downloader.cpp:326`

Temp zápisy navíc nekontrolují `flush()`/`good()` a merge umí tiše přeskočit chybějící dávku. Recovery potřebuje transakční staging s manifestem a merge až po ověření souvislosti celého rozsahu.

### 4. Vysoká – MEXC bootstrap může ignorovat historický konec a uložit otevřenou svíčku

U velkého prvního downloadu se odešle samotný `endTime`. Veřejné API při testu tuto historickou hranici ignorovalo a vrátilo aktuální data včetně otevřené svíčky:

- `mexc-cpp-api/src/mexc_spot_rest_client.cpp:100`

Jakmile je neuzavřená svíčka v tailu, append-only režim ji už finální hodnotou nenahradí.

### 5. Vysoká – OKX má několik cest k permanentní díře

- REST výsledek je newest-first, ale kontroluje se `back()`, tedy nejstarší svíčka. Aktuální `confirm=0` zůstane a už se neopraví: `okx-cpp-api/src/okx_rest_client.cpp:197`.
- Nevyřešený chybný odkaz v seznamu archivů se po retry pouze přeskočí a pokračuje se novějšími soubory: `src/okx_downloader.cpp:145`.
- Selhání zápisu se ignoruje a resume timestamp se přesto posune: `src/okx_downloader.cpp:355`.

### 6. Vysoká – TLS autentizace serveru je vypnutá

Binance, OKX a Hyperliquid načítají CA cestu, ale nezapínají `verify_peer` ani ověření hostname:

- `binance-cpp-api/src/binance_http_session.cpp:200`
- `okx-cpp-api/src/okx_http_session.cpp:151`
- `hyperliquid-cpp-api/src/hyperliquid_http_session.cpp:46`

To umožňuje MITM podvrhnout API odpovědi nebo OKX archivy. Bybit a Lighter mají ověřování nastavené správně.

### 7. Vysoká – fatální chyby vracejí exit code 0

Top-level `catch` chybu pouze zaloguje:

- `main.cpp:465`

Stejně dopadne verify/aggregate nad neexistujícím adresářem a chyby uvnitř jednotlivých workerů. Praktické ověření ukázalo, že neimplementovaná operace, neplatný interval i neexistující vstup skončí kritickou hláškou, ale proces vrátí `0`.

Detekované gaps navíc nejsou zahrnuty do `needsRepair()`:

- `include/stonky/csv_verifier.h:63`

Cron, systemd ani CI tedy nemohou výsledku procesu důvěřovat.

### 8. Vysoká – T6 close timestampy jsou chybné

Bybit a MEXC vždy přičítají jednu minutu bez ohledu na timeframe:

- `src/bybit_downloader.cpp:100`
- `src/mexc_spot_downloader.cpp:132`
- `src/mexc_futures_downloader.cpp:139`

OKX používá `_1m`, ale ještě vydělí milisekundy tisícem, takže po následném převodu skončí prakticky na open timestampu:

- `src/okx_downloader.cpp:296`

### 9. Vysoká – agregátor maskuje mezery ve zdrojových datech

Chybějící 1m svíčka uvnitř 5m bucketu nezabrání vytvoření 5m řádku. Výsledkem je zdánlivě platná, ale neúplná agregovaná svíčka:

- `src/candle_aggregator.cpp:270`

Agregátor by měl vyžadovat přesný počet a posloupnost zdrojových timestampů.

### 10. Střední až vysoká – intervaly a paralelismus

- `1M` je interně současně 43 200 a 40 320 minut. Jedna hodnota je odmítnuta, druhá skončí výjimkou: `include/stonky/downloader.h:58`. Kalendářní měsíc navíc nelze korektně modelovat pevnou délkou.
- MEXC týdenní zarovnání používá epoch modulo 7 dní, tedy čtvrtek, zatímco burza otevírá weekly candle v pondělí: `src/mexc_spot_downloader.cpp:482`.
- Pro každý symbol vznikne samostatný `std::async` thread; semaphore omezuje až práci uvnitř již vytvořeného threadu. Při „all symbols“ mohou vzniknout stovky až tisíce vláken.
- Polling futures je busy-spin a single-threaded `_st` logger se používá z workerů: `main.cpp:337`.

## Další potvrzené nálezy

- Binance bootstrap drží celou historii v paměti a při chybě pozdní stránky zahodí celý dosavadní postup.
- Duplicitní symbol ve vstupu může spustit souběžný zápis do stejného CSV a u MEXC také do stejného temp adresáře.
- `maxJobs` neřídí počet aktivních Binance/MEXC downloadů; download semaphore je natvrdo nastaven na tři.
- Lighter při timeoutu nebo 429 během prvotního zjištění listing date použije fallback `now - 30d`. Vytvořený CSV tail způsobí, že starší historii již nikdy nezkusí doplnit.
- Lighter přijímá úspěšnou, ale neúplnou JSON envelope jako prázdnou stránku a posune kurzor dál.
- OKX bootstrap zahodí první záznam, pokud leží přesně na archive floor nebo listing timestampu.
- Samostatná OKX X-Perp T6 konverze používá futures adresář místo X-Perp adresáře.
- CSV verifier kontroluje primárně timestamp a počet polí, nikoliv numerickou validitu OHLCV. Pro MEXC navíc toleruje libovolná extra pole, takže slepený nebo natržený řádek může být považován za platný tail.
- `parseSymbolsFile` při chybějícím nebo nečitelném souboru vrátí prázdný seznam, který následně znamená „všechny symboly“. Překlep v cestě tak může spustit nečekaně rozsáhlý download.

## Build, testy a provoz

- Release build prošel celý: `195/195`.
- Všechny testovací executable se sestavily, ale CTest hlásí `No tests were found`. Root aplikace nemá automatické testy kritických CSV, recovery ani agregačních cest.
- Běžná volba `ENABLE_TESTS=ON` sestaví také utility schopné měnit reálný MEXC účet. Údajně „read-only-safe“ nástroj odesílá MARKET příkazy: `mexc-cpp-api/test/close_all_futures.cpp:50`. Tyto utility nebyly spuštěny.
- README uvádí CMake 3.20 a C++20, zdrojový build však požaduje CMake 4.0 a common knihovna C++23: `CMakeLists.txt:1`, `stonky-cpp-common/CMakeLists.txt:4`.
- Čistá offline konfigurace selže, protože OKX vždy stahuje minizip-ng přes `FetchContent`.
- Root dokumentace neuvádí některé povinné závislosti, například Boost 1.88, `magic_enum` a `libsecp256k1`.
- Submoduly používají SSH URL, přestože README vede uživatele ke klonování přes HTTPS.
- V repozitáři není CI, coverage ani sanitizer build.
- Sedm kopií `stonky-cpp-common` dnes ukazuje na stejný commit, ale jejich oddělený update představuje latentní ABI/ODR riziko.
- Checked-in Visual Studio projekt odkazuje na staré nebo neexistující cesty a neobsahuje novější connectory.
- Nebyly nalezeny commitnuté API klíče ani jiné zjevné secrets.

## Dopad na existující data

- CSV vytvořená současnými cestami Binance, Bybit, OKX, Hyperliquid a Lighter je nutné považovat za numericky zaokrouhlená.
- MEXC Futures může obsahovat výrazně zkreslené ceny low-priced instrumentů.
- MEXC a OKX datasety mohou obsahovat permanentní vnitřní mezery nebo neuzavřené svíčky.
- T6 soubory pro intervaly jiné než 1m mohou mít nesprávné close timestampy; u OKX je chybný i 1m timestamp.
- Agregované soubory mohou zakrýt mezery přítomné ve zdrojových datech.
- `--repair` nedokáže dopočítat chybějící ani obnovit zaokrouhlené hodnoty. Dotčenou historii je nutné po opravě kódu znovu stáhnout, pokud je na burze ještě dostupná.

## Doporučené pořadí oprav

1. Zastavit další generování produkčních datasetů a zazálohovat stávající CSV.
2. Opravit numerickou serializaci a MEXC/OKX pagination, staging a append invariants.
3. Přidat automatické testy na návaznost stránek, crash recovery, otevřené svíčky, přesnost a disk-write failure.
4. Zapnout TLS ověřování a zavést nenulové exit kódy i strukturované výsledky workerů.
5. Opravit T6, kalendářní intervaly, agregátor a bounded thread pool.
6. Dotčenou historii znovu stáhnout. Zaokrouhlené hodnoty ani chybějící svíčky verifier sám neopraví.

## Provedené ověření

- Kontrola pracovního stromu, submodulů a `git diff --check`.
- Release build pomocí CMake 4.2.1 a GCC 15.2: úspěch, `195/195`.
- Build s `ENABLE_TESTS=ON`: všechny definované executable se sestavily.
- CTest s testy zapnutými i vypnutými: `No tests were found`, exit code `0`.
- Bezpečný lokální test `hyperliquid_signing_vectors`: všechny čtyři vektory prošly.
- CLI testy chybových větví a jejich návratových kódů.
- Veřejné, neautentizované MEXC API probe pro stránkování, samostatný `endTime`, aktuální svíčku a týdenní zarovnání.
- Pattern scan potenciálních commitnutých credentials.
- Nebyly spuštěny žádné nástroje měnící účet ani autentizované burzovní operace.

Repozitář nebyl během auditu upraven. Pracovní strom byl před uložením tohoto reportu čistý.
