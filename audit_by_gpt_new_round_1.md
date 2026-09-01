# Audit změn od commitu `7061ea94ac6ed4ef5f80bc57ccfdd74fdcb7bbca`

Datum kontroly: 2026-09-01
Kontrolovaný rozsah: `7061ea94ac6ed4ef5f80bc57ccfdd74fdcb7bbca..c6a6297`
Počet commitů: 9

## Závěr

Změny zatím nepovažuji za bezpečně uzavřené. Oprava MEXC Spot stránkování na
500 řádků a percent-encoding query parametrů vypadají správně, ale v rozsahu
zůstávají tři závažné a dvě středně závažné vady.

Po upřesnění provozního požadavku není správným řešením návrat per-symbol ani
per-CSV locků. Nad stejnou kombinací výstupních dat a burzy nesmějí současně
běžet dva procesy. Tuto podmínku musí vynutit přímo downloader při startu;
externí update skripty nejsou dostatečnou součástí bezpečnostního kontraktu.

## 1. Vysoká – downloader sám nevynucuje jedinou instanci nad stejnými daty

MEXC per-symbol, funding update a sibling locky byly odstraněny. To je v souladu
s požadavkem, aby v datových adresářích nevznikaly tisíce pomocných souborů,
které následně publikuje HTTP server. Současně ale v downloaderu nevznikla
náhrada na správné úrovni: ochrana celé instance.

Aktuální funding writer používá `AtomicFileWriter::Locking::None`:

- `src/mexc_funding_csv.h:339`
- `src/mexc_funding_csv.h:410`

Deterministický dočasný soubor `<target>.writing` se otevírá s `trunc` v
`include/stonky/atomic_file.h:42-55`. Candle downloady obdobně používají
společné `temp_<SYMBOL>`, `batch_*.tmp`, `complete.manifest` a `committed.csv`.
Bez globálního guardu proto dvě přímá spuštění CLI mohou stále zapisovat do
stejných pracovních cest.

Komentáře předpokládají per-exchange `flock` v externích update skriptech, ale
repo takový wrapper neobsahuje a veřejné CLI jej nevynucuje. Kontrola base/tailu
před publikací bez vzájemného vyloučení neuzavírá TOCTOU okno.

### Požadované řešení

Při startu downloaderu:

1. sestavit identitu z kanonické výstupní cesty a burzy;
2. neblokujícím způsobem získat jediný process-level zámek;
3. držet jej po celý běh všech operací nad danými daty;
4. uložit identitu zámku mimo datový strom, například do uživatelského runtime
   adresáře, aby ji HTTP server nemohl publikovat;
5. pokud již zámek drží jiný proces, ještě před manipulací s daty vypsat jasnou
   zprávu, například `MEXC downloader already running for /data/crypto`, a
   skončit s nenulovým návratovým kódem;
6. použít OS mechanismus, jehož vlastnictví se uvolní také po pádu procesu.

Po zavedení tohoto invariantního guardu jsou `Locking::None` a deterministické
staging cesty přijatelné. Per-file locky se vracet nemají.

## 2. Vysoká – prázdný probe může natrvalo zahodit chybějící prefix

Spot po prázdném pozdějším probe odstraní `.prefix.pending` v
`src/mexc_spot_downloader.cpp:573-603`. `probeHistoricalPrices()` ale provádí
jen jeden request omezený na 500 záznamů od `requestedStart`:
`mexc-cpp-api/src/mexc_spot_rest_client.cpp:320-348`.

Pokud `requestedStart` leží před listingem symbolu, tento request bude prázdný
i tehdy, když mezi listingem a prvním lokálním záznamem chybějí data kvůli
dřívějšímu internímu výpadku. Marker se následně smaže, běžné další spuštění
pokračuje pouze od tailu CSV a chybějící prefix už se nikdy nehledá.

Futures odstraňuje marker stejným způsobem v
`src/mexc_futures_downloader.cpp:596-623`. Jeho probe sice pokrývá širší rozsah,
ale opakovaná prázdná API odpověď stále není autoritativním důkazem listing
boundary.

Toto chování odporuje vlastnímu kontraktu v
`mexc-cpp-api/include/stonky/mexc/mexc_backward_pagination.h:45-55`, který říká,
že negativní API odpověď nikdy nemůže prokázat datum listingu. Vzhledem k
požadavku tolerovat výpadky burzy nelze druhou prázdnou odpověď považovat za
dostatečný důvod ke zrušení recovery stavu.

## 3. Vysoká – Bybit executions pagination může vrátit částečný výsledek

Nové `getExecutions()` nastavuje limit 100 záznamů a maximálně 20 stran v
`bybit-cpp-api/src/bybit_rest_client.cpp:938-989`.

Metoda ukončí stránkování také tehdy, když:

- po dvacáté stránce stále existuje další cursor;
- venue vrátí stejný neprázdný cursor podruhé.

V obou případech metoda bez chyby vrátí dosud stažené executions. Volající tak
nemůže poznat, že výsledek není úplný. Execution gateway tato data používá k
obnově zmeškaných fillů v `bybit-cpp-api/src/bybit_execution_gateway.cpp:213-235`.
Neúplný výsledek proto může podhodnotit skutečně provedené obchody.

Po dosažení bezpečnostního limitu nebo po opakovaném cursoru musí metoda selhat
fail-closed a nesmí částečný vektor vydávat za úplný.

## 4. Střední – `UnknownSymbolError` se potlačuje i pro aktivní Spot symbol

Downloader si v `src/mexc_spot_downloader.cpp:494` spočítá `expectedLive` podle
aktuálního ticker katalogu. Catch v `src/mexc_spot_downloader.cpp:827-839` ale
tuto informaci nepoužívá a `UnknownSymbolError` potlačí pro každý symbol.

Pokud klines endpoint vrátí `-1121` pro symbol, který ticker právě označil jako
aktivní, běh skončí úspěšně a může nevytvořit žádné CSV. Taková odpověď může
znamenat regresi endpointu nebo encodingu a musí být viditelnou chybou.

Tiché přeskočení je vhodné pouze pro symbol, který není v aktivním katalogu a
je zpracováván kvůli survivorship-clean seznamu nebo již existujícím archivním
datům.

## 5. Střední – nové HTTP timeouty na POSIX požadavek neohraničují

Binance používá `SO_RCVTIMEO` a `SO_SNDTIMEO`, ale komentář v
`binance-cpp-api/src/binance_http_session.cpp:42-49` správně přiznává, že
synchronní Boost.Asio po POSIX `EAGAIN` přejde na neomezené čekání. Timeout je
navíc nastaven až po DNS a TCP connectu v
`binance-cpp-api/src/binance_http_session.cpp:268-277`.

Bybit v `bybit-cpp-api/src/bybit_http_session.cpp:36-67` používá stejný socketový
mechanismus, ale dokumentuje jej jako funkční limit pro connect, TLS handshake,
write i read. Na běžném Linuxu tedy nové `setRequestTimeout()` neplní deklarovaný
účel a worker může při výpadku stále čekat bez omezení. Skutečné ohraničení
vyžaduje deadline-aware asynchronní operace nebo jiný mechanismus, který Boost
Asio respektuje i na POSIX.

## 6. Nízká – dokumentace, migrace sidecarů a verze neodpovídají kódu

- `README.md:115-137` stále tvrdí, že MEXC používá per-symbol OS lock a že
  negativní probe marker nikdy neodstraní.
- `README.md:455-465` stále popisuje odstraněný funding lock a povinný
  `.prefix-provisional` marker.
- Staré `temp_*.lock`, `*.update.lock`, `*.csv.lock` a
  `*.prefix-provisional{,.lock}` se přestaly vytvářet, ale kód je při migraci
  neodstraňuje.
- `main.cpp:35` stále hlásí verzi `2.7.0`, zatímco `ChangeLog.md` popisuje až
  verzi 2.7.4.

README je zvlášť problematické u souběhu: slibuje ochranu, kterou současné CLI
nemá. Po zavedení process-level guardu musí dokumentace popsat právě tento
kontrakt a nemá už zmiňovat per-file locky.

## Změny, které vypadají správně

- MEXC Spot stránkovací stride byl srovnán s reálným limitem 500 řádků.
- Při případné další změně venue capu pagination selže místo tichého přeskočení
  záznamů.
- MEXC Spot query parametry se percent-encodují a stejný serializovaný řetězec se
  používá pro podpis i výsledný request.
- `-1121` má vlastní typ výjimky; problém je pouze v příliš širokém potlačení v
  downloaderu.
- Odstranění funding `.prefix-provisional` markeru je samo o sobě přijatelné,
  protože funding se na každém běhu full-scanuje a slučuje s lokálním CSV.

## Ověření

- `cmake --build cmake-build-release -j2`: úspěšné.
- `ctest --test-dir cmake-build-release --output-on-failure -j2`: 13/13 testů
  úspěšných.
- `git diff --check 7061ea94ac6ed4ef5f80bc57ccfdd74fdcb7bbca..HEAD`:
  bez chyb.
- Worktree byl před vytvořením tohoto reportu čistý.

Současné testy nepokrývají:

- odmítnutí druhé instance downloaderu nad stejnými daty;
- dva po sobě jdoucí prázdné prefix probes s později dostupnou historií;
- Bybit cursor/cap ukončený před dosažením konce;
- `UnknownSymbolError` pro symbol současně přítomný v aktivním katalogu;
- skutečný wall-clock timeout síťové operace na POSIX.
