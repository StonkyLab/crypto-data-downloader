# Kontrola oprav v commitu `d76e5474a45358ce0401db7ae277f4fd68117876`

Datum kontroly: 2026-09-01
Kontrolovaný commit: `d76e547` – `Address the audit: run lock, sound MEXC probe, fail-closed executions`

## Závěr

Commit opravuje většinu bodů z předchozího auditu správným směrem, ale ještě
jej nepovažuji za finální. Zůstává jedna závažná chyba v klíčovém process-level
run locku, nedokončená MEXC Spot prefix recovery a původní problém linuxových
timeoutů v Binance a Bybit.

Nejvyšší prioritu má run lock. Jeho současná implementace negarantuje, že dvě
instance nad stejnou burzou a stejnými daty skutečně používají stejný lock.

## 1. Vysoká – run lock lze obejít rozdílným prostředím procesu

Funkce `acquireRunLock()` správně sestavuje identitu z burzy a kanonické
výstupní cesty. Samotné umístění lock souboru ale vybírá podle prostředí:

- `main.cpp:108-110` používá `$XDG_RUNTIME_DIR`, pokud je nastaven;
- jinak `main.cpp:111-115` používá výsledek
  `std::filesystem::temp_directory_path()`;
- při chybě temp adresáře dokonce použije aktuální adresář `.`.

Stejná identita dat proto může skončit v různých adresářích. Dvě instance se
stejnými parametry `-e` a `-o`, ale s rozdílným `XDG_RUNTIME_DIR`, zamknou dva
různé soubory a obě pokračují.

Reálné příklady rozdílného prostředí:

- jeden proces spuštěný ze systemd user session a druhý z cronu;
- ruční spuštění s odstraněným `XDG_RUNTIME_DIR`;
- spuštění přes `sudo` nebo pod jiným servisním prostředím;
- rozdílné nastavení `TMPDIR`, když `XDG_RUNTIME_DIR` není dostupný.

### Praktické ověření

Pro stejnou burzu `mexc` a stejný output adresář byl externě držen vypočtený
lock v runtime adresáři A.

- Downloader s `XDG_RUNTIME_DIR=A` správně skončil s hláškou, že už jiná
  instance běží.
- Downloader s `XDG_RUNTIME_DIR=B` prošel přes run guard a pokračoval k
  verifikaci dat.

Tím je reprodukováno porušení hlavního požadavku: dvě instance nad stejnými
daty mohou současně běžet.

Lock namespace musí být deterministický a nezávislý na proměnných prostředí
konkrétního spuštění. Kanonizace output cesty je správná, ale všechny procesy,
které k ní mají přístup, musejí odvodit také stejnou fyzickou identitu locku.

### Chybné rozlišení důvodu selhání

`main.cpp:419-425` považuje každý stav `!runLock->ownsLock()` za existující
druhou instanci. Stejná zpráva se proto vypíše také tehdy, když:

- runtime adresář neexistuje;
- není zapisovatelný;
- lock soubor nelze otevřít z jiného důvodu;
- vlastní OS lock operace selže jinak než kvůli contention.

Například neexistující `$XDG_RUNTIME_DIR` vyprodukoval hlášku
`A mexc downloader is already running`, přestože žádná jiná instance neběžela.
Downloader správně selže bezpečně, ale musí rozlišit „lock je obsazen“ od
„run lock nelze vytvořit nebo získat“.

## 2. Střední – MEXC Spot prefix recovery stále nemůže najít relevantní data

Commit správně přestal mazat `.prefix.pending` po prázdném probe. Tím byla
odstraněna nejhorší vlastnost předchozí verze: negativní odpověď už tichým
způsobem neprohlásí zkrácenou historii za kompletní.

Samotná recovery cesta ale stále volá:

`src/mexc_spot_downloader.cpp:573-577`

```cpp
probeHistoricalPrices(symbol, interval, requestedStart,
                      currentCsv.firstTimestamp)
```

Implementace v
`mexc-cpp-api/src/mexc_spot_rest_client.cpp:330-359` odešle jediný request s
limitem 500 od `requestedStart`. MEXC Spot tento request plní od jeho začátku,
takže probe kontroluje pouze nejstarší 500intervalové okno, nikoliv oblast
bezprostředně před prvním uloženým CSV záznamem.

Příklad:

1. `requestedStart` leží před listingem symbolu;
2. existující CSV začíná až výrazně později kvůli dřívějšímu přerušenému nebo
   provisional downloadu;
3. mezi listingem a začátkem CSV jsou dostupná starší data;
4. probe znovu prohlédne jen prázdné pre-listing okno;
5. vrátí prázdný výsledek, marker zůstane a recovery se při dalším běhu zopakuje
   úplně stejně.

Data nejsou falešně označena za kompletní, ale recovery nekonverguje a starší
dostupná data nenajde. README v `README.md:125-136` přitom stále říká, že
pozdější běh chybějící interval znovu prověří a případně sloučí starší data.

Nový backward walk v MEXC submodulu zlepšuje fresh/full download. Marker
recovery v root downloaderu jej ale nepoužívá. Recovery musí procházet oblast
zpětně od `currentCsv.firstTimestamp`, případně použít stejnou stránkovací
logiku jako nový full download; request ukotvený pouze na `requestedStart`
relevantní oblast nezkontroluje.

## 3. Střední – Binance a Bybit timeoutový bod zůstal otevřený

Commit přidává keepalive a `TCP_USER_TIMEOUT` pro OKX. To je užitečná oprava
konkrétního OKX black-hole případu, ale původní nález se týkal také nového
timeoutového kontraktu Binance a Bybit.

### Binance

`binance-cpp-api/src/binance_http_session.cpp:42-49` výslovně uvádí, že
`SO_RCVTIMEO` a `SO_SNDTIMEO` na POSIX se synchronním Boost.Asio skutečný
deadline nezavedou. Po `EAGAIN` Asio přejde na poll bez časového omezení.

Timeout je navíc nastaven až po DNS a TCP connectu v
`binance-cpp-api/src/binance_http_session.cpp:268-277`, takže tyto fáze nejsou
omezovány vůbec.

### Bybit

`bybit-cpp-api/src/bybit_http_session.cpp:36-67` používá stejný socketový
mechanismus, ale dokumentuje jej jako funkční limit connectu, TLS handshake,
write i read. Na běžném Linuxu tento kontrakt stále neplatí.

Oprava OKX tedy nezavírá původní timeoutový bod pro Binance a Bybit.

## Opravy, které jsou v commitu správně

### Bybit executions

`bybit-cpp-api/src/bybit_rest_client.cpp:938-1008` nyní:

- považuje pouze prázdný cursor za úplný konec listingu;
- při opakovaném cursoru vyhodí výjimku;
- po dosažení 20 stran s neprázdným cursorem vyhodí výjimku;
- nevrátí částečný seznam executions jako úplný výsledek.

Tento bod je opraven fail-closed.

### MEXC prefix marker

Spot i Futures už po opakované prázdné odpovědi marker nemažou:

- `src/mexc_spot_downloader.cpp:583-591`;
- `src/mexc_futures_downloader.cpp:607-611`.

Negativní API odpověď tak znovu není považována za autoritativní listing
boundary.

### MEXC Spot backward pagination

`mexc-cpp-api/src/mexc_spot_rest_client.cpp` nyní místo neúčinného probe
prochází prázdná okna zpětně. Po datech toleruje až 16 souvislých prázdných
500intervalových oken a teprve potom publikuje provisional suffix s recovery
markerem.

Je to výrazné zlepšení fresh downloadu a dlouhé venue gaps už nejsou okamžitě
zaměněny za listing boundary. Limit je vědomý praktický kompromis a při jeho
dosažení zůstává výsledek označen jako provisional.

### Aktivní `-1121`

`src/mexc_spot_downloader.cpp:815-838` nyní kontroluje `expectedLive`:

- neaktivní/delistovaný symbol lze bezpečně přeskočit;
- `-1121` pro symbol označený ticker katalogem jako aktivní zůstává viditelná
  chyba.

### Dokumentace a verze

- `main.cpp:37` hlásí verzi 2.7.5;
- README už nepopisuje odstraněné per-symbol a funding locky;
- README už netvrdí, že funding vyžaduje `.prefix-provisional` marker;
- ChangeLog popisuje nový run-level kontrakt a ostatní opravy.

### OKX black-hole ochrana

`okx-cpp-api/src/okx_http_session.cpp` přidává na podporovaných POSIX systémech
TCP keepalive parametry a `TCP_USER_TIMEOUT`. Pro skutečně black-holed navázané
spojení je to vhodnější než samotné `SO_RCVTIMEO`, které synchronní Boost.Asio
na POSIX nerespektuje jako deadline.

## Ověření

- `cmake --build cmake-build-release -j2`: úspěšné.
- `ctest --test-dir cmake-build-release --output-on-failure -j2`: 13/13 testů
  úspěšných.
- `git diff --check HEAD^..HEAD`: bez chyb.
- Praktický test potvrdil správné odmítnutí druhé instance ve stejném runtime
  adresáři a současně reprodukoval obejití locku při změně runtime adresáře.

Commit nepřidává automatické testy pro:

- dvě instance se stejnými daty a rozdílným runtime prostředím;
- chybu vytvoření run locku odlišnou od contention;
- MEXC Spot marker recovery s daty mimo nejstarší 500intervalové okno;
- Bybit repeated cursor a dosažení page capu;
- skutečný wall-clock timeout Binance a Bybit požadavku na POSIX.

## Doporučené pořadí dokončení

1. Udělat fyzickou identitu run locku deterministickou napříč všemi způsoby
   spuštění procesu a přidat integrační test dvou instancí.
2. Oddělit contention od chyby vytvoření/získání locku v konzolové hlášce.
3. Přesměrovat MEXC Spot marker recovery na backward walk relevantní oblasti.
4. Dokončit nebo pravdivě omezit timeoutový kontrakt Binance a Bybit na
   podporované platformy.
