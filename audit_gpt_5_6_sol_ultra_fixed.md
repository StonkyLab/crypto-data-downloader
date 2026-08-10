# Finální souhrn oprav auditu crypto-data-downloader

Datum aktualizace: 10. srpna 2026

Navazuje na:

- `audit_gpt_5_6_sol_ultra.md`
- `audit_fixed_by_opus.md`
- `audit_v2_gpt_5_6_sol_ultra.md`
- `audit_fixed_by_opus_finish.md`

Stav: opravy jsou implementované v aktuálním pracovním stromu; tento dokument
popisuje výsledný datový kontrakt, nikoli libovolně přesnou vědeckou numeriku.

## Závěr

Pět znovuotevřených oblastí bylo dotaženo tak, aby odpovídaly praktickému účelu
projektu:

1. CSV ukládá ceny a množství jako `double` bez další ztráty způsobené
   formátováním.
2. Výpadek burzy znehodnotí pouze příslušný agregovaný bucket, nikoli roky
   ostatních dat.
3. MEXC candle bootstrap již nemůže zapomenout, že publikoval jen dostupný
   suffix historie.
4. MEXC staging, funding a souběžné procesy mají transakční, crash-safe pravidla.
5. Regresní testy a dokumentace odpovídají těmto pravidlům.

V kontrolovaném kódu nezůstal známý kritický návrhový problém z těchto pěti
bodů. Zůstává běžné provozní riziko nestabilního a historicky omezeného MEXC
API; proto je před hromadným redownloadem stále rozumný canary běh nad několika
symboly. Staré zaokrouhlené hodnoty oprava kódu zpětně nezrekonstruuje.

## 1. Numerický kontrakt: shortest-round-trip binary64

Projekt není účetní ani vědecký decimal store. Jeho podporovaný úložný kontrakt
pro ceny, objemy a funding rate je binary64, protože do `double` je načítá také
navazující backtest pipeline.

- `csvNumber(double)` zapisuje nejkratší desetinný text, který se načte zpět na
  bitově stejný `double`.
- `cpp_dec_float_50` je u některých parserů a součtů interní mezityp, ale před
  uložením se záměrně normalizuje na `double`. Nejde o exact persistence
  původního 50místného lexému.
- Tím se opravuje skutečná chyba starého výchozího stream formátování se šesti
  platnými číslicemi i MEXC cesta přes `std::to_string(double)`, například
  `0.0000028101 -> 0.000003`.
- `NaN`, nekonečno a desetinné hodnoty mimo konečný rozsah binary64 jsou
  odmítnuty.
- Test nekontroluje shodu původního libovolně dlouhého decimal textu. Kontroluje
  správnou věc: bitově shodný `double` po zápisu a opětovném načtení.

Delší desetinný rozpis stejné binary64 hodnoty by pouze zvětšoval CSV a v
backtestu by nepřinesl další informaci. Timestampy a identifikátory nadále
procházejí celočíselnou nebo textovou cestou, nikoli přes `double`.

## 2. Agregace odolná proti výpadkům

Výchozí agregace je přísná na kvalitu každého výsledného bucketu, ale tolerantní
na mezery v celém datasetu:

- kompletní coarse buckety před i po výpadku se vždy publikují;
- bucket s chybějící source svíčkou se ve výchozím režimu vynechá;
- vynechá se také zcela prázdný target interval, který nelze syntetizovat;
- jediná mezera proto nezahodí soubor ani historický prefix/suffix;
- běh zpracuje ostatní symboly a timeframe a degradovaný výsledek oznámí exit
  kódem `2`;
- exit `1` je vyhrazen pro fatální konfiguraci, nepodporované schéma a I/O nebo
  transakční chybu; čistý výsledek vrací `0`.

Volba `--allow_partial_aggregation` je vědomý opt-in. Smí vypsat jen bucket,
který obsahuje alespoň jednu validní source svíčku. Zcela prázdný interval
zůstává vynechaný. Bucket zasažený neplatným číslem, chybnou šířkou řádku,
nečitelným timestampem, duplicitou nebo out-of-order záznamem se nevypíše ani v
partial režimu; chyba se lokalizuje na dotčený bucket a bezpečný zbytek dat se
zachová.

Agregátor dále:

- rozlišuje živý rozpracovaný trailing bucket od historického nekompletního
  trailing bucketu;
- validuje konečné binary64 hodnoty, timestamp alignment a aritmetické meze;
- chrání rewrite per-target advisory lockem a atomickou náhradou;
- před commitem provádí přesný subset proof: každý skutečný starý target řádek
  musí být buď v novém výstupu, nebo v LIS-kept bucketu explicitně pozorovaném
  jako poškozený; unseen prefix/tail ani rogue timestamp nemohou autorizovat
  hromadné smazání, ale mezera už nepřítomná v targetu další rewrite neblokuje;
- zachovává venue schéma a správné role `open/high/low/close`, Binance
  `close_time` a sumární volume/count sloupce;
- reportuje počet vynechaných a explicitně emitovaných partial bucketů.

Regrese pokrývají vnitřní i celý chybějící interval, mezeru na hranici resume,
poškozený tail, ochranu starého prefixu a tailu, neplatná čísla, overflow,
timestampy u hranice `int64_t`, chybný dopředný timestamp, live/historical
trailing bucket, atomický append, souběžný lock a podporovaná venue schémata.

## 3. MEXC candles: pagination, mezery a nevyřešený prefix

MEXC Spot i Futures pagination vrací explicitní výsledek:

- `RequestedRangeScanned`, pokud pagination prokazatelně prošla celý požadovaný
  rozsah nebo data dosáhla jeho začátku;
- `ProvisionalAvailabilityBoundary`, pokud API po opakovaných prázdných
  odpovědích ukázalo jen dostupný suffix, ale neposkytlo důkaz listing boundary.

Obecné callback API provisional výsledek odmítá. Downloader jej smí publikovat
jen s trvalým recovery stavem:

1. Před publikováním fresh suffixu atomicky vytvoří
   `<SYMBOL>.csv.prefix.pending`.
2. Marker i celý inspect/fetch/commit cyklus chrání per-symbol OS advisory lock.
3. Každý další run marker načte a prohledá interval od původně požadovaného
   začátku po první lokální svíčku.
4. Jakmile se objeví starší data, stáhne se celý rozsah a podle timestampu se
   sjednotí se všemi již uloženými řádky. API výpadek při rebuildu proto nesmaže
   lokálně známou svíčku; konflikt hodnot na stejném timestampu selže před
   atomickou náhradou. Neprovádí se delete-first.
5. Marker se odstraní až po úspěšném commitu výsledku, který pozitivně dosáhl
   původního `requestedStart`. Negativní probe marker nemaže.

Pořadí marker-before-publish zajišťuje, že ani pád procesu mezi kroky nevystaví
neoznačený newest-only suffix. Starší veřejná callback cesta nemůže provisional
data zapsat před zjištěním výsledku stránkování.

Prázdné odpovědi se opakují. Pagination má od rozsahu odvozený progress guard a
samostatný limit 128 leading empty windows; vyčerpání limitu je chyba, nikdy
důkaz delistingu ani důvod publikovat částečný neoznačený výsledek. Známý
delistovaný symbol používá bounded newest probe: prázdný probe zachová existující
CSV jako no-op, ale fresh soubor se falešně nevytvoří.

Transakční staging již nevyžaduje bezchybnou časovou kontinuitu, protože burza
může legitimně vynechat svíčku. Vyžaduje přesné schéma, konečná binary64 čísla,
alignment a striktně rostoucí timestampy. Vnitřní i base-to-first mezery se
zachovají a zalogují; duplicita, out-of-order, misalignment nebo malformed řádek
transakci odmítne. Manifest, očíslované dávky, kontrola původního CSV před
commitem a atomický replace brání slepení nekompletního stagingu.

## 4. MEXC funding a meziprocesové zápisy

Funding update drží per-symbol advisory lock přes celý
inspect/fetch/validate/commit cyklus. Existující CSV se validuje celé: přesná
hlavička, dva sloupce, konečné binary64 rate a striktně rostoucí timestampy.
Výstup se publikuje jako atomická náhrada celého souboru, takže disk-full,
pozdní chyba streamu nebo pád procesu nepoškodí předchozí CSV.

Pravidla stránkování jsou fail-closed:

- prázdná první stránka se opakuje a bez autoritativního důkazu neznamená
  „hotovo“;
- `currentPage`, `pageSize`, `totalCount`, `totalPage` a počet řádků na stránce
  musí být vzájemně konzistentní a metadata se během scanování nesmí změnit;
- konflikt dvou různých funding hodnot se stejným timestampem transakci odmítne.

Každý fresh, header-only i starší neoznačený funding CSV se při prvním update
této verze automaticky převede do provisional režimu: sidecar
`<SYMBOL>_fr.csv.prefix-provisional` vznikne atomicky pod outer lockem ještě před
fetch/commitem. Každý update pak projde všechny deklarované stránky, spojí
stažené záznamy s celým existujícím CSV podle timestampu a atomicky publikuje
union. Dočasně zkrácený, ale interně konzistentní snapshot tak může pouze přidat
data; pozdější širší scan doplní dříve neviditelnou prostřední mezeru. MEXC
neposkytuje nezávislý autoritativní důkaz začátku funding historie, proto se
marker automaticky nemaže. Jeho přítomnost je recovery stav, nikoli poškození
datasetu.

Advisory lock používá běžný trvale existující soubor; vlastnictví drží kernel a
uvolní je i po `SIGKILL`. Starý directory-style lock se záměrně neodhaduje jako
stale. Po ověření, že neběží stará verze downloaderu, je potřeba takový legacy
adresář jednorázově odstranit.

## 5. Ostatní dříve uzavřené nálezy

Zůstávají zachované dříve implementované opravy:

- OKX newest-first otevřená svíčka, foreign-link fail-closed větev, kontrola
  pozdních I/O chyb a správné X-Perp adresáře;
- průběžně validovaná Binance pagination a opravený funding writer;
- propagace výjimek symbolových workerů do `main()` a omezení skutečného počtu
  `std::async` workerů;
- validace symbolů používaných v cestách, stabilní deduplikace a thread-safe
  log sinks;
- atomické T6 writery a opravené fixed, weekly a calendar-month timestampy;
- přesný CSV verifier, který odděluje strukturální poškození od legitimně
  nevyplnitelných gaps.

## Build, testy a dokumentace

CTest registruje deterministické regresní testy bez přístupu k burzám. Pokrytí
zahrnuje binary64 round trip, CSV tail recovery, intervaly, gap-tolerant
agregaci, MEXC staging a prefix recovery, MEXC pagination policy, funding
transakce, atomické zápisy a crash recovery locku, verifier, Binance pagination
a propagaci výsledků workerů.

Finální lokální ověření aktuálního pracovního stromu:

- kompletní Release target `crypto_data_downloader` se sestavil bez chyby;
- plná konfigurace s `BUILD_DOWNLOADER=ON` prošla **11/11 CTesty**, včetně
  `mexc_backward_pagination_preserves_history` a
  `mexc_models_preserve_binary64_value`;
- samostatná Debug konfigurace s ASan+UBSan a `BUILD_DOWNLOADER=OFF` prošla
  **8/8 CTesty**; LeakSanitizer byl vypnut pomocí `detect_leaks=0`, protože v
  tomto ptrace sandboxu není použitelný;
- CLI smoke test `--version` vrátil `2.6.0` a exit kód `0`;
- `git diff --check` prošel v superprojektu i MEXC submodulu.

Počet testů je konfigurační: bez downloaderu se connector-backed testy
nesestavují. Výše uvedená čísla proto popisují přesně dva provedené běhy,
nikoli univerzální konstantu každé CMake konfigurace.

Repozitář nemá hosted CI workflow. Release, sanitizer a případná coverage
konfigurace jsou lokální CMake workflow; nic z nich se samo nespouští na GitHubu.
Destruktivní MEXC account nástroje nejsou CTest testy a vyžadují explicitní
CMake volbu.

## Dopad na existující data

Oprava kódu neobnoví informaci ztracenou starým šestiznakovým nebo MEXC
šestidesetinným zápisem. `--repair` umí napravit strukturu CSV, nikoli původní
OHLCV hodnotu. Před cíleným redownloadem je vhodné:

1. zálohovat CSV, T6 i recovery sidecary;
2. spustit verifier nad kopií;
3. provést MEXC canary přes více page boundaries a simulovat přerušený běh;
4. zkontrolovat ordering, alignment, marker a zachování legitimních gaps;
5. teprve poté redownload rozšířit a znovu vytvořit agregované CSV/T6.

Soubory `.prefix.pending`, `.prefix-provisional` a regular lock files se nemají
ručně mazat jako „odpad“. První dva nesou informaci potřebnou pro budoucí
konvergenci historického prefixu; lock files jsou identita kernelového zámku.

## Finální hodnocení

Původní zákaz MEXC redownloadu už není blokující z hlediska návrhu persistence.
Kód nyní rozlišuje legitimní výpadek uvnitř historie od neprokázaného staršího
prefixu a obě situace řeší bez kaskádového zahození dat. Hromadné produkční
stažení je přesto vhodné zahájit až po úspěšném canary běhu a finálním čistém
Release/sanitizer ověření konkrétního commitu.
