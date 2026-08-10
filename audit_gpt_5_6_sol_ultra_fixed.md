# Finální souhrn oprav auditu crypto-data-downloader

Datum dokončení: 10. srpna 2026

Původní audit: `audit_gpt_5_6_sol_ultra.md`

Následný audit oprav Opusu: `audit_v2_gpt_5_6_sol_ultra.md`

Stav: opravy jsou implementované v aktuálním pracovním stromu, zatím bez společného finálního commit hash

## Závěr

Blokující nálezy z původního i následného auditu byly opraveny. Největší změny se týkají integrity MEXC a OKX datasetů, numerické přesnosti, propagace chyb workerů, atomických zápisů, striktní agregace a verifikace CSV.

Aktuální stav prošel kompletním Release buildem všech connectorů, deseti automatickými regresními testy a samostatným ASan/UBSan během. V kontrolovaném rozsahu nezůstal známý kritický nebo vysoký blocker.

To neznamená, že se opravila již historicky poškozená data. Zaokrouhlené hodnoty, vynechané svíčky a chybné T6 timestampy nelze spolehlivě rekonstruovat z existujících souborů. Dotčenou historii je nutné po záloze znovu stáhnout, pokud ji burza ještě poskytuje.

## Implementované opravy

### 1. Numerická přesnost

- `double` hodnoty se zapisují shortest-round-trip reprezentací místo výchozí šestiznakové přesnosti streamu.
- `cpp_dec_float_50` se již nezmenšuje přes `double`; serializuje se přímo.
- Binance funding-rate writer používá stejný přesný formátovač jako ostatní CSV writery.
- Všech sedm kopií společného JSON helperu již nepoužívá `std::to_string(double)` pro decimal hodnoty.
- MEXC modely čtou stringové decimal hodnoty přesně a numerické JSON hodnoty přes jejich uloženou JSON reprezentaci.
- Ne-konečné hodnoty (`NaN`, `Inf`) se do CSV odmítnou zapsat.

Omezení: neoznačený JSON number, který parser již načetl jako binary64, nemůže zpětně získat přesnější původní API lexém. Oprava zachovává uloženou hodnotu bez dalšího šestiznakového zaokrouhlení, ale nepředstírá libovolnou 50místnou přesnost pro takový vstup.

### 2. MEXC pagination a integrita recovery

- Spot pagination respektuje exkluzivní `endTime`; další stránka začíná přímo na timestampu nejstarší svíčky, takže se již nevynechá řádek na každé 1000řádkové hranici.
- Historické dotazy používají současně `startTime` a `endTime`, aby endpoint nemohl ignorovat osamocenou historickou hranici.
- Spot i Futures filtrují otevřené svíčky podle serverového času ještě před předáním writer callbacku.
- Futures již bezpodmínečně nemaže poslední kompletní svíčku delistovaného kontraktu.
- Prázdné odpovědi se opakují a omezeně se prohledávají starší intervaly, což umožňuje najít historii delistovaných symbolů bez nekonečného request loopu.
- Rate limitery používají opakovanou kontrolu a čerstvý `steady_clock` timestamp.
- Týdenní hranice je pondělní a měsíční intervaly používají kalendářní hranice.

Původní newest-first recovery bylo nahrazeno transakčním stagingem:

- staging obsahuje manifest a přesně očíslované dávky;
- před publikováním se ověřuje schéma, počet polí, konečnost čísel, alignment a úplná časová kontinuita;
- všechny dávky musí existovat a být zakončené newline;
- bezprostředně před commitem se znovu porovná aktuální CSV tail s manifestem;
- per-symbol directory lock chrání před překryvem dvou cron procesů;
- finální soubor vzniká sibling temp zápisem a atomickou náhradou;
- nekompletní nebo cizí staging se nikdy slepě nepřipojí;
- tail repair je bezpečný i na Windows a nepřepisuje data při chybě čtení nebo zápisu.

### 3. OKX fail-closed cesty

- Neuzavřená newest-first REST svíčka se odstraňuje z `front()`.
- Nevyřešený foreign-link filename po vyčerpání retry ukončí operaci; novější archivy se nepřipojí přes chybějící období.
- Candle i funding writery kontrolují `flush()`, `close()` a stav streamu; resume timestamp se při chybě neposune.
- Fresh bootstrap zahrne záznam přesně na spodní hranici.
- Prázdný, header-only nebo neexistující výstup již nemůže skončit úspěšným exit kódem.
- `51001` je no-op pouze tehdy, pokud už existují použitelná lokální data.
- X-Perp T6 konverze používá správné X-Perp adresáře.
- REST, CDN i WebSocket cesty používají ověření CA chainu, hostname a SNI.

### 4. Binance, Lighter a ostatní downloadery

- Binance bootstrap ukládá každou ověřenou stránku průběžně; chyba pozdní stránky již nezahodí celý dosavadní download.
- Stránky se kontrolují na chronologii, meze a skutečný posun kurzoru.
- Lighter odmítne chybnou nebo neúplnou response envelope a po selhání listing discovery již nepoužije nebezpečný fallback `now - 30d`.
- Lighter signer smoke test nevypisuje privátní klíč.
- Všechny hlavní CSV writery kontrolují pozdní I/O chyby.

### 5. Propagace chyb a paralelismus

- Výjimky jednotlivých symbolových workerů se agregují a propagují až do `main()`.
- Úplné i částečné selhání downloadu již nemůže skončit exit kódem `0` jen proto, že worker vrátil prázdnou cestu.
- Slot se získává před vytvořením `std::async` workeru, takže je omezen počet skutečných OS threadů, ne pouze počet threadů uvnitř semaphore.
- `maxJobs=0` se bezpečně normalizuje na jeden job.
- Callbacky se volají sekvenčně pro úspěšně dokončené položky a neočekává se jejich thread safety.
- Duplicitní symboly se odstraňují při zachování pořadí; opraven byl i self-move bug deduplikace.
- Symboly používané jako filename components se validují před downloadem i před mazáním souborů.
- Logovací sinks jsou multithreaded varianty `_mt`.
- Top-level výjimky, prázdná agregace a prázdná verifikace vracejí nenulový exit kód; mezery mají samostatný exit kód `2`.

### 6. Agregátor a CSV verifier

Agregátor je nyní ve výchozím stavu striktní:

- vyžaduje přesný počet a návaznost source barů uvnitř coarse bucketu;
- detekuje také úplně chybějící coarse bucket mezi dvěma jinak kompletními buckety;
- nevalidní numerické hodnoty se nikdy nevypíšou ani v partial režimu;
- při chybě se transakční rewrite nepublikuje a existující výstup zůstane zachovaný;
- partial agregace je dostupná pouze explicitním `--allow_partial_aggregation`;
- podporována jsou skutečná 6-, 7-, 8- a 12sloupcová schémata používaná jednotlivými venues;
- Binance `close_time`, sumární volume/count pole a OKX/MEXC extra volume pole mají správnou agregační semantiku.

Verifier nyní:

- vyžaduje přesné známé hlavičky a přesnou šířku řádků;
- legacy Bybit 7→6 opravu povolí pouze pro známý sloupec `turnover`;
- odmítá slepené řádky, neznámé extra sloupce, `NaN`/`Inf`, header-only a empty CSV;
- kontroluje I/O chybu po čtecí smyčce;
- opravuje soubor přes temp a atomickou náhradu;
- používá kalendářní měsíční gap kontrolu, včetně OKX hranice UTC+8 a Binance inclusive `close_time`.

### 7. T6 výstupy a kalendářní timestampy

- Všechny T6 writery zapisují do zamčeného sibling temp souboru a teprve po úspěšném `flush()`/`close()` provedou atomickou náhradu.
- Selhání parseru, disk-full nebo souběžný proces již nesmaže předchozí validní T6.
- Close timestamp používá skutečný interval místo pevné jedné minuty.
- Bybit, MEXC a OKX měsíční timestampy používají kalendářní hranice; OKX respektuje UTC+8.
- MEXC weekly používá pondělní hranici.
- Lighter nepodporovaný měsíční převod explicitně odmítne místo vytvoření chybného T6.

### 8. Build, testy, CI a dokumentace

- Root CMake používá deklarované C++23 a CMake 4.0+.
- CTest obsahuje regresní testy přesnosti, CSV tail recovery, pagination, MEXC stagingu, atomických zápisů, kalendářních intervalů, agregace, verifieru a worker helperů.
- Destruktivní MEXC account tools jsou oddělené za explicitními CMake volbami a nejsou CTest testy.
- Přidány konfigurace pro sanitizéry a coverage.
- CI obsahuje test-only Release, sanitizer a coverage job, Windows/MSVC test job a plný Linux build všech connectorů.
- Přidán připnutý vcpkg manifest včetně `minizip-ng`.
- OKX preferuje nainstalovaný `minizip-ng`; síťový FetchContent je pouze explicitní fallback.
- Root i vnořené `.gitmodules` používají HTTPS.
- README popisuje aktuální závislosti, přesná venue CSV schémata, striktní agregaci a kanonický CMake workflow.
- Zastaralé Visual Studio project soubory, které odkazovaly na neexistující cesty a chybějící connectory, byly odstraněny.

## Provedené ověření

### Release build

Úspěšně sestaveny:

- `crypto_data_downloader`
- všechny exchange connector knihovny
- `binance_pagination_test`
- `mexc_models_precision_test`

Build skončil exit kódem `0`. Objevily se pouze existující warningy z Boost/date hlaviček, ne nové chyby v opravených cestách.

### CTest

Plný build: **10/10 testů prošlo**.

1. `csv_format_round_trip`
2. `csv_data_tail_recovery`
3. `downloader_month_interval`
4. `aggregation_incomplete_bucket_policy`
5. `mexc_transactional_staging`
6. `csv_verifier_rejects_corruption`
7. `future_utils_preserves_symbols`
8. `atomic_file_preserves_previous_output`
9. `binance_pagination_preserves_progress`
10. `mexc_models_preserve_decimal_text`

### Sanitizéry a coverage

- Samostatný ASan + UBSan build: **8/8 testů prošlo bez nálezu**.
- LeakSanitizer byl při lokálním běhu vypnut (`detect_leaks=0`), protože v tomto sandboxu selhává pod `ptrace`; nešlo o leak report aplikace.
- Coverage konfigurace a test-only CTest: **8/8 prošlo**.

### CLI a diff kontrola

- `--version`: exit `0`.
- Agregace nad chybějícím datasetem: exit `1` místo původního `0`.
- `git diff --check` prošel v superprojektu i ve všech upravených submodulech.

Remote GitHub Actions workflow nebyl z tohoto lokálního prostředí spuštěn; jeho YAML, manifesty a lokálně odpovídající build/test konfigurace byly ověřeny.

## Dopad na existující data a doporučený postup

Oprava kódu neobnoví informace, které staré verze již ztratily. Před redownloadem:

1. Zálohovat stávající CSV a T6 datasety.
2. Spustit verifier nad kopiemi a uložit report problémů.
3. Provést nejprve malý canary redownload několika symbolů přes page boundary.
4. Ověřit kontinuitu, poslední uzavřenou svíčku a počet řádků.
5. Poté znovu stáhnout dotčenou historii, prioritně MEXC a OKX a datasety vytvořené starými šestiznakovými writery.
6. Po redownloadu znovu spustit verifier a teprve následně regenerovat agregované CSV a T6.

U MEXC může být část staré minutové historie již mimo retention burzy. Taková data nelze doplnit bez jiného důvěryhodného zdroje.

## Poznámka ke commitování

Opravy zasahují superprojekt a sedm samostatných Git submodulů:

- `binance-cpp-api`
- `bybit-cpp-api`
- `hyperliquid-cpp-api`
- `lighter-cpp-api`
- `mexc-cpp-api`
- `okx-cpp-api`
- `stonky-cpp-common`

Při vytváření commitů je nutné nejprve commitnout změny v jednotlivých submodule repozitářích a až potom commitnout aktualizované gitlinky a root změny v superprojektu. Bez tohoto pořadí by root commit na opravené submodule commity neukazoval.

## Finální hodnocení

Původní doporučení „MEXC data zatím znovu nestahovat“ již po těchto opravách není blokující. Rozumný další krok je kontrolovaný canary redownload následovaný verifierem. Hromadný produkční redownload je vhodné spustit až po úspěšném ověření canary vzorku a po vytvoření zálohy stávajících datasetů.
