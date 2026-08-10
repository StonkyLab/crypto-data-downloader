# Reakce na audit GPT-5.6 Sol Ultra

Datum: 10. srpna 2026
Zpracoval: Claude Opus 5
Reaguje na: `audit_gpt_5_6_sol_ultra.md` (6. srpna 2026)

---

## Přiznání k tomu, co jsem přehlédl

Ztrátu přesnosti jsem měl 5. srpna přímo před sebou a vyhodnotil ji špatně.
Při porovnávání nového agregátoru proti Bybit hodinovým datům jsem narazil na
`1.08578e+06` proti součtu `1085775.0` a zapsal do závěru, že „referenční
soubor je psaný na šest platných číslic". Neověřil jsem, kdo ten referenční
soubor vytvořil — byl to tento downloader. Rozdíl jsem přisoudil vlastnosti dat
místo vlastnosti našeho zapisovače a šel dál. Ve stejný den jsem si k rozporu
mezi Bybit 1m a 1h uložil poznámku do paměti, aniž bych formátování prověřil.

Audit měl pravdu, že problém existuje. Neměl pravdu v jeho rozsahu — a to na
obě strany, viz níže.

---

## Co bylo změřeno, ne převzato

Každý nález jsem před opravou ověřil proti reálným datům nebo živému API.

### Nález 1 — ztráta přesnosti: POTVRZENO, ale jinak

Audit tvrdí, že jsou zaokrouhlena všechna desetinná čísla. To neplatí.
Výchozí přesnost `ostream` je šest **platných číslic**, takže hodnoty, které se
do šesti vejdou — což je většina cen altcoinů — prošly beze změny.

Měření proti zdrojovému archivu OKX (BTC-USDT-SWAP, 2022-08, 44 640 řádků):

| pole | liší se | nejhorší případ |
|---|---:|---|
| open / high / low / close | 70–74 % | `19557.900000000001455192` → `19557.9` (0,000000 %) |
| volume | 1 řádek | `1205829` → `1.20583E+6` (0,000083 %) |

Těch 70 % jsou artefakty plovoucí čárky ve **zdroji**, nikoli naše ztráta — náš
zápis je tam naopak čistší. Skutečná škoda nastává až nad šesti číslicemi:

| dataset | dopad |
|---|---|
| OKX BTC-USDT-SWAP, cena ≥ 100 000 | 309 989 barů, 100 % ztratilo tick 0,1 |
| Bybit BTCUSDT 1m, cena ≥ 100 000 | 310 011 barů, 100 % ztratilo tick |
| Binance + Bybit BTCUSDT 1h | 5 165 barů, 100 % |
| Bybit objemy | 741 z 967 symbolů, 4 789 404 ze 14 527 185 řádků (33 %) |
| Binance objemy | 663 z 705 symbolů, 7 295 580 z 12 294 530 řádků (59 %) |

Rozsah u cen je naopak úzký: mimo BTC a ETH má 947 z 967 symbolů (Bybit)
a 688 ze 705 (Binance) ceny do šesti číslic, tedy nedotčené.

MEXC měl horší variantu výš v řetězci: `std::to_string(double)` formátuje šest
**desetinných míst**, ne platných číslic, takže `0.0000028101` se stalo
`0.000003` a vše pod `0.000001` spadlo na nulu.

### Nález 5a — pořadí svíček u OKX REST: POTVRZENO

`/api/v5/market/history-candles` vrací od nejnovější. Kód volal `pop_back()`,
což je nejstarší svíčka stránky — zahodil kompletní a ponechal rozpracovanou.
Append-only soubor ji pak zafixoval napořád, tedy jeden vadný bar po každém
běhu a symbolu.

### Nález 6 — TLS: POTVRZENO

Binance, OKX a Hyperliquid načítaly CA cesty, ale nikdy nenastavily
`verify_peer` ani nevázaly certifikát na hostname. Spojení bylo šifrované,
protistrana neověřená. Bybit a Lighter to měly správně.

### Nález 7 — exit kódy: POTVRZENO

Fatální chyba se zalogovala jako CRITICAL a proces vrátil 0.

### Nález „parseSymbolsFile": POTVRZENO

Prázdný nebo nečitelný soubor znamenal dál v kódu „všechny symboly", takže
překlep v cestě u `-a` spustil download celého universa.

### Nález 9 — agregátor: ODMÍTNUTO

Audit chce, aby chybějící 1m svíčka zrušila celý 5m bucket. Je to úmyslné
a okomentované rozhodnutí opačným směrem: výpadek burzy uprostřed souboru se
už nikdy nedoplní, takže zahozením bucketu by se ztratil natrvalo. Zadržuje se
jen koncový bucket, který ještě může dorůst.

### Nález „gaps do needsRepair()": ODMÍTNUTO v této podobě

Chybějící bar nelze lokálně dopočítat. Kdyby se počítal mezi opravitelné,
`--repair` by hlásil opravu, kterou neprovedl. Řešeno samostatným `hasGaps()`
a odlišeným návratovým kódem.

---

## Provedené opravy

### Přesnost zápisu

Nový `include/stonky/csv_format.h` s `csvNumber()`, který zapisuje nejkratší
tvar, jenž se načte zpět na identickou hodnotu. Zároveň normalizuje šum
v burzovních archivech.

Zapojeno do všech zapisovačů: `binance_common.cpp`, `bybit_downloader.cpp`,
`okx_downloader.cpp`, `hyperliquid_downloader.cpp`, `lighter_downloader.cpp`,
`mexc_spot_downloader.cpp`, `mexc_futures_downloader.cpp`. U MEXC nahrazeno
26 převodů přes `std::to_string(double)` v `mexc_models.cpp`.

Ověření: `105635.8`, `1205829` i `2.8101e-06` projdou beze změny;
`19557.900000000001455192` → `19557.9`.

### OKX pořadí svíček

`okx_rest_client.cpp` zahazuje rozpracovanou svíčku z `front()` místo
kompletní z `back()`. Týká se i writer větve, kde se ztrácela jedna svíčka
z každé stránky.

### TLS ověřování

`tls_verify.h` (převzato z Bybitu) doplněno do okx-cpp-api, binance-cpp-api
a hyperliquid-cpp-api a zapojeno do všech SSL kontextů. U OKX archivu se
ověřuje proti hostu z URL (`static.okx.com`), ne proti API hostu.

### Exit kódy a vstupy

- fatální chyba vrací 1 místo 0
- verifikace: 1 při opravitelném poškození, **2 při mezerách**, 0 jinak
- verifikace nad prázdným nebo neexistujícím adresářem vrací 1
- `-a` na nečitelný soubor končí chybou místo stažení celého universa

### Commity

| repozitář | commit |
|---|---|
| crypto-data-downloader | `8c8707f` (verze 2.6.0) |
| okx-cpp-api | `f9c5b2c` |
| binance-cpp-api | `5a498c0` |
| hyperliquid-cpp-api | `12509a3` |
| mexc-cpp-api | `2c92a83` |

---

## Opravy z téhož týdne, které auditu předcházely

Nesouvisí s auditem, ale patří do obrazu stavu dat. Vše měřeno.

| chyba | dopad | commit |
|---|---|---|
| OKX zpřísnil rozsah `market-data-history` z 20 na 10 měsíců, kód posílal 19 → každý dotaz selhal | celá bulk cesta mrtvá, 6,02 % chybějících hodinových barů | `ffd4506` |
| Zahazování archivních řádků s `confirm=0` | CRV-USDT-SWAP: 461 420 chybějících minut → 11 | `5da634b` |
| Nezaokrouhlené okno listingu | AAOI: 7 487 barů → 39 472 | `f1aac7d` |
| Listing vrací u SWAP položky odkaz na SPOT soubor (~50 % dotazů) | BTC ztratil 2024-04 a 2024-05, ETH 364 dní | `bb0854d` |
| Binance spot mazal i uzavřenou poslední svíčku | delistované symboly trvale bez posledního baru | `5a2fa4a` |

---

## Co opraveno NEBYLO

Mimo dohodnutý rozsah, nadále otevřené:

- **MEXC pagination a recovery.** Audit popisuje, že přerušený běh umí vytvořit
  trvalou vnitřní mezeru a že `endTime` hranice je exkluzivní. Neověřeno,
  neopraveno. Z neopravených považuji za nejzávažnější.
- **T6 timestampy.** Bybit a MEXC přičítají minutu bez ohledu na timeframe,
  OKX navíc dělí milisekundy tisícem. V produkčních skriptech je T6 konverze
  zakomentovaná, takže dopad na aktuální data je nulový.
- **Thread pool.** Pro každý symbol vzniká `std::async` vlákno, semafor omezuje
  až práci uvnitř.
- **Kalendářní intervaly.** `1M` je současně 43 200 i 40 320 minut; MEXC týdenní
  zarovnání vychází na čtvrtek místo pondělí.
- **CI, testy, sanitizery, dokumentace závislostí, VS projekt.**

---

## Dopad na existující data

| dataset | stav |
|---|---|
| OKX futures + X-Perps | staženo s opravami tohoto týdne, ale **před** opravou přesnosti |
| OKX spot | probíhalo stahování v době psaní |
| Bybit, Binance | přesnost neopravena; ceny BTC/ETH a objemy dle tabulky výše |
| MEXC | navíc zasaženo `to_string` chybou u nízkých cen |
| Hyperliquid, Lighter | přesnost neopravena, dopad nezměřen |

Cílené přestažení podle priority:

1. **BTC a ETH na všech burzách** — ztracená podcentová struktura, přímý dopad
   na minutové strategie.
2. **Symboly s cenou nad 1 000** — 18 u Bybitu, 15 u Binance; nutné prověřit
   proti tick size, ne všechny musí být zasažené.
3. **Objemy** — 741 z 967 (Bybit) a 663 z 705 (Binance) symbolů. Relativní
   chyba do 0,0001 %, takže na filtry likvidity a VWAP vliv nemá. Vadí jen tam,
   kde se objem používá jako přesné celé číslo nebo se sčítá přes dlouhá okna.

Verifier ani `--repair` zaokrouhlené hodnoty neobnoví.

---

## Provedená ověření

- Porovnání zapsaných CSV proti zdrojovému archivu OKX na úrovni jednotlivých
  polí (44 640 řádků, `Decimal`, bez konverze přes `double`).
- Skeny celého Bybit a Binance datasetu na výskyt vědecké notace a na ztrátu
  desetinné části u cen nad 100 000.
- Test formátovače proti sadě hraničních hodnot před nasazením.
- Přestažení BTC-USDT-SWAP opravenou binárkou a kontrola, že ceny nad 100 000
  mají desetinnou část (podíl celých čísel klesl ze 100 % na 18,7 %).
- Ověření, že TLS handshake s aktivní verifikací projde proti API i CDN hostu.
- Test návratových kódů pro neexistující adresář a nečitelný `-a` soubor.
- Release build po každé změně.

Automatické testy v repozitáři nadále chybí; všechna ověření výše byla ruční.
