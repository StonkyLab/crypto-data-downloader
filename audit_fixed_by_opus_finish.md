# Závěrečná zpráva k opravám crypto-data-downloader

Datum: 10. srpna 2026
Zpracoval: Claude Opus 5

Navazuje na:
- `audit_gpt_5_6_sol_ultra.md` — původní audit (6. 8. 2026)
- `audit_fixed_by_opus.md` — moje první reakce
- `audit_v2_gpt_5_6_sol_ultra.md` — audit mých oprav
- `audit_gpt_5_6_sol_ultra_fixed.md` — opravy druhého modelu

---

## Shrnutí

Kód je opravený. Data opravená nejsou a část z nich je potřeba znovu stáhnout —
seznam symbolů je níže a je podstatně kratší, než se během cesty několikrát
zdálo.

Průběh měl tři kola. Původní audit našel ztrátu numerické přesnosti, kterou jsem
sám přehlédl, přestože jsem ji měl v datech před sebou. Moje opravy pak prošly
druhým auditem, který v nich našel další vady. Jeho opravy jsem prověřil a ve
dvou bodech vrátil zpět, protože zaváděly nové problémy.

---

## Co jsem přehlédl

5. srpna jsem porovnával nový agregátor proti Bybit hodinovým datům, narazil na
`1.08578e+06` proti součtu `1085775.0` a uzavřel to poznámkou, že „referenční
soubor je psaný na šest platných číslic". Neověřil jsem, kdo ten soubor
vytvořil — byl to tento downloader. Rozdíl jsem přisoudil vlastnosti dat místo
vlastnosti našeho zapisovače.

Pracoval jsem převážně proti běžícím datům a živému API. Chybu odhalilo statické
čtení kódu, tedy metoda, kterou jsem nepoužil.

---

## Opravy — kolo 1 (commit `8c8707f`)

| oblast | co bylo špatně |
|---|---|
| přesnost zápisu | `ofs << hodnota` používá výchozích šest platných číslic |
| MEXC modely | `std::to_string(double)` formátuje šest desetinných míst |
| OKX REST | endpoint vrací od nejnovější, `pop_back()` mazal nejstarší svíčku místo rozpracované |
| TLS | Binance, OKX, Hyperliquid nenastavovaly `verify_peer` ani hostname |
| exit kódy | fatální chyba vracela 0 |
| `-a` soubor | nečitelný soubor znamenal „všechny symboly" |

Nový `include/stonky/csv_format.h` s `csvNumber()` zapojen do všech zapisovačů.

---

## Opravy — kolo 2 (druhý model, commit `cb19cad`)

Jeho audit našel v mé práci reálné vady, které potvrzuji:

- Binance funding-rate writer jsem vynechal, přestože jsem tvrdil, že jsou
  opravené všechny zapisovače.
- OKX po vyčerpání retry na nevyřešený foreign-link pokračoval novějšími
  soubory, takže chybějící období zůstalo trvale.
- Zápisy nekontrolovaly stav streamu, resume timestamp se posunul i po chybě.
- Chyby jednotlivých workerů se nepropagovaly do `main()`.
- Agregace nad prázdným vstupem vracela 0.

K tomu opravil MEXC pagination a recovery, T6 timestampy, thread pool,
kalendářní intervaly a doplnil testy.

---

## Opravy — kolo 3 (commit `5d81430`)

Dvě jeho změny jsem vrátil, protože zaváděly nové problémy.

### Serializace decimálů zapisovala šum

Změnil `csvNumber(cpp_dec_float_50)` na `value.str(0, {})`, což zachová zdroj
doslova:

```
zdroj  47415.400000000001455192
→ 47415.400000000001455192   (dřív 47415.4)
```

Ověřeno, že ty číslice nenesou informaci:

```
47415.400000000001455192  vs 47415.4   bitově shodné: True
19557.900000000001455192  vs 19557.9   bitově shodné: True
0.61180000000000001       vs 0.6118    bitově shodné: True
```

Je to přesný desítkový rozpis téhož float64 — zdrojová hodnota **sama je
double**. Každý konzument těchto souborů (pandas, polars, builder Nautilus
katalogu) je čte jako float64, takže sedmnáct znaků navíc na buňku nikomu nic
nepřinese. Cena: BTC 1m soubor narostl ze 156 MB na 210 MB, na OKX futures asi
15 GB.

Vráceno na zkrácený tvar. Jeho terminologická výtka byla ale správná a je
promítnutá do komentáře: je to **normalizace**, ne obecná bezeztrátová
serializace, a je bezpečná jen proto, že zdroj je double.

### Striktní agregace zahazovala celé soubory

Nastavil `allowPartialBuckets = false` jako výchozí a při nekompletním bucketu
vrátil `failed` **před jakýmkoliv zápisem**:

```
BTC-USDT-SWAP: 2 598 773 barů, chybí 3 minuty (výpadky burzy 2022-10-26, 2023-06-22)
→ 5m: 0 barů, 1h: 0 barů, soubory vůbec nevznikly
```

Tři minuty za 4,9 roku smetly ~519 000 platných bucketů. Na OKX futures má
**140 z 567 symbolů** aspoň jednu mezeru v 1m — všechny by po příštím cronu
neměly žádné 5m ani 1h, a protože jsou to výpadky burzy, které se nikdy
nedoplní, ty soubory by nevznikly nikdy.

Věcný záměr byl přitom správný a měl jsem mu dát za pravdu dřív: částečný bucket
v CSV nelze odlišit od kompletního. Změněna proto jen sémantika selhání —
nekompletní bucket se vynechá, zbytek se zapíše, počet se nahlásí, běh skončí
kódem 2. Nevalidní číselný obsah symbol dál shodí, protože znamená nedůvěryhodný
zdroj.

Ověřeno: 519 756 barů u 5m a 43 311 u 1h, přesná shoda s nezávislým přepočtem
kompletních bucketů, dva nekompletní vynechané.

### CI

Na žádost odstraněn `.github/workflows/ci.yml` a osiřelý test manifest.
V repozitáři zůstávají volby `ENABLE_SANITIZERS` a `ENABLE_COVERAGE` (obě
vypnuté) a registrace `ctest` — nic z toho samo neběží.

---

## Dopad na data

### Ceny

Škoda vzniká jen tam, kde hodnota potřebuje víc platných číslic, než se do
zápisu vešlo. Kritérium: řád maximální ceny mínus řád ticku plus jedna.

| burza | poškozených symbolů | z toho s daty bez metadat |
|---|---:|---:|
| MEXC | **31** | 204 delistovaných neposouzeno |
| Bybit | **36** | 298 |
| Binance | **15** | 30 |
| OKX | **12** | 144 |
| Lighter | **7** | 2 |
| Hyperliquid | **0** | — |

**MEXC** (chyba až jednotky procent, viz níže):
`1000000BABYDOGE_USDT, 1000BTT_USDT, AKE_USDT, ASTEROID_USDT, B3_USDT,
BLAST_USDT, BOME_USDT, CAT_USDT, CHEEMS_USDT, DOGS_USDT, FLOKI_USDT,
HMSTR_USDT, HOT_USDT, IOST_USDT, LUNC_USDT, MEME_USDT, MEW_USDT,
NEIROCTO_USDT, NEX_USDT, NOT_USDT, PEPE_USDT, PTB_USDT, SATS_USDT, SHIB_USDT,
SLP_USDT, SPELL_USDT, SUPRA_USDT, TOSHI_USDT, VTHO_USDT, XEC_USDT, ZBCN_USDT`

**Bybit**:
`ACEUSDT, ALICEUSDT, APEUSDT, AXSUSDT, BICOUSDT, BLURUSDT, BTCUSDT, C98USDT,
CHRUSDT, DRIFTUSDT, DYDXUSDT, DYMUSDT, ENJUSDT, FILUSDT, FLOWUSDT, GMTUSDT,
GOATUSDT, ILVUSDT, LRCUSDT, LUNA2USDT, MAVIAUSDT, MELANIAUSDT, MMTUSDT,
MOVEUSDT, MYXUSDT, PIXELUSDT, PORTALUSDT, PUFFERUSDT, RAVEUSDT, SAGAUSDT,
SUSHIUSDT, TLMUSDT, USUALUSDT, WOOUSDT, WUSDT, XAIUSDT`

**Binance**:
`ACEUSDT, AXSUSDT, BTCUSDT, FILUSDT, GMTUSDT, GTCUSDT, MELANIAUSDT, MOVEUSDT,
MYXUSDT, NFPUSDT, PIXELUSDT, SKLUSDT, USDCUSDT, USUALUSDT, XAIUSDT`

**OKX**:
`1INCH-USDT-SWAP, APE-USDT-SWAP, AXS-USDT-SWAP, BTC-USDT-SWAP, DYDX-USDT-SWAP,
FIL-USDT-SWAP, GMT-USDT-SWAP, LUNA-USDT-SWAP, MOVE-USDT-SWAP, USDC-USDT-SWAP,
W-USDT-SWAP, YGG-USDT-SWAP`

**Lighter**: `BNB, BTC, LINK, SKHYNIX, SKHYNIXUSD, XAG, XRP`

**Hyperliquid**: žádný. Burza drží ceny na pěti platných číslicích; ověřeno na
vzorku 337 350 řádků, kde jen 0,27 % hodnot má šest číslic a jsou to celočíselné
ceny, které se do šesti vejdou.

### Rozsah škody u BTC

Nejlépe měřitelný případ. Tick 0,1, cena nad 100 000 potřebuje sedm číslic:

| burza | řádků nad 100 000 | ztratilo desetinnou část |
|---|---:|---:|
| OKX 1m | 309 989 | 100 % |
| Bybit 1m | 310 011 | 100 % |
| Binance / Bybit 1h | 5 165 | 100 % |

`105635.8` se uložilo jako `105636`. Pro minutové mean-reversion strategie je to
ztráta celé podcentové struktury.

### MEXC je jiný a horší případ

MEXC ceny neprošly šesticiferným zápisem, ale `.str(8, fixed)`. Škoda pochází
z `std::to_string(double)` výš v řetězci, což je šest **desetinných míst**:

```
0.0000028101 → 0.000003
```

To je chyba přes 6 %, ne 0,0001 %. Zasažené jsou symboly s cenovým krokem
jemnějším než `1e-6`; nejhorší jsou `SATS_USDT` (`1e-11`), `CHEEMS_USDT`
a `PEPE_USDT` (`1e-10`), `CAT_USDT`, `NEX_USDT`, `SHIB_USDT` (`1e-9`).

### Objemy

Plošně zasažené u všech burz kromě MEXC, který je psal `.str(10, fixed)`.
Jakákoliv hodnota nad milion má sedm číslic:

| burza | symbolů | řádků |
|---|---:|---:|
| Binance | 663 z 705 | 59 % |
| Bybit | 741 z 967 | 33 % |
| Hyperliquid | — | 14 % vzorku |

Relativní chyba do 0,0001 %. Na filtry likvidity, VWAP nebo objemové percentily
vliv nemá. Vadí jen tam, kde se objem používá jako přesné celé číslo nebo se
sčítá přes dlouhá okna.

---

## Chyby v mém vlastním měření

Seznam poškozených symbolů vznikl až napotřetí a stojí za to vědět jak.

1. Nejdřív jsem počítal jen „max cena ≥ 100 000", což chytí BTC, ale ne
   například `AXS` nebo `FIL`.
2. Pak jsem počítal číslice před desetinnou čárkou plus desetinná místa ticku.
   To u cen pod 1 počítá i vedoucí nuly — `0.060782` s tickem `1e-6` vyšlo na
   sedm číslic místo pěti. Dalo to 164 symbolů u Bybitu a 159 u Binance.
3. Pak se ukázalo, že Binance i Bybit vracejí tick s koncovými nulami
   (`0.0000100`) a `Decimal` je zachovává, takže exponent vyšel o dva řády níž.
   U Binance z toho vzniklo 485 symbolů.

Až normalizace ticku a kontrola na ručně spočítaných případech dala čísla
v tabulce výše. Před použitím seznamu doporučuji si namátkou pár symbolů ověřit.

---

## Co zůstává otevřené

- **Delistované symboly nelze posoudit** — 204 u MEXC, 298 u Bybitu, 144 u OKX,
  30 u Binance. Burza pro ně nevrací tick size. Většinou jde o levné alty, kde
  problém nehrozí, ale jistotu bez metadat nemám.
- **Opravu MEXC pagination a recovery jsem neověřil.** Je to nejrozsáhlejší
  změna v celém repozitáři a týká se právě té burzy, kde je škoda největší.
  Před hromadným stažením MEXC bych ji prověřil na malém vzorku přes hranici
  stránky.
- **Automatické testy** existují, ale `ctest` v tomto build adresáři hlásí
  7/10 — tři testy nenajdou svoje executables, protože registrované cesty
  počítají s multi-config layoutem.
- **Objemy** — rozhodnutí, zda je přestahovat, závisí na tom, jestli je někde
  používáš jako přesné číslo.

---

## Doporučený postup

1. Zálohovat stávající datasety.
2. Nasadit na VPS aktuální kód — bez toho by striktní agregace u 140 OKX
   symbolů nevyrobila žádné 5m ani 1h.
3. Začít MEXC, protože tam je škoda řádově největší; nejdřív pár symbolů přes
   hranici stránky a ověřit kontinuitu.
4. Poté cílené přestažení cenových symbolů z tabulky výše, po burzách.
5. Objemy podle toho, jak je používáš.
6. Po každém kroku spustit verifier a teprve pak regenerovat agregované soubory.

Verifier ani `--repair` zaokrouhlené hodnoty neobnoví.

---

## Provedená ověření

- Porovnání zapsaných CSV proti zdrojovému archivu OKX po jednotlivých polích
  (44 640 řádků, `Decimal`, bez konverze přes `double`).
- Důkaz, že archivní „šum" je přesný desítkový rozpis téhož float64.
- Přestažení BTC-USDT-SWAP po každé změně formátovače a kontrola velikosti
  souboru i obsahu (156 → 210 → 161 MB).
- Nezávislý přepočet agregace: 519 756 bucketů u 5m a 43 311 u 1h, přesná shoda.
- Skeny celých Bybit, Binance, Hyperliquid a Lighter datasetů; OKX na VPS.
- Přesnosti instrumentů stažené živě z pěti burz a normalizované.
- Kontrola exit kódů, TLS handshake proti API i CDN hostu, Release build po
  každé změně.

Všechna měření byla ruční. Jsou reprodukovatelná ze skriptů v této konverzaci,
ale nejsou součástí repozitáře jako regresní testy.
