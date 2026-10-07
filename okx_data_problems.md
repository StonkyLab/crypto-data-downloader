# OKX data problems — findings of 2026-10-07

Measured on the data VPS copy of `/data/crypto/okx/` synced to the dev box
(`/mnt/7054c714-3854-4b0e-bffd-5d94ebcf955b/crypto/okx/`), while re-verifying
the firm portfolio on OKX (EU migration). The per-contract work list is in
[`okx_backfill_gaps.csv`](okx_backfill_gaps.csv) next to this file.

## The problem in one sentence

OKX USDT swaps have raw history only from the day the downloader first saw
the symbol (download floors 2021-08-31, 2023-08-20, 2025-11-06), never from
their `available_since` in `info/futures_symbols.csv`, so before 2023-08 about
half of the listed roster has no bars and every OKX backtest before then runs
on a crippled universe.

## Evidence

Listed per `info/futures_symbols.csv` (`*-USDT-SWAP`) vs symbols with any 1h
bars that year vs symbols whose bars start within 7 days of listing or Jan 1:

| year | listed | any bars | from the start |
|---|---:|---:|---:|
| 2021 | 121 | 67 | 8 |
| 2022 | 141 | 76 | 75 |
| 2023 | 160 | 155 | 118 |
| 2024 | 249 | 245 | 243 |
| 2025 | 338 | 337 | 332 |

128 of 642 USDT swaps have a price or funding CSV starting more than 90 days
after `available_since`, or no CSV at all:

- **Prices (1h CSV first row)**: 64 symbols start 2021-08-31 (BTC, ETH, XRP,
  LTC, BCH, TRX, … all listed 2019-12 to 2021), 37 start 2023-08-20 (EOS, BSV,
  KNC, MATIC, XMR, …), DASH and ZEC start 2025-11 although listed 2020, 18
  symbols have no CSV at all (SRM, …). 60,346 symbol-days to backfill.
- **Funding (`*_fr.csv` first row)**: 98 symbols start 2021-08-31, a few in
  early 2022, 3 have no CSV. 32,825 symbol-days to backfill.
- Bar quality where bars exist is fine: in 2022 the median zero-volume share
  is 0.1 %, no missing hours.

Examples (available_since → first CSV row):

| symbol | listed | prices 1h from | funding from |
|---|---|---|---|
| BTC-USDT-SWAP | 2019-12-04 | 2021-08-31 | 2021-08-31 |
| MATIC-USDT-SWAP | 2021-04-29 | 2023-08-20 | 2021-08-31 |
| XMR-USDT-SWAP | 2020-05-18 | 2023-08-20 | 2021-08-31 |
| EOS-USDT-SWAP | 2019-12-04 | 2023-08-20 | 2021-08-31 |
| LUNA-USDT-SWAP | 2021-03-18 | 2022-05-28 (relaunched token only) | 2021-08-31 |
| DASH-USDT-SWAP | 2020-03-11 | 2025-11-06 | 2021-08-31 |
| SRM-USDT-SWAP | 2020-08-17 | missing | — |

## What it did to the backtests

- megafactor (top-10 by market cap): in 2021–2022 the OKX and Bybit top-10
  differed by 3.6 coins a day because MATIC, XMR and the original LUNA were
  absent on OKX (BNB is a genuine late OKX listing, 2022-12-23). Bybit
  constrained to the OKX roster loses the whole 0.3–0.4 Sharpe gap to OKX;
  prices, funding and lot sizes are not the cause.
- momo (full roster, liquidity gate): 2022 cross-section of 73 names on OKX
  vs 133 on Bybit, OKX −47 % vs Bybit +24 % that year; from 2024 the two
  venues agree.
- carry (top-50): same roster effect in 2021–2023.

## Source for the backfill (verified)

- The REST `GET /api/v5/market/history-candles` no longer knows delisted
  contracts (`code 51001` for MATIC-USDT-SWAP, XMR-USDT-SWAP).
- The bulk trade archive still serves them, including delisted ones:
  `https://static.okx.com/cdn/okex/traderecords/trades/daily/YYYYMMDD/<INST>-trades-YYYY-MM-DD.zip`
  — a zip with one CSV `instrument_name,trade_id,side,price,size,created_time`
  (`size` in contracts, `created_time` ms). Confirmed HTTP 200 with real zips
  for MATIC-USDT-SWAP 2021-10-15 (4.4 MB), 2022-06-15 (1.1 MB), 2023-01-15
  (0.7 MB), XMR-USDT-SWAP 2022-06-15, LUNA-USDT-SWAP 2022-03-15 (pre-crash
  token). Aggregate trades to 1m, then the existing `-g 5,60` step.
- Whether OKX serves funding-rate history before 2021-08-31 (bulk or REST)
  was NOT checked.

## Things to remember when fixing

- OKX sizes in contracts: 1 contract = `ctVal` base units; CSV `volume` is in
  contracts and `vol_ccy` in base. Keep that convention in the backfilled
  rows (the catalog builder converts).
- Old 2021 rows have `vol_ccy = 0`; the catalog builder derives `ctVal` for
  delisted contracts from `vol_ccy / volume`, so backfilled rows should carry
  `vol_ccy` where the source allows (trade `size` × `ctVal`).
- After the backfill the Nautilus catalog must be REBUILT from scratch
  (`build_catalog.py --exchange okx --rebuild`, then `--interval 1h`,
  `--interval 5m`, `--funding`): the delta append only compares the newest
  timestamp and would ignore older history. The X-Perp family
  (`okx/xperp/`) is unaffected.
- Then rerun megafactor, carry and momo `--exchange okx` in crypto-portfolio
  and refresh the OKX numbers in their READMEs; the 2021–2023 OKX figures
  there are explicitly marked as data-gap figures.
