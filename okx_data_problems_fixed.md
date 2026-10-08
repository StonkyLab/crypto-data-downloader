# OKX data problems — fixes and resulting state

Date: 2026-10-08
Follows: [`okx_data_problems.md`](okx_data_problems.md) (findings of 2026-10-07)
and its work list [`okx_backfill_gaps.csv`](okx_backfill_gaps.csv).

## Outcome in one sentence

Every OKX USDT swap now has 1-minute history from `max(available_since,
2021-10-01)` — 641 of 642 contracts, YFV-USDT-SWAP (delisted 2020-10) being
the only one the venue has no source for — with 5m and 1h derived from it;
funding history is unchanged, and nothing before 2021-08-31 is recoverable
from OKX for either.

## What was actually wrong

The one-sentence problem statement in the findings ("raw history only from
the day the downloader first saw the symbol") turned out to be two different
causes plus one archive limit, measured against the venue:

| cause | contracts | evidence |
|---|---:|---|
| **OKX's bulk candle archive starts 2023-08-20 for every contract the venue has since delisted**, and holds nothing at all for one delisted before that date | 37 + 18 | listing `market-data-history` module 2 empty for MATIC/XMR 2021-09..2023-02; CDN `candlesticks/monthly/202206/MATIC-USDT-SWAP-…` 404 while ETH 200; first MATIC file is daily 2023-08-20 |
| **the downloader clamped a fresh download to the instrument's `listTime`**, which for a relisted contract is the relisting date | DASH, ZEC, ZEN, ENJ, KITE, OL, PIPPIN, LUNA | archive candle listing has KITE from 2025-10-28 with `listTime` 2026-04-09, OL from 2024-11-21 with `listTime` 2025-10-29 |
| the candle archive floor 2021-08-31 | 53 live + 11 delisted listed before that | BTC monthly listing starts 2021-08-31; REST `history-candles` reaches 2020-01 for BTC but answers `51001` for delisted and only from the relisting for relisted contracts |

The trade archive covers all of them from 2021-10-01 (daily and monthly,
BTC 2021-09-15 is a 404), same six-column format since 2021, cut on UTC+8
midnights like the candles.

Funding: the bulk funding archive starts 2021-08-31 for every contract,
including delisted ones (MATIC, XMR, EOS, DASH, BSV all begin there), and the
REST funding history returns nothing before mid-2021. The 32,825 funding
symbol-days before the floor in the work list are not recoverable from OKX.

## Fixes

### Downloader (crypto_data_downloader 2.7.9, `8707a66` … `701669d`)

1. **Trade fold** — `include/stonky/okx_trades_aggregator.h`. Every stretch
   the candle listing leaves uncovered (before its first file, between two
   files, or the whole range) is offered to the trade listing (module 1) and
   folded into 1-minute bars with the archive's own conventions:
   `volume` = Σ size (contracts), `vol_ccy` = volume × ctVal,
   `vol_ccy_quote` = Σ size × price × ctVal, a flat zero-volume bar at the
   previous close for every minute without a trade. Trades are ordered by
   `created_time` with a stable sort when a file is not already sorted; a
   step back of more than a second inside one minute aborts. ctVal comes from
   the live instrument, else the `vol_ccy/volume` ratio already in the CSV,
   else the first candle archive file, else 0 (the shape of the archive's
   own 2021 rows). Verified against the archive on a day where both exist
   (MATIC-USDT-SWAP 2023-08-21): all 1430 traded minutes identical in every
   column. Regression test `test/okx_trades_aggregator_test.cpp` carries a
   real minute of that day verbatim.
2. **Hand-over at the next candle file's first real row**, not its nominal
   period start (`3cb0ad7`). The monthly candle file for 2023-08 of a delisted
   contract starts on the 20th; the first run handed over on the 1st and left
   1–19 August 2023 missing from 41 contracts, and 1–12 May 2022 from LUNA.
3. **No clamp to `listTime`** (`8707a66`). The archive listing answers an empty
   window with one request and decides where the history starts.
4. **`-s` / `-a` restrict `-g` aggregation** (`2dcc105`). Aggregation rewrites
   every derived file from scratch — an hour over the OKX tree — so a repair
   of a few symbols no longer pays for all of them.
5. **Tests are opt-in** (`d9efea8`): `BUILD_TESTING` defaults to OFF; a
   deployment build compiles the downloader alone.

### Repair tool (data-server `616261c`)

`data_update_tools/okx_fold_trades_gap.py` fills a hole inside an existing
1m CSV from the one monthly trade archive spanning it, with the same fold,
and inserts the rows in place (dry run by default, `--apply` rewrites
atomically). It streams the zip line by line — LUNA's May 2022 archive holds
66 million trades — and fills flat bars up to the row after the hole only
when the last trade is within a day of it, so a relisting stays a gap.

Found on the way: a relisted contract's monthly archive is two sorted blocks
concatenated (LUNA May 2022: the relaunched token, `trade_id` restarting at
1, before the original one), so neither row order nor `trade_id` can be
trusted across a file; inside a block `created_time` is monotonic to within
a millisecond. Every other archive probed (LUNA 2022-04, XMR 2022-03, MATIC
2021-10, MATIC 2023-08) was sorted.

## What was run

1. 2026-10-07: downloader 2.7.9, 138 derived/1m files of 46 late-starting
   contracts removed, `update_okx_futures.sh` — ~3 h of folding, ~1 h of
   aggregation. Result: 641 contracts start at `max(available_since,
   2021-10-01)`, 18 previously missing contracts created, but the August 2023
   hole above.
2. 2026-10-08: `okx_fold_trades_gap.py --apply` on the 42 affected 1m files
   (`/tmp/okx_aug2023_symbols.csv`), then
   `crypto_data_downloader -e okx -o /data/crypto/okx -c f -b 1 -g 5,60 -a …`
   for those 42 only. Minutes, not hours.

## Verification

- Start of every `*-USDT-SWAP` 1m file vs `max(available_since,
  2021-10-01)`: **641 in range, 0 late, 1 missing (YFV)**.
- The 42 repaired contracts: key minute after the former hole present,
  monotonic, no duplicates, 5m and 1h present and ending with the 1m file
  (1h/5m begin at the first *complete* hour/five minutes after the 1m start —
  the aggregator omits incomplete buckets, which is correct).
- Independent recomputation in Python `Decimal` from the CDN daily trade
  files against the stored 1m rows: MATIC-USDT-SWAP 2022-06-15 (147,110
  trades) **1440/1440**, MATIC-USDT-SWAP 2023-08-10 (inside the repaired
  stretch, 66,318 trades) **1440/1440**, every column.
- Remaining holes of a day or more are all genuine: DASH 2023-12-19 →
  2025-11-06 and ENJ 2023-08-24 → 2024-03-01 (delisted, relisted), LUNA
  2022-05-13 → 2022-05-28 (original token's end → relaunch; deliberately not
  filled with flat bars).

## Known limits

- **Funding before 2021-08-31**: not available from OKX anywhere.
- **September 2021** of any contract delisted before 2023-08-20: the candle
  archive floor is 2021-08-31 but the trade archive starts 2021-10-01.
- **YFV-USDT-SWAP**: delisted 2020-10, before both archives.
- **Prices before 2021-08-31 for the 53 live contracts listed earlier**
  (BTC, ETH, XRP, …): reachable through REST `history-candles` back to the
  listing date, without a funding counterpart — not done ("point C" in the
  discussion; momo/megafactor would gain history, carry would not).
- **ctVal of the 17 contracts that never had a candle archive** (SRM, BTT,
  TORN, …): no free source; their `vol_ccy` and `vol_ccy_quote` are 0, like
  the archive's own 2021 rows. `volume` (contracts) is exact.
- **LUNA-USDT-SWAP** holds two assets under one instId: the original token
  to 2022-05-13 and the relaunched one from 2022-05-28, separated by the gap
  above.

## Next

Per `okx_data_problems.md`: rebuild the Nautilus catalog from scratch
(`build_catalog.py --exchange okx --rebuild`, then `--interval 1h`,
`--interval 5m`, `--funding`), rerun megafactor, carry and momo
`--exchange okx`, and refresh the OKX figures in their READMEs. The X-Perp
family (`okx/xperp/`) is unaffected.
