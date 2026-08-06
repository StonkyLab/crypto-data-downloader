# Changelog
All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org/).

## [2.1.0](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.0) (2025-07-24)

- Initial release

## [2.1.1](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.1) (2025-09-28)

- [#1] Fix error while downloading binance spot data

## [2.1.2](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.2) (2025-10-28)

- Fix error while downloading binance spot data (old API version)
- Add automatic deletion of delisted symbols data

## [2.1.3](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.3) (2025-11-13)
- Improve automatic deletion of delisted symbols data

## [2.1.4](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.4) (2025-11-16)
- Add pagination to bybit getInstrumentsInfo, it downloaded data for max 500 symbols without it

## [2.1.5](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.5) (2025-11-17)
- Add continuous download for Bybit

## [2.1.6](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.6) (2026-01-18)
- Add option for handling delisted symbols data (keep or delete)

## [2.1.7](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.7) (2026-01-24)
- Change -k option for handling delisted symbols data (keep by default)
- Add -z option for t6 conversion (no conversion by default)
- Fix Bybit spot error

## [2.1.8](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.8) (2026-01-28)
- Add OKX Spot support
- Fix in Bybit candles
- Add/Improve rate limiters

## [2.1.9](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.9) (2026-01-31)
- Add MEXC support
- Many fixes I don't remember

## [2.2.0](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.2.0) (2026-02-18)
- Add support for delisting symbols on Binance
- Add support for delisting symbols on Bybit - separate Python script

## [2.2.1](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.2.1) (2026-02-24)
- Add possibility to run t6 conversion only

## [2.5.0](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.5.0) (2026-08-05)
- Fix OKX bulk history download: OKX tightened the `market-data-history` range limits from 20 months / 20 days to 10 months / 10 days, so the hardcoded 19-month and 19-day windows failed on every call (errors 50077 / 50076) and the whole bulk path silently produced nothing
- Compute OKX archive month boundaries in UTC+8, the zone the archive files are cut on
- Stop the per-symbol download at the first failed archive file instead of skipping forward; append-only CSVs cannot backfill a skipped file, so skipping turned transient failures into permanent multi-month holes
- Retry OKX archive listings and file downloads before giving up
- Keep delisted symbols found on disk in the OKX update set (unless `-d`), `/public/instruments` lists live contracts only
- Add `-g/--aggregate` to build coarser timeframes from an existing 1-minute dataset locally (OKX only publishes 1m bars in its bulk archive)
- Raise the OKX `market-data-history` rate limiter from 1 to 20 req/s (the endpoint advertises 60)
- Add `-x/--xperp` for OKX X-Perps (USD-settled perpetual-style futures, `instType=FUTURES` / `ruleType=xperp`), stored under `<output>/xperp/`. Candles come from the bulk archive; funding rates only from REST, because the archive's funding module rejects `instType=FUTURES` — that history decays after ~3 months and needs regular collection
- Parse `ruleType` on OKX instruments (separates X-Perps from the ordinary dated futures on the same endpoint)
- Floor archive listing windows onto OKX's own file boundaries: `market-data-history` returns a file only when `begin` falls on or before the day its period starts, so asking from an instrument's listing timestamp silently dropped its entire listing month
- Stop discarding archive rows flagged `confirm=0`. OKX writes 0 into the bulk files for whole stretches of history (2023-09-01 to 2024-07-18 for every symbol), and the parser threw those rows away — the cause of the multi-month holes across the dataset
- Read ZIP entries in a loop instead of trusting a single `mz_zip_reader_entry_read()`, and fail loudly on a short read; a monthly file was being truncated to its first ~116 KB
- Filter archive rows by instrument name: "futureschain" files are keyed by instrument family and can hold several contracts
- Re-request an archive listing when it links a file belonging to a different instrument than the entry names. OKX returns the SPOT archive URL from a SWAP entry in roughly half of the responses for some months, which spliced spot prices into futures series before the instrument-name filter, and left a hole after it
- Binance spot: keep the last candle when it has already closed. It was dropped unconditionally, which permanently cost every delisted symbol its final bar
