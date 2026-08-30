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

## [2.6.0](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.6.0) (2026-08-10)
- Write CSV numbers losslessly. Streaming a value used the stream's default of six SIGNIFICANT digits, so anything needing more was truncated: BTC-USDT-SWAP above $100 000 lost its 0.1 tick across 310 000 bars (105635.8 stored as 105636) and a volume of 1 205 829 became 1.20583e+06. Affected Binance, Bybit, OKX, Hyperliquid, Lighter and MEXC
- OKX: drop the still-forming candle instead of the oldest one. The REST endpoint answers newest-first, so the code discarded a complete candle and kept the partial one, which the append-only CSV then froze permanently
- MEXC: stop converting prices through `std::to_string(double)`, which formats six decimal places and collapsed everything below 0.000001 to zero
- Enable TLS peer and hostname verification on Binance, OKX and Hyperliquid (Bybit and Lighter already had it). The OKX archive host is verified against the host in the download URL
- Return a non-zero exit code on fatal errors; verification exits 1 for repairable damage and 2 for gaps, so cron can tell them apart
- Refuse to fall back to "all exchange symbols" when `-a` points at an empty or unreadable file

## [2.6.1](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.6.1) (2026-08-10)
- Complete the lossless-number fix: the Binance funding-rate writer was missed in 2.6.0 and still wrote six significant digits
- Serialise decimal values as normalised binary64, shortest round-tripping form. Writing the 50-digit expansion verbatim carried no information (the source values are doubles) and grew BTC-USDT-SWAP 1m from 156 MB to 210 MB
- Aggregation (`-g`) no longer abandons a whole symbol over incomplete source buckets: a bucket with missing bars is omitted, everything else is written, and the run exits 2. Exchange outages never backfill, so failing on them meant 140 of 567 OKX futures symbols would never get a 5m or 1h file at all
- Aggregation reads each source twice and localises damage to the affected bucket: a forward timestamp outlier or a malformed number costs one bucket instead of poisoning everything after it. Rewrites are protected by a per-target advisory lock, atomic replacement and a subset proof so a corrupted source cannot authorise mass deletion of previously published rows
- OKX: stop appending newer archive files past an unresolved foreign-link listing entry; the missing period stays the tail instead of becoming a permanent internal hole. Candle and funding writers check stream state after flush/close and no longer advance the resume timestamp on a failed write
- MEXC spot pagination respects the exclusive `endTime`, so one candle is no longer lost at every 1000-row page boundary; historical requests send both bounds so the API cannot ignore a lone historical `endTime` and answer with current data
- MEXC downloads go through transactional staging: a manifest with numbered batches, schema/alignment/monotonicity validation, per-symbol advisory locks and atomic replacement. An interrupted run can no longer splice a newest-first fragment past the old tail and create a permanent middle gap
- MEXC records unproven history boundaries in `.prefix.pending` / `.prefix-provisional` sidecar files next to the CSV; later runs keep probing for the older prefix and merge it in by timestamp union. These sidecars and the `.lock` files are recovery state — do not delete them
- Propagate per-symbol worker failures to the process exit code; a run where symbols failed can no longer exit 0. Aggregation over a missing dataset exits 1
- Cap the number of concurrently created download threads by acquiring the slot before `std::async`, deduplicate symbol lists, validate symbols used as filename components, switch log sinks to the multithreaded variants
- T6 writers publish via locked sibling temp files and atomic replacement, and use the actual interval for close timestamps instead of a hardcoded minute; Bybit/MEXC/OKX monthly and MEXC weekly conversions use calendar boundaries (Monday weeks, UTC+8 for OKX)
- Add deterministic regression tests (11 CTest targets) covering number round-trips, tail recovery, pagination, staging, atomic writes, intervals, aggregation policy and the verifier
- Remove the GitHub Actions workflow; sanitizer and coverage remain as local, opt-in CMake configurations

## [2.6.2](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.6.2) (2026-08-11)
- Accept non-ASCII exchange symbols again. The 2.6.1 symbol validation allowed only ASCII letters, digits, `_` and `-`, so Binance perpetuals with CJK names (币安人生USDT, 龙虾USDT — on disk in this dataset for years) were rejected as "unsafe", and because validation threw over the whole list, one such symbol aborted the entire exchange run. The check now rejects only what genuinely cannot be a file name: empty, `.`/`..`, path separators, control bytes, Windows-reserved punctuation and names over 240 bytes
- A rejected symbol is skipped with an error instead of aborting the run; filtering a non-empty list down to nothing still fails, because an empty list means "the whole exchange" downstream and that silent flip would be worse than stopping

## [2.7.0](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.7.0) (2026-08-11)
- Add `--since YYYY-MM-DD` (or milliseconds): the oldest date a symbol without local data may reach for. Intended for the point where old years are archived off the server — without it every fresh symbol pulls the full history back in
- The floor applies only where there is no usable local data: the fallback for a missing/empty/header-only CSV, the lower bound of Lighter's listing-date probe and the cutoff of a first MEXC funding scan. A file that already holds records always resumes from its own tail, so `--since` cannot skip forward over stored data and open a gap
- Route all eight downloaders' hardcoded oldest-history constants through one shared `historyFloor()` policy instead of fifteen separate copies
- Parse `--since` strictly and timezone-independently: reject normalized invalid dates, trailing characters, whitespace, signed/overflowing millisecond values and future timestamps instead of silently accepting a different floor
- Keep the fresh floor inclusive while treating a real CSV tail as exclusive, including candle and funding boundaries; an existing tail always wins over a later `--since`
- Reconcile MEXC prefix markers after archiving, align raw millisecond floors to the first eligible candle, and keep MEXC funding scans from jumping over an existing tail
- Discover OKX delisted symbols from individually compressed `.csv.gz`, `.csv.xz`, `.csv.bz2` and `.csv.zst` files when the live CSV has been archived
- Make the process-wide floor storage race-free while retaining the set-before-workers contract
- Add deterministic parser, fresh/resume boundary, archive-name, MEXC marker/alignment and funding-cutoff regression coverage
- Drop the per-target `.csv.lock` files the aggregator left next to 5m/1h data. Concurrent runs are serialized per exchange by a flock in the update scripts instead — one mechanism for every venue and phase, invisible in the data directories. `AtomicFileWriter` keeps sibling locking for the MEXC staging and repair paths


## [2.7.1](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.7.1) (2026-08-29)
- Fix MEXC spot 1h/1m/5m history arriving with half of every file missing. `/api/v3/klines` truncates every response at 500 rows whatever `limit` requests (1000 and 721 both come back as 500) and fills a window from its `startTime`, but the backward paginator strode 1000 intervals per window and then moved the cursor to the oldest returned row. Each window therefore served only its older half and the cursor stepped over the newer half unfetched, leaving alternating 500-bar blocks and 500-bar holes at 50.7 % coverage — and, because the very first window is the newest, no data at all for the last ~500 hours. Stride now equals the venue's row cap
- Fail closed if MEXC ever truncates a window again: a cap-sized page whose newest row stops short of the window top now raises `IncompletePaginationError` for that symbol instead of silently skipping the unfetched remainder

## [2.7.2](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.7.2) (2026-08-30)
- Percent-encode MEXC Spot query parameters. MEXC lists symbols with CJK names — 龙虾USDT, 牛来USDT, 我踏马来了USDT and 币安人生USDT are live with real volume — and their UTF-8 bytes written straight into the query string made the request malformed, so the venue answered 400 with an empty body and those symbols were skipped on every run. Encoding is the identity for the ASCII names, decimal numbers and hex signatures every other parameter uses, so the signed payload of existing calls is unchanged byte for byte. The futures session is deliberately left alone: its symbols are ASCII (`BTC_USDT`) and that signing path serves live trading
