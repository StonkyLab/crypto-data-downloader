# Crypto Data Downloader

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![CMake](https://img.shields.io/badge/CMake-4.0+-green.svg)](https://cmake.org/)

A high-performance command-line utility for downloading historical market data (OHLCV candles) and funding rates from major cryptocurrency exchanges.

## Features

- **Multi-Exchange Support**: Binance, Bybit, OKX, MEXC, Hyperliquid, and Lighter
- **Multiple Data Types**: OHLCV candles and funding rate history
- **Parallel Downloads**: Configurable concurrent job processing
- **Flexible Output**: CSV format with optional T6 (Zorro) conversion
- **Incremental Updates**: Only downloads new data since last run
- **Symbol Filtering**: Download specific symbols or all available pairs
- **Multiple Timeframes**: Support for various bar sizes (1m, 5m, 15m, 1h, etc.)
- **Market Categories**: Supports both Spot and Futures markets
- **Data Integrity**: Self-healing resume after interrupted writes, plus a built-in verify/repair mode (`-y` / `-r`)

## Supported Exchanges

| Exchange     | Futures | Spot | Candles | Funding Rates |
|--------------|:-------:|:----:|:-------:|:-------------:|
| Binance      | ✅ | ✅ | ✅ | ✅ |
| Bybit        | ✅ | ✅ | ✅ | ✅ |
| OKX          | ✅ | ✅ | ✅ | ✅ |
| MEXC         | ✅ | ✅ | ✅ | ✅ |
| Hyperliquid  | ✅ | ❌ | ✅ | ✅ |
| Lighter      | ✅ | ❌ | ✅ | ✅ |

### Exchange-Specific Notes

#### Lighter

Lighter supports **perpetuals only** (no Spot). Symbols are coin names without a quote suffix (e.g. `BTC`, `ETH`).

Lighter's mainnet (zkLighter) launched in late 2024. API host: `mainnet.zklighter.elliot.ai`.

On the **first run** for a symbol the downloader probes its listing date and
paginates from that date; subsequent runs append only data since the last CSV
record. If the listing-date probe fails, the run fails closed instead of
silently committing a shortened history.

**Funding rates** cadence is 1 hour, available from late 2024.

> **Note:** The API sits behind an AWS WAF that returns HTTP 405 + CAPTCHA under sustained flooding. Downloads are sequential (`maxConcurrentDownloadJobs{1}`) and the retry loop also catches "405" / "captcha" patterns.

**Optional: higher rate limits via tier upgrade**

Lighter's Standard tier (60 req/min, ~1 req/s) is the default. To go faster, set up a Lighter account and export a read-only auth token before running the downloader:

| Tier      | /candles  | `LIGHTER_MIN_REQUEST_INTERVAL_MS`    |
|-----------|-----------|--------------------------------------|
| Standard  | 60/min    | unset (default 1000)                 |
| Premium   | 80/min    | `750`                                |
| Plus      | 400/min   | `150`                                |
| Builder   | 800/min   | `75`                                 |

> **Důležité:** mít `LIGHTER_AUTH_TOKEN` nastavený sám o sobě **nepovyšuje tvůj tier** — Plus/Builder vyžadují explicit opt-in. Pokud máš jen token a jsi pořád Standard, nech `LIGHTER_MIN_REQUEST_INTERVAL_MS` nenastavený (downloader použije bezpečných 1000 ms). Nastavení nižšího intervalu než tvůj reálný tier vede k AWS WAF banu IP adresy.

Setup (one-time):

1. Register an account at lighter.xyz with an Ethereum wallet (no deposit required, gas < $1).
2. Generate a read-only API key via the Lighter Python SDK (`system_setup.py` in `elliottech/lighter-python`).
3. For Plus or Builder tier, open a Discord #support ticket — Plus requires the IP whitelist, Builder requires a brief use-case description.
4. Export the token:
   ```bash
   export LIGHTER_AUTH_TOKEN="ro:account_index:scope:expiry_unix:nonce_hex"
   export LIGHTER_MIN_REQUEST_INTERVAL_MS=75  # adjust to your tier
   ```

Without the env vars the downloader falls back to Standard tier — works fine for incremental updates, slow for first-time bootstraps. See the [Lighter rate-limits docs](https://apidocs.lighter.xyz/docs/rate-limits) for details.

#### Hyperliquid

Hyperliquid supports **perpetual futures only** (no Spot). Symbols are coin names without a quote suffix (e.g. `BTC`, `ETH`, `SOL`), not trading pairs.

**Candle history available via API:**

| Interval | Available history |
|----------|-------------------|
| 1m       | ~3.5 days         |
| 5m       | ~2–4 weeks        |
| 15m      | ~5 weeks          |
| 1h       | ~6 months         |
| 8h+      | from March 2023   |

On the first run the downloader fetches the last 5 000 candles of the requested interval. Subsequent runs append only new data since the last recorded timestamp. For intervals shorter than 8h, data before a per-interval cutoff simply does not exist in the Hyperliquid API — use an external source (e.g. 0xArchive) for a full bootstrap.

**Funding rates** are available for all perpetuals back to **May 2023** (full history).

> **Note:** Downloads are sequential due to Hyperliquid API rate limits.

#### MEXC Historical Data Limits

MEXC API has **undocumented limits** for historical candlestick data:

| Interval | Spot Available | Futures Available |
|----------|----------------|-------------------|
| 1m       | ~30 days       | ~30 days          |
| 5m       | ~270 days      | ~360 days         |
| 15m      | ~270 days      | ~180-365 days     |
| 30m      | ~270 days      | 5+ years          |
| **1h+**  | **Complete**   | **Complete**      |

> **Recommendation:** Use **1h (hourly)** or larger intervals for complete MEXC historical data.

MEXC candle downloads deliberately tolerate exchange outages. A committed CSV
must have the exact venue schema, finite binary64-compatible values, aligned
timestamps and strictly increasing rows, but missing aligned candle slots are
preserved as gaps instead of invalidating the rest of the history. Duplicate,
out-of-order, misaligned or malformed rows still fail the transaction.

Concurrent runs are excluded once, for the whole process: at startup the
downloader takes a non-blocking OS advisory lock keyed on the exchange and the
canonical output directory, and holds it through downloading, aggregation and
verification. A second run over the same data exits immediately with
`A mexc downloader is already running for /data/crypto`. The lock file lives in
`$XDG_RUNTIME_DIR` (or the system temp directory), deliberately outside the data
tree so an HTTP server publishing that tree never serves it; its ownership is a
kernel file description, released on exit, on SIGKILL and on a crash alike.
There are no per-symbol or per-file lock files.

Updates are published through validated staging plus atomic replacement. If a
first download reaches only a provisional MEXC availability boundary, the usable
suffix may be published, but `<SYMBOL>.csv.prefix.pending` is written first.
Every later run probes the missing interval. If older candles become available,
the downloaded range is merged by timestamp with every row already stored
locally and the union atomically replaces the suffix. A transient API omission
therefore cannot erase a candle that is already on disk; conflicting values at
the same timestamp fail closed. The marker is removed only after a positive scan
reaches the originally requested start. Negative probes never silently declare a
shortened history complete — on MEXC Spot in particular a probe anchored at the
requested start only ever inspects the oldest 500-interval window of the range,
so its empty answer proves nothing about a gap further up.

When an explicit `--since` is supplied after archiving, an orphan marker whose
CSV was removed is retired. If a live suffix and marker remain, the marker is
rebased to the first candle boundary at or after the new floor, so recovery can
fill a wanted gap but cannot restore intentionally archived rows before it.

#### MEXC Delisted Symbols

MEXC Futures API does not provide a bulk endpoint for delisted contracts — the `/api/v1/contract/detail` endpoint returns only active symbols. However, historical data for delisted symbols **is still available** when queried individually.

To download data for delisted MEXC futures symbols, maintain your own list of delisted symbols and pass it via the `-s` parameter:

```bash
./crypto_data_downloader -e mexc -c f -s "HOOK_USDT,BNKR_USDT,LUNA2USDT" -o /data/mexc
```

For a known delisted symbol the downloader uses a bounded newest-candle probe
instead of walking years of empty post-delisting windows. An empty probe safely
preserves an existing CSV as a no-op; for a fresh symbol it refuses to create a
false-success file with no data.

## Requirements

- C++23 compatible compiler (GCC 13+, Clang 17+, or a current MSVC 2022)
- CMake 4.0 or later
- Git (for submodules)

### Dependencies

- [OpenSSL](https://www.openssl.org/) - TLS/SSL support
- [Boost 1.88+](https://www.boost.org/) - Networking (Beast, Asio)
- [zlib](https://zlib.net/) - Bybit archive decompression
- [minizip-ng 4.x](https://github.com/zlib-ng/minizip-ng) - OKX archive decompression (fetched only when no package is installed)
- [spdlog](https://github.com/gabime/spdlog) - Logging
- [nlohmann/json](https://github.com/nlohmann/json) - JSON parsing
- [cxxopts](https://github.com/jarro2783/cxxopts) - Command-line parsing
- [magic_enum](https://github.com/Neargye/magic_enum) - enum reflection
- [libsecp256k1](https://github.com/bitcoin-core/secp256k1) - Hyperliquid signing support

## Installation

### Windows (canonical CMake workflow)

CMake is the source of truth for Windows builds. The repository does not keep a
hand-maintained Visual Studio solution or project files; generate them from
`CMakeLists.txt` with Visual Studio 2022 as shown below.

1. **Install CMake**: Download from [cmake.org](https://cmake.org/download/)

2. **Install Visual Studio 2022**: Download [Visual Studio Community](https://visualstudio.microsoft.com/downloads/) and install with **Desktop development with C++** workload.

3. **Install the pinned vcpkg release** (package manager):
   ```powershell
   git clone --branch 2025.10.17 https://github.com/Microsoft/vcpkg.git C:\vcpkg
   cd C:\vcpkg
   .\bootstrap-vcpkg.bat
   ```

4. **Clone the project and initialize every submodule**:
   ```powershell
   git clone https://github.com/vitakot/crypto_data_downloader.git
   cd crypto_data_downloader
   git submodule update --init --recursive
   ```

5. **Generate a Visual Studio 2022 build**. The vcpkg toolchain reads and
   installs the checked-in dependency manifest during configuration:
   ```powershell
   cmake -S . -B build -G "Visual Studio 17 2022" -A x64 "-DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake"
   cmake --build build --config Release --parallel
   ctest --test-dir build -C Release --output-on-failure
   ```

   The checked-in `vcpkg.json` installs the complete dependency set, including
   the `minizip-ng` port (not the unrelated legacy `minizip` port). Visual
   Studio can also open the repository folder directly and consume the same
   CMake project.

### Linux (Ubuntu/Debian)

1. **Install build tools**. Verify that CMake is 4.0 or newer; older
   Ubuntu/Debian releases may require the official CMake packages or binaries
   instead of the distribution version:
   ```bash
   sudo apt update
   sudo apt install -y build-essential git ninja-build curl zip unzip tar pkg-config autoconf automake libtool
   ```

2. **Clone the project and initialize every submodule**:
   ```bash
   git clone https://github.com/vitakot/crypto_data_downloader.git
   cd crypto_data_downloader
   git submodule update --init --recursive
   ```

3. **Install the pinned vcpkg release, then build**:
   ```bash
   git clone --branch 2025.10.17 https://github.com/Microsoft/vcpkg.git ../vcpkg
   ../vcpkg/bootstrap-vcpkg.sh -disableMetrics
   cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
       -DCMAKE_TOOLCHAIN_FILE=../vcpkg/scripts/buildsystems/vcpkg.cmake
   cmake --build build --parallel 2
   ctest --test-dir build --output-on-failure
   ```

The manifest uses `minizip-ng`; when building without vcpkg and without an
installed minizip-ng CMake package, the OKX connector fetches minizip-ng 4.0.7.
Set `-DOKX_FETCH_MINIZIP=OFF` for a fully offline, fail-closed configuration.

### Tests, sanitizers, and coverage

The deterministic regression suite does not need exchange connectivity. A
minimal test-only build avoids all connector and minizip dependencies:

```bash
cmake -S . -B build-tests -DBUILD_DOWNLOADER=OFF -DBUILD_TESTING=ON
cmake --build build-tests -j2
ctest --test-dir build-tests --output-on-failure
```

With GCC or Clang, add `-DENABLE_SANITIZERS=ON` to run the same suite under
AddressSanitizer and UndefinedBehaviorSanitizer. Use a separate build directory
with `-DENABLE_COVERAGE=ON` for compiler coverage instrumentation. Both options
fail configuration on unsupported compilers. No hosted CI workflow is committed;
run these checks locally before publishing a change.

Live MEXC account tools are not part of CTest; destructive tools require the
explicit connector option `ENABLE_DESTRUCTIVE_MEXC_TOOLS=ON`.

## Usage

```bash
crypto_data_downloader [OPTIONS]
```

### Command-Line Options

| Option | Long Form | Description | Default |
|--------|-----------|-------------|---------|
| `-e` | `--exchange` | Exchange: `bnb` (Binance), `bybit`, `okx`, `mexc`, `hl` (Hyperliquid), `lt` (Lighter) | `bnb` |
| `-t` | `--data_type` | Data type: `c` (candles), `fr` (funding rates) | `c` |
| `-o` | `--output` | Output directory path | *required* |
| `-s` | `--symbols` | Symbols to download (comma-separated) or `all` | `all` |
| `-a` | `--assets_file` | Path to Zorro Assets file (alternative to `-s`) | - |
| `-j` | `--jobs` | Maximum parallel download jobs | auto |
| `-b` | `--bar_size` | Bar size in minutes (1, 5, 15, 30, 60, etc.; exchange-native `1M` is `43200`) | `1` |
| `-c` | `--category` | Market category: `f` (futures), `s` (spot) | `f` |
| `-d` | `--delete_delisted` | Delete delisted symbols data files | - |
| `-z` | `--t6_conversion` | Convert existing CSV data to T6 format (Zorro Trader) without downloading | - |
| `-g` | `--aggregate` | Aggregate the `-b` bar size into coarser timeframes (comma-separated minutes) without downloading | - |
| - | `--allow_partial_aggregation` | Emit a partial coarse candle from the available valid source bars; by default an incomplete bucket alone is skipped | - |
| - | `--since` | Earliest UTC date (`YYYY-MM-DD`) or epoch-millisecond timestamp for a symbol with no usable local CSV | - |
| `-x` | `--xperp` | OKX only: download X-Perps instead of USDT swaps, into `<output>/xperp/` | - |
| `-y` | `--verify` | Verify CSV data integrity (torn lines, duplicates, ordering, gaps) without downloading | - |
| `-r` | `--repair` | Verify and repair CSV data files in place | - |
| `-v` | `--version` | Print version and exit | - |
| `-h` | `--help` | Print help and exit | - |

### Examples

**Download all Binance futures 1-minute candles:**
```bash
./crypto_data_downloader -e bnb -t c -o /data/binance -c f
```

**Download specific symbols from Bybit:**
```bash
./crypto_data_downloader -e bybit -s "BTCUSDT,ETHUSDT,SOLUSDT" -o /data/bybit
```

**Download 5-minute candles:**
```bash
./crypto_data_downloader -e bnb -b 5 -o /data/binance_5m
```

**Download funding rate history from OKX:**
```bash
./crypto_data_downloader -e okx -t fr -o /data/okx
```

**Continue with a bounded live history after archiving old CSV files:**
```bash
./crypto_data_downloader -e okx -o /data/okx --since 2024-01-01
```

`--since` is used only when a symbol's active CSV is missing, empty or contains
only its header. A CSV that still contains records always resumes after its own
tail, even when that tail is older than `--since`, so the option cannot create a
gap in existing live data. The first available record whose timestamp is equal
to the fresh floor is included. Dates are interpreted as midnight UTC and must
be an exact, valid `YYYY-MM-DD`; epoch milliseconds must contain decimal digits
only. Future timestamps and the Unix epoch itself are rejected.

For OKX `all` mode, individually compressed files named
`<symbol>.csv.gz`, `.csv.xz`, `.csv.bz2` or `.csv.zst` still identify delisted
symbols after the live CSV is removed. Container archives such as `tar.gz` or
ZIP files are not inspected; pass those symbols explicitly with `-s` or `-a`.

**Download OKX perpetual candles and build 5m/1h from them:**
```bash
./crypto_data_downloader -e okx -c f -b 1 -o /data/okx    # 1m only — see OKX notes below
./crypto_data_downloader -o /data/okx -b 1 -g 5,60        # local aggregation, no network
```

Aggregation is gap-tolerant by default. A closed target bucket is emitted only
when all expected source bars are present and contiguous; an incomplete bucket
is skipped without discarding any complete buckets before or after the outage.
This includes outages spanning one or more entire target intervals. The command
processes every symbol and target, publishes the usable output, and exits with
code `2` when at least one closed bucket had to be omitted.

If a bucket with at least one valid source bar should deliberately be represented
by a partial OHLCV candle, opt in explicitly:

```bash
./crypto_data_downloader -o /data/okx -b 1 -g 5 --allow_partial_aggregation
```

An entirely absent target interval cannot be synthesized even in partial mode;
it remains omitted and is reported through exit code `2`. Derived files are
rebuilt through an atomic replacement on each CLI run, so a later repair of the
source can insert a previously missing bucket. A fatal unsupported schema or
I/O failure still preserves the previous target and exits with code `1`.

Before that atomic rewrite is committed, every row that already exists in the
target must either reappear in the new output or correspond to a source bucket
that was explicitly observed and classified as damaged. An unseen prefix,
tail, or isolated rogue timestamp therefore cannot authorize bulk deletion,
while a gap that was already absent from the target does not block later
updates.

The aggregator resolves columns by their exact header names and preserves the
venue's schema. It keeps the first `open`, maximum `high`, minimum `low`, and
last `close`; known quantity/count columns are summed with decimal
multiprecision and stored as the shortest round-tripping binary64 text used by
the backtest pipeline. Binance's `timestamp` remains the bucket open time,
`close_time` becomes the bucket's inclusive end, and `ignore` is copied from
the final source row. An unknown/unsupported header or a read/write failure is
fatal. Torn-width, invalid-timestamp, non-finite numeric, duplicate and
out-of-order source rows taint their affected bucket, which is never emitted;
the remaining safe buckets are still published. The source damage can be
diagnosed separately with `--verify`.

The trailing in-progress bucket is always held back. Calendar-month aggregation
is also intentionally unavailable through `-g`: `43200` selects the exchanges'
native `1M` interval, but a real calendar month cannot be represented by a fixed
number of minutes.

### OKX history — what the exchange actually serves

OKX splits historical market data between a bulk file archive
(`/api/v5/public/market-data-history`, ZIP files on `static.okx.com`) and the
paginated REST endpoints. The downloader uses both, and the split is not
symmetric between candles and funding:

| | bulk archive | REST endpoint |
|---|---|---|
| candles | 1-minute only, monthly files for complete months plus daily files for the last ~1 year | `/market/history-candles`, any bar size, back to the listing date, 100 bars per request |
| funding | monthly files only, **no** daily aggregation | `/public/funding-rate-history`, **last ~3 months only** |

Consequences worth knowing before trusting the dataset:

- **History starts 2021-09.** The oldest archive file of any instrument is the
  `2021-09` batch (2021-08-31 16:00 UTC). Candles reach further back through the
  REST endpoint, but funding does not — so the downloader treats the archive
  floor as the start of history for both.
- **Funding for 2021-10, 2021-11 and 2021-12 does not exist.** OKX never
  published those monthly files. The hole is in the exchange's archive, not in
  the downloader; it cannot be filled from any endpoint.
- **Archive files are cut on UTC+8 midnights.** A "2024-09" file starts
  2024-08-31 16:00 UTC. Month boundaries are computed in that zone.
- **Range limits are 10 months / 10 days.** `market-data-history` rejects longer
  ranges with error `50077` / `50076`. The downloader walks the history in
  9-month and 9-day windows.
- **Higher timeframes are built locally** with `-g`, because the archive only
  carries 1-minute bars.
- **Delisted symbols stay in the update set.** `/public/instruments` lists only
  live contracts, so when downloading `all`, symbols found on disk but no longer
  on the exchange are refreshed too (unless `-d` is given). The bulk archive
  still serves them, and dropping them would put survivorship bias into the
  dataset.

### OKX X-Perps (`-x`)

Alongside the USDT swaps OKX lists **X-Perps** — `instType=FUTURES`,
`ruleType=xperp`, instrument IDs like `BTC-USD_UM_XPERP-310404`. They carry a
nominal ~2031 expiry that exists only to satisfy EEA regulation; economically
they are linear, USD-settled perpetuals with 8-hour funding. The instrument
catalog is byte-identical on `www.okx.com` and `eea.okx.com`, so market data
needs no EEA host.

`-x` switches the downloader to that product. Data land in `<output>/xperp/`,
not in `futures/`, because the two products settle in different currencies and
only the file name would otherwise distinguish them.

The data sources are asymmetric, and one of them decays:

- **Candles: bulk archive works.** `instType=FUTURES` with the family name
  (`BTC-USD_UM_XPERP`) returns `...-futureschain-candlesticks-YYYY-MM.zip`
  files in the same 10-column format as the swap archive.
- **Funding: no bulk archive at all.** The archive's funding module accepts
  only `instType=SWAP` and answers `50016 "Parameter instType doesn't match
  parameter module"` for futures. The REST endpoint does serve X-Perp funding,
  but only for roughly the last 3 months — so **X-Perp funding history is lost
  unless it is collected regularly**. The product listed in 2026-03; by
  2026-08 the first ~5 weeks of BTC X-Perp funding were already unreachable.

**Download MEXC futures candles (hourly recommended):**
```bash
./crypto_data_downloader -e mexc -c f -b 60 -o /data/mexc
```

**Download MEXC spot data:**
```bash
./crypto_data_downloader -e mexc -c s -b 60 -o /data/mexc_spot
```

**Download MEXC funding rate history:**
```bash
./crypto_data_downloader -e mexc -t fr -o /data/mexc
```

MEXC funding updates publish through atomic whole-file replacement. Empty first
pages are retried and ambiguous pagination fails closed. Every run scans all
declared pages, validates stable pagination metadata, merges the complete
download with the complete local CSV by timestamp, and atomically publishes the
union — never an incremental append from the stored tail, because MEXC exposes
no authoritative start-of-history and an exact tail overlap cannot prove a
truncated snapshot did not omit a middle page. A temporarily shortened but
internally consistent API snapshot can therefore add records but cannot hide an
older or middle gap forever; a later wider snapshot fills it.

**Download Hyperliquid perpetuals — 1h candles (all symbols):**
```bash
./crypto_data_downloader -e hl -c f -b 60 -o /data/hyperliquid
```

**Download specific Hyperliquid symbols:**
```bash
./crypto_data_downloader -e hl -b 60 -s "BTC,ETH,SOL" -o /data/hyperliquid
```

**Download Hyperliquid funding rate history (all symbols, from May 2023):**
```bash
./crypto_data_downloader -e hl -t fr -o /data/hyperliquid
```

**Download Lighter perpetuals — 1h candles (all symbols):**
```bash
./crypto_data_downloader -e lt -c f -b 60 -o /data/lighter
```

**Download specific Lighter symbols:**
```bash
./crypto_data_downloader -e lt -b 60 -s "BTC,ETH,SOL" -o /data/lighter
```

**Download Lighter funding rate history (all symbols):**
```bash
./crypto_data_downloader -e lt -t fr -o /data/lighter
```

**Download Binance spot data:**
```bash
./crypto_data_downloader -e bnb -c s -o /data/binance_spot
```

**Delete data for delisted symbols:**
```bash
./crypto_data_downloader -e bnb -d -o /data/binance
```

**Convert existing CSV files to T6 format (Zorro) without downloading:**
```bash
./crypto_data_downloader -z -o /data/binance -e bnb -b 1 -c f
```

**Convert spot 1-hour data to T6:**
```bash
./crypto_data_downloader -z -o /data/binance -e bnb -b 60 -c s
```

**Verify Bybit 1-minute candle data integrity (no download, no modification):**
```bash
./crypto_data_downloader -e bybit -o /data/bybit -b 1 -y
```

**Verify and repair Bybit funding rate files in place:**
```bash
./crypto_data_downloader -e bybit -o /data/bybit -t fr -r
```

### Data Integrity

An interrupted run (kill, power loss, full disk) can leave a partially written
last line in a CSV file. Since v2.4.0 the downloader self-heals on the next
run: the torn tail is truncated and the download resumes from the last valid
record. (Older versions instead reset the resume point and silently appended a
full duplicate of the symbol's history — if your data predates v2.4.0, run
`--repair` once per data directory to clean it up.)

The verify mode (`-y`) checks every CSV file in the selected directory for
torn/glued lines, duplicate timestamps, out-of-order blocks and gaps. The
repair mode (`-r`) additionally rewrites affected files in place (atomic
replace): malformed lines are dropped, duplicates removed (first occurrence
wins), ordering restored, and legacy Bybit 7-column rows converted to the
current 6-column format. Gaps are only reported — missing data cannot be
restored locally; delete the affected file and re-download where the exchange
still serves the range.

A reported gap is not by itself structural corruption: it may be a real
exchange outage that no re-download can fill. In particular, MEXC persistence
keeps valid aligned rows on both sides of such a gap, and local aggregation
omits only the affected coarse bucket instead of rejecting the symbol's full
history.

Gap analysis is automatically skipped for Binance Spot, where missing bars are
exchange-native rather than a data defect (the kline API omits zero-trade
intervals entirely; exchange-wide outages in 2021 also left holes in every
symbol trading at the time). A re-download can never fill these gaps — the
data does not exist on the exchange.

> **Note:** Versions before 2.4.0 filtered zero-volume bars from Hyperliquid
> and Lighter downloads, leaving artificial gaps (both exchanges serve a
> continuous series where no-trade bars legitimately carry zero volume).
> Since 2.4.0 all bars are stored as-is. To refill the artificial gaps in
> previously downloaded data, delete the affected price CSVs and re-download
> (Lighter retains full history; Hyperliquid only the most recent ~5 000 bars
> per interval).

## Output Format

### Candle Data (CSV)

Files are saved to `<output_dir>/futures/prices/csv/<timeframe>/<SYMBOL>.csv`
(or `spot/prices/csv/` for spot). Candle CSV is intentionally venue-native;
the exact canonical headers are:

| Exchange / market | Header |
|-------------------|--------|
| Binance Spot and Futures | `close_time,open,high,low,close,volume,timestamp,quote_av,trades,tb_base_av,tb_quote_av,ignore` |
| Bybit Spot and Futures | `open_time,open,high,low,close,volume` |
| OKX Spot and Futures | `open_time,open,high,low,close,volume,vol_ccy,vol_ccy_quote` |
| MEXC Futures | `open_time,open,high,low,close,volume,amount` |
| MEXC Spot | `open_time,open,high,low,close,volume,quote_asset_volume` |
| Hyperliquid Futures | `open_time,open,high,low,close,volume` |
| Lighter Futures | `open_time,open,high,low,close,volume` |

All time fields are Unix milliseconds. For Binance, `timestamp` is the open
time while `close_time` is the inclusive close time. A six-column example is:

```csv
open_time,open,high,low,close,volume
1704067200000,42000.50,42150.00,41980.25,42100.75,1234.56
...
```

Numeric market-data cells use the project's binary64 storage contract. Values
are written as the shortest decimal text that parses back to exactly the same
`double`; non-finite and out-of-range values are rejected. Decimal strings may
be parsed through a multiprecision type internally, but CSV persistence is not
an arbitrary-precision decimal contract: it intentionally normalizes to the
same float64 representation consumed by the backtest pipeline.

### Funding Rate Data (CSV)

Files are saved to `<output_dir>/futures/funding_rates/csv/<SYMBOL>_fr.csv`:

```csv
funding_time,funding_rate
1704067200000,0.0001
...
```

### T6 Format (Zorro)

The `-z` (or `--t6_conversion`) option runs a standalone conversion of existing CSV files to binary T6 files compatible with the [Zorro](https://zorro-project.com/) trading platform. No data is downloaded — only CSV files already present in `futures/prices/csv/` or `spot/prices/csv/` are converted. Output is written to `futures/prices/t6/` or `spot/prices/t6/` respectively.

## Project Structure

```
crypto_data_downloader/
├── include/stonky/           # Header files
│   ├── binance/              # Binance-specific downloader
│   ├── bybit/                # Bybit-specific downloader
│   ├── okx/                  # OKX-specific downloader
│   ├── mexc/                 # MEXC-specific downloader
│   ├── hyperliquid/          # Hyperliquid-specific downloader
│   ├── lighter/              # Lighter-specific downloader
│   └── downloader.h          # Common utilities
├── src/                      # Implementation files
├── binance-cpp-api/          # Binance API wrapper (submodule)
├── bybit-cpp-api/            # Bybit API wrapper (submodule)
├── okx-cpp-api/              # OKX API wrapper (submodule)
├── mexc-cpp-api/             # MEXC API wrapper (submodule)
├── hyperliquid-cpp-api/      # Hyperliquid API wrapper (submodule)
├── lighter-cpp-api/          # Lighter API wrapper (submodule)
├── stonky-cpp-common/        # Common utilities (submodule)
├── test/                     # Deterministic CTest regression suite
├── vcpkg.json                # Pinned cross-platform dependency manifest
├── CMakeLists.txt            # Build configuration
└── main.cpp                  # Entry point
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

**Vítězslav Kot** - [vitakot](https://github.com/vitakot)

## Acknowledgments

- Exchange API wrappers are maintained as separate submodules
- Thanks to all contributors and users of this project

## Disclaimer

This software is for educational and research purposes only. Use at your own risk. The author is not responsible for any financial losses incurred through the use of this software or the data it downloads.
