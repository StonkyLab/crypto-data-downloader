# Crypto Data Downloader

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![CMake](https://img.shields.io/badge/CMake-3.20+-green.svg)](https://cmake.org/)

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

On the **first run** for a symbol the downloader requests the most recent ~5 000 candles of the chosen interval; subsequent runs append only data since the last CSV record.

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

#### MEXC Delisted Symbols

MEXC Futures API does not provide a bulk endpoint for delisted contracts — the `/api/v1/contract/detail` endpoint returns only active symbols. However, historical data for delisted symbols **is still available** when queried individually.

To download data for delisted MEXC futures symbols, maintain your own list of delisted symbols and pass it via the `-s` parameter:

```bash
./crypto_data_downloader -e mexc -c f -s "HOOK_USDT,BNKR_USDT,LUNA2USDT" -o /data/mexc
```

## Requirements

- C++20 compatible compiler (GCC 11+, Clang 14+, MSVC 2022)
- CMake 3.20 or later
- Git (for submodules)

### Dependencies

- [OpenSSL](https://www.openssl.org/) - TLS/SSL support
- [Boost](https://www.boost.org/) - Networking (Beast, Asio)
- [spdlog](https://github.com/gabime/spdlog) - Logging
- [nlohmann/json](https://github.com/nlohmann/json) - JSON parsing
- [cxxopts](https://github.com/jarro2783/cxxopts) - Command-line parsing
- [libpqxx](https://github.com/jtv/libpqxx) - PostgreSQL client (optional)

## Installation

### Windows

1. **Install CMake**: Download from [cmake.org](https://cmake.org/download/)

2. **Install Visual Studio 2022**: Download [Visual Studio Community](https://visualstudio.microsoft.com/downloads/) and install with **Desktop development with C++** workload.

3. **Install vcpkg** (package manager):
   ```powershell
   git clone https://github.com/Microsoft/vcpkg.git C:\vcpkg
   cd C:\vcpkg
   .\bootstrap-vcpkg.bat
   .\vcpkg integrate install
   ```

4. **Install dependencies**:
   ```powershell
   vcpkg install cxxopts:x64-windows
   vcpkg install libpqxx:x64-windows
   vcpkg install spdlog:x64-windows
   vcpkg install openssl:x64-windows
   vcpkg install boost:x64-windows
   vcpkg install nlohmann-json:x64-windows
   ```

5. **Build the project**:
   ```powershell
   git clone https://github.com/vitakot/crypto_data_downloader.git
   cd crypto_data_downloader
   git submodule update --init --recursive
   mkdir build && cd build
   cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake ..
   cmake --build . --config Release -j
   ```

### Linux (Ubuntu/Debian)

1. **Install build tools**:
   ```bash
   sudo apt update
   sudo apt install -y cmake build-essential git
   ```

2. **Install dependencies**:
   ```bash
   sudo apt install -y \
       libssl-dev \
       libboost-all-dev \
       libspdlog-dev \
       nlohmann-json3-dev \
       libcxxopts-dev \
       libpq-dev \
       libpqxx-dev
   ```

3. **Build the project**:
   ```bash
   git clone https://github.com/vitakot/crypto_data_downloader.git
   cd crypto_data_downloader
   git submodule update --init --recursive
   mkdir build && cd build
   cmake -DCMAKE_BUILD_TYPE=Release ..
   cmake --build . -j$(nproc)
   ```

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
| `-b` | `--bar_size` | Bar size in minutes (1, 5, 15, 30, 60, etc.) | `1` |
| `-c` | `--category` | Market category: `f` (futures), `s` (spot) | `f` |
| `-d` | `--delete_delisted` | Delete delisted symbols data files | - |
| `-z` | `--t6_conversion` | Convert existing CSV data to T6 format (Zorro Trader) without downloading | - |
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

## Output Format

### Candle Data (CSV)

Files are saved to `<output_dir>/futures/prices/csv/<timeframe>/<SYMBOL>.csv` (or `spot/prices/csv/` for spot):

```csv
open_time,open,high,low,close,volume
1704067200000,42000.50,42150.00,41980.25,42100.75,1234.56
...
```

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
