/**
OKX Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/okx/okx_downloader.h"
#include "stonky/csv_data.h"
#include "stonky/okx/okx.h"
#include "stonky/okx/okx_rest_client.h"
#include "stonky/okx/okx_market_data_utils.h"
#include "stonky/downloader.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/semaphore.h"
#include "csv.h"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <thread>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/ranges.h>

using namespace stonky::okx;

namespace stonky {

namespace {
constexpr std::int64_t MS_PER_DAY = 24LL * 60 * 60 * 1000;

/// OKX cuts every bulk archive file on a UTC+8 midnight, so a "2024-09" file
/// starts at 2024-08-31 16:00 UTC. Month/day boundaries used to decide which
/// files to ask for must be computed in that zone, not in UTC.
constexpr std::int64_t HK_OFFSET_MS = 8LL * 60 * 60 * 1000;

/// `/api/v5/public/market-data-history` rejects a range longer than 10 months
/// for `monthly` (error 50077) and 10 days for `daily` (error 50076). The
/// limits used to be 20/20; when OKX tightened them the previous 19-month /
/// 19-day windows started failing on EVERY call, which killed the whole bulk
/// path and left multi-month holes in the dataset. 9 keeps a safety margin.
constexpr std::int64_t MAX_MONTHLY_RANGE_MS = 270LL * MS_PER_DAY;
constexpr std::int64_t MAX_DAILY_RANGE_MS = 9LL * MS_PER_DAY;

/// Oldest bulk archive file of any instrument: the "2021-09" batch, starting
/// 2021-08-31 16:00 UTC. Anything older exists only through the paginated REST
/// candle endpoint and has no funding-rate counterpart at all, so the archive
/// floor is treated as the start of history.
constexpr std::int64_t ARCHIVE_FLOOR_MS = 1630425600000;

/// A monthly file covers at most 31 days; a daily file exactly one. Used to
/// decide whether a file can still hold records newer than what is stored.
constexpr std::int64_t MONTHLY_FILE_SPAN_MS = 32LL * MS_PER_DAY;

constexpr int MAX_REQUEST_ATTEMPTS = 4;

/// How many times a listing window is re-requested while it keeps handing back
/// a download URL that belongs to a different instrument than the entry it is
/// filed under. Each retry is an independent draw, so a handful suffices.
constexpr int MAX_LISTING_ATTEMPTS = 8;

/// File name a download URL actually points at, without the query string.
std::string urlFileName(const std::string &url) {
    const auto query = url.find('?');
    const auto path = query == std::string::npos ? url : url.substr(0, query);
    const auto slash = path.rfind('/');
    return slash == std::string::npos ? path : path.substr(slash + 1);
}

/// Start of the UTC+8 calendar month containing `tsMs`, as a UTC timestamp.
std::int64_t hkMonthStartMs(const std::int64_t tsMs) {
    using namespace std::chrono;
    const auto local = sys_time<milliseconds>(milliseconds{tsMs + HK_OFFSET_MS});
    const year_month_day ymd{floor<days>(local)};
    const sys_days firstOfMonth{ymd.year() / ymd.month() / 1};
    return firstOfMonth.time_since_epoch().count() * MS_PER_DAY - HK_OFFSET_MS;
}

/// Start of the UTC+8 calendar day containing `tsMs`, as a UTC timestamp.
std::int64_t hkDayStartMs(const std::int64_t tsMs) {
    using namespace std::chrono;
    const auto local = sys_time<milliseconds>(milliseconds{tsMs + HK_OFFSET_MS});
    return floor<days>(local).time_since_epoch().count() * MS_PER_DAY - HK_OFFSET_MS;
}

/**
 * Floor a listing window's start onto the archive's own file boundaries.
 *
 * market-data-history returns a file only when `begin` falls on or before the
 * day its period STARTS — a window of [2026-07-09, 2026-07-31 16:00] answers
 * with nothing at all, even though the 2026-07 file covers most of it. Asking
 * from an instrument's listing timestamp (or from any mid-period resume point)
 * therefore drops the whole period silently: a symbol listed on the 9th lost
 * every bar of its listing month.
 */
std::int64_t archiveWindowStart(const std::int64_t tsMs, const DateAggrType dateAggrType) {
    return dateAggrType == DateAggrType::monthly ? hkMonthStartMs(tsMs) : hkDayStartMs(tsMs);
}

/// Run a request with bounded retries. Transient OKX failures (rate limiting,
/// gateway hiccups) must not translate into skipped archive files — a skipped
/// file becomes a permanent hole, because the resume logic only ever appends
/// after the last stored record.
template<typename Fn>
auto withRetry(const std::string &what, Fn &&fn) -> decltype(fn()) {
    std::string lastError;
    for (int attempt = 1; attempt <= MAX_REQUEST_ATTEMPTS; ++attempt) {
        try {
            return fn();
        } catch (const std::exception &e) {
            lastError = e.what();
            // A delisted/unknown instrument is a terminal answer, not a hiccup.
            if (lastError.find("code: 51001") != std::string::npos ||
                lastError.find("code: 51000") != std::string::npos) {
                throw;
            }
            if (attempt < MAX_REQUEST_ATTEMPTS) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500 * attempt));
            }
        }
    }
    throw std::runtime_error(fmt::format("{} failed after {} attempts: {}", what, MAX_REQUEST_ATTEMPTS, lastError));
}

/// Enumerate every archive file covering [begin, end), walking the range in
/// windows the endpoint accepts. Results are de-duplicated by file name
/// (adjacent windows overlap on partial months) and sorted oldest first.
std::vector<MarketDataFileInfo> listArchiveFiles(const RESTClient &client,
                                                 const MarketDataModule module,
                                                 const InstrumentType instrumentType,
                                                 const std::string &instFamilyOrId,
                                                 const DateAggrType dateAggrType,
                                                 const std::int64_t begin,
                                                 const std::int64_t end) {
    const std::int64_t windowMs = dateAggrType == DateAggrType::monthly ? MAX_MONTHLY_RANGE_MS : MAX_DAILY_RANGE_MS;
    std::map<std::string, MarketDataFileInfo> unique;

    // Flooring happens here rather than at the call sites so no caller can
    // forget it and lose a period without any error surfacing.
    for (std::int64_t windowStart = archiveWindowStart(begin, dateAggrType); windowStart < end;) {
        const std::int64_t windowEnd = std::min(windowStart + windowMs, end);

        // The listing intermittently links a file belonging to a DIFFERENT
        // instrument than the entry names: for BTC-USDT 2024-04..2024-08 about
        // half of the responses give a SWAP entry the URL of the SPOT archive.
        // Downloading it would splice spot prices into a swap series — the
        // instrument-name filter in parseCandlesCsv() rejects those rows, but
        // that turns the mismatch into a silent hole. The discrepancy is
        // visible in the listing itself, so re-request until the URL agrees
        // with the name it is filed under.
        std::set<std::string> mismatched;

        for (int attempt = 1; attempt <= MAX_LISTING_ATTEMPTS; ++attempt) {
            const auto history = withRetry(
                fmt::format("market-data-history {} [{}, {}]", instFamilyOrId, windowStart, windowEnd),
                [&] {
                    return client.getMarketDataHistory(module, instrumentType, instFamilyOrId, dateAggrType,
                                                       windowStart, windowEnd);
                });

            bool sawMismatch = false;
            for (const auto &detail: history.details) {
                for (const auto &fileInfo: detail.groupDetails) {
                    if (urlFileName(fileInfo.url) != fileInfo.filename) {
                        if (!unique.contains(fileInfo.filename)) {
                            mismatched.insert(fileInfo.filename);
                            sawMismatch = true;
                        }
                        continue;
                    }
                    unique.try_emplace(fileInfo.filename, fileInfo);
                    mismatched.erase(fileInfo.filename);
                }
            }

            if (!sawMismatch || mismatched.empty()) {
                break;
            }
        }

        if (!mismatched.empty()) {
            spdlog::error(fmt::format(
                "{}: archive kept linking foreign files for {} after {} listing attempts — those periods "
                "are MISSING from this run: {}",
                instFamilyOrId, mismatched.size(), MAX_LISTING_ATTEMPTS, fmt::join(mismatched, ", ")));
        }

        windowStart = windowEnd;
    }

    std::vector<MarketDataFileInfo> files;
    files.reserve(unique.size());
    for (auto &[_, fileInfo]: unique) {
        files.push_back(fileInfo);
    }
    std::ranges::sort(files, [](const MarketDataFileInfo &a, const MarketDataFileInfo &b) {
        return a.dateTs < b.dateTs;
    });
    return files;
}
} // namespace

struct OKXDownloader::P {
    std::unique_ptr<RESTClient> okxClient;
    mutable Semaphore maxConcurrentConvertJobs;
    mutable std::recursive_mutex locker;
    Semaphore maxConcurrentDownloadJobs{3};
    bool deleteDelistedData = false;
    bool xperp = false;
    MarketCategory marketCategory = MarketCategory::Futures;

    /// Instruments of the product being downloaded. X-Perps share the FUTURES
    /// endpoint with 39 ordinary dated futures, so `instType` alone is not a
    /// sufficient filter — `ruleType` is what separates them.
    [[nodiscard]] std::vector<Instrument> productInstruments(const InstrumentType instrumentType) const {
        auto instruments = okxClient->getInstruments(xperp ? InstrumentType::FUTURES : instrumentType);

        if (xperp) {
            std::erase_if(instruments, [](const Instrument &i) { return i.ruleType != "xperp"; });
        }
        return instruments;
    }

    static bool writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path);

    static int64_t checkSymbolCSVFile(const std::string &path);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path, bool rewrite);

    static bool readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles);

    void convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths, const std::string &outDirPath) const;

    static int64_t checkFundingRatesCSVFile(const std::string &path);

    static bool writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path);

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData) : okxClient(
                                                                                 std::make_unique<RESTClient>(
                                                                                     "", "", "")),
                                                                             maxConcurrentConvertJobs(maxJobs),
                                                                             deleteDelistedData(deleteDelistedData) {
    }
};

OKXDownloader::OKXDownloader(std::uint32_t maxJobs, const MarketCategory marketCategory, bool deleteDelistedData,
                             const bool xperp) : m_p(std::make_unique<P>(maxJobs, deleteDelistedData)) {
    m_p->marketCategory = marketCategory;
    m_p->xperp = xperp;
}

OKXDownloader::~OKXDownloader() = default;

bool OKXDownloader::P::readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles) {
    try {
        io::CSVReader<6> in(path);
        in.read_header(io::ignore_extra_column, "open_time", "open", "high", "low", "close", "volume");

        Candle candle;
        double o, h, l, c, vol = 0.0;
        while (in.read_row(candle.ts, o, h, l, c, vol)) {
            candle.o = o;
            candle.h = h;
            candle.l = l;
            candle.c = c;
            candle.vol = vol;
            candles.push_back(candle);
        }
    } catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }

    return true;
}

bool OKXDownloader::P::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path) {
    const std::filesystem::path pathToT6File{t6Path};

    std::ofstream ofs;
    ofs.open(pathToT6File.string(), std::ios::trunc | std::ios::binary);

    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open file: {}", t6Path));
        return false;
    }

    std::vector<Candle> candles;
    if (!readCandlesFromCSVFile(csvPath, candles)) {
        spdlog::error(fmt::format("Couldn't read candles from csv file: {}", csvPath));
        return false;
    }

    const auto numMsForInterval = OKX::numberOfMsForBarSize(BarSize::_1m) / 1000;

    for (auto &candle: std::ranges::reverse_view(candles)) {
        T6 t6;
        t6.fOpen = candle.o.convert_to<float>();
        t6.fHigh = candle.h.convert_to<float>();
        t6.fLow = candle.l.convert_to<float>();
        t6.fClose = candle.c.convert_to<float>();
        t6.fVal = 0.0;
        t6.fVol = candle.vol.convert_to<float>();
        t6.time = convertTimeMs(candle.ts + numMsForInterval);
        ofs.write(reinterpret_cast<char *>(&t6), sizeof(T6));
    }

    ofs.close();
    return true;
}

void OKXDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
                                          const std::string &outDirPath) const {
    std::vector<std::future<std::pair<std::string, bool> > > futures;
    std::vector<std::pair<std::string, bool> > readyFutures;

    for (const auto &path: filePaths) {
        if (path.empty()) {
            continue;
        }
        std::filesystem::path t6FilePath = outDirPath;
        const auto fileName = path.filename().replace_extension("t6");
        t6FilePath.append(fileName.string());

        spdlog::info(fmt::format("Converting symbol: {}...", path.filename().replace_extension("").string()));

        futures.push_back(
            std::async(std::launch::async,
                       [](const std::filesystem::path &csvPath, const std::filesystem::path &t6Path,
                          Semaphore &maxJobs) -> std::pair<std::string, bool> {
                           std::scoped_lock w(maxJobs);
                           std::pair<std::string, bool> retVal;
                           retVal.first = csvPath.filename().replace_extension("").string();
                           retVal.second = writeCSVCandlesToZorroT6File(csvPath.string(), t6Path.string());
                           return retVal;
                       }, path, t6FilePath, std::ref(maxConcurrentConvertJobs)));
    }

    do {
        for (auto &future: futures) {
            if (isReady(future)) {
                readyFutures.push_back(future.get());
                if (readyFutures.back().second) {
                    spdlog::info(fmt::format("Symbol: {} converted", readyFutures.back().first));
                } else {
                    spdlog::error(fmt::format("Symbol: {} conversion failed", readyFutures.back().first));
                }
            }
        }
    } while (readyFutures.size() < futures.size());
}

bool OKXDownloader::P::writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path,
                                             const bool rewrite) {
    const std::filesystem::path pathToCSVFile{path};

    std::ofstream ofs;

    if (!rewrite) {
        ofs.open(pathToCSVFile.string(), std::ios::app);
    } else {
        ofs.open(pathToCSVFile.string(), std::ios::trunc);
    }

    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open file: {}", path));
        return false;
    }

    uint64_t fileSize;

    try {
        fileSize = std::filesystem::file_size(pathToCSVFile.string());
    } catch (const std::filesystem::filesystem_error &) {
        fileSize = 0;
    }

    if (fileSize == 0) {
        ofs << "open_time,open,high,low,close,volume,vol_ccy,vol_ccy_quote" << std::endl;
    }

    for (const auto &candle: candles) {
        ofs << candle.ts << ",";
        ofs << candle.o << ",";
        ofs << candle.h << ",";
        ofs << candle.l << ",";
        ofs << candle.c << ",";
        ofs << candle.vol << ",";
        ofs << candle.volCcy << ",";
        ofs << candle.volCcyQuote << std::endl;
    }

    ofs.close();
    return true;
}

int64_t OKXDownloader::P::checkSymbolCSVFile(const std::string &path) {
    constexpr int64_t oldestBybitDate = 1420070400000; /// Thursday 1. January 2015 0:00:00
    // Self-healing read: a torn tail (interrupted write) is truncated instead of
    // resetting the resume point to the oldest-date sentinel, which used to
    // silently re-download and append the entire history.
    return CsvData::lastValidRecord(path, 8, oldestBybitDate).timestamp;
}

int64_t OKXDownloader::P::checkFundingRatesCSVFile(const std::string &path) {
    constexpr int64_t oldestDate = 1420070400000; /// Thursday 1. January 2015 0:00:00
    return CsvData::lastValidRecord(path, 2, oldestDate).timestamp;
}

bool OKXDownloader::P::writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path) {
    const std::filesystem::path pathToCSVFile{path};

    std::ofstream ofs;
    ofs.open(pathToCSVFile.string(), std::ios::app);

    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open file: {}", path));
        return false;
    }

    uint64_t fileSize;

    try {
        fileSize = std::filesystem::file_size(pathToCSVFile.string());
    } catch (const std::filesystem::filesystem_error &) {
        fileSize = 0;
    }


    if (fileSize == 0) {
        ofs << "funding_time,funding_rate"
                << std::endl;
    }

    for (const auto &record: fr) {
        ofs << record.fundingTime << ",";
        ofs << record.fundingRate << std::endl;
    }

    ofs.close();

    return true;
}

void OKXDownloader::updateMarketData(const std::string &dirPath,
                                     const std::vector<std::string> &symbols,
                                     CandleInterval candleInterval,
                                     const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                     const onSymbolCompleted &onSymbolCompletedCB,
                                     const bool convertToT6) const {

    auto okxBarSize = BarSize::_1m;

    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;

    if (const auto isOk = OKX::isValidBarSize(barSizeInMinutes, okxBarSize); !isOk) {
        throw std::invalid_argument("invalid OKX bar size: " + std::to_string(barSizeInMinutes) + " m");
    }

    // Market data history endpoint only supports 1-minute candles
    if (okxBarSize != BarSize::_1m) {
        throw std::invalid_argument(
            "OKX bulk market data history only publishes 1-minute candles; download with -b 1 and build "
            "higher timeframes locally with -g (e.g. -b 1 -g 5,60)");
    }

    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    std::string csvDirName;
    std::string t6DirName;
    InstrumentType instrumentType;

    switch (m_p->marketCategory) {
        case MarketCategory::Spot:
            instrumentType = InstrumentType::SPOT;
            csvDirName = CSV_SPOT_DIR;
            t6DirName = T6_SPOT_DIR;
            break;
        case MarketCategory::Futures:
            instrumentType = InstrumentType::SWAP;
            csvDirName = CSV_FUT_DIR;
            t6DirName = T6_FUT_DIR;
            break;
    }

    if (m_p->xperp) {
        instrumentType = InstrumentType::FUTURES;
        csvDirName = CSV_XPERP_DIR;
        t6DirName = T6_XPERP_DIR;
    }

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInstruments = m_p->productInstruments(instrumentType);

    // Create a map from instId to Instrument for quick lookup
    std::map<std::string, Instrument> instrumentMap;
    std::set<std::string> exchangeSymbolSet;
    for (const auto &inst: exchangeInstruments) {
        instrumentMap[inst.instId] = inst;
        exchangeSymbolSet.insert(inst.instId);
    }

    if (symbolsToUpdate.empty()) {
        for (const auto &el: exchangeInstruments) {
            // X-Perps settle in USD, so the USDT filter that selects the swap
            // universe would reject every one of them.
            if (m_p->xperp || el.settleCcy == "USDT" || el.quoteCcy == "USDT") {
                if (el.state != InstrumentStatus::live && m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(el.instId);
                } else {
                    symbolsToUpdate.push_back(el.instId);
                }
            }
        }

        // Scan existing CSV files for symbols no longer on the exchange. Unless
        // they are being deleted they stay in the update set: `/public/instruments`
        // only ever lists live contracts, so a delisted symbol dropped here would
        // stop being maintained and would vanish from a rebuilt dataset, silently
        // introducing survivorship bias. The bulk archive still serves them.
        std::filesystem::path csvDir = finalPath;
        csvDir.append(csvDirName);
        csvDir.append(Downloader::minutesToString(barSizeInMinutes));

        std::size_t delistedKept = 0;

        if (std::filesystem::exists(csvDir)) {
            for (const auto &entry: std::filesystem::directory_iterator(csvDir)) {
                if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                    const auto stem = entry.path().stem().string();
                    if (!exchangeSymbolSet.contains(stem)) {
                        if (m_p->deleteDelistedData) {
                            symbolsToDelete.push_back(stem);
                        } else {
                            symbolsToUpdate.push_back(stem);
                            delistedKept++;
                        }
                    }
                }
            }
        }

        if (delistedKept > 0) {
            spdlog::info(fmt::format("Keeping {} delisted symbols found on disk in the update set", delistedKept));
        }
    } else {
        std::vector<std::string> tempSymbols;

        for (const auto &symbol: symbolsToUpdate) {
            auto it = std::ranges::find_if(exchangeInstruments, [symbol](const Instrument &i) {
                return i.instId == symbol;
            });

            if (it == exchangeInstruments.end()) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                } else {
                    tempSymbols.push_back(symbol);
                }
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", symbol));
            } else if (it->state != InstrumentStatus::live) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                } else {
                    tempSymbols.push_back(it->instId);
                }
            } else {
                tempSymbols.push_back(it->instId);
            }
        }

        symbolsToUpdate = tempSymbols;
    }

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this, &barSizeInMinutes, &csvDirName, &t6DirName, convertToT6, &instrumentMap,
                        instrumentType](
                   const std::string &symbol,
                   Semaphore &maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;
                           std::filesystem::path symbolFilePathT6 = finalPath;

                           symbolFilePathCsv.append(csvDirName);
                           symbolFilePathT6.append(t6DirName);

                           symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                           symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes)); {
                               if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                                   throw std::runtime_error(fmt::format("Failed to create {}, err: {}",
                                                                        symbolFilePathCsv.string(),
                                                                        err.message().c_str()));
                               }
                           }

                           if (convertToT6) {
                               if (const auto err = createDirectoryRecursively(symbolFilePathT6.string())) {
                                   throw std::runtime_error(fmt::format("Failed to create {}, err: {}",
                                                                        symbolFilePathCsv.string(),
                                                                        err.message().c_str()));
                               }
                           }

                           symbolFilePathCsv.append(symbol + ".csv");
                           symbolFilePathT6.append(symbol + ".t6");

                           const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                           int64_t fromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());

                           // Get instFamily and listTime for market data history API
                           std::string instFamilyOrId;
                           int64_t instrumentListTime = 0;

                           auto it = instrumentMap.find(symbol);
                           if (it != instrumentMap.end()) {
                               instrumentListTime = it->second.listTime;
                               if (instrumentType == InstrumentType::SPOT) {
                                   instFamilyOrId = symbol;
                               } else {
                                   instFamilyOrId = it->second.instFamily;
                               }
                           } else {
                               // Instrument not in map (likely delisted), derive instFamily from instId
                               if (instrumentType == InstrumentType::SPOT) {
                                   instFamilyOrId = symbol;
                               } else {
                                   // E.g. "BTC-USDT-SWAP" -> "BTC-USDT"
                                   const auto lastDash = symbol.rfind('-');
                                   if (lastDash != std::string::npos) {
                                       instFamilyOrId = symbol.substr(0, lastDash);
                                   } else {
                                       instFamilyOrId = symbol;
                                   }
                               }
                               spdlog::info(fmt::format("Instrument {} not in map (possibly delisted), using derived instFamily: {}",
                                                        symbol, instFamilyOrId));
                           }

                           // Use instrument listing time if file doesn't exist or has older data
                           if (instrumentListTime > 0 && fromTimeStamp < instrumentListTime) {
                               fromTimeStamp = instrumentListTime;
                           }

                           // Nothing older than the bulk archive floor is fetched. Going deeper is
                           // possible through the paginated REST candle endpoint (it reaches back to
                           // the listing date), but those bars would have no funding-rate counterpart,
                           // because the funding archive starts at the same floor and the REST funding
                           // endpoint only serves the last ~3 months.
                           fromTimeStamp = std::max(fromTimeStamp, ARCHIVE_FLOOR_MS);

                            try {
                                int64_t totalNewCandles = 0;
                                int64_t lastSavedTimestamp = fromTimeStamp;
                                bool stoppedEarly = false;

                                // Monthly files exist only for complete UTC+8 months; the running
                                // month is served by daily files and the last hours by REST.
                                const int64_t monthlyCutoff = hkMonthStartMs(nowTimestamp);

                                // Oldest first, monthly files then the daily files of the running
                                // month: strictly ascending, which is what append-only resume needs.
                                std::vector<std::pair<MarketDataFileInfo, int64_t> > archiveFiles;

                                for (const auto &fileInfo: listArchiveFiles(
                                         *m_p->okxClient, MarketDataModule::Candles1m, instrumentType,
                                         instFamilyOrId, DateAggrType::monthly,
                                         fromTimeStamp, monthlyCutoff)) {
                                    archiveFiles.emplace_back(fileInfo, MONTHLY_FILE_SPAN_MS);
                                }
                                for (const auto &fileInfo: listArchiveFiles(
                                         *m_p->okxClient, MarketDataModule::Candles1m, instrumentType,
                                         instFamilyOrId, DateAggrType::daily,
                                         std::max(fromTimeStamp, monthlyCutoff), nowTimestamp)) {
                                    archiveFiles.emplace_back(fileInfo, MS_PER_DAY);
                                }

                                for (const auto &[fileInfo, fileSpanMs]: archiveFiles) {
                                    if (fileInfo.dateTs + fileSpanMs <= lastSavedTimestamp) {
                                        continue; // fully covered by what is already stored
                                    }

                                    std::vector<Candle> candles;
                                    try {
                                        candles = withRetry(fmt::format("download {}", fileInfo.filename), [&] {
                                            const auto zipData = RESTClient::downloadMarketDataFile(fileInfo.url);
                                            const auto csvData = okx::utils::extractZip(zipData);
                                            // Archive files are keyed by instrument family, so keep
                                            // only this contract's rows — see parseCandlesCsv()
                                            return okx::utils::parseCandlesCsv(csvData, symbol);
                                        });
                                    } catch (const std::exception &e) {
                                        // Never skip forward past a failed file: the CSV is append-only
                                        // and resumes from its last record, so a skipped file would turn
                                        // into a permanent hole. Stop here and let the next run retry.
                                        spdlog::warn(fmt::format(
                                            "Symbol: {}: archive file {} could not be downloaded ({}), "
                                            "stopping at {} — the next run resumes from there",
                                            symbol, fileInfo.filename, e.what(), lastSavedTimestamp));
                                        stoppedEarly = true;
                                        break;
                                    }

                                    if (candles.empty()) {
                                        continue;
                                    }

                                    std::ranges::sort(candles, [](const Candle &a, const Candle &b) {
                                        return a.ts < b.ts;
                                    });

                                    std::vector<Candle> newCandles;
                                    for (const auto &candle: candles) {
                                        if (candle.ts > lastSavedTimestamp) {
                                            newCandles.push_back(candle);
                                        }
                                    }

                                    if (!newCandles.empty()) {
                                        P::writeCandlesToCSVFile(newCandles, symbolFilePathCsv.string(), false);
                                        totalNewCandles += static_cast<int64_t>(newCandles.size());
                                        lastSavedTimestamp = newCandles.back().ts;
                                    }
                                }

                               // Bulk files only cover complete days, so the last hours come from the
                               // paginated REST endpoint.
                               if (!stoppedEarly && lastSavedTimestamp < nowTimestamp) {
                                   auto recentCandles = m_p->okxClient->getHistoricalPrices(
                                       symbol, BarSize::_1m, lastSavedTimestamp, nowTimestamp);

                                   if (!recentCandles.empty()) {
                                       // Sort by timestamp (API returns newest first)
                                       std::ranges::sort(recentCandles, [](const Candle &a, const Candle &b) {
                                           return a.ts < b.ts;
                                       });

                                       // Filter out candles we already have
                                       std::vector<Candle> newCandles;
                                       for (const auto &candle : recentCandles) {
                                           if (candle.ts > lastSavedTimestamp) {
                                               newCandles.push_back(candle);
                                           }
                                       }

                                       if (!newCandles.empty()) {
                                           P::writeCandlesToCSVFile(newCandles, symbolFilePathCsv.string(), false);
                                           totalNewCandles += static_cast<int64_t>(newCandles.size());
                                       }
                                   }
                               }

                               if (totalNewCandles > 0) {
                                   spdlog::info(fmt::format("CSV file for symbol: {} updated ({} new candles)",
                                                            symbol, totalNewCandles));
                               }

                               // Return the path if the CSV file exists (for T6 conversion)
                               if (std::filesystem::exists(symbolFilePathCsv)) {
                                   return symbolFilePathCsv;
                               }
                               if (totalNewCandles == 0 && !std::filesystem::exists(symbolFilePathCsv)) {
                                   spdlog::warn(fmt::format("No data available for symbol: {}", symbol));
                               }
                           } catch (const std::exception &e) {
                               const std::string errStr = e.what();
                               if (errStr.find("code: 51001") != std::string::npos || errStr.find("code: 51000") != std::string::npos) {
                                   spdlog::info(fmt::format("Symbol: {} is delisted or does not exist on OKX", symbol));
                               } else {
                                   spdlog::warn(fmt::format("Updating candles for symbol: {} failed, reason: {}",
                                                            symbol, e.what()));
                               }
                           }
                           return "";
                       }, s, std::ref(m_p->maxConcurrentDownloadJobs)));
    }

    do {
        for (auto &future: futures) {
            if (isReady(future)) {
                csvFilePaths.push_back(future.get());
            }
        }
    } while (csvFilePaths.size() < futures.size());

    std::filesystem::path T6Directory = finalPath;

    T6Directory.append(t6DirName);
    T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    if (convertToT6) {
        std::filesystem::path csvDirectory = finalPath;
        csvDirectory.append(csvDirName);
        csvDirectory.append(Downloader::minutesToString(barSizeInMinutes));

        std::vector<std::filesystem::path> allCsvFiles;
        if (std::filesystem::exists(csvDirectory)) {
            for (const auto &entry: std::filesystem::directory_iterator(csvDirectory)) {
                if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                    allCsvFiles.push_back(entry.path());
                }
            }
        }

        if (!allCsvFiles.empty()) {
            if (const auto err = createDirectoryRecursively(T6Directory.string())) {
                throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(),
                                                     err.message().c_str()));
            }
            spdlog::info(fmt::format("Converting from csv to t6..."));
            m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string());
        }
    }

    if (m_p->deleteDelistedData) {
        for (const auto &symbol: symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = finalPath;
            std::filesystem::path symbolFilePathT6 = finalPath;

            symbolFilePathCsv.append(csvDirName);
            symbolFilePathT6.append(t6DirName);

            symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
            symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes));

            symbolFilePathCsv = symbolFilePathCsv.lexically_normal();
            symbolFilePathT6 = symbolFilePathT6.lexically_normal();

            symbolFilePathCsv.append(symbol + ".csv");
            symbolFilePathT6.append(symbol + ".t6");

            if (std::filesystem::exists(symbolFilePathCsv)) {
                std::filesystem::remove(symbolFilePathCsv);
                spdlog::info(fmt::format("Removing csv file for delisted symbol: {}, file: {}...", symbol,
                             symbolFilePathCsv.string()));
            }

            if (std::filesystem::exists(symbolFilePathT6)) {
                std::filesystem::remove(symbolFilePathT6);
                spdlog::info(fmt::format("Removing t6 file for delisted symbol: {}, file: {}...", symbol,
                             symbolFilePathT6.string()));
            }
        }
    }
}

void OKXDownloader::updateMarketData(const std::string &connectionString,
                                     const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                     const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: OKXDownloader::updateMarketData");
}

void OKXDownloader::updateFundingRateData(const std::string &dirPath,
                                          const std::vector<std::string> &symbols,
                                          const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                          const onSymbolCompleted &onSymbolCompletedCB) const {
    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    const std::string frDirName = m_p->xperp ? CSV_XPERP_FR_DIR : CSV_FUT_FR_DIR;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (m_p->xperp) {
        // The bulk archive's funding module rejects instType=FUTURES
        // ("Parameter instType doesn't match parameter module"), so X-Perp
        // funding exists only through the REST endpoint — and that serves the
        // last ~3 months. Anything older is already unrecoverable, which makes
        // this the one dataset here that decays if it is not collected
        // regularly.
        spdlog::warn("X-Perp funding rates come from the REST endpoint only (no bulk archive); "
                     "it serves roughly the last 3 months, so run this regularly or history is lost");
    }

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInstruments = m_p->productInstruments(InstrumentType::SWAP);

    // Build set of all known symbols from exchange for filesystem-based delisting detection
    std::set<std::string> exchangeSymbolSet;
    for (const auto &el: exchangeInstruments) {
        exchangeSymbolSet.insert(el.instId);
    }

    if (symbolsToUpdate.empty()) {
        for (const auto &el: exchangeInstruments) {
            // X-Perps settle in USD — see updateMarketData() for the same filter
            if (m_p->xperp || el.settleCcy == "USDT") {
                if (el.state != InstrumentStatus::live && m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(el.instId);
                } else {
                    symbolsToUpdate.push_back(el.instId);
                }
            }
        }

        // Scan existing CSV files for symbols no longer on the exchange. Unless
        // they are being deleted they stay in the update set — see the same
        // reasoning in updateMarketData(): dropping them would introduce
        // survivorship bias into a rebuilt dataset.
        std::filesystem::path frDir = finalPath;
        frDir.append(frDirName);

        std::size_t delistedKept = 0;

        if (std::filesystem::exists(frDir)) {
            for (const auto &entry: std::filesystem::directory_iterator(frDir)) {
                if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                    auto stem = entry.path().stem().string();
                    if (stem.ends_with("_fr")) {
                        stem = stem.substr(0, stem.size() - 3);
                    }
                    if (!exchangeSymbolSet.contains(stem)) {
                        if (m_p->deleteDelistedData) {
                            symbolsToDelete.push_back(stem);
                        } else {
                            symbolsToUpdate.push_back(stem);
                            delistedKept++;
                        }
                    }
                }
            }
        }

        if (delistedKept > 0) {
            spdlog::info(fmt::format("Keeping {} delisted symbols found on disk in the update set", delistedKept));
        }
    } else {
        std::vector<std::string> tempSymbols;

        for (const auto &symbol: symbolsToUpdate) {
            auto it = std::ranges::find_if(exchangeInstruments, [symbol](const Instrument &i) {
                return i.instId == symbol;
            });

            if (it == exchangeInstruments.end()) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                } else {
                    tempSymbols.push_back(symbol);
                }
                spdlog::info(fmt::format(
                    "Symbol: {} not found on Exchange, probably delisted", symbol));
            } else if (it->state != InstrumentStatus::live) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                } else {
                    tempSymbols.push_back(it->instId);
                }
            } else {
                tempSymbols.push_back(it->instId);
            }
        }

        symbolsToUpdate = tempSymbols;
    }

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this, &frDirName](const std::string &symbol,
                                                     Semaphore &maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;


                           symbolFilePathCsv.append(frDirName);
                           if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string());
                               err.value() != 0) {
                               throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}",
                                                                    symbolFilePathCsv.string(), err.value()));
                           }

                           symbolFilePathCsv.append(symbol + "_fr.csv");

                           const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           spdlog::info(fmt::format("Updating FR for symbol: {}...", symbol));

                           int64_t fromTimeStamp = P::checkFundingRatesCSVFile(symbolFilePathCsv.string());

                           // Extract instFamily from symbol ("BTC-USDT-SWAP" -> "BTC-USDT")
                           std::string instFamily;
                           const auto lastDash = symbol.rfind('-');
                           if (lastDash != std::string::npos) {
                               instFamily = symbol.substr(0, lastDash);
                           } else {
                               instFamily = symbol;
                           }

                           // The funding archive starts at the same floor as the candle archive and
                           // the REST funding endpoint only serves the last ~3 months, so there is no
                           // source for anything older.
                           fromTimeStamp = std::max(fromTimeStamp, ARCHIVE_FLOOR_MS);

                           try {
                               int64_t totalNewRates = 0;
                               int64_t lastSavedTimestamp = fromTimeStamp;
                               bool stoppedEarly = false;

                               // Funding rates have no daily aggregation — monthly files for complete
                               // UTC+8 months, REST for the running month. For X-Perps there are no
                               // files at all: the archive's funding module accepts only instType=SWAP,
                               // so the whole history has to come from REST.
                               const int64_t monthlyCutoff = m_p->xperp ? fromTimeStamp : hkMonthStartMs(nowTimestamp);

                               // 1. Download historical data via bulk ZIP files (monthly)
                               for (const auto &fileInfo: listArchiveFiles(
                                        *m_p->okxClient, MarketDataModule::FundingRate, InstrumentType::SWAP,
                                        instFamily, DateAggrType::monthly, fromTimeStamp, monthlyCutoff)) {
                                   if (fileInfo.dateTs + MONTHLY_FILE_SPAN_MS <= lastSavedTimestamp) {
                                       continue; // fully covered by what is already stored
                                   }

                                   std::vector<FundingRate> rates;
                                   try {
                                       rates = withRetry(fmt::format("download {}", fileInfo.filename), [&] {
                                           const auto zipData = RESTClient::downloadMarketDataFile(fileInfo.url);
                                           const auto csvData = okx::utils::extractZip(zipData);
                                           return okx::utils::parseFundingRateCsv(csvData, symbol);
                                       });
                                   } catch (const std::exception &e) {
                                       // Append-only file: skipping forward past a failure would leave a
                                       // permanent hole, so stop and let the next run resume from here.
                                       spdlog::warn(fmt::format(
                                           "Symbol: {}: archive file {} could not be downloaded ({}), "
                                           "stopping at {} — the next run resumes from there",
                                           symbol, fileInfo.filename, e.what(), lastSavedTimestamp));
                                       stoppedEarly = true;
                                       break;
                                   }

                                   if (rates.empty()) {
                                       continue;
                                   }

                                   std::ranges::sort(rates, [](const FundingRate &a, const FundingRate &b) {
                                       return a.fundingTime < b.fundingTime;
                                   });

                                   std::vector<FundingRate> newRates;
                                   for (const auto &rate : rates) {
                                       if (rate.fundingTime > lastSavedTimestamp) {
                                           newRates.push_back(rate);
                                       }
                                   }

                                   if (!newRates.empty()) {
                                       P::writeFundingRatesToCSVFile(newRates, symbolFilePathCsv.string());
                                       totalNewRates += static_cast<int64_t>(newRates.size());
                                       lastSavedTimestamp = newRates.back().fundingTime;
                                   }
                               }

                               // 2. Fill the gap between last bulk file and now using REST API
                               if (!stoppedEarly && lastSavedTimestamp < nowTimestamp) {
                                   auto recentRates = m_p->okxClient->getFundingRates(
                                       symbol, lastSavedTimestamp, nowTimestamp, 1000);

                                   if (!recentRates.empty()) {
                                       std::ranges::sort(recentRates, [](const FundingRate &a, const FundingRate &b) {
                                           return a.fundingTime < b.fundingTime;
                                       });

                                       std::vector<FundingRate> newRates;
                                       for (const auto &rate : recentRates) {
                                           if (rate.fundingTime > lastSavedTimestamp) {
                                               newRates.push_back(rate);
                                           }
                                       }

                                       if (!newRates.empty()) {
                                           P::writeFundingRatesToCSVFile(newRates, symbolFilePathCsv.string());
                                           totalNewRates += static_cast<int64_t>(newRates.size());
                                       }
                                   }
                               }

                               if (totalNewRates > 0) {
                                   spdlog::info(fmt::format("CSV file for symbol: {} updated ({} new FR records)",
                                                            symbol, totalNewRates));
                               }
                               return symbolFilePathCsv;

                           } catch (const std::exception &e) {
                               const std::string errStr = e.what();
                               if (errStr.find("code: 51001") != std::string::npos || errStr.find("code: 51000") != std::string::npos) {
                                   spdlog::debug("REST API confirmed symbol {} is delisted (no latest data)", symbol);
                               } else {
                                   spdlog::warn(fmt::format("Updating symbol: {} failed, reason: {}",
                                                            symbol, e.what()));
                               }
                           }
                           return "";
                       }, s, std::ref(m_p->maxConcurrentDownloadJobs)));
    }

    do {
        for (auto &future: futures) {
            if (isReady(future)) {
                csvFilePaths.push_back(future.get());
            }
        }
    } while (csvFilePaths.size() < futures.size());

    if (m_p->deleteDelistedData) {
        for (const auto &symbol: symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = finalPath;
            symbolFilePathCsv.append(frDirName);
            symbolFilePathCsv = symbolFilePathCsv.lexically_normal();
            symbolFilePathCsv.append(symbol + "_fr.csv");

            if (std::filesystem::exists(symbolFilePathCsv)) {
                std::filesystem::remove(symbolFilePathCsv);
                spdlog::info("Removing csv file for delisted symbol: {}, file: {}...", symbol,
                             symbolFilePathCsv.string());
            }
        }
    }
}

void OKXDownloader::convertToT6(const std::string &dirPath, const CandleInterval candleInterval) const {
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    const std::filesystem::path finalPath(dirPath);

    std::string csvDirName;
    std::string t6DirName;

    switch (m_p->marketCategory) {
        case MarketCategory::Spot:
            csvDirName = CSV_SPOT_DIR;
            t6DirName = T6_SPOT_DIR;
            break;
        case MarketCategory::Futures:
            csvDirName = CSV_FUT_DIR;
            t6DirName = T6_FUT_DIR;
            break;
    }

    std::filesystem::path csvDirectory = finalPath;
    csvDirectory.append(csvDirName);
    csvDirectory.append(Downloader::minutesToString(barSizeInMinutes));

    std::filesystem::path T6Directory = finalPath;
    T6Directory.append(t6DirName);
    T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    std::vector<std::filesystem::path> allCsvFiles;
    if (std::filesystem::exists(csvDirectory)) {
        for (const auto &entry: std::filesystem::directory_iterator(csvDirectory)) {
            if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                allCsvFiles.push_back(entry.path());
            }
        }
    }

    if (!allCsvFiles.empty()) {
        if (const auto err = createDirectoryRecursively(T6Directory.string())) {
            throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(),
                                                 err.message().c_str()));
        }
        spdlog::info("Converting from csv to t6...");
        m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string());
    }
}
}
