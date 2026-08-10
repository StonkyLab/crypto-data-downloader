/**
MEXC Futures Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/mexc/mexc_futures_downloader.h"
#include "stonky/atomic_file.h"
#include "stonky/csv_format.h"
#include "stonky/csv_data.h"
#include "stonky/mexc/mexc_futures_rest_client.h"
#include "stonky/mexc/mexc.h"
#include "stonky/downloader.h"
#include "stonky/utils/semaphore.h"
#include "stonky/utils/utils.h"
#include "stonky/interface/exchange_enums.h"
#include "stonky/future_utils.h"
#include "mexc_staging.h"
#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>
#include <future>
#include <set>
#include <tuple>
#include <spdlog/fmt/ranges.h>
#include <ranges>
#include <algorithm>
#include "csv.h"

using namespace stonky::mexc;
using namespace stonky::mexc::futures;

namespace stonky {
struct MEXCFuturesDownloader::P {
    std::unique_ptr<RESTClient> mexcFuturesClient;
    mutable Semaphore maxConcurrentConvertJobs;
    Semaphore maxConcurrentDownloadJobs;
    bool deleteDelistedData = false;

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData) :
        mexcFuturesClient(std::make_unique<RESTClient>("", "")), maxConcurrentConvertJobs(normalizedJobCount(maxJobs)),
        maxConcurrentDownloadJobs(boundedJobCount(maxJobs, 3)), deleteDelistedData(deleteDelistedData) {}

    static CsvData::TailCheck checkSymbolCSVFile(const std::string &path);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path);

    // Write candles to temp file (no header, used for batch files)
    static bool writeCandlesToTempFile(const std::vector<Candle> &candles, const std::string &path);

    // Atomically replace the CSV with the old data plus a complete, validated transaction.
    static bool mergeTempFilesToCSV(const std::string &tempDir, const std::string &csvPath,
                                    const std::string &symbol);

    // Recover only complete staging.  Interrupted partial staging is discarded.
    static bool recoverAndMergeTempFiles(const std::string &tempDir, const std::string &csvPath,
                                         const std::string &symbol);

    static int64_t checkFundingRatesCSVFile(const std::string &path);

    static bool writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path);

    static bool writeHistoricalFundingRatesToCSVFile(const std::vector<HistoricalFundingRate> &fr, const std::string &path);

    static mexc::CandleInterval vkIntervalToMexcInterval(stonky::CandleInterval interval);

    struct CsvCandle {
        int64_t openTime{};
        double open{};
        double high{};
        double low{};
        double close{};
        double volume{};
    };

    static bool readCandlesFromCSVFile(const std::string &path, std::vector<CsvCandle> &candles);
    static bool writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path,
                                             std::int64_t intervalMs,
                                             mexc_staging::Alignment alignment);
    void convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
                            const std::string &outDirPath, std::int64_t intervalMs,
                            mexc_staging::Alignment alignment) const;
};

namespace {
mexc_staging::Alignment stagingAlignment(const mexc::CandleInterval interval) {
    if (interval == mexc::CandleInterval::_1W) {
        return mexc_staging::Alignment::WeekMonday;
    }
    if (interval == mexc::CandleInterval::_1M) {
        return mexc_staging::Alignment::CalendarMonth;
    }
    return mexc_staging::Alignment::Fixed;
}
}

mexc::CandleInterval MEXCFuturesDownloader::P::vkIntervalToMexcInterval(const CandleInterval interval) {
    switch (interval) {
        case CandleInterval::_1m:
            return mexc::CandleInterval::_1m;
        case CandleInterval::_5m:
            return mexc::CandleInterval::_5m;
        case CandleInterval::_15m:
            return mexc::CandleInterval::_15m;
        case CandleInterval::_30m:
            return mexc::CandleInterval::_30m;
        case CandleInterval::_1h:
            return mexc::CandleInterval::_60m;
        case CandleInterval::_4h:
            return mexc::CandleInterval::_4h;
        case CandleInterval::_8h:
            return mexc::CandleInterval::_8h;
        case CandleInterval::_1d:
            return mexc::CandleInterval::_1d;
        case CandleInterval::_1w:
            return mexc::CandleInterval::_1W;
        case CandleInterval::_1M:
            return mexc::CandleInterval::_1M;
        default:
            throw std::invalid_argument("Unsupported candle interval for MEXC");
    }
}

bool MEXCFuturesDownloader::P::readCandlesFromCSVFile(const std::string &path, std::vector<CsvCandle> &candles) {
    try {
        io::CSVReader<7> in(path);
        in.read_header(io::ignore_extra_column, "open_time", "open", "high", "low", "close", "volume", "amount");

        CsvCandle candle;
        double amount = 0.0;
        while (in.read_row(candle.openTime, candle.open, candle.high, candle.low, candle.close,
                           candle.volume, amount)) {
            candles.push_back(candle);
        }
    } catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }

    return true;
}

bool MEXCFuturesDownloader::P::writeCSVCandlesToZorroT6File(
    const std::string &csvPath, const std::string &t6Path, const std::int64_t intervalMs,
    const mexc_staging::Alignment alignment) {
    const std::filesystem::path pathToT6File{t6Path};

    AtomicFileWriter output(pathToT6File);
    if (!output.isOpen()) {
        spdlog::error(fmt::format("Couldn't prepare file {}: {}", t6Path, output.error()));
        return false;
    }
    auto &ofs = output.stream();

    std::vector<CsvCandle> candles;
    if (!readCandlesFromCSVFile(csvPath, candles) || candles.empty()) {
        spdlog::error(fmt::format("Couldn't read candles from csv file: {}", csvPath));
        return false;
    }

    for (const auto &candle: std::ranges::reverse_view(candles)) {
        T6 t6{};
        t6.fOpen = static_cast<float>(candle.open);
        t6.fHigh = static_cast<float>(candle.high);
        t6.fLow = static_cast<float>(candle.low);
        t6.fClose = static_cast<float>(candle.close);
        t6.fVal = 0.0;
        t6.fVol = static_cast<float>(candle.volume);
        t6.time = convertTimeMs(mexc_staging::nextTimestamp(candle.openTime, intervalMs, alignment));
        ofs.write(reinterpret_cast<char *>(&t6), sizeof(T6));
        if (!ofs.good()) {
            spdlog::error(fmt::format("Couldn't write file: {}", t6Path));
            return false;
        }
    }

    std::string error;
    if (!output.commit(error)) {
        spdlog::error(fmt::format("Couldn't commit T6 file {}: {}", t6Path, error));
        return false;
    }
    return true;
}

void MEXCFuturesDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
                                                   const std::string &outDirPath,
                                                   const std::int64_t intervalMs,
                                                   const mexc_staging::Alignment alignment) const {
    std::vector<std::future<std::pair<std::string, bool>>> futures;
    std::vector<std::pair<std::string, bool>> readyFutures;

    for (const auto &path: filePaths) {
        if (path.empty()) {
            continue;
        }
        std::filesystem::path t6FilePath = outDirPath;
        const auto fileName = path.filename().replace_extension("t6");
        t6FilePath.append(fileName.string());

        spdlog::info(fmt::format("Converting symbol: {}...", path.filename().replace_extension("").string()));

        futures.push_back(
            launchBounded(maxConcurrentConvertJobs,
                       [](const std::filesystem::path &csvPath, const std::filesystem::path &t6Path,
                          const std::int64_t intervalMs,
                          const mexc_staging::Alignment alignment) -> std::pair<std::string, bool> {
                           std::pair<std::string, bool> retVal;
                           retVal.first = csvPath.filename().replace_extension("").string();
                           retVal.second = writeCSVCandlesToZorroT6File(csvPath.string(), t6Path.string(),
                                                                       intervalMs, alignment);
                           if (!retVal.second) {
                               throw std::runtime_error(fmt::format("T6 conversion failed for {}",
                                                                    retVal.first));
                           }
                           return retVal;
                       }, path, t6FilePath, intervalMs, alignment));
    }

    readyFutures = waitAllOrThrow(futures);
    for (const auto &[symbol, converted]: readyFutures) {
        if (converted) {
            spdlog::info(fmt::format("Symbol: {} converted", symbol));
        }
    }
}

CsvData::TailCheck MEXCFuturesDownloader::P::checkSymbolCSVFile(const std::string &path) {
    constexpr int64_t oldestDate = 1577836800000; // Wednesday 1. January 2020 0:00:00
    // Self-healing read: a torn tail (interrupted write) is truncated instead of
    // resetting the resume point to the oldest-date sentinel.
    return CsvData::lastValidRecord(path, 7, oldestDate);
}

bool MEXCFuturesDownloader::P::writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path) {
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
        ofs << "open_time,open,high,low,close,volume,amount" << std::endl;
    }

    for (const auto &candle: candles) {
        ofs << candle.openTime << ",";
        ofs << csvNumber(candle.open) << ",";
        ofs << csvNumber(candle.high) << ",";
        ofs << csvNumber(candle.low) << ",";
        ofs << csvNumber(candle.close) << ",";
        ofs << csvNumber(candle.volume) << ",";
        ofs << csvNumber(candle.amount) << std::endl;
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Couldn't flush file: {}", path));
        return false;
    }
    ofs.close();
    return ofs.good();
}

bool MEXCFuturesDownloader::P::writeCandlesToTempFile(const std::vector<Candle> &candles, const std::string &path) {
    std::ofstream ofs;
    ofs.open(path, std::ios::trunc);

    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open temp file: {}", path));
        return false;
    }

    for (const auto &candle: candles) {
        ofs << candle.openTime << ",";
        ofs << csvNumber(candle.open) << ",";
        ofs << csvNumber(candle.high) << ",";
        ofs << csvNumber(candle.low) << ",";
        ofs << csvNumber(candle.close) << ",";
        ofs << csvNumber(candle.volume) << ",";
        ofs << csvNumber(candle.amount) << std::endl;
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Couldn't flush temp file: {}", path));
        return false;
    }
    ofs.close();
    return ofs.good();
}

bool MEXCFuturesDownloader::P::mergeTempFilesToCSV(const std::string &tempDir,
                                                    const std::string &csvPath,
                                                    const std::string &symbol) {
    const auto manifest = mexc_staging::readManifest(tempDir);
    if (!manifest) {
        spdlog::error(fmt::format("Symbol {}: refusing to merge staging without a complete manifest",
                                  symbol));
        return false;
    }

    std::string error;
    if (!mexc_staging::commit(tempDir, *manifest, csvPath,
                              "open_time,open,high,low,close,volume,amount", error)) {
        spdlog::error(fmt::format("Symbol {}: staging commit failed: {}", symbol, error));
        return false;
    }

    mexc_staging::discard(tempDir);
    spdlog::info(fmt::format("Committed {} validated batches for symbol: {}",
                             manifest->batchCount, symbol));
    return true;
}

bool MEXCFuturesDownloader::P::recoverAndMergeTempFiles(const std::string &tempDir,
                                                         const std::string &csvPath,
                                                         const std::string &symbol) {
    if (!std::filesystem::exists(tempDir)) {
        return false;
    }

    const auto manifest = mexc_staging::readManifest(tempDir);
    if (!manifest) {
        spdlog::warn(fmt::format(
            "Symbol {}: discarding incomplete MEXC staging; the requested range will be downloaded again",
            symbol));
        mexc_staging::discard(tempDir);
        return false;
    }

    const auto tail = checkSymbolCSVFile(csvPath);
    if (tail.foundValid && tail.timestamp == manifest->lastTimestamp) {
        mexc_staging::discard(tempDir);
        return true;
    }
    if (tail.foundValid != manifest->baseHasData || tail.timestamp != manifest->baseTimestamp) {
        spdlog::warn(fmt::format(
            "Symbol {}: discarding stale staging because the CSV tail changed", symbol));
        mexc_staging::discard(tempDir);
        return false;
    }

    std::string error;
    if (!mexc_staging::validate(tempDir, *manifest, error)) {
        spdlog::warn(fmt::format("Symbol {}: discarding invalid complete staging: {}", symbol, error));
        mexc_staging::discard(tempDir);
        return false;
    }

    if (!mergeTempFilesToCSV(tempDir, csvPath, symbol)) {
        throw std::runtime_error(fmt::format("Could not recover complete staging for {}", symbol));
    }
    spdlog::info(fmt::format("Symbol {}: recovered a complete staged transaction", symbol));
    return true;
}

int64_t MEXCFuturesDownloader::P::checkFundingRatesCSVFile(const std::string &path) {
    constexpr int64_t oldestDate = 1577836800000; // Wednesday 1. January 2020 0:00:00
    return CsvData::lastValidRecord(path, 2, oldestDate).timestamp;
}

bool MEXCFuturesDownloader::P::writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path) {
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
        ofs << "funding_time,funding_rate" << std::endl;
    }

    for (const auto &record: fr) {
        ofs << record.timestamp << ",";
        ofs << csvNumber(record.fundingRate) << std::endl;
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Couldn't flush file: {}", path));
        return false;
    }
    ofs.close();
    return ofs.good();
}

bool MEXCFuturesDownloader::P::writeHistoricalFundingRatesToCSVFile(const std::vector<HistoricalFundingRate> &fr, const std::string &path) {
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
        ofs << "funding_time,funding_rate" << std::endl;
    }

    for (const auto &record: fr) {
        ofs << record.settleTime << ",";
        ofs << csvNumber(record.fundingRate) << std::endl;
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Couldn't flush file: {}", path));
        return false;
    }
    ofs.close();
    return ofs.good();
}

MEXCFuturesDownloader::MEXCFuturesDownloader(std::uint32_t maxJobs, bool deleteDelistedData) : m_p(std::make_unique<P>(maxJobs, deleteDelistedData)) {}

MEXCFuturesDownloader::~MEXCFuturesDownloader() = default;

void MEXCFuturesDownloader::updateMarketData(const std::string &dirPath, const std::vector<std::string> &symbols, CandleInterval candleInterval,
                                             const onSymbolsToUpdate &onSymbolsToUpdateCB, const onSymbolCompleted &onSymbolCompletedCB, const bool convertToT6) const {
    const auto mexcCandleInterval = P::vkIntervalToMexcInterval(candleInterval);
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;

    std::vector<std::future<std::filesystem::path>> futures;
    const std::filesystem::path finalPath(dirPath);
    validateSymbolFileComponents(symbols);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    // Get all contract details from the exchange
    const auto contractDetails = m_p->mexcFuturesClient->getContractDetails();
    std::set<std::string> activeSymbols;
    std::set<std::string> allSymbols;

    // Helper to detect non-crypto contracts (tokenized stocks, indices, commodities, forex, etc.)
    auto isTradFiContract = [](const ContractDetail &c) {
        static const std::vector<std::string> tradFiTags = {"Stock", "stockindex", "Commodities", "Forex"};
        return std::ranges::any_of(c.conceptPlate, [](const std::string &plate) {
            return std::ranges::any_of(tradFiTags, [&plate](const std::string &tag) { return plate.find(tag) != std::string::npos; });
        });
    };

    for (const auto &contract: contractDetails) {
        if (contract.symbol.find("USDT") != std::string::npos && !isTradFiContract(contract)) {
            allSymbols.insert(contract.symbol);
            if (contract.state == ContractState::Enabled) {
                activeSymbols.insert(contract.symbol);
            }
        }
    }

    if (symbolsToUpdate.empty()) {
        spdlog::info("Updating all symbols");

        if (m_p->deleteDelistedData) {
            // Only download active symbols
            for (const auto &sym: activeSymbols) {
                symbolsToUpdate.push_back(sym);
            }

            // Scan existing CSV files for symbols no longer active
            std::filesystem::path csvDir = finalPath;
            csvDir.append(CSV_FUT_DIR);
            csvDir.append(Downloader::minutesToString(barSizeInMinutes));

            if (std::filesystem::exists(csvDir)) {
                for (const auto &entry: std::filesystem::directory_iterator(csvDir)) {
                    if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                        if (const auto stem = entry.path().stem().string(); !activeSymbols.contains(stem)) {
                            symbolsToDelete.push_back(stem);
                        }
                    }
                }
            }
        } else {
            // Download all symbols (including delisted) to avoid survivorship bias
            for (const auto &sym: allSymbols) {
                symbolsToUpdate.push_back(sym);
            }
        }
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));

        std::vector<std::string> tempSymbols;
        for (const auto &symbol: symbolsToUpdate) {
            if (!allSymbols.contains(symbol) && !activeSymbols.contains(symbol)) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                    spdlog::info(fmt::format("Symbol: {} is delisted, scheduling for deletion", symbol));
                } else {
                    tempSymbols.push_back(symbol);
                    spdlog::info(fmt::format("Symbol: {} is delisted, attempting download of historical data", symbol));
                }
            } else if (m_p->deleteDelistedData && !activeSymbols.contains(symbol)) {
                symbolsToDelete.push_back(symbol);
                spdlog::info(fmt::format("Symbol: {} is not active (delisted), scheduling for deletion", symbol));
            } else {
                tempSymbols.push_back(symbol);
            }
        }
        symbolsToUpdate = tempSymbols;
    }

    deduplicatePreserveOrder(symbolsToUpdate);
    validateSymbolFileComponents(symbolsToUpdate);
    if (onSymbolsToUpdateCB) {
        onSymbolsToUpdateCB(symbolsToUpdate);
    }

    // Display warning about historical data limits for minute intervals
    if (barSizeInMinutes <= 30) {
        spdlog::warn("═══════════════════════════════════════════════════════════════════════");
        spdlog::warn("MEXC FUTURES API - HISTORICAL DATA LIMITS WARNING");
        spdlog::warn("═══════════════════════════════════════════════════════════════════════");
        spdlog::warn("MEXC Futures API has undocumented limits for historical data.");
        spdlog::warn("Complete history will NOT be downloaded for minute intervals!");
        spdlog::warn("");
        spdlog::warn("Available historical data by interval:");
        spdlog::warn("┌──────────────┬────────────────────────────────────┐");
        spdlog::warn("│  Interval    │  Available History                 │");
        spdlog::warn("├──────────────┼────────────────────────────────────┤");
        spdlog::warn("│     1m       │  ~30 days                          │");
        spdlog::warn("│     5m       │  ~360 days (~1 year)               │");
        spdlog::warn("│    15m       │  ~180-365 days                     │");
        spdlog::warn("│    30m       │  5+ years (complete)               │");
        spdlog::warn("│     1h       │  5+ years (complete)               │");
        spdlog::warn("│     1d       │  Complete history                  │");
        spdlog::warn("└──────────────┴────────────────────────────────────┘");
        spdlog::warn("");
        spdlog::warn(fmt::format("Current interval: {}m - Limited history available!", barSizeInMinutes));
        spdlog::warn("═══════════════════════════════════════════════════════════════════════");
    }

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(launchBounded(
                m_p->maxConcurrentDownloadJobs,
                [finalPath, this, &mexcCandleInterval, &barSizeInMinutes,
                 &activeSymbols](const std::string &symbol) -> std::filesystem::path {
                    std::filesystem::path symbolFilePathCsv = finalPath;

                    symbolFilePathCsv.append(CSV_FUT_DIR);
                    symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                    symbolFilePathCsv = symbolFilePathCsv.lexically_normal();

                    if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                        throw std::runtime_error(fmt::format("Failed to create {}, err: {}", symbolFilePathCsv.string(), err.message().c_str()));
                    }

                    symbolFilePathCsv.append(symbol + ".csv");

                    // The API bounds are seconds, while Candle::openTime and
                    // CSV timestamps are milliseconds.  Weekly candles are
                    // Monday-aligned and months use calendar boundaries.
                    const auto intervalMs = MEXC::numberOfMsForCandleInterval(mexcCandleInterval);
                    const auto alignment = stagingAlignment(mexcCandleInterval);
                    const auto nowMs = m_p->mexcFuturesClient->getServerTime();
                    const auto currentOpen = mexc_staging::currentPeriodOpen(nowMs, intervalMs, alignment);
                    const auto lastCompletedOpen =
                        mexc_staging::previousPeriodOpen(currentOpen, intervalMs, alignment);
                    const bool expectedLive = activeSymbols.contains(symbol);

                    spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                    try {
                        // Temp directory for this symbol's batches
                        std::filesystem::path tempDir = symbolFilePathCsv.parent_path();
                        tempDir.append("temp_" + symbol);
                        mexc_staging::DirectoryLock symbolLock(tempDir.string() + ".lock");

                        // Recover only complete transactions; partial newest-
                        // first staging is unsafe and is re-downloaded.
                        P::recoverAndMergeTempFiles(tempDir.string(), symbolFilePathCsv.string(), symbol);

                        auto tail = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                        if (tail.foundValid && tail.timestamp > lastCompletedOpen) {
                            std::string repairError;
                            if (!mexc_staging::truncateAfter(symbolFilePathCsv, lastCompletedOpen,
                                                             repairError,
                                                             "open_time,open,high,low,close,volume,amount",
                                                             intervalMs, alignment)) {
                                throw std::runtime_error(fmt::format(
                                    "Could not remove old open/future MEXC Futures tail: {}", repairError));
                            }
                            spdlog::warn(fmt::format(
                                "Symbol {}: removed old open/future CSV tail newer than {}", symbol,
                                lastCompletedOpen));
                            tail = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                        }
                        if (tail.foundValid && tail.timestamp == lastCompletedOpen) {
                            spdlog::info(fmt::format("No new candles for symbol: {}", symbol));
                            return symbolFilePathCsv;
                        }

                        const auto actualFromMs = tail.foundValid
                            ? mexc_staging::nextTimestamp(tail.timestamp, intervalMs, alignment)
                            : tail.timestamp;
                        if (actualFromMs > lastCompletedOpen || actualFromMs % 1000 != 0) {
                            throw std::runtime_error("Invalid MEXC Futures download range");
                        }

                        // Futures end is inclusive.  Include the current open
                        // bar so the client can discard it and retain the last
                        // completed bar at lastCompletedOpen.
                        const auto apiStartTime = actualFromMs / 1000;
                        const auto apiEndTime = currentOpen / 1000;
                        auto candles = m_p->mexcFuturesClient->getHistoricalPrices(
                            symbol, mexcCandleInterval, apiStartTime, apiEndTime);
                        std::erase_if(candles, [actualFromMs, lastCompletedOpen](const Candle &candle) {
                            return candle.openTime < actualFromMs || candle.openTime > lastCompletedOpen;
                        });
                        if (candles.empty()) {
                            if (!expectedLive && tail.foundValid) {
                                spdlog::info(fmt::format(
                                    "No newer candles for delisted MEXC Futures symbol: {}", symbol));
                                return symbolFilePathCsv;
                            }
                            throw std::runtime_error("MEXC Futures returned no candles for a non-empty range");
                        }

                        mexc_staging::discard(tempDir);
                        if (const auto err = createDirectoryRecursively(tempDir.string())) {
                            throw std::runtime_error(fmt::format("Failed to create temp dir {}, err: {}",
                                                                 tempDir.string(), err.message().c_str()));
                        }

                        constexpr std::size_t candlesPerTempFile = 10000;
                        std::int32_t tempFileCounter = 0;
                        for (std::size_t offset = 0; offset < candles.size(); offset += candlesPerTempFile) {
                            const auto end = std::min(candles.size(), offset + candlesPerTempFile);
                            const std::vector<Candle> chunk(candles.begin() + static_cast<std::ptrdiff_t>(offset),
                                                            candles.begin() + static_cast<std::ptrdiff_t>(end));
                            ++tempFileCounter;
                            const auto tempFile = mexc_staging::batchPath(tempDir, tempFileCounter);
                            if (!P::writeCandlesToTempFile(chunk, tempFile.string())) {
                                throw std::runtime_error(fmt::format("Failed to write staging batch {}",
                                                                     tempFile.string()));
                            }
                        }

                        mexc_staging::Manifest manifest;
                        manifest.batchCount = tempFileCounter;
                        manifest.intervalMs = intervalMs;
                        manifest.alignment = alignment;
                        manifest.baseTimestamp = tail.timestamp;
                        manifest.baseHasData = tail.foundValid;
                        manifest.requestedStart = actualFromMs;
                        manifest.expectedEnd = expectedLive ? lastCompletedOpen : candles.back().openTime;
                        manifest.firstTimestamp = candles.front().openTime;
                        manifest.lastTimestamp = candles.back().openTime;

                        std::string manifestError;
                        if (!mexc_staging::writeManifest(tempDir, manifest, manifestError)) {
                            throw std::runtime_error(fmt::format("Invalid MEXC Futures staging: {}",
                                                                 manifestError));
                        }
                        if (!P::mergeTempFilesToCSV(tempDir.string(), symbolFilePathCsv.string(), symbol)) {
                            throw std::runtime_error(fmt::format("Failed to commit MEXC Futures data for {}",
                                                                 symbol));
                        }
                        spdlog::info(fmt::format("CSV file for symbol: {} updated ({} candles in {} staged batches)",
                                                 symbol, candles.size(), tempFileCounter));

                        if (std::filesystem::exists(symbolFilePathCsv)) {
                            return symbolFilePathCsv;
                        }
                    } catch (const std::exception &e) {
                        spdlog::warn(fmt::format("Updating candles for symbol: {} failed, reason: {}", symbol, e.what()));
                        throw;
                    }
                    return "";
                },
                s));
    }

    csvFilePaths = waitAllOrThrow(futures, [&onSymbolCompletedCB](const std::filesystem::path &path) {
        if (onSymbolCompletedCB && !path.empty()) {
            onSymbolCompletedCB(path.stem().string());
        }
    });

    if (convertToT6) {
        std::filesystem::path T6Directory = finalPath;
        T6Directory.append(T6_FUT_DIR);
        T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

        std::filesystem::path csvDirectory = finalPath;
        csvDirectory.append(CSV_FUT_DIR);
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
                throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(), err.message().c_str()));
            }
            spdlog::info(fmt::format("Converting from csv to t6..."));
            m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string(),
                                    MEXC::numberOfMsForCandleInterval(mexcCandleInterval),
                                    stagingAlignment(mexcCandleInterval));
        }
    }

    if (m_p->deleteDelistedData) {
        for (const auto &symbol: symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = finalPath;
            symbolFilePathCsv.append(CSV_FUT_DIR);
            symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
            symbolFilePathCsv = symbolFilePathCsv.lexically_normal();
            symbolFilePathCsv.append(symbol + ".csv");

            if (std::filesystem::exists(symbolFilePathCsv)) {
                std::filesystem::remove(symbolFilePathCsv);
                spdlog::info(fmt::format("Removing csv file for delisted symbol: {}, file: {}...", symbol, symbolFilePathCsv.string()));
            }
        }
    }
}

void MEXCFuturesDownloader::updateMarketData(const std::string &connectionString, const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                             const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: MEXCFuturesDownloader::updateMarketData()");
}

void MEXCFuturesDownloader::updateFundingRateData(const std::string &dirPath, const std::vector<std::string> &symbols, const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                                  const onSymbolCompleted &onSymbolCompletedCB) const {
    const std::filesystem::path finalPath(dirPath);
    validateSymbolFileComponents(symbols);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::string> symbolsToDelete;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    // Get all contract details from the exchange
    const auto contractDetails = m_p->mexcFuturesClient->getContractDetails();
    std::set<std::string> activeSymbols;
    std::set<std::string> allSymbols;
    auto isTradFiContract = [](const ContractDetail &c) {
        static const std::vector<std::string> tradFiTags = {"Stock", "stockindex", "Commodities", "Forex"};
        return std::ranges::any_of(c.conceptPlate, [](const std::string &plate) {
            return std::ranges::any_of(tradFiTags, [&plate](const std::string &tag) { return plate.find(tag) != std::string::npos; });
        });
    };

    for (const auto &contract: contractDetails) {
        if (contract.symbol.find("USDT") != std::string::npos && !isTradFiContract(contract)) {
            allSymbols.insert(contract.symbol);
            if (contract.state == ContractState::Enabled) {
                activeSymbols.insert(contract.symbol);
            }
        }
    }

    if (symbolsToUpdate.empty()) {
        spdlog::info("Updating all symbols");

        if (m_p->deleteDelistedData) {
            // Only download active symbols
            for (const auto &sym: activeSymbols) {
                symbolsToUpdate.push_back(sym);
            }

            // Scan existing CSV files for symbols no longer active
            std::filesystem::path frDir = finalPath;
            frDir.append(CSV_FUT_FR_DIR);

            if (std::filesystem::exists(frDir)) {
                for (const auto &entry: std::filesystem::directory_iterator(frDir)) {
                    if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                        auto stem = entry.path().stem().string();
                        if (stem.ends_with("_fr")) {
                            stem = stem.substr(0, stem.size() - 3);
                        }
                        if (!activeSymbols.contains(stem)) {
                            symbolsToDelete.push_back(stem);
                        }
                    }
                }
            }
        } else {
            // Download all symbols (including delisted) to avoid survivorship bias
            for (const auto &sym: allSymbols) {
                symbolsToUpdate.push_back(sym);
            }
        }
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));

        std::vector<std::string> tempSymbols;
        for (const auto &symbol: symbolsToUpdate) {
            if (!allSymbols.contains(symbol) && !activeSymbols.contains(symbol)) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                    spdlog::info(fmt::format("Symbol: {} is delisted, scheduling for deletion", symbol));
                } else {
                    tempSymbols.push_back(symbol);
                    spdlog::info(fmt::format("Symbol: {} is delisted, attempting download of historical data", symbol));
                }
            } else if (m_p->deleteDelistedData && !activeSymbols.contains(symbol)) {
                symbolsToDelete.push_back(symbol);
                spdlog::info(fmt::format("Symbol: {} is not active (delisted), scheduling for deletion", symbol));
            } else {
                tempSymbols.push_back(symbol);
            }
        }
        symbolsToUpdate = tempSymbols;
    }

    deduplicatePreserveOrder(symbolsToUpdate);
    validateSymbolFileComponents(symbolsToUpdate);
    if (onSymbolsToUpdateCB) {
        onSymbolsToUpdateCB(symbolsToUpdate);
    }

    // Create funding rate directory
    std::filesystem::path frDir = finalPath;
    frDir.append(CSV_FUT_FR_DIR);

    if (const auto err = createDirectoryRecursively(frDir.string()); err.value() != 0) {
        throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}", frDir.string(), err.value()));
    }

    // Download complete funding rate history for each symbol.
    std::vector<std::future<std::string>> fundingFutures;
    for (const auto &symbol: symbolsToUpdate) {
        fundingFutures.push_back(launchBounded(
            m_p->maxConcurrentDownloadJobs,
            [this, frDir](const std::string &symbol) -> std::string {
                try {
                    spdlog::info(fmt::format("Downloading funding rate history for symbol: {}...", symbol));

                    std::filesystem::path symbolFilePathCsv = frDir;
                    symbolFilePathCsv.append(symbol + "_fr.csv");
                    const int64_t lastTimestamp = P::checkFundingRatesCSVFile(symbolFilePathCsv.string());

                    std::vector<HistoricalFundingRate> newRates;
                    int32_t currentPage = 1;
                    bool hasMoreData = true;

                    while (hasMoreData) {
                        constexpr int32_t pageSize = 1000;
                        auto response = m_p->mexcFuturesClient->getContractFundingRateHistory(
                            symbol, currentPage, pageSize);
                        if (response.resultList.empty()) {
                            break;
                        }

                        bool foundOldData = false;
                        for (const auto &rate: response.resultList) {
                            if (rate.settleTime <= lastTimestamp) {
                                foundOldData = true;
                                break;
                            }
                            newRates.push_back(rate);
                        }

                        if (foundOldData || currentPage >= response.totalPage) {
                            hasMoreData = false;
                        } else {
                            ++currentPage;
                        }
                        spdlog::info(fmt::format(
                            "Symbol {}: downloaded page {}/{}, {} new rates so far", symbol,
                            currentPage - 1, response.totalPage, newRates.size()));
                    }

                    if (!newRates.empty()) {
                        std::ranges::reverse(newRates);
                        if (!P::writeHistoricalFundingRatesToCSVFile(newRates,
                                                                     symbolFilePathCsv.string())) {
                            throw std::runtime_error("failed to write funding-rate CSV");
                        }
                        spdlog::info(fmt::format("Symbol {}: saved {} funding rates to CSV",
                                                 symbol, newRates.size()));
                    } else {
                        spdlog::info(fmt::format("Symbol {}: no new funding rates", symbol));
                    }

                    return symbol;
                } catch (const std::exception &e) {
                    spdlog::warn(fmt::format(
                        "Failed to download funding rates for symbol: {}, reason: {}", symbol,
                        e.what()));
                    throw;
                }
            }, symbol));
    }
    std::ignore = waitAllOrThrow(fundingFutures, [&onSymbolCompletedCB](const std::string &symbol) {
        if (onSymbolCompletedCB) {
            onSymbolCompletedCB(symbol);
        }
    });

    if (m_p->deleteDelistedData) {
        for (const auto &symbol: symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = finalPath;
            symbolFilePathCsv.append(CSV_FUT_FR_DIR);
            symbolFilePathCsv = symbolFilePathCsv.lexically_normal();
            symbolFilePathCsv.append(symbol + "_fr.csv");

            if (std::filesystem::exists(symbolFilePathCsv)) {
                std::filesystem::remove(symbolFilePathCsv);
                spdlog::info(fmt::format("Removing csv file for delisted symbol: {}, file: {}...", symbol, symbolFilePathCsv.string()));
            }
        }
    }
}

void MEXCFuturesDownloader::convertToT6(const std::string &dirPath, const CandleInterval candleInterval) const {
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    const auto mexcCandleInterval = P::vkIntervalToMexcInterval(candleInterval);
    const std::filesystem::path finalPath(dirPath);

    std::filesystem::path csvDirectory = finalPath;
    csvDirectory.append(CSV_FUT_DIR);
    csvDirectory.append(Downloader::minutesToString(barSizeInMinutes));

    std::filesystem::path T6Directory = finalPath;
    T6Directory.append(T6_FUT_DIR);
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
            throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(), err.message().c_str()));
        }
        spdlog::info(fmt::format("Converting from csv to t6..."));
        m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string(),
                                MEXC::numberOfMsForCandleInterval(mexcCandleInterval),
                                stagingAlignment(mexcCandleInterval));
    }
}
} // namespace stonky
