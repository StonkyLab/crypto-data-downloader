/**
MEXC Spot Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/mexc/mexc_spot_downloader.h"
#include "stonky/atomic_file.h"
#include "stonky/csv_format.h"
#include "stonky/csv_data.h"
#include "stonky/mexc/mexc_spot_rest_client.h"
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
#include <spdlog/fmt/ranges.h>
#include <ranges>
#include "csv.h"

using namespace stonky::mexc;
using namespace stonky::mexc::spot;

namespace stonky {
struct MEXCSpotDownloader::P {
    std::unique_ptr<RESTClient> mexcSpotClient;
    mutable Semaphore maxConcurrentConvertJobs;
    Semaphore maxConcurrentDownloadJobs;
    bool deleteDelistedData = false;

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData) :
        mexcSpotClient(std::make_unique<RESTClient>("", "")), maxConcurrentConvertJobs(normalizedJobCount(maxJobs)),
        maxConcurrentDownloadJobs(boundedJobCount(maxJobs, 3)), deleteDelistedData(deleteDelistedData) {}

    static CsvData::TailCheck checkSymbolCSVFile(const std::string &path);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path);

    // Write candles to temp file (no header, used for batch files)
    static bool writeCandlesToTempFile(const std::vector<Candle> &candles, const std::string &path);

    // Atomically replace the CSV with the old data plus a complete, validated transaction.
    static bool mergeTempFilesToCSV(const std::string &tempDir, const std::string &csvPath,
                                    const std::string &symbol);

    // Recover only a transaction carrying a complete manifest.  Incomplete
    // newest-first staging from older versions is discarded and re-downloaded.
    static bool recoverAndMergeTempFiles(const std::string &tempDir, const std::string &csvPath,
                                         const std::string &symbol);

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

mexc::CandleInterval MEXCSpotDownloader::P::vkIntervalToMexcInterval(const stonky::CandleInterval interval) {
    switch (interval) {
        case stonky::CandleInterval::_1m:
            return mexc::CandleInterval::_1m;
        case stonky::CandleInterval::_5m:
            return mexc::CandleInterval::_5m;
        case stonky::CandleInterval::_15m:
            return mexc::CandleInterval::_15m;
        case stonky::CandleInterval::_30m:
            return mexc::CandleInterval::_30m;
        case stonky::CandleInterval::_1h:
            return mexc::CandleInterval::_60m; // Spot uses 60m instead of 1h
        case stonky::CandleInterval::_4h:
            return mexc::CandleInterval::_4h;
        case stonky::CandleInterval::_8h:
            return mexc::CandleInterval::_8h;
        case stonky::CandleInterval::_1d:
            return mexc::CandleInterval::_1d;
        case stonky::CandleInterval::_1w:
            return mexc::CandleInterval::_1W;
        case stonky::CandleInterval::_1M:
            return mexc::CandleInterval::_1M;
        default:
            throw std::invalid_argument("Unsupported candle interval for MEXC Spot");
    }
}

bool MEXCSpotDownloader::P::readCandlesFromCSVFile(const std::string &path, std::vector<CsvCandle> &candles) {
    try {
        io::CSVReader<7> in(path);
        in.read_header(io::ignore_extra_column, "open_time", "open", "high", "low", "close", "volume", "quote_asset_volume");

        CsvCandle candle;
        double quoteVol = 0.0;
        while (in.read_row(candle.openTime, candle.open, candle.high, candle.low, candle.close,
                           candle.volume, quoteVol)) {
            candles.push_back(candle);
        }
    } catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }

    return true;
}

bool MEXCSpotDownloader::P::writeCSVCandlesToZorroT6File(
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

void MEXCSpotDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
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

CsvData::TailCheck MEXCSpotDownloader::P::checkSymbolCSVFile(const std::string &path) {
    // Default to January 1, 2020 (same as Futures)
    // For newly listed tokens or intervals with limited history, backward pagination
    // will stop when API returns empty results
    constexpr int64_t defaultStartDate = 1577836800000; // Wednesday 1. January 2020 0:00:00
    // Self-healing read: a torn tail (interrupted write) is truncated instead of
    // resetting the resume point to the default start date.
    return CsvData::lastValidRecord(path, 7, defaultStartDate);
}

bool MEXCSpotDownloader::P::writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path) {
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
        ofs << "open_time,open,high,low,close,volume,quote_asset_volume" << std::endl;
    }

    for (const auto &candle: candles) {
        ofs << candle.openTime << ",";
        ofs << csvNumber(candle.open) << ",";
        ofs << csvNumber(candle.high) << ",";
        ofs << csvNumber(candle.low) << ",";
        ofs << csvNumber(candle.close) << ",";
        ofs << csvNumber(candle.volume) << ",";
        ofs << csvNumber(candle.quoteAssetVolume) << std::endl;
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Couldn't flush file: {}", path));
        return false;
    }
    ofs.close();
    return ofs.good();
}

bool MEXCSpotDownloader::P::writeCandlesToTempFile(const std::vector<Candle> &candles, const std::string &path) {
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
        ofs << csvNumber(candle.quoteAssetVolume) << std::endl;
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Couldn't flush temp file: {}", path));
        return false;
    }
    ofs.close();
    return ofs.good();
}

bool MEXCSpotDownloader::P::mergeTempFilesToCSV(const std::string &tempDir,
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
                              "open_time,open,high,low,close,volume,quote_asset_volume", error)) {
        spdlog::error(fmt::format("Symbol {}: staging commit failed: {}", symbol, error));
        return false;
    }

    mexc_staging::discard(tempDir);
    spdlog::info(fmt::format("Committed {} validated batches for symbol: {}",
                             manifest->batchCount, symbol));
    return true;
}

bool MEXCSpotDownloader::P::recoverAndMergeTempFiles(const std::string &tempDir,
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
        // The atomic replacement completed and only directory cleanup was interrupted.
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

MEXCSpotDownloader::MEXCSpotDownloader(std::uint32_t maxJobs, bool deleteDelistedData) : m_p(std::make_unique<P>(maxJobs, deleteDelistedData)) {}

MEXCSpotDownloader::~MEXCSpotDownloader() = default;

void MEXCSpotDownloader::updateMarketData(const std::string &dirPath, const std::vector<std::string> &symbols, CandleInterval candleInterval,
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

    // Get all active symbols from ticker endpoint
    const auto tickers = m_p->mexcSpotClient->getTickerPrice("");
    std::set<std::string> activeSymbols;
    for (const auto &ticker: tickers) {
        if (ticker.symbol.find("USDT") != std::string::npos) {
            activeSymbols.insert(ticker.symbol);
        }
    }

    if (symbolsToUpdate.empty()) {
        spdlog::info("Updating all symbols");
        for (const auto &sym: activeSymbols) {
            symbolsToUpdate.push_back(sym);
        }

        // Scan existing CSV files for symbols no longer on the exchange
        if (m_p->deleteDelistedData) {
            std::filesystem::path csvDir = finalPath;
            csvDir.append(CSV_SPOT_DIR);
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
        }
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));

        std::vector<std::string> tempSymbols;
        for (const auto &symbol: symbolsToUpdate) {
            if (activeSymbols.contains(symbol)) {
                tempSymbols.push_back(symbol);
            } else {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                } else {
                    tempSymbols.push_back(symbol);
                }
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", symbol));
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
        spdlog::warn("MEXC SPOT API - HISTORICAL DATA LIMITS WARNING");
        spdlog::warn("═══════════════════════════════════════════════════════════════════════");
        spdlog::warn("MEXC Spot API has undocumented limits for historical data.");
        spdlog::warn("Complete history will NOT be downloaded for minute intervals!");
        spdlog::warn("");
        spdlog::warn("Available historical data by interval:");
        spdlog::warn("┌──────────────┬────────────────────────────────────┐");
        spdlog::warn("│  Interval    │  Available History                 │");
        spdlog::warn("├──────────────┼────────────────────────────────────┤");
        spdlog::warn("│     1m       │  ~30 days                          │");
        spdlog::warn("│     5m       │  ~270 days (~9 months)             │");
        spdlog::warn("│    15m       │  ~270 days (~9 months)             │");
        spdlog::warn("│    30m       │  ~270 days (~9 months)             │");
        spdlog::warn("│     1h       │  5+ years (complete)               │");
        spdlog::warn("│     4h       │  5+ years (complete)               │");
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

                    symbolFilePathCsv.append(CSV_SPOT_DIR);
                    symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                    symbolFilePathCsv = symbolFilePathCsv.lexically_normal();

                    if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                        throw std::runtime_error(fmt::format("Failed to create {}, err: {}", symbolFilePathCsv.string(), err.message().c_str()));
                    }

                    symbolFilePathCsv.append(symbol + ".csv");

                    // MEXC Spot API uses timestamps in milliseconds.  Weekly
                    // candles open on Monday UTC; monthly candles use calendar
                    // month boundaries rather than an epoch-based 30-day grid.
                    const auto intervalMs = MEXC::numberOfMsForCandleInterval(mexcCandleInterval);
                    const auto alignment = stagingAlignment(mexcCandleInterval);
                    const auto nowMs = m_p->mexcSpotClient->getServerTime();
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

                        // Only a fully validated transaction with a completion
                        // manifest may be recovered.  Partial older staging is
                        // discarded so it can never jump across a gap.
                        P::recoverAndMergeTempFiles(tempDir.string(), symbolFilePathCsv.string(), symbol);

                        auto tail = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                        if (tail.foundValid && tail.timestamp > lastCompletedOpen) {
                            std::string repairError;
                            if (!mexc_staging::truncateAfter(symbolFilePathCsv, lastCompletedOpen,
                                                             repairError,
                                                             "open_time,open,high,low,close,volume,quote_asset_volume",
                                                             intervalMs, alignment)) {
                                throw std::runtime_error(fmt::format(
                                    "Could not remove old open/future MEXC Spot tail: {}", repairError));
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

                        const auto actualFromTimeStamp = tail.foundValid
                            ? mexc_staging::nextTimestamp(tail.timestamp, intervalMs, alignment)
                            : tail.timestamp;
                        if (actualFromTimeStamp > lastCompletedOpen) {
                            throw std::runtime_error("Invalid MEXC Spot download range");
                        }
                        const auto apiEndTime = mexc_staging::nextTimestamp(lastCompletedOpen,
                                                                           intervalMs, alignment);
                        spdlog::debug(fmt::format("Symbol {}: Calling API from {} to {}", symbol,
                                                  actualFromTimeStamp, apiEndTime));

                        // The REST client already accumulated all pages for its
                        // return value.  Stage only after it completes, in
                        // chronological chunks, so an interrupted pagination
                        // cannot leave a recoverable newest-only prefix.
                        const auto candles = m_p->mexcSpotClient->getHistoricalPrices(
                            symbol, mexcCandleInterval, actualFromTimeStamp, apiEndTime);
                        if (candles.empty()) {
                            if (!expectedLive && tail.foundValid) {
                                spdlog::info(fmt::format(
                                    "No newer candles for delisted MEXC Spot symbol: {}", symbol));
                                return symbolFilePathCsv;
                            }
                            throw std::runtime_error("MEXC Spot returned no candles for a non-empty range");
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
                        manifest.requestedStart = actualFromTimeStamp;
                        manifest.expectedEnd = expectedLive ? lastCompletedOpen : candles.back().openTime;
                        manifest.firstTimestamp = candles.front().openTime;
                        manifest.lastTimestamp = candles.back().openTime;

                        std::string manifestError;
                        if (!mexc_staging::writeManifest(tempDir, manifest, manifestError)) {
                            throw std::runtime_error(fmt::format("Invalid MEXC Spot staging: {}",
                                                                 manifestError));
                        }
                        if (!P::mergeTempFilesToCSV(tempDir.string(), symbolFilePathCsv.string(), symbol)) {
                            throw std::runtime_error(fmt::format("Failed to commit MEXC Spot data for {}",
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
        T6Directory.append(T6_SPOT_DIR);
        T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

        std::filesystem::path csvDirectory = finalPath;
        csvDirectory.append(CSV_SPOT_DIR);
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
            symbolFilePathCsv.append(CSV_SPOT_DIR);
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

void MEXCSpotDownloader::updateMarketData(const std::string &connectionString, const onSymbolsToUpdate &onSymbolsToUpdateCB, const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: MEXCSpotDownloader::updateMarketData()");
}

void MEXCSpotDownloader::updateFundingRateData(const std::string &dirPath, const std::vector<std::string> &symbols, const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                               const onSymbolCompleted &onSymbolCompletedCB) const {
    spdlog::warn("Funding rates are not available for MEXC Spot market");
}

void MEXCSpotDownloader::convertToT6(const std::string &dirPath, const CandleInterval candleInterval) const {
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    const auto mexcCandleInterval = P::vkIntervalToMexcInterval(candleInterval);
    const std::filesystem::path finalPath(dirPath);

    std::filesystem::path csvDirectory = finalPath;
    csvDirectory.append(CSV_SPOT_DIR);
    csvDirectory.append(Downloader::minutesToString(barSizeInMinutes));

    std::filesystem::path T6Directory = finalPath;
    T6Directory.append(T6_SPOT_DIR);
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
