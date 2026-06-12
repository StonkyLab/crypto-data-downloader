/**
MEXC Spot Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/mexc/mexc_spot_downloader.h"
#include "stonky/csv_data.h"
#include "stonky/mexc/mexc_spot_rest_client.h"
#include "stonky/mexc/mexc.h"
#include "stonky/downloader.h"
#include "stonky/utils/semaphore.h"
#include "stonky/utils/utils.h"
#include "stonky/interface/exchange_enums.h"
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
    Semaphore maxConcurrentDownloadJobs{3};
    bool deleteDelistedData = false;

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData) :
        mexcSpotClient(std::make_unique<RESTClient>("", "")), maxConcurrentConvertJobs(maxJobs), deleteDelistedData(deleteDelistedData) {}

    static int64_t checkSymbolCSVFile(const std::string &path);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path);

    // Write candles to temp file (no header, used for batch files)
    static bool writeCandlesToTempFile(const std::vector<Candle> &candles, const std::string &path);

    // Merge numbered temp files in reverse order and append to main CSV
    static bool mergeTempFilesToCSV(const std::string &tempDir, int batchCount, const std::string &csvPath, const std::string &symbol);

    // Check for existing temp files from interrupted download and merge them
    // Returns the number of batches that were merged (0 if none found)
    static int recoverAndMergeTempFiles(const std::string &tempDir, const std::string &csvPath, const std::string &symbol);

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
    static bool writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path);
    void convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths, const std::string &outDirPath) const;
};

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

bool MEXCSpotDownloader::P::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path) {
    const std::filesystem::path pathToT6File{t6Path};

    std::ofstream ofs;
    ofs.open(pathToT6File.string(), std::ios::trunc | std::ios::binary);

    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open file: {}", t6Path));
        return false;
    }

    std::vector<CsvCandle> candles;
    if (!readCandlesFromCSVFile(csvPath, candles)) {
        spdlog::error(fmt::format("Couldn't read candles from csv file: {}", csvPath));
        return false;
    }

    constexpr int64_t oneMinuteMs = 60000;

    for (const auto &candle: std::ranges::reverse_view(candles)) {
        T6 t6;
        t6.fOpen = static_cast<float>(candle.open);
        t6.fHigh = static_cast<float>(candle.high);
        t6.fLow = static_cast<float>(candle.low);
        t6.fClose = static_cast<float>(candle.close);
        t6.fVal = 0.0;
        t6.fVol = static_cast<float>(candle.volume);
        t6.time = convertTimeMs(candle.openTime + oneMinuteMs);
        ofs.write(reinterpret_cast<char *>(&t6), sizeof(T6));
    }

    ofs.close();
    return true;
}

void MEXCSpotDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
                                                const std::string &outDirPath) const {
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

int64_t MEXCSpotDownloader::P::checkSymbolCSVFile(const std::string &path) {
    // Default to January 1, 2020 (same as Futures)
    // For newly listed tokens or intervals with limited history, backward pagination
    // will stop when API returns empty results
    constexpr int64_t defaultStartDate = 1577836800000; // Wednesday 1. January 2020 0:00:00
    // Self-healing read: a torn tail (interrupted write) is truncated instead of
    // resetting the resume point to the default start date.
    return CsvData::lastValidRecord(path, 7, defaultStartDate, true).timestamp;
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
        ofs << candle.open.str(8, std::ios_base::fixed) << ",";
        ofs << candle.high.str(8, std::ios_base::fixed) << ",";
        ofs << candle.low.str(8, std::ios_base::fixed) << ",";
        ofs << candle.close.str(8, std::ios_base::fixed) << ",";
        ofs << candle.volume.str(10, std::ios_base::fixed) << ",";
        ofs << candle.quoteAssetVolume.str(10, std::ios_base::fixed) << std::endl;
    }

    ofs.close();
    return true;
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
        ofs << candle.open.str(8, std::ios_base::fixed) << ",";
        ofs << candle.high.str(8, std::ios_base::fixed) << ",";
        ofs << candle.low.str(8, std::ios_base::fixed) << ",";
        ofs << candle.close.str(8, std::ios_base::fixed) << ",";
        ofs << candle.volume.str(10, std::ios_base::fixed) << ",";
        ofs << candle.quoteAssetVolume.str(10, std::ios_base::fixed) << std::endl;
    }

    ofs.close();
    return true;
}

bool MEXCSpotDownloader::P::mergeTempFilesToCSV(const std::string &tempDir, const int batchCount, const std::string &csvPath, const std::string &symbol) {
    if (batchCount == 0) {
        return true;
    }

    // Check if CSV file exists and has header
    bool needsHeader = false;
    try {
        if (!std::filesystem::exists(csvPath) || std::filesystem::file_size(csvPath) == 0) {
            needsHeader = true;
        }
    } catch (const std::filesystem::filesystem_error &) {
        needsHeader = true;
    }

    std::ofstream ofs;
    ofs.open(csvPath, std::ios::app);

    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open CSV file for merge: {}", csvPath));
        return false;
    }

    if (needsHeader) {
        ofs << "open_time,open,high,low,close,volume,quote_asset_volume" << std::endl;
    }

    // Read temp files in FORWARD order (newest batch first)
    // Batches are numbered 1, 2, 3... where 1 is newest (from backward pagination)
    // Each batch itself is chronological (oldest to newest within batch)
    // To get final chronological order, we write oldest batches first
    // Since batch 1 = newest data, batch N = oldest data, we need to reverse:
    // Write N first, then N-1, ..., 2, 1
    for (int i = batchCount; i >= 1; --i) {
        std::filesystem::path tempFile = tempDir;
        tempFile.append(fmt::format("batch_{:05d}.tmp", i));

        if (!std::filesystem::exists(tempFile)) {
            spdlog::warn(fmt::format("Temp file missing: {}", tempFile.string()));
            continue;
        }

        if (std::ifstream ifs(tempFile.string()); ifs.is_open()) {
            ofs << ifs.rdbuf();
            ifs.close();
        }

        // Delete temp file after reading
        std::filesystem::remove(tempFile);
    }

    ofs.close();

    // Remove temp directory and all its contents
    try {
        std::filesystem::remove_all(tempDir);
    } catch (const std::exception &e) {
        spdlog::warn(fmt::format("Failed to remove temp directory {}: {}", tempDir, e.what()));
    }

    spdlog::info(fmt::format("Merged {} batches for symbol: {}", batchCount, symbol));
    return true;
}

int MEXCSpotDownloader::P::recoverAndMergeTempFiles(const std::string &tempDir, const std::string &csvPath, const std::string &symbol) {
    if (!std::filesystem::exists(tempDir)) {
        return 0;
    }

    // Count existing batch files
    int maxBatchNum = 0;
    for (const auto &entry: std::filesystem::directory_iterator(tempDir)) {
        if (entry.is_regular_file()) {
            if (const std::string filename = entry.path().filename().string(); filename.starts_with("batch_") && filename.ends_with(".tmp")) {
                // Extract batch number from filename like "batch_00005.tmp"
                try {
                    const int batchNum = std::stoi(filename.substr(6, 5));
                    maxBatchNum = std::max(maxBatchNum, batchNum);
                } catch (...) {
                    // Ignore malformed filenames
                }
            }
        }
    }

    if (maxBatchNum == 0) {
        // No valid batch files found, clean up directory
        try {
            std::filesystem::remove_all(tempDir);
        } catch (...) {
        }
        return 0;
    }

    spdlog::info(fmt::format("Symbol {}: Found {} temp batches from interrupted download, recovering...", symbol, maxBatchNum));

    // Merge the recovered batches
    if (mergeTempFilesToCSV(tempDir, maxBatchNum, csvPath, symbol)) {
        spdlog::info(fmt::format("Symbol {}: Successfully recovered {} batches from previous download", symbol, maxBatchNum));
        return maxBatchNum;
    }

    return 0;
}

MEXCSpotDownloader::MEXCSpotDownloader(std::uint32_t maxJobs, bool deleteDelistedData) : m_p(std::make_unique<P>(maxJobs, deleteDelistedData)) {}

MEXCSpotDownloader::~MEXCSpotDownloader() = default;

void MEXCSpotDownloader::updateMarketData(const std::string &dirPath, const std::vector<std::string> &symbols, CandleInterval candleInterval,
                                          const onSymbolsToUpdate &onSymbolsToUpdateCB, const onSymbolCompleted &onSymbolCompletedCB, const bool convertToT6) const {
    const auto mexcCandleInterval = P::vkIntervalToMexcInterval(candleInterval);
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;

    std::vector<std::future<std::filesystem::path>> futures;
    const std::filesystem::path finalPath(dirPath);
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
        futures.push_back(std::async(
                std::launch::async,
                [finalPath, this, &mexcCandleInterval, &barSizeInMinutes, &onSymbolCompletedCB](const std::string &symbol, Semaphore &maxJobs) -> std::filesystem::path {
                    std::scoped_lock w(maxJobs);
                    std::filesystem::path symbolFilePathCsv = finalPath;

                    symbolFilePathCsv.append(CSV_SPOT_DIR);
                    symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                    symbolFilePathCsv = symbolFilePathCsv.lexically_normal();

                    if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                        throw std::runtime_error(fmt::format("Failed to create {}, err: {}", symbolFilePathCsv.string(), err.message().c_str()));
                    }

                    symbolFilePathCsv.append(symbol + ".csv");

                    // MEXC Spot API uses timestamps in MILLISECONDS
                    auto nowTimestamp = std::chrono::milliseconds(std::time(nullptr) * 1000).count();

                    // Calculate interval in milliseconds and round down to current interval boundary
                    // alignedNow is the open time of the currently IN-PROGRESS candle
                    // The last COMPLETED candle opened at (alignedNow - intervalMs)
                    const int64_t intervalMs = barSizeInMinutes * 60 * 1000;
                    const int64_t alignedNow = (nowTimestamp / intervalMs) * intervalMs;
                    // Last completed candle's open time
                    nowTimestamp = alignedNow - intervalMs;

                    spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                    const int64_t fromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());

                    try {
                        // Temp directory for this symbol's batches
                        std::filesystem::path tempDir = symbolFilePathCsv.parent_path();
                        tempDir.append("temp_" + symbol);

                        // RECOVERY: Check for temp files from previous interrupted download
                        P::recoverAndMergeTempFiles(tempDir.string(), symbolFilePathCsv.string(), symbol);

                        // Re-check the CSV file for last timestamp (may have changed after recovery)
                        int64_t recoveredFromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());

                        spdlog::debug(fmt::format("Symbol {}: nowTimestamp={}, recoveredFromTimeStamp={}, diff={}", symbol, nowTimestamp, recoveredFromTimeStamp,
                                                  nowTimestamp - recoveredFromTimeStamp));

                        // Determine where to start downloading from
                        if (recoveredFromTimeStamp >= nowTimestamp) {
                            if (recoveredFromTimeStamp > nowTimestamp) {
                                // CSV has incomplete/future candle (from buggy old download)
                                spdlog::warn(fmt::format("Symbol {}: CSV has future candle at {}, re-downloading from beginning", symbol, recoveredFromTimeStamp));
                                recoveredFromTimeStamp = fromTimeStamp;
                            } else {
                                // Last CSV candle equals current candle boundary, no new complete data yet
                                spdlog::info(fmt::format("No new candles for symbol: {}", symbol));
                                // Clean up temp directory
                                try {
                                    std::filesystem::remove_all(tempDir.string());
                                } catch (const std::exception &e) {
                                    spdlog::warn(fmt::format("Failed to remove temp directory {}: {}", tempDir.string(), e.what()));
                                }

                                if (onSymbolCompletedCB) {
                                    onSymbolCompletedCB(symbol);
                                }

                                if (std::filesystem::exists(symbolFilePathCsv)) {
                                    return symbolFilePathCsv;
                                }
                                return "";
                            }
                        }
                        // else: Normal case, recoveredFromTimeStamp < nowTimestamp, proceed with download

                        // Create temp directory for new batches
                        if (const auto err = createDirectoryRecursively(tempDir.string())) {
                            throw std::runtime_error(fmt::format("Failed to create temp dir {}, err: {}", tempDir.string(), err.message().c_str()));
                        }

                        // Batch counter - batches arrive in reverse chronological order
                        int tempFileCounter = 0;
                        int apiBatchCounter = 0;
                        constexpr int apiBatchesPerTempFile = 10;
                        std::vector<Candle> accumulatedCandles;

                        // Use callback-based API - REST client handles the pagination
                        // Calculate actual start time (next candle after last in CSV)
                        const int64_t actualFromTimeStamp = recoveredFromTimeStamp + intervalMs;

                        // If actualFromTimeStamp > nowTimestamp, no new candles
                        // (use > not >= because when they're equal, there's exactly one candle)
                        if (actualFromTimeStamp > nowTimestamp) {
                            spdlog::info(fmt::format("No new candles for symbol: {}", symbol));
                            // Clean up temp directory
                            try {
                                std::filesystem::remove_all(tempDir.string());
                            } catch (const std::exception &e) {
                                spdlog::warn(fmt::format("Failed to remove temp directory {}: {}", tempDir.string(), e.what()));
                            }

                            if (onSymbolCompletedCB) {
                                onSymbolCompletedCB(symbol);
                            }

                            if (std::filesystem::exists(symbolFilePathCsv)) {
                                return symbolFilePathCsv;
                            }
                            return "";
                        }

                        // Progressive saving via callback
                        // MEXC API uses exclusive end time, so add 1 interval to include the last candle
                        const int64_t apiEndTime = nowTimestamp + intervalMs;
                        spdlog::debug(fmt::format("Symbol {}: Calling API from {} to {}", symbol, actualFromTimeStamp, apiEndTime));
                        std::ignore = m_p->mexcSpotClient->getHistoricalPrices(
                                symbol, mexcCandleInterval, actualFromTimeStamp, apiEndTime,
                                [&tempDir, &symbol, &tempFileCounter, &apiBatchCounter, &accumulatedCandles, apiBatchesPerTempFile](const std::vector<Candle> &cnd) {
                                    if (!cnd.empty()) {
                                        apiBatchCounter++;
                                        // Prepend candles - batches arrive newest-first from backward pagination
                                        // Each batch from API is chronological (oldest to newest within batch)
                                        // By prepending, accumulated candles stay chronological overall
                                        // Temp file 1 will have data from batch 1-10 (newest), temp file N will have the oldest
                                        // Merge reads N to 1, so the oldest data ends up first in CSV
                                        accumulatedCandles.insert(accumulatedCandles.begin(), cnd.begin(), cnd.end());

                                        // Write to temp file every N API batches
                                        if (apiBatchCounter % apiBatchesPerTempFile == 0) {
                                            tempFileCounter++;
                                            std::filesystem::path tempFile = tempDir;
                                            tempFile.append(fmt::format("batch_{:05d}.tmp", tempFileCounter));

                                            if (P::writeCandlesToTempFile(accumulatedCandles, tempFile.string())) {
                                                spdlog::info(fmt::format("Symbol {}: saved temp file {} ({} candles from {} API batches)", symbol, tempFileCounter,
                                                                         accumulatedCandles.size(), apiBatchesPerTempFile));
                                            } else {
                                                spdlog::warn(fmt::format("Symbol {}: failed to save temp file {}", symbol, tempFileCounter));
                                            }
                                            accumulatedCandles.clear();
                                        }
                                    }
                                });

                        // Write any remaining accumulated candles
                        if (!accumulatedCandles.empty()) {
                            tempFileCounter++;
                            std::filesystem::path tempFile = tempDir;
                            tempFile.append(fmt::format("batch_{:05d}.tmp", tempFileCounter));

                            if (P::writeCandlesToTempFile(accumulatedCandles, tempFile.string())) {
                                spdlog::info(fmt::format("Symbol {}: saved final temp file {} ({} candles)", symbol, tempFileCounter, accumulatedCandles.size()));
                            }
                        }

                        // Merge all temp files to main CSV (in reverse order for chronological result)
                        if (tempFileCounter > 0) {
                            if (P::mergeTempFilesToCSV(tempDir.string(), tempFileCounter, symbolFilePathCsv.string(), symbol)) {
                                spdlog::info(fmt::format("CSV file for symbol: {} updated ({} API batches in {} temp files)", symbol, apiBatchCounter, tempFileCounter));
                            }
                        } else {
                            spdlog::info(fmt::format("No new candles for symbol: {}", symbol));
                            // Clean up temp directory even when no new data
                            try {
                                std::filesystem::remove_all(tempDir.string());
                            } catch (const std::exception &e) {
                                spdlog::warn(fmt::format("Failed to remove temp directory {}: {}", tempDir.string(), e.what()));
                            }
                        }

                        if (onSymbolCompletedCB) {
                            onSymbolCompletedCB(symbol);
                        }

                        if (std::filesystem::exists(symbolFilePathCsv)) {
                            return symbolFilePathCsv;
                        }
                    } catch (const std::exception &e) {
                        spdlog::warn(fmt::format("Updating candles for symbol: {} failed, reason: {}", symbol, e.what()));
                    }
                    return "";
                },
                s, std::ref(m_p->maxConcurrentDownloadJobs)));
    }

    do {
        for (auto &future: futures) {
            if (isReady(future)) {
                csvFilePaths.push_back(future.get());
            }
        }
    } while (csvFilePaths.size() < futures.size());

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
            m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string());
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
        m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string());
    }
}
} // namespace stonky
