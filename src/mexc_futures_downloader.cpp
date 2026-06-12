/**
MEXC Futures Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/mexc/mexc_futures_downloader.h"
#include "stonky/csv_data.h"
#include "stonky/mexc/mexc_futures_rest_client.h"
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
#include <tuple>
#include <spdlog/fmt/ranges.h>
#include <ranges>
#include "csv.h"

using namespace stonky::mexc;
using namespace stonky::mexc::futures;

namespace stonky {
struct MEXCFuturesDownloader::P {
    std::unique_ptr<RESTClient> mexcFuturesClient;
    mutable Semaphore maxConcurrentConvertJobs;
    Semaphore maxConcurrentDownloadJobs{3};
    bool deleteDelistedData = false;

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData) :
        mexcFuturesClient(std::make_unique<RESTClient>("", "")), maxConcurrentConvertJobs(maxJobs), deleteDelistedData(deleteDelistedData) {}

    static int64_t checkSymbolCSVFile(const std::string &path);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path);

    // Write candles to temp file (no header, used for batch files)
    static bool writeCandlesToTempFile(const std::vector<Candle> &candles, const std::string &path);

    // Merge numbered temp files in reverse order and append to main CSV
    static bool mergeTempFilesToCSV(const std::string &tempDir, int batchCount, const std::string &csvPath, const std::string &symbol);

    // Check for existing temp files from interrupted download and merge them
    // Returns the number of batches that were merged (0 if none found)
    static int recoverAndMergeTempFiles(const std::string &tempDir, const std::string &csvPath, const std::string &symbol);

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
    static bool writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path);
    void convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths, const std::string &outDirPath) const;
};

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

bool MEXCFuturesDownloader::P::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path) {
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

void MEXCFuturesDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
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

int64_t MEXCFuturesDownloader::P::checkSymbolCSVFile(const std::string &path) {
    constexpr int64_t oldestDate = 1577836800000; // Wednesday 1. January 2020 0:00:00
    // Self-healing read: a torn tail (interrupted write) is truncated instead of
    // resetting the resume point to the oldest-date sentinel.
    return CsvData::lastValidRecord(path, 7, oldestDate, true).timestamp;
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
        ofs << candle.open.str(8, std::ios_base::fixed) << ",";
        ofs << candle.high.str(8, std::ios_base::fixed) << ",";
        ofs << candle.low.str(8, std::ios_base::fixed) << ",";
        ofs << candle.close.str(8, std::ios_base::fixed) << ",";
        ofs << candle.volume.str(10, std::ios_base::fixed) << ",";
        ofs << candle.amount.str(10, std::ios_base::fixed) << std::endl;
    }

    ofs.close();
    return true;
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
        ofs << candle.open.str(8, std::ios_base::fixed) << ",";
        ofs << candle.high.str(8, std::ios_base::fixed) << ",";
        ofs << candle.low.str(8, std::ios_base::fixed) << ",";
        ofs << candle.close.str(8, std::ios_base::fixed) << ",";
        ofs << candle.volume.str(10, std::ios_base::fixed) << ",";
        ofs << candle.amount.str(10, std::ios_base::fixed) << std::endl;
    }

    ofs.close();
    return true;
}

bool MEXCFuturesDownloader::P::mergeTempFilesToCSV(const std::string &tempDir, const int batchCount, const std::string &csvPath, const std::string &symbol) {
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
        ofs << "open_time,open,high,low,close,volume,amount" << std::endl;
    }

    // Read temp files in REVERSE order (oldest batch first)
    // Batches are numbered 1, 2, 3... where 1 is newest, N is oldest
    // We need to concatenate N, N-1, ..., 2, 1 to get chronological order
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

int MEXCFuturesDownloader::P::recoverAndMergeTempFiles(const std::string &tempDir, const std::string &csvPath, const std::string &symbol) {
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
        ofs << record.fundingRate << std::endl;
    }

    ofs.close();
    return true;
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
        ofs << record.fundingRate << std::endl;
    }

    ofs.close();
    return true;
}

MEXCFuturesDownloader::MEXCFuturesDownloader(std::uint32_t maxJobs, bool deleteDelistedData) : m_p(std::make_unique<P>(maxJobs, deleteDelistedData)) {}

MEXCFuturesDownloader::~MEXCFuturesDownloader() = default;

void MEXCFuturesDownloader::updateMarketData(const std::string &dirPath, const std::vector<std::string> &symbols, CandleInterval candleInterval,
                                             const onSymbolsToUpdate &onSymbolsToUpdateCB, const onSymbolCompleted &onSymbolCompletedCB, const bool convertToT6) const {
    const auto mexcCandleInterval = P::vkIntervalToMexcInterval(candleInterval);
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;

    std::vector<std::future<std::filesystem::path>> futures;
    const std::filesystem::path finalPath(dirPath);
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
        futures.push_back(std::async(
                std::launch::async,
                [finalPath, this, &mexcCandleInterval, &barSizeInMinutes, &onSymbolCompletedCB](const std::string &symbol, Semaphore &maxJobs) -> std::filesystem::path {
                    std::scoped_lock w(maxJobs);
                    std::filesystem::path symbolFilePathCsv = finalPath;

                    symbolFilePathCsv.append(CSV_FUT_DIR);
                    symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                    symbolFilePathCsv = symbolFilePathCsv.lexically_normal();

                    if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                        throw std::runtime_error(fmt::format("Failed to create {}, err: {}", symbolFilePathCsv.string(), err.message().c_str()));
                    }

                    symbolFilePathCsv.append(symbol + ".csv");

                    // MEXC futures API uses timestamps in SECONDS
                    auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count();

                    // Calculate interval in seconds and round down to last COMPLETED candle
                    // Example for 1h: if now is 10:34, current candle is 10:00, last completed is 09:00
                    // We use this as END time for API (which is inclusive), so 09:00 is the last candle we download
                    const int64_t intervalSeconds = barSizeInMinutes * 60;
                    const int64_t alignedNow = (nowTimestamp / intervalSeconds) * intervalSeconds;
                    nowTimestamp = alignedNow - intervalSeconds;

                    spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                    const int64_t fromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                    // Convert from ms to seconds for MEXC API
                    const int64_t fromTimeSec = fromTimeStamp / 1000;

                    try {
                        // Temp directory for this symbol's batches
                        std::filesystem::path tempDir = symbolFilePathCsv.parent_path();
                        tempDir.append("temp_" + symbol);

                        // RECOVERY: Check for temp files from previous interrupted download
                        // If found, merge them into the main CSV before continuing
                        P::recoverAndMergeTempFiles(tempDir.string(), symbolFilePathCsv.string(), symbol);

                        // Re-check the CSV file for last timestamp (may have changed after recovery)
                        const int64_t recoveredFromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                        const int64_t recoveredFromTimeSec = recoveredFromTimeStamp / 1000;

                        // Determine where to start downloading from
                        int64_t actualFromTimeSec;


                        if (recoveredFromTimeSec > nowTimestamp) {
                            // CSV has incomplete/future candle (from buggy old download)
                            // Start from nowTimestamp to re-download properly
                            spdlog::warn(fmt::format("Symbol {}: CSV has future candle at {}, re-downloading from {}", symbol, recoveredFromTimeSec, nowTimestamp));
                            actualFromTimeSec = fromTimeSec; // Use original fromTimeSec to re-download
                        } else if (recoveredFromTimeSec == nowTimestamp) {
                            // Last CSV candle equals last completed candle, no new data
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
                        } else {
                            // Normal case: download from next candle after last CSV candle
                            actualFromTimeSec = recoveredFromTimeSec + intervalSeconds;

                            // Check if we have an inverted range (no new candles)
                            if (actualFromTimeSec > nowTimestamp) {
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

                        // If actualFromTimeSec == nowTimestamp, we need to extend the range by 1 second
                        // to ensure MEXC API returns the candle (API doesn't handle startTime == endTime well)
                        int64_t apiEndTime = nowTimestamp;
                        if (actualFromTimeSec == nowTimestamp) {
                            apiEndTime = nowTimestamp + 1;
                        }


                        // Create temp directory for new batches
                        if (const auto err = createDirectoryRecursively(tempDir.string())) {
                            throw std::runtime_error(fmt::format("Failed to create temp dir {}, err: {}", tempDir.string(), err.message().c_str()));
                        }

                        // Batch counter - batches arrive in reverse chronological order
                        // Batch 1 = newest, Batch N = oldest
                        int tempFileCounter = 0;
                        int apiBatchCounter = 0;

                        // Accumulate multiple API batches before writing to temp file
                        // This reduces disk I/O while still providing crash recovery
                        constexpr int apiBatchesPerTempFile = 10;
                        std::vector<Candle> accumulatedCandles;

                        // Progressive saving: accumulate batches, write periodically
                        std::ignore = m_p->mexcFuturesClient->getHistoricalPrices(
                                symbol, mexcCandleInterval, actualFromTimeSec, apiEndTime,
                                [&tempDir, &symbol, &tempFileCounter, &apiBatchCounter, &accumulatedCandles, apiBatchesPerTempFile](const std::vector<Candle> &cnd) {
                                    if (!cnd.empty()) {
                                        apiBatchCounter++;
                                        // Prepend new candles (they are older than what we have)
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
            m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string());
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

    if (onSymbolsToUpdateCB) {
        onSymbolsToUpdateCB(symbolsToUpdate);
    }

    // Create funding rate directory
    std::filesystem::path frDir = finalPath;
    frDir.append(CSV_FUT_FR_DIR);

    if (const auto err = createDirectoryRecursively(frDir.string()); err.value() != 0) {
        throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}", frDir.string(), err.value()));
    }

    // Download complete funding rate history for each symbol
    for (const auto &symbol: symbolsToUpdate) {
        spdlog::info(fmt::format("Downloading funding rate history for symbol: {}...", symbol));

        std::filesystem::path symbolFilePathCsv = frDir;
        symbolFilePathCsv.append(symbol + "_fr.csv");

        const int64_t lastTimestamp = P::checkFundingRatesCSVFile(symbolFilePathCsv.string());

        try {
            std::vector<HistoricalFundingRate> newRates;
            int32_t currentPage = 1;
            bool hasMoreData = true;

            // Download all pages until we reach data we already have or no more data
            while (hasMoreData) {
                constexpr int32_t pageSize = 1000;
                auto response = m_p->mexcFuturesClient->getContractFundingRateHistory(symbol, currentPage, pageSize);

                if (response.resultList.empty()) {
                    break;
                }

                // Check if we've reached data we already have
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
                    currentPage++;
                }

                spdlog::info(fmt::format("Symbol {}: downloaded page {}/{}, {} new rates so far", symbol, currentPage - 1, response.totalPage, newRates.size()));
            }

            // Write new rates to CSV (they come in reverse chronological order, newest first)
            if (!newRates.empty()) {
                // Reverse to write in chronological order (oldest first)
                std::ranges::reverse(newRates);

                if (P::writeHistoricalFundingRatesToCSVFile(newRates, symbolFilePathCsv.string())) {
                    spdlog::info(fmt::format("Symbol {}: saved {} funding rates to CSV", symbol, newRates.size()));
                }
            } else {
                spdlog::info(fmt::format("Symbol {}: no new funding rates", symbol));
            }

        } catch (const std::exception &e) {
            spdlog::warn(fmt::format("Failed to download funding rates for symbol: {}, reason: {}", symbol, e.what()));
        }

        if (onSymbolCompletedCB) {
            onSymbolCompletedCB(symbol);
        }
    }

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
        m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string());
    }
}
} // namespace stonky
