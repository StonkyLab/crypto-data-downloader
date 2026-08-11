/**
Hyperliquid Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/hyperliquid/hyperliquid_downloader.h"
#include "stonky/history_floor.h"
#include "stonky/atomic_file.h"
#include "stonky/csv_data.h"
#include "stonky/csv_format.h"
#include "stonky/download_resume.h"
#include "stonky/downloader.h"
#include "stonky/future_utils.h"
#include "stonky/hyperliquid/hyperliquid_rest_client.h"
#include "stonky/hyperliquid/hyperliquid.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/semaphore.h"
#include "csv.h"
#include <filesystem>
#include <fstream>
#include <set>
#include <spdlog/spdlog.h>
#include <ranges>
#include <future>
#include <spdlog/fmt/ranges.h>

using namespace stonky::hyperliquid;

namespace stonky {
struct HyperliquidDownloader::P {
    std::unique_ptr<RESTClient> hlClient;
    mutable Semaphore maxConcurrentConvertJobs;
    mutable std::recursive_mutex locker;
    Semaphore maxConcurrentDownloadJobs;
    bool deleteDelistedData = false;

    static bool writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path,
                                             stonky::CandleInterval interval);

    static DownloadResume checkSymbolCSVFile(const std::string &path);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path,
                                      DownloadResume resume);

    static bool readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles);

    void convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths, const std::string &outDirPath,
                            stonky::CandleInterval interval) const;

    static DownloadResume checkFundingRatesCSVFile(const std::string &path);

    static bool writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path,
                                           DownloadResume resume);

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData)
        : hlClient(std::make_unique<RESTClient>()),
          maxConcurrentConvertJobs(normalizedJobCount(maxJobs)),
          maxConcurrentDownloadJobs(boundedJobCount(maxJobs, 1)),
          deleteDelistedData(deleteDelistedData) {
    }
};

HyperliquidDownloader::HyperliquidDownloader(std::uint32_t maxJobs, bool deleteDelistedData)
    : m_p(std::make_unique<P>(maxJobs, deleteDelistedData)) {
}

HyperliquidDownloader::~HyperliquidDownloader() = default;

bool HyperliquidDownloader::P::readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles) {
    try {
        io::CSVReader<6> in(path);
        in.read_header(io::ignore_extra_column, "open_time", "open", "high", "low", "close", "volume");

        Candle candle;
        while (in.read_row(candle.startTime, candle.open, candle.high, candle.low, candle.close, candle.volume)) {
            candles.push_back(candle);
        }
    } catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }
    return true;
}

bool HyperliquidDownloader::P::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path,
                                                            stonky::CandleInterval interval) {
    const std::filesystem::path pathToT6File{t6Path};

    AtomicFileWriter output(pathToT6File);
    if (!output.isOpen()) {
        spdlog::error(fmt::format("Couldn't prepare file {}: {}", t6Path, output.error()));
        return false;
    }
    auto &ofs = output.stream();

    std::vector<Candle> candles;
    if (!readCandlesFromCSVFile(csvPath, candles) || candles.empty()) {
        spdlog::error(fmt::format("Couldn't read candles from csv file: {}", csvPath));
        return false;
    }

    for (const auto &candle: std::ranges::reverse_view(candles)) {
        T6 t6;
        t6.fOpen = static_cast<float>(candle.open);
        t6.fHigh = static_cast<float>(candle.high);
        t6.fLow = static_cast<float>(candle.low);
        t6.fClose = static_cast<float>(candle.close);
        t6.fVal = 0.0;
        t6.fVol = static_cast<float>(candle.volume);
        t6.time = convertTimeMs(Downloader::candleCloseTimestampMs(candle.startTime, interval));
        ofs.write(reinterpret_cast<char *>(&t6), sizeof(T6));
    }

    std::string error;
    if (!output.commit(error)) {
        spdlog::error(fmt::format("Couldn't commit T6 file {}: {}", t6Path, error));
        return false;
    }
    return true;
}

void HyperliquidDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
                                                   const std::string &outDirPath,
                                                   stonky::CandleInterval interval) const {
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
            launchBounded(maxConcurrentConvertJobs,
                       [interval](const std::filesystem::path &csvPath, const std::filesystem::path &t6Path) -> std::pair<std::string, bool> {
                           std::pair<std::string, bool> retVal;
                           retVal.first = csvPath.filename().replace_extension("").string();
                           retVal.second = writeCSVCandlesToZorroT6File(csvPath.string(), t6Path.string(), interval);
                           return retVal;
                       }, path, t6FilePath));
    }

    readyFutures = waitAllOrThrow(futures);
    for (const auto &[symbol, converted]: readyFutures) {
        if (converted) {
            spdlog::info(fmt::format("Symbol: {} converted", symbol));
        } else {
            throw std::runtime_error(fmt::format("Symbol: {} conversion failed", symbol));
        }
    }
}

bool HyperliquidDownloader::P::writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path,
                                                     DownloadResume resume) {
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
        ofs << "open_time,open,high,low,close,volume" << std::endl;
    }

    for (const auto &candle: candles) {
        if (!shouldPersistTimestamp(candle.startTime, resume)) {
            continue;
        }
        ofs << candle.startTime << ",";
        ofs << csvNumber(candle.open) << ",";
        ofs << csvNumber(candle.high) << ",";
        ofs << csvNumber(candle.low) << ",";
        ofs << csvNumber(candle.close) << ",";
        ofs << csvNumber(candle.volume) << std::endl;
        resume = {candle.startTime, true};
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Write to file failed (disk full?): {}", path));
        ofs.close();
        return false;
    }
    ofs.close();
    return ofs.good();
}

DownloadResume HyperliquidDownloader::P::checkSymbolCSVFile(const std::string &path) {
    const std::int64_t oldestHyperliquidDate = historyFloor(1672531200000LL); /// Sunday 1. January 2023 0:00:00
    // Self-healing read: a torn tail (interrupted write) is truncated instead of
    // resetting the resume point to the oldest-date sentinel.
    return downloadResume(CsvData::lastValidRecord(path, 6, oldestHyperliquidDate));
}

DownloadResume HyperliquidDownloader::P::checkFundingRatesCSVFile(const std::string &path) {
    const std::int64_t oldestHyperliquidDate = historyFloor(1672531200000LL); /// Sunday 1. January 2023 0:00:00
    return downloadResume(CsvData::lastValidRecord(path, 2, oldestHyperliquidDate));
}

bool HyperliquidDownloader::P::writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr,
                                                          const std::string &path,
                                                          DownloadResume resume) {
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
        if (!shouldPersistTimestamp(record.time, resume)) {
            continue;
        }
        ofs << record.time << ",";
        ofs << csvNumber(record.fundingRate) << std::endl;
        resume = {record.time, true};
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Write to file failed (disk full?): {}", path));
        ofs.close();
        return false;
    }
    ofs.close();
    return ofs.good();
}

void HyperliquidDownloader::updateMarketData(const std::string &dirPath,
                                             const std::vector<std::string> &symbols,
                                             CandleInterval candleInterval,
                                             const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                             const onSymbolCompleted &onSymbolCompletedCB,
                                             const bool convertToT6) const {
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    hyperliquid::CandleInterval hlInterval;

    if (!Hyperliquid::isValidCandleResolution(barSizeInMinutes, hlInterval)) {
        throw std::invalid_argument("invalid Hyperliquid candle resolution: " + std::to_string(barSizeInMinutes) + " m");
    }

    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::string> symbolsToDelete;
    std::vector<std::filesystem::path> csvFilePaths;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info("Updating all symbols");
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto allAssets = m_p->hlClient->getPerpetualAssets(true);

    std::set<std::string> knownNames;
    for (const auto &a: allAssets) {
        knownNames.insert(a.name);
    }

    if (symbolsToUpdate.empty()) {
        for (const auto &a: allAssets) {
            if (a.isDelisted && m_p->deleteDelistedData) {
                symbolsToDelete.push_back(a.name);
            } else {
                symbolsToUpdate.push_back(a.name);
            }
        }

        if (m_p->deleteDelistedData) {
            std::filesystem::path csvDir = finalPath;
            csvDir.append(CSV_FUT_DIR);
            csvDir.append(Downloader::minutesToString(barSizeInMinutes));

            if (std::filesystem::exists(csvDir)) {
                for (const auto &entry: std::filesystem::directory_iterator(csvDir)) {
                    if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                        const auto stem = entry.path().stem().string();
                        if (!knownNames.contains(stem)) {
                            symbolsToDelete.push_back(stem);
                        }
                    }
                }
            }
        }
    } else {
        std::vector<std::string> tempSymbols;
        for (const auto &sym: symbolsToUpdate) {
            auto it = std::ranges::find_if(allAssets, [sym](const PerpAsset &a) {
                return a.name == sym;
            });

            if (it == allAssets.end()) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(sym);
                }
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", sym));
            } else if (it->isDelisted) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(sym);
                } else {
                    tempSymbols.push_back(sym);
                }
            } else {
                tempSymbols.push_back(sym);
            }
        }
        symbolsToUpdate = tempSymbols;
    }

    deduplicatePreserveOrder(symbolsToUpdate);
    removeUnsafeSymbolFileComponents(symbolsToUpdate);
    if (onSymbolsToUpdateCB) {
        onSymbolsToUpdateCB(symbolsToUpdate);
    }

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            launchBounded(m_p->maxConcurrentDownloadJobs,
                       [finalPath, this, &hlInterval, &barSizeInMinutes, convertToT6](
                   const std::string &symbol) -> std::filesystem::path {
                           std::filesystem::path symbolFilePathCsv = finalPath;
                           std::filesystem::path symbolFilePathT6 = finalPath;

                           symbolFilePathCsv.append(CSV_FUT_DIR);
                           symbolFilePathT6.append(T6_FUT_DIR);

                           symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                           symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes));

                           if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                               throw std::runtime_error(fmt::format("Failed to create {}, err: {}",
                                                                    symbolFilePathCsv.string(),
                                                                    err.message().c_str()));
                           }
                           if (convertToT6) {
                               if (const auto err = createDirectoryRecursively(symbolFilePathT6.string())) {
                                   throw std::runtime_error(fmt::format("Failed to create {}, err: {}",
                                                                        symbolFilePathT6.string(),
                                                                        err.message().c_str()));
                               }
                           }

                           symbolFilePathCsv.append(symbol + ".csv");
                           symbolFilePathT6.append(symbol + ".t6");

                           const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                           auto isRateLimitError = [](const std::string &msg) {
                               return msg.find("too many") != std::string::npos ||
                                      msg.find("429") != std::string::npos ||
                                      msg.find("rate limit") != std::string::npos;
                           };
                           constexpr int maxRetries = 5;
                           for (int attempt = 0; attempt < maxRetries; ++attempt) {
                               // Re-read CSV state at start of each attempt — a previous attempt
                               // may have written batches to disk before hitting a 429.
                               const auto resume = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                               const int64_t retentionStart =
                                   nowTimestamp - static_cast<int64_t>(barSizeInMinutes) * 5000LL * 60000;
                               const int64_t effectiveFrom = freshDownloadStart(resume, retentionStart);
                               try {
                                   std::ignore = m_p->hlClient->getHistoricalPrices(
                                       symbol, hlInterval, effectiveFrom, nowTimestamp,
                                       [symbolFilePathCsv, symbol](
                                       const std::vector<hyperliquid::Candle> &cnd) {
                                           // Zero-volume bars are stored as-is: the exchange serves a
                                           // continuous series and no-trade bars are needed downstream
                                           // (MTM, funding, SL triggers). Note: bars before real trading
                                           // began (pre-March-2023 era) also carry v=0 oracle prices —
                                           // excluding those is an upstream universe-selection concern.
                                           if (!cnd.empty()) {
                                               const auto persisted = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                                               if (!P::writeCandlesToCSVFile(cnd, symbolFilePathCsv.string(),
                                                                             persisted)) {
                                                   // Abort pagination — continuing after a failed batch write
                                                   // would leave a permanent gap inside the CSV.
                                                   throw std::runtime_error(
                                                       fmt::format("CSV write failed for symbol: {}", symbol));
                                               }
                                           }
                                       });
                                   spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                   return symbolFilePathCsv;
                               } catch (const std::exception &e) {
                                   const std::string errMsg = e.what();
                                   if (isRateLimitError(errMsg) && attempt < maxRetries - 1) {
                                       const int waitMs = 1000 * (1 << attempt);
                                       spdlog::warn(fmt::format(
                                           "Rate limit for symbol: {}, retry {}/{} in {} ms: {}",
                                           symbol, attempt + 1, maxRetries - 1, waitMs, errMsg));
                                       std::this_thread::sleep_for(std::chrono::milliseconds(waitMs));
                                   } else {
                                       throw std::runtime_error(fmt::format(
                                           "Updating candles for symbol {} failed (attempt {}/{}): {}",
                                           symbol, attempt + 1, maxRetries, errMsg));
                                   }
                               }
                           }
                           return "";
                       }, s));
    }

    csvFilePaths = waitAllOrThrow(futures, [&onSymbolCompletedCB](const std::filesystem::path &path) {
        if (onSymbolCompletedCB && !path.empty()) {
            onSymbolCompletedCB(path.stem().string());
        }
    });

    if (convertToT6) {
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
                throw std::runtime_error(
                    fmt::format("Failed to create {}, err: {}", T6Directory.string(), err.message().c_str()));
            }
            spdlog::info("Converting from csv to t6...");
            m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string(), candleInterval);
        }
    }

    if (m_p->deleteDelistedData) {
        for (const auto &symbol: symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = finalPath;
            std::filesystem::path symbolFilePathT6 = finalPath;

            symbolFilePathCsv.append(CSV_FUT_DIR);
            symbolFilePathT6.append(T6_FUT_DIR);

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

void HyperliquidDownloader::updateMarketData(const std::string &connectionString,
                                             const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                             const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: HyperliquidDownloader::updateMarketData");
}

void HyperliquidDownloader::updateFundingRateData(const std::string &dirPath,
                                                  const std::vector<std::string> &symbols,
                                                  const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                                  const onSymbolCompleted &onSymbolCompletedCB) const {
    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info("Updating all symbols");
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto allAssets = m_p->hlClient->getPerpetualAssets(true);

    std::set<std::string> knownNames;
    for (const auto &a: allAssets) {
        knownNames.insert(a.name);
    }

    if (symbolsToUpdate.empty()) {
        for (const auto &a: allAssets) {
            if (a.isDelisted && m_p->deleteDelistedData) {
                symbolsToDelete.push_back(a.name);
            } else {
                symbolsToUpdate.push_back(a.name);
            }
        }

        if (m_p->deleteDelistedData) {
            std::filesystem::path frDir = finalPath;
            frDir.append(CSV_FUT_FR_DIR);

            if (std::filesystem::exists(frDir)) {
                for (const auto &entry: std::filesystem::directory_iterator(frDir)) {
                    if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                        auto stem = entry.path().stem().string();
                        if (stem.ends_with("_fr")) {
                            stem = stem.substr(0, stem.size() - 3);
                        }
                        if (!knownNames.contains(stem)) {
                            symbolsToDelete.push_back(stem);
                        }
                    }
                }
            }
        }
    } else {
        std::vector<std::string> tempSymbols;
        for (const auto &sym: symbolsToUpdate) {
            auto it = std::ranges::find_if(allAssets, [sym](const PerpAsset &a) {
                return a.name == sym;
            });

            if (it == allAssets.end()) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(sym);
                }
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", sym));
            } else if (it->isDelisted) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(sym);
                } else {
                    tempSymbols.push_back(sym);
                }
            } else {
                tempSymbols.push_back(sym);
            }
        }
        symbolsToUpdate = tempSymbols;
    }

    deduplicatePreserveOrder(symbolsToUpdate);
    removeUnsafeSymbolFileComponents(symbolsToUpdate);
    if (onSymbolsToUpdateCB) {
        onSymbolsToUpdateCB(symbolsToUpdate);
    }

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            launchBounded(m_p->maxConcurrentDownloadJobs,
                       [finalPath, this](const std::string &symbol) -> std::filesystem::path {
                           std::filesystem::path symbolFilePathCsv = finalPath;

                           symbolFilePathCsv.append(CSV_FUT_FR_DIR);

                           if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string());
                               err.value() != 0) {
                               throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}",
                                                                    symbolFilePathCsv.string(), err.value()));
                           }

                           symbolFilePathCsv.append(symbol + "_fr.csv");

                           const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           spdlog::info(fmt::format("Updating FR for symbol: {}...", symbol));

                           auto isRateLimitError = [](const std::string &msg) {
                               return msg.find("too many") != std::string::npos ||
                                      msg.find("429") != std::string::npos ||
                                      msg.find("rate limit") != std::string::npos;
                           };
                           constexpr int maxRetries = 5;
                           for (int attempt = 0; attempt < maxRetries; ++attempt) {
                               // Re-read CSV state at start of each attempt — a previous attempt
                               // may have written rows to disk before hitting a 429.
                               const auto resume = P::checkFundingRatesCSVFile(symbolFilePathCsv.string());
                               try {
                                   const auto fr = m_p->hlClient->getFundingRates(symbol, requestStartTimestamp(resume),
                                                                                   nowTimestamp);
                                   if (!fr.empty()) {
                                       if (P::writeFundingRatesToCSVFile(fr, symbolFilePathCsv.string(), resume)) {
                                           spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                           return symbolFilePathCsv;
                                       }
                                       throw std::runtime_error(fmt::format(
                                           "CSV funding-rate write failed for symbol {}", symbol));
                                   }
                                   break;
                               } catch (const std::exception &e) {
                                   const std::string errMsg = e.what();
                                   if (isRateLimitError(errMsg) && attempt < maxRetries - 1) {
                                       const int waitMs = 1000 * (1 << attempt);
                                       spdlog::warn(fmt::format(
                                           "Rate limit for symbol: {}, retry {}/{} in {} ms: {}",
                                           symbol, attempt + 1, maxRetries - 1, waitMs, errMsg));
                                       std::this_thread::sleep_for(std::chrono::milliseconds(waitMs));
                                   } else {
                                       throw std::runtime_error(fmt::format(
                                           "Updating funding rates for symbol {} failed (attempt {}/{}): {}",
                                           symbol, attempt + 1, maxRetries, errMsg));
                                   }
                               }
                           }
                           return "";
                       }, s));
    }

    csvFilePaths = waitAllOrThrow(futures, [&onSymbolCompletedCB](const std::filesystem::path &path) {
        if (onSymbolCompletedCB && !path.empty()) {
            auto symbol = path.stem().string();
            if (symbol.ends_with("_fr")) {
                symbol.resize(symbol.size() - 3);
            }
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
                spdlog::info(fmt::format("Removing csv file for delisted symbol: {}, file: {}...", symbol,
                                         symbolFilePathCsv.string()));
            }
        }
    }
}

void HyperliquidDownloader::convertToT6(const std::string &dirPath, const CandleInterval candleInterval) const {
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    hyperliquid::CandleInterval hlInterval;

    if (!Hyperliquid::isValidCandleResolution(barSizeInMinutes, hlInterval)) {
        throw std::invalid_argument("invalid Hyperliquid candle resolution: " + std::to_string(barSizeInMinutes) + " m");
    }

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
            throw std::runtime_error(
                fmt::format("Failed to create {}, err: {}", T6Directory.string(), err.message().c_str()));
        }
        spdlog::info("Converting from csv to t6...");
        m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string(), candleInterval);
    }
}
}
