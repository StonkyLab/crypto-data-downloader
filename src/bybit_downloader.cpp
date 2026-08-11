/**
Bybit Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/bybit/bybit_downloader.h"
#include "stonky/history_floor.h"
#include "stonky/atomic_file.h"
#include "stonky/csv_data.h"
#include "stonky/csv_format.h"
#include "stonky/download_resume.h"
#include "stonky/downloader.h"
#include "stonky/future_utils.h"
#include "stonky/bybit/bybit_rest_client.h"
#include "stonky/bybit/bybit.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/semaphore.h"
#include "csv.h"
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <spdlog/spdlog.h>
#include <ranges>
#include <regex>
#include <future>
#include <spdlog/fmt/ranges.h>

using namespace stonky::bybit;

namespace stonky {
struct BybitDownloader::P {
    std::unique_ptr<RESTClient> bybitClient;
    mutable Semaphore maxConcurrentConvertJobs;
    mutable std::recursive_mutex locker;
    Semaphore maxConcurrentDownloadJobs;
    MarketCategory marketCategory = MarketCategory::Futures;
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

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData) : bybitClient(std::make_unique<RESTClient>("", "")),
                                              maxConcurrentConvertJobs(normalizedJobCount(maxJobs)),
                                              maxConcurrentDownloadJobs(boundedJobCount(maxJobs, 5)),
                                              deleteDelistedData(deleteDelistedData) {
    }
};

BybitDownloader::BybitDownloader(std::uint32_t maxJobs, const MarketCategory marketCategory, bool deleteDelistedData) : m_p(
    std::make_unique<P>(maxJobs, deleteDelistedData)) {
    m_p->marketCategory = marketCategory;
}

BybitDownloader::~BybitDownloader() = default;

bool BybitDownloader::P::readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles) {
    try {
        io::CSVReader<6> in(path);
        in.read_header(io::ignore_extra_column, "open_time", "open", "high", "low", "close", "volume");

        Candle candle;
        while (in.read_row(candle.startTime, candle.open, candle.high, candle.low, candle.close,
                           candle.volume)) {
            candles.push_back(candle);
        }
    } catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }

    return true;
}

bool BybitDownloader::P::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path,
                                                       const stonky::CandleInterval interval) {
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

void BybitDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
                                            const std::string &outDirPath,
                                            const stonky::CandleInterval interval) const {
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

bool BybitDownloader::P::writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path,
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
        ofs << "open_time,open,high,low,close,volume"
                << std::endl;
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

DownloadResume BybitDownloader::P::checkSymbolCSVFile(const std::string &path) {
    const std::int64_t oldestBybitDate = historyFloor(1420070400000LL); /// Thursday 1. January 2015 0:00:00
    // Self-healing read: a torn tail (interrupted write) is truncated instead of
    // resetting the resume point to oldestBybitDate, which used to silently
    // re-download and append the entire history.
    return downloadResume(CsvData::lastValidRecord(path, 6, oldestBybitDate));
}

DownloadResume BybitDownloader::P::checkFundingRatesCSVFile(const std::string &path) {
    const std::int64_t oldestBybitDate = historyFloor(1420070400000LL); /// Thursday 1. January 2015 0:00:00
    return downloadResume(CsvData::lastValidRecord(path, 2, oldestBybitDate));
}

bool BybitDownloader::P::writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path,
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
        ofs << "funding_time,funding_rate"
                << std::endl;
    }

    for (const auto &record: fr) {
        if (!shouldPersistTimestamp(record.fundingRateTimestamp, resume)) {
            continue;
        }
        ofs << record.fundingRateTimestamp << ",";
        ofs << csvNumber(record.fundingRate) << std::endl;
        resume = {record.fundingRateTimestamp, true};
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

void BybitDownloader::updateMarketData(const std::string &dirPath,
                                       const std::vector<std::string> &symbols,
                                       CandleInterval candleInterval,
                                       const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                       const onSymbolCompleted &onSymbolCompletedCB,
                                       const bool convertToT6) const {
    auto category = Category::linear;
    std::string csvDirName;
    std::string t6DirName;
    std::vector<std::string> symbolsToDelete;

    switch (m_p->marketCategory) {
        case MarketCategory::Spot:
            category = Category::spot;
            csvDirName = CSV_SPOT_DIR;
            t6DirName = T6_SPOT_DIR;
            break;
        case MarketCategory::Futures:
            category = Category::linear;
            csvDirName = CSV_FUT_DIR;
            t6DirName = T6_FUT_DIR;
            break;
    }
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    auto bybitCandleInterval = bybit::CandleInterval::_1;

    if (const auto isOk = Bybit::isValidCandleResolution(barSizeInMinutes, bybitCandleInterval); !isOk) {
        throw std::invalid_argument("invalid Bybit candle resolution: " + std::to_string(barSizeInMinutes) + " m");
    }

    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;

    // Map symbol -> deliveryTime for delisted symbols (used as upper time bound)
    std::map<std::string, int64_t> symbolDeliveryDates;
    // Spot delisted symbols whose deliveryTime must be fetched from public.bybit.com
    std::set<std::string> delistedSpotSymbols;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    std::vector<Instrument> exchangeSymbols = m_p->bybitClient->getInstrumentsInfo(category);
    {
        const auto closedSymbols = m_p->bybitClient->getInstrumentsInfo(category, "", false, "Closed");
        exchangeSymbols.insert(exchangeSymbols.end(), closedSymbols.begin(), closedSymbols.end());
    }

    // Build set of all known symbols from exchange for filesystem-based delisting detection
    std::set<std::string> exchangeSymbolSet;
    for (const auto &el: exchangeSymbols) {
        exchangeSymbolSet.insert(el.symbol);
    }

    if (symbolsToUpdate.empty()) {
        for (const auto &el: exchangeSymbols) {
            if (el.quoteCoin == "USDT" && el.contractType == ContractType::LinearPerpetual) {
                if (el.contractStatus != ContractStatus::Trading && m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(el.symbol);
                } else {
                    symbolsToUpdate.push_back(el.symbol);
                    if (el.contractStatus != ContractStatus::Trading) {
                        if (el.deliveryTime > 0) {
                            symbolDeliveryDates[el.symbol] = el.deliveryTime;
                        } else if (m_p->marketCategory == MarketCategory::Spot) {
                            delistedSpotSymbols.insert(el.symbol);
                        }
                    }
                }
            }
        }

        // Scan existing CSV files for symbols no longer on the exchange
        if (m_p->deleteDelistedData) {
            std::filesystem::path csvDir = finalPath;
            csvDir.append(csvDirName);
            csvDir.append(Downloader::minutesToString(barSizeInMinutes));

            if (std::filesystem::exists(csvDir)) {
                for (const auto &entry: std::filesystem::directory_iterator(csvDir)) {
                    if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                        const auto stem = entry.path().stem().string();
                        if (!exchangeSymbolSet.contains(stem)) {
                            symbolsToDelete.push_back(stem);
                        }
                    }
                }
            }
        }
    } else {
        std::vector<std::string> tempSymbols;

        for (const auto &sym: symbolsToUpdate) {
            auto it = std::ranges::find_if(exchangeSymbols, [sym](const Instrument &i) {
                return i.symbol == sym;
            });

            if (it == exchangeSymbols.end()) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(sym);
                } else if (m_p->marketCategory == MarketCategory::Spot) {
                    // Spot: status=Closed bulk call may not return all delisted symbols;
                    // attempt download anyway since kline API still serves historical data
                    tempSymbols.push_back(sym);
                    delistedSpotSymbols.insert(sym);
                }
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", sym));
            } else if (it->contractStatus != ContractStatus::Trading) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(sym);
                } else {
                    tempSymbols.push_back(it->symbol);
                    if (it->deliveryTime > 0) {
                        symbolDeliveryDates[it->symbol] = it->deliveryTime;
                    } else if (m_p->marketCategory == MarketCategory::Spot) {
                        delistedSpotSymbols.insert(it->symbol);
                    }
                }
            } else {
                tempSymbols.push_back(it->symbol);
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
                       [finalPath, this, &bybitCandleInterval, &barSizeInMinutes, &category, &csvDirName, &t6DirName, convertToT6, &symbolDeliveryDates, &delistedSpotSymbols](
                   const std::string &symbol) -> std::filesystem::path {
                           std::filesystem::path symbolFilePathCsv = finalPath;
                           std::filesystem::path symbolFilePathT6 = finalPath;

                           symbolFilePathCsv.append(csvDirName);
                           symbolFilePathT6.append(t6DirName);

                           symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                           symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes));
                           {
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

                           // For delisted symbols, use deliveryTime as upper time bound
                           auto endTimestamp = nowTimestamp;
                           if (const auto dit = symbolDeliveryDates.find(symbol); dit != symbolDeliveryDates.end()) {
                               endTimestamp = dit->second;
                           } else if (delistedSpotSymbols.contains(symbol)) {
                               // Spot delisted: fetch the actual last trade timestamp from the daily gz file.
                               // Round up to the next candle boundary so the pop_back condition in
                               // getHistoricalPrices does not remove the last complete candle.
                               if (const auto ts = m_p->bybitClient->fetchLastTimestampForDelistedSpotSymbol(symbol); ts > 0) {
                                   const int64_t intervalMs = static_cast<int64_t>(barSizeInMinutes) * 60000LL;
                                   endTimestamp = ((ts / intervalMs) + 1) * intervalMs;
                               }
                           }

                           spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                           {
                               const auto initialResume = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                               if (std::to_string(initialResume.timestamp).length() < 13) {
                                   throw std::runtime_error(fmt::format(
                                       "Old data format for symbol {}, delete file {} before retrying",
                                       symbol, symbolFilePathCsv.string()));
                               }
                           }

                           auto isRateLimitError = [](const std::string &msg) {
                               return msg.find("10006") != std::string::npos ||
                                      msg.find("too many") != std::string::npos ||
                                      msg.find("429") != std::string::npos;
                           };
                           constexpr int maxRetries = 5;
                           for (int attempt = 0; attempt < maxRetries; ++attempt) {
                               // Re-read CSV state at start of each attempt — a previous attempt
                               // may have written batches to disk before hitting a 429.
                               const auto resume = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                               try {
                                   std::ignore = m_p->bybitClient->getHistoricalPrices(category,
                                       symbol,
                                       bybitCandleInterval,
                                       resume.timestamp,
                                       endTimestamp, 200, [symbolFilePathCsv, symbol](const std::vector<Candle> &cnd) {
                                           if (!cnd.empty()) {
                                               const auto persisted = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                                               if (!P::writeCandlesToCSVFile(cnd, symbolFilePathCsv.string(), persisted)) {
                                                   // Abort pagination — continuing after a failed batch write
                                                   // would leave a permanent gap inside the CSV.
                                                   throw std::runtime_error(fmt::format("CSV write failed for symbol: {}", symbol));
                                               }
                                           }
                                       });

                                   spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                   return symbolFilePathCsv;
                               } catch (const std::exception &e) {
                                   const std::string errMsg = e.what();
                                   if (isRateLimitError(errMsg) && attempt < maxRetries - 1) {
                                       const int waitMs = 1000 * (1 << attempt);
                                       spdlog::warn(fmt::format("Rate limit for symbol: {}, retry {}/{} in {} ms: {}",
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

    std::filesystem::path T6Directory = finalPath;

    T6Directory.append(t6DirName);
    T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    if (convertToT6) {
        // Scan CSV directory for ALL .csv files — not just those from the current
        // download batch — so that symbols whose API call failed this run but whose
        // CSV exists from a previous run still get converted.
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
                throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(), err.message().c_str()));
            }
            spdlog::info(fmt::format("Converting from csv to t6..."));
            m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string(), candleInterval);
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
                spdlog::info(fmt::format("Removing csv file for delisted symbol: {}, file: {}...", symbol, symbolFilePathCsv.string()));
            }

            if (std::filesystem::exists(symbolFilePathT6)) {
                std::filesystem::remove(symbolFilePathT6);
                spdlog::info(fmt::format("Removing t6 file for delisted symbol: {}, file: {}...", symbol, symbolFilePathT6.string()));
            }
        }
    }
}

void BybitDownloader::updateMarketData(const std::string &connectionString,
                                       const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                       const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: BybitDownloader::updateMarketData");
}

void BybitDownloader::updateFundingRateData(const std::string &dirPath,
                                            const std::vector<std::string> &symbols,
                                            const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                            const onSymbolCompleted &onSymbolCompletedCB) const {
    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    // Map symbol -> deliveryTime for delisted symbols (used as upper time bound)
    std::map<std::string, int64_t> symbolDeliveryDates;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    auto instrumentsInfo = m_p->bybitClient->getInstrumentsInfo(Category::linear);
    {
        const auto closedSymbols = m_p->bybitClient->getInstrumentsInfo(Category::linear, "", false, "Closed");
        instrumentsInfo.insert(instrumentsInfo.end(), closedSymbols.begin(), closedSymbols.end());
    }

    // Build set of all known symbols from exchange for filesystem-based delisting detection
    std::set<std::string> exchangeSymbolSet;
    for (const auto &el: instrumentsInfo) {
        exchangeSymbolSet.insert(el.symbol);
    }

    if (symbolsToUpdate.empty()) {
        constexpr auto symbolContract = ContractType::LinearPerpetual;

        for (const auto &el: instrumentsInfo) {
            if (el.contractType == symbolContract && el.quoteCoin == "USDT") {
                if (el.contractStatus != ContractStatus::Trading && m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(el.symbol);
                } else {
                    symbolsToUpdate.push_back(el.symbol);
                    if (el.contractStatus != ContractStatus::Trading && el.deliveryTime > 0) {
                        symbolDeliveryDates[el.symbol] = el.deliveryTime;
                    }
                }
            }
        }

        // Scan existing CSV files for symbols no longer on the exchange
        if (m_p->deleteDelistedData) {
            std::filesystem::path frDir = finalPath;
            frDir.append(CSV_FUT_FR_DIR);

            if (std::filesystem::exists(frDir)) {
                for (const auto &entry: std::filesystem::directory_iterator(frDir)) {
                    if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                        auto stem = entry.path().stem().string();
                        // Remove _fr suffix to get symbol name
                        if (stem.ends_with("_fr")) {
                            stem = stem.substr(0, stem.size() - 3);
                        }
                        if (!exchangeSymbolSet.contains(stem)) {
                            symbolsToDelete.push_back(stem);
                        }
                    }
                }
            }
        }
    } else {
        std::vector<std::string> tempSymbols;

        for (const auto &symbol: symbolsToUpdate) {
            auto it = std::ranges::find_if(instrumentsInfo, [symbol](const Instrument &i) {
                return i.symbol == symbol;
            });

            if (it == instrumentsInfo.end()) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                }
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", symbol));
            } else if (it->contractStatus != ContractStatus::Trading) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                } else {
                    tempSymbols.push_back(it->symbol);
                    if (it->deliveryTime > 0) {
                        symbolDeliveryDates[it->symbol] = it->deliveryTime;
                    }
                }
            } else {
                tempSymbols.push_back(it->symbol);
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
                       [finalPath, this, &symbolDeliveryDates](const std::string &symbol) -> std::filesystem::path {
                           std::filesystem::path symbolFilePathCsv = finalPath;

                           symbolFilePathCsv.append(CSV_FUT_FR_DIR);

                           if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string());
                               err.value() != 0) {
                               throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}",
                                                                    symbolFilePathCsv.string(), err.value()));
                           }

                           symbolFilePathCsv.append(symbol + "_fr.csv");

                           const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           // For delisted symbols, use deliveryTime as upper time bound
                           auto endTimestamp = nowTimestamp;
                           if (const auto dit = symbolDeliveryDates.find(symbol); dit != symbolDeliveryDates.end()) {
                               endTimestamp = dit->second;
                           }

                           spdlog::info(fmt::format("Updating FR for symbol: {}...", symbol));

                           auto isRateLimitError = [](const std::string &msg) {
                               return msg.find("10006") != std::string::npos ||
                                      msg.find("too many") != std::string::npos ||
                                      msg.find("429") != std::string::npos;
                           };
                           constexpr int maxRetries = 5;
                           for (int attempt = 0; attempt < maxRetries; ++attempt) {
                               // Re-read CSV state at start of each attempt — a previous attempt
                               // may have written rows to disk before hitting a 429.
                               const auto resume = P::checkFundingRatesCSVFile(symbolFilePathCsv.string());
                               try {
                                   if (const auto fr = m_p->bybitClient->getFundingRates(
                                       Category::linear, symbol, requestStartTimestamp(resume), endTimestamp); !fr.empty()) {
                                       if (P::writeFundingRatesToCSVFile(fr, symbolFilePathCsv.string(), resume)) {
                                           spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                           return symbolFilePathCsv;
                                       }
                                       throw std::runtime_error(fmt::format(
                                           "CSV funding-rate write failed for symbol {}", symbol));
                                   } else {
                                       break;
                                   }
                               } catch (const std::exception &e) {
                                   const std::string errMsg = e.what();
                                   if (isRateLimitError(errMsg) && attempt < maxRetries - 1) {
                                       const int waitMs = 1000 * (1 << attempt);
                                       spdlog::warn(fmt::format("Rate limit for symbol: {}, retry {}/{} in {} ms: {}",
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
                spdlog::info(fmt::format("Removing csv file for delisted symbol: {}, file: {}...", symbol, symbolFilePathCsv.string()));
            }
        }
    }
}

void BybitDownloader::convertToT6(const std::string &dirPath, const CandleInterval candleInterval) const {
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    auto bybitCandleInterval = bybit::CandleInterval::_1;
    if (!Bybit::isValidCandleResolution(barSizeInMinutes, bybitCandleInterval)) {
        throw std::invalid_argument("invalid Bybit candle resolution: " + std::to_string(barSizeInMinutes) + " m");
    }
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
            throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(), err.message().c_str()));
        }
        spdlog::info(fmt::format("Converting from csv to t6..."));
        m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string(), candleInterval);
    }
}
}
