/**
Binance Futures Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/binance/binance_futures_downloader.h"
#include "stonky/binance/binance_futures_rest_client.h"
#include "stonky/binance/binance_common.h"
#include "stonky/csv_data.h"
#include "stonky/downloader.h"
#include "stonky/utils/semaphore.h"
#include "stonky/utils/utils.h"
#include "stonky/binance/binance.h"
#include "stonky/interface/exchange_enums.h"
#include "stonky/utils/magic_enum_wrapper.hpp"
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <spdlog/spdlog.h>
#include <regex>
#include <future>
#include <spdlog/fmt/ranges.h>

using namespace stonky::binance;

namespace stonky {
struct BinanceFuturesDownloader::P {
    std::unique_ptr<futures::RESTClient> bnbFuturesClient;
    std::unique_ptr<BinanceCommon> binanceCommon;
    mutable Semaphore maxConcurrentConvertJobs;
    Semaphore maxConcurrentDownloadJobs{3};
    bool deleteDelistedData = false;

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData) : bnbFuturesClient(
                                                                                 std::make_unique<futures::RESTClient>(
                                                                                     "", "")),
                                                                             binanceCommon(
                                                                                 std::make_unique<BinanceCommon>(
                                                                                     maxJobs)),
                                                                             maxConcurrentConvertJobs(maxJobs),
                                                                             deleteDelistedData(deleteDelistedData) {
    }

    static int64_t checkFundingRatesCSVFile(const std::string &path);

    static bool writeFundingRatesToCSVFile(const std::vector<futures::FundingRate> &fr, const std::string &path,
                                           std::int64_t lastTs);
};

BinanceFuturesDownloader::BinanceFuturesDownloader(std::uint32_t maxJobs, bool deleteDelistedData) : m_p(
    std::make_unique<P>(maxJobs, deleteDelistedData)) {
}

BinanceFuturesDownloader::~BinanceFuturesDownloader() = default;

int64_t BinanceFuturesDownloader::P::checkFundingRatesCSVFile(const std::string &path) {
    constexpr int64_t oldestBNBDate = 1420070400000; /// Thursday 1. January 2015 0:00:00
    // Self-healing read: a torn tail (interrupted write) is truncated instead of
    // resetting the resume point to oldestBNBDate.
    return CsvData::lastValidRecord(path, 2, oldestBNBDate).timestamp;
}

bool BinanceFuturesDownloader::P::writeFundingRatesToCSVFile(const std::vector<futures::FundingRate> &fr,
                                                             const std::string &path, const std::int64_t lastTs) {
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

    // Monotonic filter: skips both the inclusive resume-boundary record and
    // page-boundary duplicates the client merges into one vector (records
    // repeated at 1000-row fetch boundaries).
    std::int64_t prevTs = lastTs;
    for (const auto &record: fr) {
        if (record.fundingTime <= prevTs) {
            continue;
        }
        ofs << record.fundingTime << ",";
        ofs << record.fundingRate << std::endl;
        prevTs = record.fundingTime;
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Write to file failed (disk full?): {}", path));
        ofs.close();
        return false;
    }
    ofs.close();

    return true;
}

void BinanceFuturesDownloader::updateMarketData(const std::string &dirPath, const std::vector<std::string> &symbols,
                                                CandleInterval candleInterval,
                                                const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                                const onSymbolCompleted &onSymbolCompletedCB,
                                                const bool convertToT6) const {
    auto bnbCandleInterval = binance::CandleInterval::_1m;
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;

    if (const auto isOk = binance::Binance::isValidCandleResolution(barSizeInMinutes, bnbCandleInterval); !isOk) {
        throw std::invalid_argument("invalid Binance candle resolution: " + std::to_string(barSizeInMinutes) + " m");
    }

    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    // Map symbol -> deliveryDate for delisted symbols (used as upper time bound)
    std::map<std::string, int64_t> symbolDeliveryDates;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInfo = m_p->bnbFuturesClient->getExchangeInfo();

    for (const auto &limit: exchangeInfo.rateLimits) {
        if (limit.rateLimitType == RateLimitType::REQUEST_WEIGHT && limit.intervalNum == 1 &&
            limit.interval == RateLimitInterval::MINUTE) {
            spdlog::info(fmt::format("Weight limit: {}", limit.limit));
            m_p->bnbFuturesClient->setAPIWeightLimit(limit.limit);
        }
    }

    // Build set of all known symbols from exchange for filesystem-based delisting detection
    std::set<std::string> exchangeSymbolSet;
    for (const auto &el: exchangeInfo.symbols) {
        exchangeSymbolSet.insert(el.symbol);
    }

    if (symbolsToUpdate.empty()) {
        constexpr auto symbolContract = futures::ContractType::PERPETUAL;
        const auto symbolType = std::string(magic_enum::enum_name(symbolContract));

        for (const auto &el: exchangeInfo.symbols) {
            if (el.contractType == symbolType && el.quoteAsset == "USDT") {
                if (el.status != ContractStatus::TRADING && m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(el.symbol);
                } else {
                    symbolsToUpdate.push_back(el.symbol);
                    if (el.status != ContractStatus::TRADING && el.deliveryDate > 0) {
                        symbolDeliveryDates[el.symbol] = el.deliveryDate;
                    }
                }
            }
        }

        // Scan existing CSV files for symbols no longer on the exchange
        if (m_p->deleteDelistedData) {
            std::filesystem::path csvDir = finalPath;
            csvDir.append(CSV_FUT_DIR);
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

        for (const auto &symbol: symbolsToUpdate) {
            auto it = std::ranges::find_if(exchangeInfo.symbols, [symbol](const futures::Symbol &s) {
                return s.symbol == symbol;
            });

            if (it == exchangeInfo.symbols.end()) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                } else {
                    tempSymbols.push_back(symbol);
                }
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", symbol));
            } else if (it->status != ContractStatus::TRADING) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                    spdlog::info(fmt::format("Symbol: {} is not trading (status: {}), will be deleted", symbol,
                                             magic_enum::enum_name(it->status)));
                } else {
                    tempSymbols.push_back(it->symbol);
                    if (it->deliveryDate > 0) {
                        symbolDeliveryDates[it->symbol] = it->deliveryDate;
                    }
                }
            } else {
                tempSymbols.push_back(it->symbol);
            }
        }

        symbolsToUpdate = tempSymbols;
    }

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this, &bnbCandleInterval, &barSizeInMinutes, convertToT6, &symbolDeliveryDates](
                   const std::string &symbol,
                   Semaphore &maxJobs) ->
                   std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;
                           std::filesystem::path symbolFilePathT6 = finalPath;

                           symbolFilePathCsv.append(CSV_FUT_DIR);
                           symbolFilePathT6.append(T6_FUT_DIR);

                           symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                           symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes));

                           symbolFilePathCsv = symbolFilePathCsv.lexically_normal();
                           symbolFilePathT6 = symbolFilePathT6.lexically_normal(); {
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

                           auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           // Do not download the last minute - it is not complete yet.
                           nowTimestamp = nowTimestamp - 60000;

                           // For delisted symbols, use deliveryDate as upper time bound
                           auto endTimestamp = nowTimestamp;
                           if (const auto dit = symbolDeliveryDates.find(symbol); dit != symbolDeliveryDates.end()) {
                               endTimestamp = dit->second;
                           }

                           spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                           const int64_t fromTimeStamp = BinanceCommon::checkSymbolCSVFile(symbolFilePathCsv.string());

                           try {
                               const auto candles = m_p->bnbFuturesClient->getHistoricalPrices(symbol,
                                   bnbCandleInterval,
                                   fromTimeStamp,
                                   endTimestamp, 1500);

                               if (!candles.empty()) {
                                   if (BinanceCommon::writeCandlesToCSVFile(candles, symbolFilePathCsv.string())) {
                                       spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                   }
                               }
                               // Return the path if the CSV file exists (for T6 conversion)
                               if (std::filesystem::exists(symbolFilePathCsv)) {
                                   return symbolFilePathCsv;
                               }
                           } catch (const std::exception &e) {
                               spdlog::warn(fmt::format("Updating candles for symbol: {} failed, reason: {}",
                                                        symbol, e.what()));
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

    T6Directory.append(T6_FUT_DIR);
    T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    if (convertToT6) {
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
                throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(),
                                                     err.message().c_str()));
            }
            spdlog::info(fmt::format("Converting from csv to t6..."));
            m_p->binanceCommon->convertFromCSVToT6(allCsvFiles, T6Directory.string());
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

void BinanceFuturesDownloader::updateMarketData(const std::string &connectionString,
                                                const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                                const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: BinanceFuturesDownloader::updateMarketData()");
}

void BinanceFuturesDownloader::updateFundingRateData(const std::string &dirPath,
                                                     const std::vector<std::string> &symbols,
                                                     const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                                     const onSymbolCompleted &onSymbolCompletedCB) const {
    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    // Map symbol -> deliveryDate for delisted symbols (used as upper time bound)
    std::map<std::string, int64_t> symbolDeliveryDates;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInfo = m_p->bnbFuturesClient->getExchangeInfo();

    for (const auto &limit: exchangeInfo.rateLimits) {
        if (limit.rateLimitType == RateLimitType::REQUEST_WEIGHT && limit.intervalNum == 1 &&
            limit.interval == RateLimitInterval::MINUTE) {
            spdlog::info(fmt::format("Weight limit: {}", limit.limit));
            m_p->bnbFuturesClient->setAPIWeightLimit(limit.limit);
        }
    }

    // Build set of all known symbols from exchange for filesystem-based delisting detection
    std::set<std::string> exchangeSymbolSet;
    for (const auto &el: exchangeInfo.symbols) {
        exchangeSymbolSet.insert(el.symbol);
    }

    if (symbolsToUpdate.empty()) {
        constexpr auto symbolContract = futures::ContractType::PERPETUAL;
        const auto symbolType = std::string(magic_enum::enum_name(symbolContract));

        for (const auto &el: exchangeInfo.symbols) {
            if (el.contractType == symbolType && el.quoteAsset == "USDT") {
                if (el.status != ContractStatus::TRADING && m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(el.symbol);
                } else {
                    symbolsToUpdate.push_back(el.symbol);
                    if (el.status != ContractStatus::TRADING && el.deliveryDate > 0) {
                        symbolDeliveryDates[el.symbol] = el.deliveryDate;
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
            auto it = std::ranges::find_if(exchangeInfo.symbols, [symbol](const futures::Symbol &s) {
                return s.symbol == symbol;
            });

            if (it == exchangeInfo.symbols.end()) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                } else {
                    tempSymbols.push_back(symbol);
                }
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", symbol));
            } else if (it->status != ContractStatus::TRADING) {
                if (m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                    spdlog::info(fmt::format("Symbol: {} is not trading (status: {}), will be deleted", symbol,
                                             magic_enum::enum_name(it->status)));
                } else {
                    tempSymbols.push_back(it->symbol);
                    if (it->deliveryDate > 0) {
                        symbolDeliveryDates[it->symbol] = it->deliveryDate;
                    }
                }
            } else {
                tempSymbols.push_back(it->symbol);
            }
        }

        symbolsToUpdate = tempSymbols;
    }

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this, &symbolDeliveryDates](const std::string &symbol,
                                         Semaphore &maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;

                           symbolFilePathCsv.append(CSV_FUT_FR_DIR);

                           if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string());
                               err.value() != 0) {
                               throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}",
                                                                    symbolFilePathCsv.string(), err.value()));
                           }

                           symbolFilePathCsv.append(symbol + "_fr.csv");

                           auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           // For delisted symbols, use deliveryDate as upper time bound
                           auto endTimestamp = nowTimestamp;
                           if (auto dit = symbolDeliveryDates.find(symbol); dit != symbolDeliveryDates.end()) {
                               endTimestamp = dit->second;
                           }

                           spdlog::info(fmt::format("Updating FR for symbol: {}...", symbol));

                           const int64_t fromTimeStamp = P::checkFundingRatesCSVFile(symbolFilePathCsv.string());

                           try {
                               const auto fr = m_p->bnbFuturesClient->getFundingRates(
                                   symbol, fromTimeStamp, endTimestamp,
                                   1000);
                               if (!fr.empty()) {
                                   if (fr.size() == 1) {
                                       if (fromTimeStamp == fr.front().fundingTime) {
                                           spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                           return symbolFilePathCsv;
                                       }
                                   }

                                   if (P::writeFundingRatesToCSVFile(fr, symbolFilePathCsv.string(), fromTimeStamp)) {
                                       spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                       return symbolFilePathCsv;
                                   }
                               }
                           } catch (const std::exception &e) {
                               spdlog::warn(fmt::format("Updating symbol: {} failed, reason: {}",
                                                        symbol, e.what()));
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

void BinanceFuturesDownloader::convertToT6(const std::string &dirPath, const CandleInterval candleInterval) const {
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
            throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(),
                                                 err.message().c_str()));
        }
        spdlog::info(fmt::format("Converting from csv to t6..."));
        m_p->binanceCommon->convertFromCSVToT6(allCsvFiles, T6Directory.string());
    }
}
}
