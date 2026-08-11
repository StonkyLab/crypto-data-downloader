/**
Binance Spot Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/binance/binance_spot_downloader.h"
#include "stonky/binance/binance_spot_rest_client.h"
#include "stonky/binance/binance_common.h"
#include "stonky/downloader.h"
#include "stonky/future_utils.h"
#include "stonky/utils/semaphore.h"
#include "stonky/utils/utils.h"
#include "stonky/binance/binance.h"
#include "stonky/interface/exchange_enums.h"
#include <filesystem>
#include <set>
#include <spdlog/spdlog.h>
#include <regex>
#include <future>
#include <spdlog/fmt/ranges.h>

using namespace stonky::binance;

namespace stonky {
struct BinanceSpotDownloader::P {
    std::unique_ptr<spot::RESTClient> bnbSpotClient;
    std::unique_ptr<BinanceCommon> binanceCommon;
    mutable Semaphore maxConcurrentConvertJobs;
    Semaphore maxConcurrentDownloadJobs;
    bool deleteDelistedData = false;

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData) : bnbSpotClient(
                                                                                 std::make_unique<spot::RESTClient>(
                                                                                     "", "")),
                                                                             binanceCommon(
                                                                                 std::make_unique<BinanceCommon>(
                                                                                     maxJobs)),
                                                                             maxConcurrentConvertJobs(normalizedJobCount(maxJobs)),
                                                                             maxConcurrentDownloadJobs(boundedJobCount(maxJobs, 3)),
                                                                             deleteDelistedData(deleteDelistedData) {
    }
};

BinanceSpotDownloader::BinanceSpotDownloader(std::uint32_t maxJobs, bool deleteDelistedData) : m_p(
    std::make_unique<P>(maxJobs, deleteDelistedData)) {
}

BinanceSpotDownloader::~BinanceSpotDownloader() = default;

void BinanceSpotDownloader::updateMarketData(const std::string &dirPath, const std::vector<std::string> &symbols,
                                             CandleInterval candleInterval,
                                             const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                             const onSymbolCompleted &onSymbolCompletedCB,
                                             const bool convertToT6) const {
    auto bnbCandleInterval = binance::CandleInterval::_1m;
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;

    if (const auto isOk = Binance::isValidCandleResolution(barSizeInMinutes, bnbCandleInterval); !isOk) {
        throw std::invalid_argument("invalid Binance candle resolution: " + std::to_string(barSizeInMinutes) + " m");
    }

    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInfo = m_p->bnbSpotClient->getExchangeInfo();

    for (const auto &limit: exchangeInfo.rateLimits) {
        if (limit.rateLimitType == RateLimitType::REQUEST_WEIGHT && limit.intervalNum == 1 &&
            limit.interval == RateLimitInterval::MINUTE) {
            spdlog::info(fmt::format("Weight limit: {}", limit.limit));
            m_p->bnbSpotClient->setAPIWeightLimit(limit.limit);
        }
    }

    // Build set of all known symbols from exchange for filesystem-based delisting detection
    std::set<std::string> exchangeSymbolSet;
    for (const auto &el: exchangeInfo.symbols) {
        exchangeSymbolSet.insert(el.symbol);
    }

    if (symbolsToUpdate.empty()) {
        for (const auto &el: exchangeInfo.symbols) {
            if (el.quoteAsset == "USDT") {
                if (el.status != ContractStatus::TRADING && m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(el.symbol);
                } else {
                    symbolsToUpdate.push_back(el.symbol);
                }
            }
        }

        // Scan existing CSV files for symbols no longer on the exchange
        if (m_p->deleteDelistedData) {
            std::filesystem::path csvDir = finalPath;
            csvDir.append(CSV_SPOT_DIR);
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
            auto it = std::ranges::find_if(exchangeInfo.symbols, [symbol](const spot::Symbol &s) {
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
                } else {
                    tempSymbols.push_back(it->symbol);
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
                       [finalPath, this, &bnbCandleInterval, &barSizeInMinutes, convertToT6](const std::string &symbol) ->
                   std::filesystem::path {
                           std::filesystem::path symbolFilePathCsv = finalPath;
                           std::filesystem::path symbolFilePathT6 = finalPath;

                           symbolFilePathCsv.append(CSV_SPOT_DIR);
                           symbolFilePathT6.append(T6_SPOT_DIR);

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

                           auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           // Do not download the last minute - it is not complete yet.
                           nowTimestamp = nowTimestamp - 60000;

                           spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                           const int64_t fromTimeStamp = BinanceCommon::checkSymbolCSVFile(symbolFilePathCsv.string());

                           try {
                               const auto written = BinanceCommon::downloadCandlesToCSVFile(
                                   [this, &symbol, bnbCandleInterval](const std::int64_t start,
                                                                    const std::int64_t end,
                                                                    const std::int32_t limit) {
                                       return m_p->bnbSpotClient->getHistoricalPricesSingle(
                                           symbol, bnbCandleInterval, start, end, limit);
                                   }, fromTimeStamp, nowTimestamp, symbolFilePathCsv.string());

                               if (written > 0) {
                                   spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                               }
                               // Return the path if the CSV file exists (for T6 conversion)
                               if (std::filesystem::exists(symbolFilePathCsv)) {
                                   return symbolFilePathCsv;
                               }
                           } catch (const std::exception &e) {
                               spdlog::warn(fmt::format("Updating candles for symbol: {} failed, reason: {}",
                                                        symbol, e.what()));
                               throw;
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

    T6Directory.append(T6_SPOT_DIR);
    T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    if (convertToT6) {
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

            symbolFilePathCsv.append(CSV_SPOT_DIR);
            symbolFilePathT6.append(T6_SPOT_DIR);

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

void BinanceSpotDownloader::updateMarketData(const std::string &connectionString,
                                             const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                             const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: BinanceSpotDownloader::updateMarketData()");
}

void BinanceSpotDownloader::updateFundingRateData(const std::string &dirPath, const std::vector<std::string> &symbols,
                                                  const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                                  const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: BinanceSpotDownloader::updateFundingRateData()");
}

void BinanceSpotDownloader::convertToT6(const std::string &dirPath, const CandleInterval candleInterval) const {
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
            throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(),
                                                 err.message().c_str()));
        }
        spdlog::info(fmt::format("Converting from csv to t6..."));
        m_p->binanceCommon->convertFromCSVToT6(allCsvFiles, T6Directory.string());
    }
}
}
