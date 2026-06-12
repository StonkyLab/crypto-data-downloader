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
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/ranges.h>

using namespace stonky::okx;

namespace stonky {
struct OKXDownloader::P {
    std::unique_ptr<RESTClient> okxClient;
    mutable Semaphore maxConcurrentConvertJobs;
    mutable std::recursive_mutex locker;
    Semaphore maxConcurrentDownloadJobs{3};
    bool deleteDelistedData = false;
    MarketCategory marketCategory = MarketCategory::Futures;

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

OKXDownloader::OKXDownloader(std::uint32_t maxJobs, const MarketCategory marketCategory, bool deleteDelistedData) : m_p(
    std::make_unique<P>(maxJobs, deleteDelistedData)) {
    m_p->marketCategory = marketCategory;
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
        throw std::invalid_argument("OKX market data history only supports 1-minute candles, you must aggregate to higher time frames manually");
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

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInstruments = m_p->okxClient->getInstruments(instrumentType);

    // Create a map from instId to Instrument for quick lookup
    std::map<std::string, Instrument> instrumentMap;
    std::set<std::string> exchangeSymbolSet;
    for (const auto &inst: exchangeInstruments) {
        instrumentMap[inst.instId] = inst;
        exchangeSymbolSet.insert(inst.instId);
    }

    if (symbolsToUpdate.empty()) {
        for (const auto &el: exchangeInstruments) {
            if (el.settleCcy == "USDT" || el.quoteCcy == "USDT") {
                if (el.state != InstrumentStatus::live && m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(el.instId);
                } else {
                    symbolsToUpdate.push_back(el.instId);
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

                            try {
                                // OKX API limits: max 20 days for daily, max 20 months for monthly
                                constexpr int64_t maxMonthlyRangeMs = 19LL * 30 * 24 * 60 * 60 * 1000;
                                constexpr int64_t maxDailyRangeMs = 19LL * 24 * 60 * 60 * 1000; // 19 days in ms

                                int64_t currentStart = fromTimeStamp;
                                int64_t totalNewCandles = 0;
                                int64_t lastSavedTimestamp = fromTimeStamp;

                                // 1. Download historical data via bulk ZIP files (Monthly intervals)
                                    while (currentStart < nowTimestamp - 30LL * 24 * 60 * 60 * 1000) {
                                        int64_t currentEnd = std::min(currentStart + maxMonthlyRangeMs, nowTimestamp);
                                        auto history = m_p->okxClient->getMarketDataHistory(
                                            MarketDataModule::Candles1m,
                                            instrumentType,
                                            instFamilyOrId,
                                            DateAggrType::monthly,
                                            currentStart,
                                            currentEnd);

                                        currentStart = currentEnd;

                                        if (history.details.empty()) {
                                            continue;
                                        }

                                        std::vector<MarketDataFileInfo> allFiles;
                                        for (const auto &detail: history.details) {
                                            for (const auto &fileInfo: detail.groupDetails) {
                                                allFiles.push_back(fileInfo);
                                            }
                                        }

                                        std::ranges::sort(allFiles, [](const MarketDataFileInfo &a, const MarketDataFileInfo &b) {
                                            return a.dateTs < b.dateTs;
                                        });

                                        for (const auto &fileInfo: allFiles) {
                                            if (fileInfo.dateTs + 30LL * 24 * 60 * 60 * 1000 > lastSavedTimestamp) {
                                                const auto zipData = RESTClient::downloadMarketDataFile(fileInfo.url);
                                                const auto csvData = okx::utils::extractZip(zipData);
                                                auto candles = okx::utils::parseCandlesCsv(csvData);

                                                if (!candles.empty()) {
                                                    std::ranges::sort(candles, [](const Candle &a, const Candle &b) {
                                                        return a.ts < b.ts;
                                                    });
                                                    std::vector<Candle> newCandles;
                                                    for (const auto &candle : candles) {
                                                        if (candle.ts > lastSavedTimestamp)
                                                            newCandles.push_back(candle);
                                                    }
                                                    if (!newCandles.empty()) {
                                                        P::writeCandlesToCSVFile(newCandles, symbolFilePathCsv.string(), false);
                                                        totalNewCandles += static_cast<int64_t>(newCandles.size());
                                                        lastSavedTimestamp = newCandles.back().ts;
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // 2. Download recent historical data via bulk ZIP files (Daily intervals)
                                    while (currentStart < nowTimestamp) {
                                        int64_t currentEnd = std::min(currentStart + maxDailyRangeMs, nowTimestamp);

                                        const auto history = m_p->okxClient->getMarketDataHistory(
                                            MarketDataModule::Candles1m,
                                            instrumentType,
                                            instFamilyOrId,
                                            DateAggrType::daily,
                                            currentStart,
                                            currentEnd);

                                        if (history.details.empty()) {
                                            currentStart = currentEnd;
                                            continue;
                                        }

                                        std::vector<MarketDataFileInfo> allFiles;
                                        for (const auto &detail : history.details) {
                                            for (const auto &fileInfo : detail.groupDetails) {
                                                allFiles.push_back(fileInfo);
                                            }
                                        }

                                        std::ranges::sort(allFiles, [](const MarketDataFileInfo &a, const MarketDataFileInfo &b) {
                                            return a.dateTs < b.dateTs;
                                        });

                                        for (const auto &fileInfo : allFiles) {
                                            if (fileInfo.dateTs + 24LL * 60 * 60 * 1000 <= lastSavedTimestamp)
                                                continue;

                                            const auto zipData = RESTClient::downloadMarketDataFile(fileInfo.url);
                                            const auto csvData = okx::utils::extractZip(zipData);
                                            auto candles = okx::utils::parseCandlesCsv(csvData);

                                            if (candles.empty())
                                                continue;

                                            std::ranges::sort(candles, [](const Candle &a, const Candle &b) {
                                                return a.ts < b.ts;
                                            });

                                            std::vector<Candle> newCandles;
                                            for (const auto &candle : candles) {
                                                if (candle.ts > lastSavedTimestamp)
                                                    newCandles.push_back(candle);
                                            }

                                            if (!newCandles.empty()) {
                                                P::writeCandlesToCSVFile(newCandles, symbolFilePathCsv.string(), false);
                                                totalNewCandles += static_cast<int64_t>(newCandles.size());
                                                lastSavedTimestamp = newCandles.back().ts;
                                            }
                                        }

                                        currentStart = currentEnd;
                                    }

                               // Fill the gap between last downloaded file and now using REST API
                               // The file-based endpoint only has complete days, so recent data needs REST API
                               if (lastSavedTimestamp < nowTimestamp) {

                                   // spdlog::info("Filling the gap between last downloaded file and now time...");

                                   auto recentCandles = m_p->okxClient->getHistoricalPrices(
                                       symbol, BarSize::_1m, lastSavedTimestamp, nowTimestamp);

                                   if (!recentCandles.empty()) {
                                       // spdlog::info(fmt::format("Found {} missing candles", recentCandles.size()));
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
                                   spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
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

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInstruments = m_p->okxClient->getInstruments(InstrumentType::SWAP);

    // Build set of all known symbols from exchange for filesystem-based delisting detection
    std::set<std::string> exchangeSymbolSet;
    for (const auto &el: exchangeInstruments) {
        exchangeSymbolSet.insert(el.instId);
    }

    if (symbolsToUpdate.empty()) {
        for (const auto &el: exchangeInstruments) {
            if (el.settleCcy == "USDT") {
                if (el.state != InstrumentStatus::live && m_p->deleteDelistedData) {
                    symbolsToDelete.push_back(el.instId);
                } else {
                    symbolsToUpdate.push_back(el.instId);
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
                       [finalPath, this](const std::string &symbol,
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

                           try {
                               // OKX API limit: max 20 months for monthly aggregation, use 19 to be safe
                               constexpr int64_t maxMonthlyRangeMs = 19LL * 30 * 24 * 60 * 60 * 1000;

                               int64_t currentStart = fromTimeStamp;
                               int64_t totalNewRates = 0;
                               int64_t lastSavedTimestamp = fromTimeStamp;

                               // 1. Download historical data via bulk ZIP files (monthly)
                               while (currentStart < nowTimestamp) {
                                   int64_t currentEnd = std::min(currentStart + maxMonthlyRangeMs, nowTimestamp);

                                   const auto history = m_p->okxClient->getMarketDataHistory(
                                       MarketDataModule::FundingRate,
                                       InstrumentType::SWAP,
                                       instFamily,
                                       DateAggrType::monthly,
                                       currentStart,
                                       currentEnd);

                                   if (history.details.empty()) {
                                       currentStart = currentEnd;
                                       continue;
                                   }

                                   // Collect all files and sort by date (oldest first)
                                   std::vector<MarketDataFileInfo> allFiles;
                                   for (const auto &detail : history.details) {
                                       for (const auto &fileInfo : detail.groupDetails) {
                                           allFiles.push_back(fileInfo);
                                       }
                                   }

                                   std::ranges::sort(allFiles, [](const MarketDataFileInfo &a, const MarketDataFileInfo &b) {
                                       return a.dateTs < b.dateTs;
                                   });

                                   for (const auto &fileInfo : allFiles) {
                                       // Skip files older than what we already have (month + ~31 days)
                                       if (fileInfo.dateTs + 31LL * 24 * 60 * 60 * 1000 <= lastSavedTimestamp) {
                                           continue;
                                       }

                                       const auto zipData = RESTClient::downloadMarketDataFile(fileInfo.url);
                                       const auto csvData = okx::utils::extractZip(zipData);
                                       auto rates = okx::utils::parseFundingRateCsv(csvData);

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

                                   currentStart = currentEnd;
                               }

                               // 2. Fill the gap between last bulk file and now using REST API
                               if (lastSavedTimestamp < nowTimestamp) {
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
            symbolFilePathCsv.append(CSV_FUT_FR_DIR);
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
