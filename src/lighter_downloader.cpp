/**
Lighter Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/lighter/lighter_downloader.h"
#include "stonky/downloader.h"
#include "stonky/lighter/lighter_rest_client.h"
#include "stonky/lighter/lighter.h"
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

using namespace stonky::lighter;

namespace stonky {
struct LighterDownloader::P {
    std::unique_ptr<RESTClient> ltClient;
    mutable Semaphore maxConcurrentConvertJobs;
    mutable std::recursive_mutex locker;
    Semaphore maxConcurrentDownloadJobs{1};
    bool deleteDelistedData = false;

    static bool writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path,
                                             lighter::CandleInterval interval);

    static int64_t checkSymbolCSVFile(const std::string &path);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path, std::int64_t lastTs);

    static bool readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles);

    void convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths, const std::string &outDirPath,
                            lighter::CandleInterval interval) const;

    static int64_t checkFundingRatesCSVFile(const std::string &path);

    static bool writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path);

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData)
        : ltClient(std::make_unique<RESTClient>()),
          maxConcurrentConvertJobs(maxJobs),
          deleteDelistedData(deleteDelistedData) {
    }
};

LighterDownloader::LighterDownloader(std::uint32_t maxJobs, bool deleteDelistedData)
    : m_p(std::make_unique<P>(maxJobs, deleteDelistedData)) {
}

LighterDownloader::~LighterDownloader() = default;

bool LighterDownloader::P::readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles) {
    try {
        io::CSVReader<6> in(path);
        in.read_header(io::ignore_extra_column, "open_time", "open", "high", "low", "close", "volume");

        Candle candle;
        while (in.read_row(candle.openTime, candle.open, candle.high, candle.low, candle.close, candle.baseVolume)) {
            candles.push_back(candle);
        }
    } catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }
    return true;
}

bool LighterDownloader::P::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path,
                                                        lighter::CandleInterval interval) {
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

    const auto numMSecondsForInterval = Lighter::numberOfMsForCandleInterval(interval);

    for (const auto &candle: std::ranges::reverse_view(candles)) {
        T6 t6;
        t6.fOpen = static_cast<float>(candle.open);
        t6.fHigh = static_cast<float>(candle.high);
        t6.fLow = static_cast<float>(candle.low);
        t6.fClose = static_cast<float>(candle.close);
        t6.fVal = 0.0;
        t6.fVol = static_cast<float>(candle.baseVolume);
        t6.time = convertTimeMs(candle.openTime + numMSecondsForInterval);
        ofs.write(reinterpret_cast<char *>(&t6), sizeof(T6));
    }

    ofs.close();
    return true;
}

void LighterDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
                                               const std::string &outDirPath,
                                               lighter::CandleInterval interval) const {
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
                       [interval](const std::filesystem::path &csvPath, const std::filesystem::path &t6Path,
                                  Semaphore &maxJobs) -> std::pair<std::string, bool> {
                           std::scoped_lock w(maxJobs);
                           std::pair<std::string, bool> retVal;
                           retVal.first = csvPath.filename().replace_extension("").string();
                           retVal.second = writeCSVCandlesToZorroT6File(csvPath.string(), t6Path.string(), interval);
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

bool LighterDownloader::P::writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path,
                                                  std::int64_t lastTs) {
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
        if (candle.openTime == lastTs) {
            continue;
        }
        ofs << candle.openTime << ",";
        ofs << candle.open << ",";
        ofs << candle.high << ",";
        ofs << candle.low << ",";
        ofs << candle.close << ",";
        ofs << candle.baseVolume << std::endl;
    }

    ofs.close();
    return true;
}

int64_t LighterDownloader::P::checkSymbolCSVFile(const std::string &path) {
    constexpr int64_t oldestLighterDate = 1704067200000; /// Monday 1. January 2024 0:00:00

    std::ifstream ifs;
    ifs.open(path, std::ios::ate);

    if (!ifs.is_open()) {
        if (std::filesystem::exists(path)) {
            spdlog::error(fmt::format("Couldn't open file: {}", path));
        }
        return oldestLighterDate;
    }

    const std::streampos size = ifs.tellg();
    char c;
    std::string row;
    int endLines = 0;

    for (int i = 1; i <= size; i++) {
        ifs.seekg(-i, std::ios::end);
        ifs.get(c);

        if (c == '\n') {
            endLines++;
            if (endLines >= 1 && !row.empty()) {
                std::ranges::reverse(row);

                const auto records = splitString(row, ',');

                if (records.size() != 6) {
                    spdlog::error(fmt::format("Wrong records number in the CSV file: {}", path));
                    ifs.close();
                    return oldestLighterDate;
                }
                ifs.close();
                return std::stoll(records[0]);
            }
        } else {
            row.push_back(c);
        }
    }
    ifs.close();
    return oldestLighterDate;
}

int64_t LighterDownloader::P::checkFundingRatesCSVFile(const std::string &path) {
    constexpr int64_t oldestLighterDate = 1704067200000; /// Monday 1. January 2024 0:00:00

    std::ifstream ifs;
    ifs.open(path, std::ios::ate);

    if (!ifs.is_open()) {
        if (std::filesystem::exists(path)) {
            spdlog::error(fmt::format("Couldn't open file: {}", path));
        }
        return oldestLighterDate;
    }

    const std::streampos size = ifs.tellg();
    char c;
    std::string row;
    int endLines = 0;

    for (int i = 1; i <= size; i++) {
        ifs.seekg(-i, std::ios::end);
        ifs.get(c);

        if (c == '\n') {
            endLines++;
            if (endLines >= 1 && !row.empty()) {
                std::ranges::reverse(row);

                const auto records = splitString(row, ',');

                if (records.size() != 2) {
                    spdlog::error(fmt::format("Wrong records number in the CSV file: {}", path));
                    ifs.close();
                    return oldestLighterDate;
                }
                ifs.close();
                return std::stoll(records[0]);
            }
        } else {
            row.push_back(c);
        }
    }
    ifs.close();
    return oldestLighterDate;
}

bool LighterDownloader::P::writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr,
                                                       const std::string &path) {
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
        ofs << record.fundingTime << ",";
        ofs << record.fundingRate << std::endl;
    }

    ofs.close();
    return true;
}

void LighterDownloader::updateMarketData(const std::string &dirPath,
                                         const std::vector<std::string> &symbols,
                                         CandleInterval candleInterval,
                                         const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                         const onSymbolCompleted &onSymbolCompletedCB,
                                         const bool convertToT6) const {
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    lighter::CandleInterval ltInterval;

    if (!Lighter::isValidCandleResolution(barSizeInMinutes, ltInterval)) {
        throw std::invalid_argument("invalid Lighter candle resolution: " + std::to_string(barSizeInMinutes) + " m");
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

    const auto allAssets = m_p->ltClient->getPerpetualAssets(true);

    std::set<std::string> knownNames;
    for (const auto &a: allAssets) {
        knownNames.insert(a.symbol);
    }

    if (symbolsToUpdate.empty()) {
        for (const auto &a: allAssets) {
            if (a.isDelisted && m_p->deleteDelistedData) {
                symbolsToDelete.push_back(a.symbol);
            } else {
                symbolsToUpdate.push_back(a.symbol);
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
                return a.symbol == sym;
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

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this, &ltInterval, &barSizeInMinutes, convertToT6](
                   const std::string &symbol,
                   Semaphore &maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
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
                                      msg.find("405") != std::string::npos ||
                                      msg.find("captcha") != std::string::npos ||
                                      msg.find("rate limit") != std::string::npos;
                           };
                           constexpr int64_t oldestLighterDate = 1704067200000LL;
                           constexpr int maxRetries = 5;
                           for (int attempt = 0; attempt < maxRetries; ++attempt) {
                               // Re-read CSV state at start of each attempt — a previous attempt
                               // may have written batches to disk before hitting a 429.
                               const int64_t fromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                               const int64_t effectiveFrom = (fromTimeStamp == oldestLighterDate)
                                   ? nowTimestamp - static_cast<int64_t>(barSizeInMinutes) * 5000LL * 60000
                                   : fromTimeStamp;
                               try {
                                   std::ignore = m_p->ltClient->getHistoricalPrices(
                                       symbol, ltInterval, effectiveFrom, nowTimestamp,
                                       [symbolFilePathCsv, symbol, fromTimeStamp](
                                       const std::vector<lighter::Candle> &cnd) {
                                           // Filter out zero-volume candles — defensive against
                                           // synthetic / oracle-only bars on newly listed markets.
                                           std::vector<lighter::Candle> real;
                                           for (const auto &c : cnd) {
                                               if (c.baseVolume > 0.0) real.push_back(c);
                                           }
                                           if (!real.empty()) {
                                               if (!P::writeCandlesToCSVFile(real, symbolFilePathCsv.string(),
                                                                             fromTimeStamp)) {
                                                   spdlog::warn(
                                                       fmt::format("CSV file for symbol: {} update failed", symbol));
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
                                       spdlog::warn(fmt::format(
                                           "Updating candles for symbol: {} failed (attempt {}/{}): {}",
                                           symbol, attempt + 1, maxRetries, errMsg));
                                       break;
                                   }
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
            m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string(), ltInterval);
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

void LighterDownloader::updateMarketData(const std::string &connectionString,
                                         const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                         const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: LighterDownloader::updateMarketData");
}

void LighterDownloader::updateFundingRateData(const std::string &dirPath,
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

    const auto allAssets = m_p->ltClient->getPerpetualAssets(true);

    std::set<std::string> knownNames;
    for (const auto &a: allAssets) {
        knownNames.insert(a.symbol);
    }

    if (symbolsToUpdate.empty()) {
        for (const auto &a: allAssets) {
            if (a.isDelisted && m_p->deleteDelistedData) {
                symbolsToDelete.push_back(a.symbol);
            } else {
                symbolsToUpdate.push_back(a.symbol);
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
                return a.symbol == sym;
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

                           auto isRateLimitError = [](const std::string &msg) {
                               return msg.find("too many") != std::string::npos ||
                                      msg.find("429") != std::string::npos ||
                                      msg.find("405") != std::string::npos ||
                                      msg.find("captcha") != std::string::npos ||
                                      msg.find("rate limit") != std::string::npos;
                           };
                           constexpr int maxRetries = 5;
                           for (int attempt = 0; attempt < maxRetries; ++attempt) {
                               // Re-read CSV state at start of each attempt — a previous attempt
                               // may have written rows to disk before hitting a 429.
                               const int64_t fromTimeStamp = P::checkFundingRatesCSVFile(symbolFilePathCsv.string());
                               try {
                                   const auto fr = m_p->ltClient->getFundingRates(symbol, fromTimeStamp + 1,
                                                                                   nowTimestamp);
                                   if (!fr.empty()) {
                                       if (fr.size() == 1 && fromTimeStamp == fr.front().fundingTime) {
                                           spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                           return symbolFilePathCsv;
                                       }
                                       if (P::writeFundingRatesToCSVFile(fr, symbolFilePathCsv.string())) {
                                           spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                           return symbolFilePathCsv;
                                       }
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
                                       spdlog::warn(fmt::format(
                                           "Updating symbol: {} failed (attempt {}/{}): {}",
                                           symbol, attempt + 1, maxRetries, errMsg));
                                       break;
                                   }
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
                spdlog::info(fmt::format("Removing csv file for delisted symbol: {}, file: {}...", symbol,
                                         symbolFilePathCsv.string()));
            }
        }
    }
}

void LighterDownloader::convertToT6(const std::string &dirPath, const CandleInterval candleInterval) const {
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    lighter::CandleInterval ltInterval;

    if (!Lighter::isValidCandleResolution(barSizeInMinutes, ltInterval)) {
        throw std::invalid_argument("invalid Lighter candle resolution: " + std::to_string(barSizeInMinutes) + " m");
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
        m_p->convertFromCSVToT6(allCsvFiles, T6Directory.string(), ltInterval);
    }
}
}
