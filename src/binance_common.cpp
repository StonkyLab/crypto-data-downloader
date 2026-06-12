/**
Binance Downloader Common

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/binance/binance_common.h"
#include "stonky/binance/binance_models.h"
#include "stonky/csv_data.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/semaphore.h"
#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>
#include <ranges>
#include <regex>
#include <future>
#include "csv.h"

namespace stonky::binance {
struct BinanceCommon::P {
    mutable Semaphore maxConcurrentConvertJobs;
    Semaphore maxConcurrentDownloadJobs{3};

    explicit P(const std::uint32_t maxJobs) : maxConcurrentConvertJobs(maxJobs) {
    }
};

BinanceCommon::BinanceCommon(std::uint32_t maxJobs) : m_p(std::make_unique<P>(maxJobs)) {
}

BinanceCommon::~BinanceCommon() = default;

bool BinanceCommon::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path) {
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

    for (const auto &candle: std::ranges::reverse_view(candles)) {
        T6 t6;
        t6.fOpen = static_cast<float>(candle.open);
        t6.fHigh = static_cast<float>(candle.high);
        t6.fLow = static_cast<float>(candle.low);
        t6.fClose = static_cast<float>(candle.close);
        t6.fVal = 0.0;
        t6.fVol = static_cast<float>(candle.volume);
        t6.time = convertTimeMs(candle.closeTime);
        ofs.write(reinterpret_cast<char *>(&t6), sizeof(T6));
    }

    ofs.close();
    return true;
}

bool BinanceCommon::readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles) {
    try {
        io::CSVReader<7> in(path);
        in.read_header(io::ignore_extra_column, "close_time", "open", "high", "low", "close", "volume", "timestamp");

        Candle candle;
        while (in.read_row(candle.closeTime, candle.open, candle.high, candle.low, candle.close,
                           candle.volume, candle.openTime)) {
            candles.push_back(candle);
        }
    } catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }

    return true;
}

bool BinanceCommon::writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path) {
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
        ofs << "close_time,open,high,low,close,volume,timestamp,quote_av,trades,tb_base_av,tb_quote_av,ignore"
                << std::endl;
    }

    for (const auto &candle: candles) {
        ofs << candle.closeTime << ",";
        ofs << candle.open << ",";
        ofs << candle.high << ",";
        ofs << candle.low << ",";
        ofs << candle.close << ",";
        ofs << candle.volume << ",";
        ofs << candle.openTime << ",";
        ofs << candle.quoteVolume << ",";
        ofs << candle.numberOfTrades << ",";
        ofs << candle.takerBuyVolume << ",";
        ofs << candle.takerQuoteVolume << ",";
        ofs << candle.ignore << std::endl;
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

int64_t BinanceCommon::checkSymbolCSVFile(const std::string &path) {
    constexpr int64_t oldestBNBDate = 1420070400000; /// Thursday 1. January 2015 0:00:00
    // Self-healing read: a torn tail (interrupted write) is truncated instead of
    // resetting the resume point to oldestBNBDate, which used to silently
    // re-download and append the entire history.
    return CsvData::lastValidRecord(path, 12, oldestBNBDate).timestamp;
}

void BinanceCommon::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
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
                       }, path, t6FilePath, std::ref(m_p->maxConcurrentConvertJobs)));
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
}
