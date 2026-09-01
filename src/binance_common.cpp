/**
Binance Downloader Common

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/binance/binance_common.h"
#include "stonky/history_floor.h"
#include "stonky/binance/binance_models.h"
#include "stonky/atomic_file.h"
#include "stonky/csv_data.h"
#include "stonky/csv_format.h"
#include "stonky/future_utils.h"
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

    explicit P(const std::uint32_t maxJobs) : maxConcurrentConvertJobs(normalizedJobCount(maxJobs)) {
    }
};

BinanceCommon::BinanceCommon(std::uint32_t maxJobs) : m_p(std::make_unique<P>(maxJobs)) {
}

BinanceCommon::~BinanceCommon() = default;

bool BinanceCommon::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path) {
    const std::filesystem::path pathToT6File{t6Path};

    AtomicFileWriter output(pathToT6File, std::ios::binary,
                            AtomicFileWriter::Locking::None);
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
        t6.time = convertTimeMs(candle.closeTime);
        ofs.write(reinterpret_cast<char *>(&t6), sizeof(T6));
    }

    std::string error;
    if (!output.commit(error)) {
        spdlog::error(fmt::format("Write to T6 file failed: {}: {}", t6Path, error));
        return false;
    }
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
        ofs << csvNumber(candle.open) << ",";
        ofs << csvNumber(candle.high) << ",";
        ofs << csvNumber(candle.low) << ",";
        ofs << csvNumber(candle.close) << ",";
        ofs << csvNumber(candle.volume) << ",";
        ofs << candle.openTime << ",";
        ofs << csvNumber(candle.quoteVolume) << ",";
        ofs << candle.numberOfTrades << ",";
        ofs << csvNumber(candle.takerBuyVolume) << ",";
        ofs << csvNumber(candle.takerQuoteVolume) << ",";
        ofs << candle.ignore << std::endl;
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

std::size_t BinanceCommon::downloadCandlesToCSVFile(const CandlePageFetcher &fetchPage,
                                                    const std::int64_t startTime,
                                                    const std::int64_t endTime,
                                                    const std::string &path,
                                                    const std::int32_t pageLimit) {
    if (!fetchPage) {
        throw std::invalid_argument("Binance candle page fetcher is empty");
    }
    if (pageLimit <= 0) {
        throw std::invalid_argument("Binance candle page limit must be positive");
    }
    if (startTime >= endTime) {
        return 0;
    }

    std::int64_t cursor = startTime;
    std::int64_t persistedTail = startTime;
    std::size_t written = 0;

    while (cursor < endTime) {
        auto response = fetchPage(cursor, endTime, pageLimit);
        if (response.empty()) {
            break;
        }

        for (std::size_t i = 1; i < response.size(); ++i) {
            if (response[i].closeTime <= response[i - 1].closeTime) {
                throw std::runtime_error("Binance candle page is not strictly chronological");
            }
        }

        const auto nextCursor = response.back().closeTime;
        if (nextCursor <= cursor) {
            throw std::runtime_error("Binance candle pagination made no forward progress");
        }

        // Binance filters pages by open time, so the final response can include
        // a candle whose close lies beyond endTime. Never persist that candle.
        std::vector<Candle> complete;
        complete.reserve(response.size());
        for (const auto &candle: response) {
            if (candle.closeTime > persistedTail && candle.closeTime <= endTime) {
                complete.push_back(candle);
            }
        }

        if (!complete.empty()) {
            if (!writeCandlesToCSVFile(complete, path)) {
                throw std::runtime_error("failed to write Binance candle CSV page");
            }
            persistedTail = complete.back().closeTime;
            written += complete.size();
        }
        cursor = nextCursor;
    }

    return written;
}

int64_t BinanceCommon::checkSymbolCSVFile(const std::string &path) {
    const std::int64_t oldestBNBDate = historyFloor(1420070400000LL); /// Thursday 1. January 2015 0:00:00
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
            launchBounded(m_p->maxConcurrentConvertJobs,
                       [](const std::filesystem::path &csvPath, const std::filesystem::path &t6Path) -> std::pair<std::string, bool> {
                           std::pair<std::string, bool> retVal;
                           retVal.first = csvPath.filename().replace_extension("").string();
                           retVal.second = writeCSVCandlesToZorroT6File(csvPath.string(), t6Path.string());
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
}
