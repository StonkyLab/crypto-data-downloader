#include "stonky/binance/binance_common.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
stonky::binance::Candle candle(const std::int64_t openTime, const std::int64_t closeTime) {
    stonky::binance::Candle result;
    result.openTime = openTime;
    result.closeTime = closeTime;
    result.open = 1.0;
    result.high = 2.0;
    result.low = 0.5;
    result.close = 1.5;
    result.volume = 10.0;
    result.quoteVolume = 15.0;
    result.numberOfTrades = 3;
    result.takerBuyVolume = 4.0;
    result.takerQuoteVolume = 5.0;
    result.ignore = "0";
    return result;
}

std::size_t lineCount(const std::filesystem::path &path) {
    std::ifstream input(path);
    std::size_t count = 0;
    for (std::string line; std::getline(input, line);) {
        ++count;
    }
    return count;
}
} // namespace

int main() {
    const auto directory = std::filesystem::temp_directory_path() /
                           ("crypto_data_downloader_binance_pages_" + std::to_string(
                               std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(directory);
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() {
            std::error_code error;
            std::filesystem::remove_all(path, error);
        }
    } cleanup{directory};

    const auto completePath = directory / "complete.csv";
    std::vector<std::int64_t> cursors;
    const auto written = stonky::binance::BinanceCommon::downloadCandlesToCSVFile(
        [&cursors](const std::int64_t start, const std::int64_t, const std::int32_t) {
            cursors.push_back(start);
            if (start == 0) {
                return std::vector{candle(0, 59999), candle(60000, 119999)};
            }
            return std::vector{candle(120000, 179999), candle(180000, 239999)};
        }, 0, 180000, completePath.string(), 2);
    if (written != 3 || cursors != std::vector<std::int64_t>{0, 119999} ||
        lineCount(completePath) != 4) {
        std::cerr << "Binance page boundaries skipped/duplicated a candle or persisted past endTime\n";
        return 1;
    }

    const auto interruptedPath = directory / "interrupted.csv";
    bool failed = false;
    try {
        (void) stonky::binance::BinanceCommon::downloadCandlesToCSVFile(
            [](const std::int64_t start, const std::int64_t, const std::int32_t) {
                if (start == 0) {
                    return std::vector{candle(0, 59999)};
                }
                throw std::runtime_error("simulated late HTTP failure");
            }, 0, 180000, interruptedPath.string(), 1);
    } catch (const std::runtime_error &) {
        failed = true;
    }
    if (!failed || lineCount(interruptedPath) != 2) {
        std::cerr << "Late Binance page failure discarded or over-reported durable progress\n";
        return 1;
    }

    bool stalled = false;
    try {
        (void) stonky::binance::BinanceCommon::downloadCandlesToCSVFile(
            [](const std::int64_t, const std::int64_t, const std::int32_t) {
                return std::vector{candle(0, 0)};
            }, 0, 180000, (directory / "stalled.csv").string(), 1);
    } catch (const std::runtime_error &) {
        stalled = true;
    }
    if (!stalled) {
        std::cerr << "Stalled Binance pagination was accepted\n";
        return 1;
    }

    return 0;
}
