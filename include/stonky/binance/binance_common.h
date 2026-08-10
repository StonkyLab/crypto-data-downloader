/**
Binance Downloader Common

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_BINANCE_COMMON_H
#define INCLUDE_STONKY_BINANCE_COMMON_H

#include "stonky/binance/binance_models.h"
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace stonky::binance {
class BinanceCommon {
    struct P;
    std::unique_ptr<P> m_p{};

public:
    explicit BinanceCommon(std::uint32_t maxJobs);

    ~BinanceCommon();

    static bool writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path);

    static bool readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path);

    using CandlePageFetcher =
        std::function<std::vector<Candle>(std::int64_t, std::int64_t, std::int32_t)>;

    /**
     * Fetch and durably append one bounded page at a time. A later HTTP error
     * therefore preserves already downloaded history, and the next invocation
     * resumes from the CSV's last valid record instead of starting over.
     * Returns the number of appended candles and throws on stalled pagination
     * or a write failure.
     */
    static std::size_t downloadCandlesToCSVFile(const CandlePageFetcher &fetchPage,
                                                std::int64_t startTime,
                                                std::int64_t endTime,
                                                const std::string &path,
                                                std::int32_t pageLimit = 1500);

    static int64_t checkSymbolCSVFile(const std::string &path);

    void convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths, const std::string &outDirPath) const;
};
}
#endif //INCLUDE_STONKY_BINANCE_COMMON_H
