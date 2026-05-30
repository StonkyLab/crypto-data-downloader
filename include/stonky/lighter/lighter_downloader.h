/**
Lighter Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_LIGHTER_DOWNLOADER_H
#define INCLUDE_STONKY_LIGHTER_DOWNLOADER_H

#include "stonky/interface/i_exchange_downloader.h"
#include <string>
#include <vector>
#include <memory>

namespace stonky {
class LighterDownloader final : public IExchangeDownloader {
    struct P;
    std::unique_ptr<P> m_p{};

public:
    explicit LighterDownloader(std::uint32_t maxJobs, bool deleteDelistedData = false);

    ~LighterDownloader() override;

    void updateMarketData(const std::string &dirPath,
                          const std::vector<std::string> &symbols,
                          CandleInterval candleInterval,
                          const onSymbolsToUpdate &onSymbolsToUpdateCB,
                          const onSymbolCompleted &onSymbolCompletedCB,
                          bool convertToT6) const override;

    void updateMarketData(const std::string &connectionString,
                          const onSymbolsToUpdate &onSymbolsToUpdateCB,
                          const onSymbolCompleted &onSymbolCompletedCB) const override;

    void updateFundingRateData(const std::string &dirPath,
                               const std::vector<std::string> &symbols,
                               const onSymbolsToUpdate &onSymbolsToUpdateCB,
                               const onSymbolCompleted &onSymbolCompletedCB) const override;

    void convertToT6(const std::string &dirPath, CandleInterval candleInterval) const override;
};
}
#endif //INCLUDE_STONKY_LIGHTER_DOWNLOADER_H
