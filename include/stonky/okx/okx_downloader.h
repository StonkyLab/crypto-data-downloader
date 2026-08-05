/**
OKX Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_OKX_DOWNLOADER_H
#define INCLUDE_STONKY_OKX_DOWNLOADER_H

#include "stonky/interface/i_exchange_downloader.h"
#include <string>
#include <vector>
#include <memory>

namespace stonky {
class OKXDownloader final : public IExchangeDownloader {
    struct P;
    std::unique_ptr<P> m_p{};

public:
    /**
     * @param xperp download OKX X-Perps (USD-settled, `instType=FUTURES`,
     *              `ruleType=xperp`) instead of the USDT swaps. They are a
     *              separate product with a separate directory tree, and their
     *              funding rates exist only through the REST endpoint — the
     *              bulk archive's funding module rejects `instType=FUTURES`.
     */
    explicit OKXDownloader(std::uint32_t maxJobs, MarketCategory marketCategory, bool deleteDelistedData = false,
                           bool xperp = false);

    ~OKXDownloader() override;

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

#endif //INCLUDE_STONKY_OKX_DOWNLOADER_H
