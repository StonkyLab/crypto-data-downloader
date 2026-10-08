/**
Kraken Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_KRAKEN_DOWNLOADER_H
#define INCLUDE_STONKY_KRAKEN_DOWNLOADER_H

#include "stonky/interface/i_exchange_downloader.h"
#include <string>
#include <vector>
#include <memory>

namespace stonky {
/**
 * Kraken Spot (-c s) and Kraken Futures (-c f).
 *
 * Futures: executed-trade candles from the charts API, whole history since
 * listing, every resolution the venue serves; funding history (hourly, the
 * venue answers roughly the last year). Perpetuals only (PF_ / PI_),
 * delisted ones included unless deleteDelistedData.
 *
 * Spot: the OHLC endpoint serves 720 candles and nothing older, so 1-minute
 * bars are folded from the public trade tape, which is paginated back to a
 * pair's first trade. Only -b 1 is downloadable; coarser bars are built with
 * -g. No funding rates.
 */
class KrakenDownloader final : public IExchangeDownloader {
    struct P;
    std::unique_ptr<P> m_p{};

public:
    explicit KrakenDownloader(std::uint32_t maxJobs, MarketCategory marketCategory,
                              bool deleteDelistedData = false);

    ~KrakenDownloader() override;

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
#endif //INCLUDE_STONKY_KRAKEN_DOWNLOADER_H
