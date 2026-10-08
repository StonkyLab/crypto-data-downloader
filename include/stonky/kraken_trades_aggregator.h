/**
Kraken spot trade tape -> 1-minute bars

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_KRAKEN_TRADES_AGGREGATOR_H
#define INCLUDE_STONKY_KRAKEN_TRADES_AGGREGATOR_H

#include "stonky/kraken/kraken_models.h"
#include <algorithm>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace stonky {

/**
 * Folds Kraken's public spot trade tape (GET /0/public/Trades) into the
 * 1-minute bars the venue's own OHLC endpoint shows.
 *
 * Why this exists: /0/public/OHLC serves the last 720 candles of a pair and
 * nothing older, whatever `since` says, while the trade tape is paginated
 * back to the pair's first trade (2013 for XBTUSD). Folding the tape is the
 * only way to get the history through the API.
 *
 * Conventions reproduced from the OHLC endpoint:
 *  - open/high/low/close follow tape order, volume is the sum of base
 *    amounts (summed as decimals, so 0.1 + 0.2 + 0.3 comes out as 0.6);
 *  - a minute without a trade is a flat zero-volume bar at the previous
 *    close (ABEUR shows 701 such bars in a 720-minute window), so the fold
 *    fills them in — between trades and, through finish(), up to the present;
 *  - the first bar of a pair starts at its first trade, nothing is filled
 *    before it.
 *
 * Usage: resume() with the persisted tail, feed() each page in tape order,
 * finish() with the first minute that is still open. A bar is emitted only
 * once its minute is closed by a later trade or by finish(), so a minute cut
 * by a page boundary, or still receiving trades, is never written twice or
 * written short. The venue repeats the cursor's trade at the top of the next
 * page; it is dropped here by trade id.
 */
class KrakenTradesAggregator {
public:
    using Decimal = kraken::Decimal;
    static constexpr std::int64_t MINUTE_MS = 60'000;

    /// Seed the fold with what is already on disk: bars at or before `lastTs`
    /// are never emitted again, and `lastClose` (when known) lets the empty
    /// minutes right after the tail be filled instead of skipped.
    void resume(const std::int64_t lastTs, const std::optional<double> lastClose) {
        pending_.reset();
        lastTs_ = lastTs;
        lastClose_ = lastClose;
        lastTradeId_ = 0;
    }

    /// Fold one page of trades. Returns every bar it closed.
    std::vector<kraken::Candle> feed(const std::vector<kraken::Trade> &trades) {
        std::vector<kraken::Candle> closed;
        for (const auto &trade: trades) {
            if (trade.tradeId <= lastTradeId_) {
                continue; // the previous page's cursor trade, repeated by the inclusive `since`
            }
            lastTradeId_ = trade.tradeId;
            const std::int64_t minute = trade.timeMs - trade.timeMs % MINUTE_MS;
            if (minute <= lastTs_) {
                continue; // already persisted
            }
            if (pending_ && minute < pending_->bar.openTime) {
                throw std::runtime_error("Kraken trade tape steps back from minute " +
                                         std::to_string(pending_->bar.openTime) + " to trade " +
                                         std::to_string(trade.tradeId) + " at " + std::to_string(trade.timeMs));
            }
            if (pending_ && minute != pending_->bar.openTime) {
                emit(closed);
            }
            const double price = trade.price.convert_to<double>();
            if (!pending_) {
                fillFlatUpTo(closed, minute);
                Pending fresh;
                fresh.bar.openTime = minute;
                fresh.bar.open = price;
                fresh.bar.high = price;
                fresh.bar.low = price;
                fresh.bar.close = price;
                pending_ = std::move(fresh);
            }
            auto &bar = pending_->bar;
            bar.high = std::max(bar.high, price);
            bar.low = std::min(bar.low, price);
            bar.close = price;
            pending_->volume += trade.volume;
        }
        return closed;
    }

    /// Close the pending bar if its minute ended before `untilExclusive` and
    /// fill flat bars up to there. A bar whose minute is still open stays
    /// unemitted; the next run re-reads that minute's trades from the tape.
    std::vector<kraken::Candle> finish(const std::int64_t untilExclusive) {
        std::vector<kraken::Candle> closed;
        if (pending_ && pending_->bar.openTime + MINUTE_MS <= untilExclusive) {
            emit(closed);
        }
        if (!pending_) {
            fillFlatUpTo(closed, untilExclusive);
        }
        return closed;
    }

private:
    struct Pending {
        kraken::Candle bar;
        Decimal volume{0};
    };

    void emit(std::vector<kraken::Candle> &out) {
        kraken::Candle bar = pending_->bar;
        bar.volume = pending_->volume.convert_to<double>();
        lastTs_ = bar.openTime;
        lastClose_ = bar.close;
        out.push_back(bar);
        pending_.reset();
    }

    void fillFlatUpTo(std::vector<kraken::Candle> &out, const std::int64_t untilExclusive) {
        if (!lastClose_) {
            return; // nothing to carry forward yet
        }
        for (std::int64_t ts = lastTs_ + MINUTE_MS; ts < untilExclusive; ts += MINUTE_MS) {
            kraken::Candle flat;
            flat.openTime = ts;
            flat.open = *lastClose_;
            flat.high = *lastClose_;
            flat.low = *lastClose_;
            flat.close = *lastClose_;
            flat.volume = 0.0;
            out.push_back(flat);
            lastTs_ = ts;
        }
    }

    std::int64_t lastTs_{-1};
    std::optional<double> lastClose_;
    std::int64_t lastTradeId_{0};
    std::optional<Pending> pending_;
};

} // namespace stonky

#endif // INCLUDE_STONKY_KRAKEN_TRADES_AGGREGATOR_H
