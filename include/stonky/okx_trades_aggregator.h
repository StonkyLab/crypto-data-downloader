/**
OKX trade records -> 1-minute bars

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_OKX_TRADES_AGGREGATOR_H
#define INCLUDE_STONKY_OKX_TRADES_AGGREGATOR_H

#include <boost/multiprecision/cpp_dec_float.hpp>
#include <algorithm>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace stonky {

/**
 * One 1-minute bar built from OKX trade records, in the candle archive's own
 * units: `vol` in contracts, `volCcy` in base currency (contracts x ctVal),
 * `volCcyQuote` in quote currency (sum of size x price x ctVal).
 */
struct OkxMinuteBar {
    using Decimal = boost::multiprecision::cpp_dec_float_50;

    std::int64_t ts{};
    Decimal o{};
    Decimal h{};
    Decimal l{};
    Decimal c{};
    Decimal vol{};
    Decimal volCcy{};
    Decimal volCcyQuote{};
};

/**
 * Folds OKX bulk trade archives (`instrument_name,trade_id,side,price,size,
 * created_time`) into the 1-minute bars the candle archive would have held.
 *
 * Why this exists: OKX's bulk candle archive starts 2023-08-20 for every
 * contract the venue has since delisted and holds nothing at all for a
 * contract delisted before that date, while its trade archive covers every
 * contract from 2021-10-01. Folding the trades reproduces the candles
 * exactly — verified on MATIC-USDT-SWAP 2023-08-21, where both exist: all
 * 1430 traded minutes match OHLC and every volume column bit for bit.
 *
 * Conventions reproduced from the candle archive:
 *  - a minute without a trade is a flat bar at the previous close with zero
 *    volume (20 of 1440 on that MATIC day), so the fold fills them in;
 *  - OHLC follow execution order, which is the file's row order (trade_id is
 *    strictly increasing). created_time is NOT a usable sort key: LUNA's May
 *    2022 archive has it stepping back by up to a month between adjacent rows
 *    while trade_id never does, so it only picks the minute. A trade_id going
 *    backwards aborts;
 *  - `size` is in contracts, so `volCcy` needs the contract value. A caller
 *    that cannot establish it passes zero and gets zero in both currency
 *    columns, which is what the 2021 rows of the candle archive carry too.
 *
 * Usage: `resume()` with the persisted tail, `feed()` each archive file in
 * chronological order, `finish()` at the end of the stretch the trades are
 * standing in for. Bars are emitted only once their minute is closed by a
 * later trade or by `finish()`, so a minute split across two files cannot be
 * written twice.
 */
class OkxTradesAggregator {
public:
    using Decimal = OkxMinuteBar::Decimal;
    static constexpr std::int64_t MINUTE_MS = 60'000;

    OkxTradesAggregator(std::string instId, Decimal contractValue)
        : instId_(std::move(instId)), contractValue_(std::move(contractValue)) {}

    /// Seed the fold with what is already on disk: bars at or before `lastTs`
    /// are never emitted again, and `lastClose` (when known) lets the first
    /// missing minutes after the tail be filled instead of skipped.
    void resume(const std::int64_t lastTs, std::optional<Decimal> lastClose) {
        pending_.reset();
        lastTs_ = lastTs;
        lastClose_ = std::move(lastClose);
    }

    /// Fold one trade archive file. Returns every bar closed by it.
    std::vector<OkxMinuteBar> feed(const std::string_view csv) {
        struct Trade {
            std::int64_t ts;
            std::string_view price;
            std::string_view size;
        };
        std::vector<Trade> trades;

        std::size_t pos = 0;
        std::int64_t lastTradeId = -1;
        while (pos < csv.size()) {
            auto eol = csv.find('\n', pos);
            if (eol == std::string_view::npos) {
                eol = csv.size();
            }
            auto line = csv.substr(pos, eol - pos);
            pos = eol + 1;
            if (!line.empty() && line.back() == '\r') {
                line.remove_suffix(1);
            }
            if (line.empty() || line.starts_with("instrument_name")) {
                continue;
            }

            std::string_view field[6];
            std::size_t count = 0;
            std::size_t start = 0;
            while (count < 6) {
                const auto comma = line.find(',', start);
                field[count++] = line.substr(start, comma == std::string_view::npos ? std::string_view::npos
                                                                                     : comma - start);
                if (comma == std::string_view::npos) {
                    break;
                }
                start = comma + 1;
            }
            if (count != 6) {
                throw std::runtime_error("OKX trade record does not have six fields: " + std::string(line));
            }
            if (field[0] != instId_) {
                continue; // a family archive can carry a sibling contract's rows
            }
            const auto digits = [&line](const std::string_view text, const char *what) {
                std::int64_t value = 0;
                for (const char ch: text) {
                    if (ch < '0' || ch > '9') {
                        throw std::runtime_error(std::string("OKX trade record has a non-numeric ") + what +
                                                 ": " + std::string(line));
                    }
                    value = value * 10 + (ch - '0');
                }
                return value;
            };
            const auto tradeId = digits(field[1], "trade_id");
            if (tradeId < lastTradeId) {
                throw std::runtime_error("OKX trade archive is not in execution order (trade_id " +
                                         std::to_string(tradeId) + " after " + std::to_string(lastTradeId) + ")");
            }
            lastTradeId = tradeId;
            trades.push_back({digits(field[5], "created_time"), field[3], field[4]});
        }

        std::vector<OkxMinuteBar> closed;
        for (const auto &trade: trades) {
            const std::int64_t minute = trade.ts - trade.ts % MINUTE_MS;
            if (minute <= lastTs_) {
                continue; // already persisted
            }
            if (pending_ && pending_->ts != minute) {
                closeInto(closed, minute);
            }
            const Decimal price{std::string(trade.price)};
            const Decimal size{std::string(trade.size)};
            if (!pending_) {
                fillFlatUpTo(closed, minute);
                pending_ = OkxMinuteBar{minute, price, price, price, price, Decimal{0}, Decimal{0}, Decimal{0}};
            }
            auto &bar = *pending_;
            bar.h = std::max(bar.h, price);
            bar.l = std::min(bar.l, price);
            bar.c = price;
            bar.vol += size;
            bar.volCcyQuote += size * price * contractValue_;
        }
        return closed;
    }

    /// Close the bar in progress and fill flat bars up to `untilExclusive`,
    /// the first minute the next data source is responsible for.
    std::vector<OkxMinuteBar> finish(const std::int64_t untilExclusive) {
        std::vector<OkxMinuteBar> closed;
        if (pending_) {
            emit(closed, *pending_);
            pending_.reset();
        }
        fillFlatUpTo(closed, untilExclusive);
        return closed;
    }

private:
    void closeInto(std::vector<OkxMinuteBar> &out, const std::int64_t nextMinute) {
        emit(out, *pending_);
        pending_.reset();
        fillFlatUpTo(out, nextMinute);
    }

    void emit(std::vector<OkxMinuteBar> &out, OkxMinuteBar bar) {
        bar.volCcy = bar.vol * contractValue_;
        lastTs_ = bar.ts;
        lastClose_ = bar.c;
        out.push_back(std::move(bar));
    }

    void fillFlatUpTo(std::vector<OkxMinuteBar> &out, const std::int64_t untilExclusive) {
        if (!lastClose_) {
            return; // nothing to carry forward yet
        }
        for (std::int64_t ts = lastTs_ + MINUTE_MS; ts < untilExclusive; ts += MINUTE_MS) {
            out.push_back(OkxMinuteBar{ts, *lastClose_, *lastClose_, *lastClose_, *lastClose_, Decimal{0},
                                       Decimal{0}, Decimal{0}});
            lastTs_ = ts;
        }
    }

    std::string instId_;
    Decimal contractValue_;
    std::int64_t lastTs_{-1};
    std::optional<Decimal> lastClose_;
    std::optional<OkxMinuteBar> pending_;
};

} // namespace stonky

#endif // INCLUDE_STONKY_OKX_TRADES_AGGREGATOR_H
