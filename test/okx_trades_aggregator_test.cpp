#include "stonky/okx_trades_aggregator.h"
#include "stonky/csv_format.h"

#include <iostream>
#include <string>
#include <string_view>
#include <vector>

namespace {

using stonky::OkxMinuteBar;
using stonky::OkxTradesAggregator;
using Decimal = OkxMinuteBar::Decimal;

std::string row(const OkxMinuteBar &bar) {
    return std::to_string(bar.ts) + "," + stonky::csvNumber(bar.o) + "," + stonky::csvNumber(bar.h) + "," +
           stonky::csvNumber(bar.l) + "," + stonky::csvNumber(bar.c) + "," + stonky::csvNumber(bar.vol) + "," +
           stonky::csvNumber(bar.volCcy) + "," + stonky::csvNumber(bar.volCcyQuote);
}

bool expect(const std::vector<OkxMinuteBar> &bars, const std::vector<std::string> &want, const char *what) {
    bool ok = bars.size() == want.size();
    for (std::size_t i = 0; ok && i < want.size(); ++i) {
        ok = row(bars[i]) == want[i];
    }
    if (!ok) {
        std::cerr << what << ": got " << bars.size() << " bars\n";
        for (const auto &bar: bars) {
            std::cerr << "  " << row(bar) << '\n';
        }
        std::cerr << "want " << want.size() << " bars\n";
        for (const auto &line: want) {
            std::cerr << "  " << line << '\n';
        }
    }
    return ok;
}

// The second traded minute of MATIC-USDT-SWAP on 2023-08-21, verbatim from
// static.okx.com/cdn/okex/traderecords/trades/daily/20230821/. The candle
// archive's row for that minute is
//   0.5747,0.5748,0.5744,0.5744,1199,11990,6889.352,1692547260000
// with ctVal 10. Every column has to come out identical.
constexpr std::string_view REAL_MINUTE =
    "instrument_name,trade_id,side,price,size,created_time\n"
    "MATIC-USDT-SWAP,104136693,buy,0.5747,38.0,1692547262958\n"
    "MATIC-USDT-SWAP,104136694,buy,0.5747,19.0,1692547262958\n"
    "MATIC-USDT-SWAP,104136695,buy,0.5747,2.0,1692547262958\n"
    "MATIC-USDT-SWAP,104136696,buy,0.5747,49.0,1692547262958\n"
    "MATIC-USDT-SWAP,104136697,sell,0.5748,1.0,1692547268461\n"
    "MATIC-USDT-SWAP,104136698,sell,0.5748,391.0,1692547268461\n"
    "MATIC-USDT-SWAP,104136699,sell,0.5747,1.0,1692547276839\n"
    "MATIC-USDT-SWAP,104136700,sell,0.5745,10.0,1692547298345\n"
    "MATIC-USDT-SWAP,104136701,sell,0.5745,391.0,1692547298345\n"
    "MATIC-USDT-SWAP,104136702,sell,0.5744,38.0,1692547299797\n"
    "MATIC-USDT-SWAP,104136703,sell,0.5744,100.0,1692547299797\n"
    "MATIC-USDT-SWAP,104136704,sell,0.5744,157.0,1692547299797\n"
    "MATIC-USDT-SWAP,104136705,buy,0.5744,1.0,1692547312696\n"
    "MATIC-USDT-SWAP,104136706,buy,0.5744,1.0,1692547314962\n";

constexpr std::string_view SYNTHETIC =
    "instrument_name,trade_id,side,price,size,created_time\r\n"
    "X-USDT-SWAP,1,buy,1.5,2.0,60000\r\n"
    "X-USDT-SWAP,2,sell,1.7,1.0,60500\r\n"
    "OTHER-USDT-SWAP,3,buy,9,9,60600\r\n"
    "X-USDT-SWAP,4,sell,1.4,3.0,119999\r\n"
    "X-USDT-SWAP,5,buy,1.6,1.0,240000\r\n";

} // namespace

int main() {
    bool ok = true;

    {
        OkxTradesAggregator fold("MATIC-USDT-SWAP", Decimal{10});
        auto bars = fold.feed(REAL_MINUTE);
        const auto rest = fold.finish(1692547320000);
        bars.insert(bars.end(), rest.begin(), rest.end());
        ok &= expect(bars, {"1692547260000,0.5747,0.5748,0.5744,0.5744,1199,11990,6889.352"},
                     "real MATIC minute does not reproduce the archive candle");
    }

    {
        // Three trades in one minute, a sibling contract's row ignored, two
        // empty minutes filled flat at the last close, then one more trade and
        // a flat fill up to the hand-over point.
        OkxTradesAggregator fold("X-USDT-SWAP", Decimal{10});
        auto bars = fold.feed(SYNTHETIC);
        const auto rest = fold.finish(360000);
        bars.insert(bars.end(), rest.begin(), rest.end());
        ok &= expect(bars,
                     {"60000,1.5,1.7,1.4,1.4,6,60,89", "120000,1.4,1.4,1.4,1.4,0,0,0", "180000,1.4,1.4,1.4,1.4,0,0,0",
                      "240000,1.6,1.6,1.6,1.6,1,10,16", "300000,1.6,1.6,1.6,1.6,0,0,0"},
                     "synthetic fold");
    }

    {
        // Resuming after a persisted tail: the stored minute is not emitted
        // again and the gap right after it is filled from the stored close.
        OkxTradesAggregator fold("X-USDT-SWAP", Decimal{10});
        fold.resume(60000, Decimal{"1.4"});
        auto bars = fold.feed(SYNTHETIC);
        const auto rest = fold.finish(300000);
        bars.insert(bars.end(), rest.begin(), rest.end());
        ok &= expect(bars,
                     {"120000,1.4,1.4,1.4,1.4,0,0,0", "180000,1.4,1.4,1.4,1.4,0,0,0", "240000,1.6,1.6,1.6,1.6,1,10,16"},
                     "resume after persisted tail");
    }

    {
        // Resuming without a known close: nothing can be carried forward, so
        // the first bar is the first traded minute and no flat bar precedes it.
        OkxTradesAggregator fold("X-USDT-SWAP", Decimal{10});
        fold.resume(60000, std::nullopt);
        auto bars = fold.feed(SYNTHETIC);
        const auto rest = fold.finish(300000);
        bars.insert(bars.end(), rest.begin(), rest.end());
        ok &= expect(bars, {"240000,1.6,1.6,1.6,1.6,1,10,16"}, "resume without a known close");
    }

    {
        // Unknown contract value: contracts are still counted, both currency
        // columns are zero — the same shape as the archive's own 2021 rows.
        OkxTradesAggregator fold("X-USDT-SWAP", Decimal{0});
        auto bars = fold.feed(SYNTHETIC);
        const auto rest = fold.finish(300000);
        bars.insert(bars.end(), rest.begin(), rest.end());
        ok &= !bars.empty() && row(bars.front()) == "60000,1.5,1.7,1.4,1.4,6,0,0";
        if (!ok) {
            std::cerr << "unknown contract value did not zero the currency columns\n";
        }
    }

    {
        // A minute split across two files is closed once, by the later file.
        OkxTradesAggregator fold("X-USDT-SWAP", Decimal{1});
        auto first = fold.feed("X-USDT-SWAP,1,buy,2,1,60000\n");
        auto second = fold.feed("X-USDT-SWAP,2,buy,3,1,60001\nX-USDT-SWAP,3,buy,4,1,120000\n");
        const auto rest = fold.finish(180000);
        second.insert(second.end(), rest.begin(), rest.end());
        ok &= first.empty() && expect(second, {"60000,2,3,2,3,2,2,5", "120000,4,4,4,4,1,1,4"},
                                      "minute split across two files");
    }

    return ok ? 0 : 1;
}
