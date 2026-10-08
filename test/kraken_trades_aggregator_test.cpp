#include "stonky/kraken_trades_aggregator.h"
#include "stonky/csv_format.h"

#include <iostream>
#include <string>
#include <tuple>
#include <vector>

namespace {

using stonky::KrakenTradesAggregator;
using stonky::kraken::Candle;
using stonky::kraken::Trade;

constexpr std::int64_t MINUTE = KrakenTradesAggregator::MINUTE_MS;
constexpr std::int64_t T0 = 1704067200000; // 2024-01-01 00:00:00 UTC

Trade trade(const std::int64_t id, const std::int64_t timeMs, const char *price, const char *volume) {
    Trade t;
    t.tradeId = id;
    t.timeMs = timeMs;
    t.price = stonky::kraken::Decimal{price};
    t.volume = stonky::kraken::Decimal{volume};
    t.side = 'b';
    t.orderType = 'l';
    return t;
}

std::string row(const Candle &bar) {
    return std::to_string(bar.openTime) + "," + stonky::csvNumber(bar.open) + "," + stonky::csvNumber(bar.high) +
           "," + stonky::csvNumber(bar.low) + "," + stonky::csvNumber(bar.close) + "," +
           stonky::csvNumber(bar.volume);
}

bool expect(const std::vector<Candle> &bars, const std::vector<std::string> &want, const char *what) {
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

std::string ts(const int minutes) { return std::to_string(T0 + minutes * MINUTE); }

} // namespace

int main() {
    bool ok = true;

    // Three trades in one minute, nothing for two minutes, one trade in the
    // fourth: the first bar closes when the later trade arrives, the two
    // empty minutes are flat at its close, the fourth minute stays pending.
    {
        KrakenTradesAggregator fold;
        fold.resume(-1, std::nullopt);
        const auto closed = fold.feed({
            trade(1, T0 + 1000, "100.5", "0.1"),
            trade(2, T0 + 20000, "102", "0.2"),
            trade(3, T0 + 59999, "99.5", "0.3"),
            trade(4, T0 + 3 * MINUTE + 500, "101", "1"),
        });
        ok = expect(closed, {ts(0) + ",100.5,102,99.5,99.5,0.6", ts(1) + ",99.5,99.5,99.5,99.5,0",
                             ts(2) + ",99.5,99.5,99.5,99.5,0"},
                    "first page") && ok;

        // The still-open fourth minute is not emitted by finish() ...
        ok = expect(fold.finish(T0 + 3 * MINUTE + 1), {}, "finish inside the open minute") && ok;
        // ... but is once a whole minute has passed, with the flat fill after it.
        ok = expect(fold.finish(T0 + 6 * MINUTE),
                    {ts(3) + ",101,101,101,101,1", ts(4) + ",101,101,101,101,0", ts(5) + ",101,101,101,101,0"},
                    "finish after the minute closed") && ok;
    }

    // The next page repeats the cursor's trade: it must not be counted twice.
    {
        KrakenTradesAggregator fold;
        fold.resume(-1, std::nullopt);
        std::ignore = fold.feed({trade(10, T0 + 100, "5", "1"), trade(11, T0 + 200, "6", "2")});
        const auto closed = fold.feed({trade(11, T0 + 200, "6", "2"), trade(12, T0 + MINUTE + 100, "7", "1")});
        ok = expect(closed, {ts(0) + ",5,6,5,6,3"}, "repeated cursor trade") && ok;
    }

    // Resuming from a persisted tail: bars at or before it are skipped, the
    // empty minutes after it are filled from the persisted close.
    {
        KrakenTradesAggregator fold;
        fold.resume(T0, 42.0);
        const auto closed = fold.feed({
            trade(1, T0 + 30000, "41", "1"),               // inside the persisted minute: ignored
            trade(2, T0 + 3 * MINUTE + 10, "43", "0.5"),
            trade(3, T0 + 4 * MINUTE + 10, "44", "0.25"),
        });
        ok = expect(closed, {ts(1) + ",42,42,42,42,0", ts(2) + ",42,42,42,42,0", ts(3) + ",43,43,43,43,0.5"},
                    "resume from tail") && ok;
    }

    // A fresh pair starts at its first trade: nothing is filled before it.
    {
        KrakenTradesAggregator fold;
        fold.resume(T0 - MINUTE, std::nullopt);
        const auto closed = fold.feed({trade(1, T0 + 5 * MINUTE, "1", "1"), trade(2, T0 + 6 * MINUTE, "2", "1")});
        ok = expect(closed, {ts(5) + ",1,1,1,1,1"}, "fresh pair") && ok;
    }

    // A tape that steps back across a minute is refused rather than folded wrong.
    {
        KrakenTradesAggregator fold;
        fold.resume(-1, std::nullopt);
        bool threw = false;
        try {
            std::ignore = fold.feed({trade(1, T0 + 2 * MINUTE, "1", "1"), trade(2, T0 + MINUTE, "1", "1")});
        } catch (const std::runtime_error &) {
            threw = true;
        }
        if (!threw) {
            std::cerr << "a backwards tape was accepted\n";
            ok = false;
        }
    }

    if (!ok) {
        return 1;
    }
    std::cout << "kraken trades fold: ok\n";
    return 0;
}
