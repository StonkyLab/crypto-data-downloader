/**
Candle Aggregator - builds coarser bar files from a 1-minute CSV dataset

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_CANDLE_AGGREGATOR_H
#define INCLUDE_STONKY_CANDLE_AGGREGATOR_H

#include <cstdint>
#include <string>
#include <vector>

namespace stonky {

/**
 * Local, network-free aggregation of 1-minute CSV bar files into coarser
 * timeframes.
 *
 * OKX only publishes 1-minute bars in its bulk history archive, so every
 * higher timeframe has to be derived locally. The aggregation is exchange
 * agnostic: columns are resolved by header name, so the output file carries
 * exactly the same columns as its source.
 *
 * Historical exchange outages are localized: an incomplete, absent or unsafe
 * source bucket is omitted without preventing complete buckets on either side
 * from being published. Report::incompleteBuckets records the degradation so
 * callers can distinguish a complete result from one with holes.
 * To prevent a forward timestamp outlier from poisoning a long valid suffix,
 * each source is read twice and a compact timestamp/keep index is retained for
 * the second pass; rendered CSV rows themselves are streamed to disk.
 *
 * OHLC values are carried through as the ORIGINAL text of the contributing
 * source rows (open of the first minute, close of the last, text of the min/max
 * rows). Every numeric input and aggregate sum must fit the project's binary64
 * storage contract within 0.001 %; damage is localized to its target bucket
 * instead of aborting unrelated symbols or years.
 */
class CandleAggregator {
public:
    struct Options {
        /// Source timeframe in minutes; must divide every target timeframe
        std::int32_t sourceMinutes{1};
        /// Target timeframes in minutes, e.g. {5, 60}
        std::vector<std::int32_t> targetMinutes;
        /// Maximum number of symbols aggregated in parallel
        std::uint32_t maxJobs{1};
        /// Atomically rebuild target files from scratch instead of appending after their tail.
        /// Rewrites and appends are cross-process serialized per target file.
        bool rewrite{false};
        /// Emit buckets even when one or more source bars are missing. Disabled
        /// by default because a partial bucket is indistinguishable from a
        /// complete OHLCV row in the resulting CSV.
        bool allowPartialBuckets{false};
    };

    struct Report {
        std::string symbol;
        std::int32_t targetMinutes{};
        std::int64_t barsWritten{};
        std::int64_t incompleteBuckets{};
        /// Structurally incomplete but numerically safe buckets emitted by
        /// explicit --allow_partial_aggregation request.
        std::int64_t partialBucketsWritten{};
        /// Incomplete buckets not written (unsafe, wholly absent, or partial
        /// output not explicitly enabled).
        std::int64_t omittedIncompleteBuckets{};
        bool failed{false};
        std::string error;
    };

    /**
     * Aggregate every `<pricesCsvDir>/<source>/*.csv` file into
     * `<pricesCsvDir>/<target>/` for each requested target timeframe.
     *
     * A wall-clock-current trailing bucket is never emitted. A trailing bucket
     * whose interval has already closed is treated like every other historical
     * gap: omitted by default or emitted only by explicit partial mode.
     *
     * @param pricesCsvDir directory holding the per-timeframe subdirectories
     *                     (e.g. `<out>/futures/prices/csv`)
     */
    static std::vector<Report> aggregateDirectory(const std::string &pricesCsvDir, const Options &options);
};

} // namespace stonky

#endif // INCLUDE_STONKY_CANDLE_AGGREGATOR_H
