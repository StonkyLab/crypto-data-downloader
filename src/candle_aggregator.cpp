/**
Candle Aggregator - builds coarser bar files from a 1-minute CSV dataset

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/candle_aggregator.h"
#include "stonky/atomic_file.h"
#include "stonky/csv_format.h"
#include "stonky/interface/exchange_enums.h" // downloader.h expects CandleInterval to be declared
#include "stonky/downloader.h"
#include "stonky/future_utils.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/semaphore.h"
#include <algorithm>
#include <array>
#include <boost/math/special_functions/fpclassify.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>
#include <charconv>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <future>
#include <limits>
#include <optional>
#include <set>
#include <spdlog/spdlog.h>
#include <system_error>

namespace stonky {

namespace {
constexpr std::int64_t MS_PER_MINUTE = 60000;

bool checkedAdd(const std::int64_t lhs, const std::int64_t rhs, std::int64_t &result) {
    if ((rhs > 0 && lhs > std::numeric_limits<std::int64_t>::max() - rhs) ||
        (rhs < 0 && lhs < std::numeric_limits<std::int64_t>::min() - rhs)) {
        return false;
    }
    result = lhs + rhs;
    return true;
}

bool checkedSubtract(const std::int64_t lhs, const std::int64_t rhs, std::int64_t &result) {
    if (rhs == std::numeric_limits<std::int64_t>::min()) {
        return false;
    }
    return checkedAdd(lhs, -rhs, result);
}

/// Per-column handling inside a bucket
enum class Role { TimeOpen, TimeClose, Open, High, Low, Close, Sum, Last };

/// Extra value columns known to be additive across a bucket. A source column
/// outside this set (and outside the OHLC set) is rejected rather than guessed
/// at — silently summing something like `close_time` would corrupt the output.
const std::set<std::string> &summableColumns() {
    static const std::set<std::string> cols{
        "volume", "vol", "vol_ccy", "vol_ccy_quote", "vol_quote",
        "quote_volume", "quote_asset_volume", "quote_av", "turnover", "amount",
        "trades", "num_trades", "tb_base_av", "tb_quote_av"
    };
    return cols;
}

bool parseInt64(const std::string &field, std::int64_t &out) {
    if (field.empty()) {
        return false;
    }
    const auto [ptr, ec] = std::from_chars(field.data(), field.data() + field.size(), out);
    return ec == std::errc() && ptr == field.data() + field.size();
}

using Decimal = boost::multiprecision::cpp_dec_float_50;

/**
 * Market-data cells are stored for a binary64 backtest pipeline. Reject a
 * decimal which cannot survive that conversion within the documented 0.001 %
 * tolerance. In particular this catches finite multiprecision values such as
 * 1e1000 (double overflow) and tiny non-zero values which would underflow to
 * zero instead of letting them escape as Inf/0 or throw while rendering.
 */
bool isBinary64Compatible(const Decimal &value) {
    if (!boost::math::isfinite(value)) {
        return false;
    }

    static const Decimal MAX_BINARY64{std::numeric_limits<double>::max()};
    const auto absoluteValue = boost::multiprecision::abs(value);
    if (absoluteValue > MAX_BINARY64) {
        return false;
    }

    double narrowed{};
    try {
        narrowed = value.convert_to<double>();
    } catch (const std::exception &) {
        return false;
    }
    if (!std::isfinite(narrowed)) {
        return false;
    }
    if (value == 0) {
        return narrowed == 0;
    }
    if (narrowed == 0) {
        return false;
    }

    // Every normal binary64 conversion is many orders of magnitude inside the
    // 0.001 % contract. Only subnormals need the relatively expensive explicit
    // multiprecision error check, keeping the hot path over multi-year files
    // cheap.
    if (std::abs(narrowed) >= std::numeric_limits<double>::min()) {
        return true;
    }

    static const Decimal MAX_RELATIVE_ERROR{"0.00001"}; // 0.001 %
    const Decimal restored{narrowed};
    const auto relativeError = boost::multiprecision::abs((restored - value) / value);
    return relativeError <= MAX_RELATIVE_ERROR;
}

bool parseDecimal(const std::string &field, Decimal &out) {
    // Require the whole field to be a conventional finite decimal/scientific
    // lexeme before handing it to multiprecision (which otherwise accepts some
    // prefixes). This keeps NaN/Inf and glued text out of aggregate output.
    std::size_t i = 0;
    if (i < field.size() && (field[i] == '+' || field[i] == '-')) {
        ++i;
    }
    bool haveDigit = false;
    while (i < field.size() && field[i] >= '0' && field[i] <= '9') {
        haveDigit = true;
        ++i;
    }
    if (i < field.size() && field[i] == '.') {
        ++i;
        while (i < field.size() && field[i] >= '0' && field[i] <= '9') {
            haveDigit = true;
            ++i;
        }
    }
    if (!haveDigit) {
        return false;
    }
    if (i < field.size() && (field[i] == 'e' || field[i] == 'E')) {
        ++i;
        if (i < field.size() && (field[i] == '+' || field[i] == '-')) {
            ++i;
        }
        const auto exponentStart = i;
        while (i < field.size() && field[i] >= '0' && field[i] <= '9') {
            ++i;
        }
        if (i == exponentStart) {
            return false;
        }
    }
    if (i != field.size()) {
        return false;
    }

    try {
        out = Decimal(field);
        return isBinary64Compatible(out);
    } catch (const std::exception &) {
        return false;
    }
}

/// Accumulate additive columns in decimal to avoid per-row rounding, then store
/// the final value using the project's shortest round-tripping binary64 text.
std::string formatSum(const Decimal &value) {
    return csvNumber(value);
}

struct Layout {
    std::vector<Role> roles;
    std::size_t timeIdx{};
    std::size_t highIdx{};
    std::size_t lowIdx{};
    bool primaryTimeIsClose{};
};

/// Resolve column roles from the source header. Throws when the header is not
/// an OHLCV layout this aggregator can reason about.
Layout resolveLayout(const std::string &header) {
    auto names = splitString(header, ',');
    for (auto &name: names) {
        if (!name.empty() && name.back() == '\r') {
            name.pop_back();
        }
    }
    Layout layout;
    layout.roles.reserve(names.size());

    bool haveTime = false, haveOpen = false, haveHigh = false, haveLow = false, haveClose = false;
    const bool hasOpenTimeColumn = std::ranges::find(names, "open_time") != names.end() ||
                                   std::ranges::find(names, "time") != names.end() ||
                                   std::ranges::find(names, "timestamp") != names.end();

    for (std::size_t i = 0; i < names.size(); ++i) {
        const auto &name = names[i];

        if (name == "open_time" || name == "time" || name == "timestamp") {
            layout.roles.push_back(Role::TimeOpen);
            layout.timeIdx = i;
            haveTime = true;
            layout.primaryTimeIsClose = false;
        } else if (name == "close_time") {
            layout.roles.push_back(Role::TimeClose);
            // Binance also carries its exact open time in `timestamp`; prefer
            // that for bucket membership while keeping close_time in column 0.
            if (!hasOpenTimeColumn) {
                layout.timeIdx = i;
                haveTime = true;
                layout.primaryTimeIsClose = true;
            }
        } else if (name == "open") {
            layout.roles.push_back(Role::Open);
            haveOpen = true;
        } else if (name == "high") {
            layout.roles.push_back(Role::High);
            layout.highIdx = i;
            haveHigh = true;
        } else if (name == "low") {
            layout.roles.push_back(Role::Low);
            layout.lowIdx = i;
            haveLow = true;
        } else if (name == "close") {
            layout.roles.push_back(Role::Close);
            haveClose = true;
        } else if (summableColumns().contains(name)) {
            layout.roles.push_back(Role::Sum);
        } else if (name == "ignore") {
            // Binance's reserved final column is not additive. Preserve the
            // value from the final contributing source row.
            layout.roles.push_back(Role::Last);
        } else {
            throw std::runtime_error(fmt::format("unsupported column '{}' in header '{}'", name, header));
        }
    }

    if (!haveTime || !haveOpen || !haveHigh || !haveLow || !haveClose) {
        throw std::runtime_error(fmt::format("header is not an OHLCV layout: '{}'", header));
    }
    if (layout.roles.empty() ||
        (layout.roles.front() != Role::TimeOpen && layout.roles.front() != Role::TimeClose)) {
        throw std::runtime_error("the first CSV column must be an open/close timestamp");
    }
    return layout;
}

/// One bucket under construction. OHLC are the verbatim source strings of the
/// contributing rows (no parse/format round trip, so no precision is lost on
/// assets quoted with many decimals); only Sum columns are accumulated.
struct Bucket {
    std::int64_t start{};
    std::int64_t firstTs{-1};
    std::int64_t lastTs{-1};
    std::int64_t rows{};
    std::vector<std::string> fields; // output row, indexed like the source
    std::vector<Decimal> sums;
    Decimal high{};
    Decimal low{};
    bool contiguous{true};
    bool valid{true};
    bool empty{true};
};

struct BucketRange {
    std::int64_t first{};
    std::int64_t last{};
};

/**
 * Mark a longest strictly increasing timestamp subsequence. This is a
 * deterministic minimum-discard repair for local ordering corruption: in
 * 0..4,100,5..99 it rejects only the early 100 instead of letting that single
 * forward jump suppress every legitimate row from 5 through 99.
 *
 * The caller retains one int64 timestamp and one packed bit per usable source
 * row. The larger predecessor/tail scratch vectors are local to this function
 * and are released before the aggregation/output pass begins.
 */
std::vector<bool> selectLongestIncreasingTimestamps(
    const std::vector<std::int64_t> &timestamps) {
    std::vector<bool> keep(timestamps.size(), false);
    if (timestamps.empty()) {
        return keep;
    }

    {
        constexpr auto NONE = std::numeric_limits<std::size_t>::max();
        std::vector<std::size_t> tailIndices;
        std::vector<std::size_t> predecessors(timestamps.size(), NONE);
        tailIndices.reserve(timestamps.size());

        for (std::size_t i = 0; i < timestamps.size(); ++i) {
            const auto position = std::lower_bound(
                tailIndices.begin(), tailIndices.end(), timestamps[i],
                [&](const std::size_t tailIndex, const std::int64_t value) {
                    return timestamps[tailIndex] < value;
                });
            const auto length = static_cast<std::size_t>(position - tailIndices.begin());
            if (length > 0) {
                predecessors[i] = tailIndices[length - 1];
            }
            if (position == tailIndices.end()) {
                tailIndices.push_back(i);
            } else {
                *position = i;
            }
        }

        for (auto index = tailIndices.back(); index != NONE; index = predecessors[index]) {
            keep[index] = true;
        }
    }

    return keep;
}

bool applyRow(Bucket &bucket, const Layout &layout, const std::vector<std::string> &fields,
              const std::int64_t ts, const std::int64_t bucketStart, const std::int64_t sourceMs) {
    std::vector<Decimal> values(fields.size());
    bool rowValid = true;
    std::int64_t rowOpenTime = ts;
    if (layout.primaryTimeIsClose &&
        (!checkedSubtract(ts, sourceMs, rowOpenTime) ||
         !checkedAdd(rowOpenTime, 1, rowOpenTime))) {
        rowValid = false;
    }
    for (std::size_t i = 0; i < layout.roles.size(); ++i) {
        if (layout.roles[i] == Role::TimeOpen || layout.roles[i] == Role::TimeClose) {
            std::int64_t fieldTime{};
            std::int64_t expected = rowOpenTime;
            if (layout.roles[i] == Role::TimeClose &&
                (!checkedAdd(rowOpenTime, sourceMs, expected) ||
                 !checkedSubtract(expected, 1, expected))) {
                rowValid = false;
            }
            if (!parseInt64(fields[i], fieldTime) || fieldTime != expected) {
                rowValid = false;
            }
        } else if (!parseDecimal(fields[i], values[i])) {
            rowValid = false;
        }
    }

    bucket.valid = bucket.valid && rowValid;

    if (bucket.empty) {
        bucket.start = bucketStart;
        bucket.firstTs = ts;
        bucket.fields = fields;
        bucket.sums.assign(fields.size(), Decimal{0});
        bucket.empty = false;
        bucket.high = values[layout.highIdx];
        bucket.low = values[layout.lowIdx];
    } else {
        if (ts != bucket.lastTs + sourceMs) {
            bucket.contiguous = false;
        }
        for (std::size_t i = 0; i < layout.roles.size(); ++i) {
            switch (layout.roles[i]) {
                case Role::High: {
                    if (bucket.valid && values[i] > bucket.high) {
                        bucket.high = values[i];
                        bucket.fields[i] = fields[i];
                    }
                    break;
                }
                case Role::Low: {
                    if (bucket.valid && values[i] < bucket.low) {
                        bucket.low = values[i];
                        bucket.fields[i] = fields[i];
                    }
                    break;
                }
                case Role::Close:
                case Role::Last:
                    bucket.fields[i] = fields[i]; // rows arrive in ascending ts order
                    break;
                default:
                    break;
            }
        }
    }

    for (std::size_t i = 0; i < layout.roles.size(); ++i) {
        if (layout.roles[i] == Role::Sum) {
            if (bucket.valid) {
                bucket.sums[i] += values[i];
            }
        }
    }
    bucket.lastTs = ts;
    ++bucket.rows;
    return rowValid;
}

bool sumsFitBinary64(const Bucket &bucket, const Layout &layout) {
    for (std::size_t i = 0; i < layout.roles.size(); ++i) {
        if (layout.roles[i] == Role::Sum && !isBinary64Compatible(bucket.sums[i])) {
            return false;
        }
    }
    return true;
}

std::string renderBucket(const Bucket &bucket, const Layout &layout, const std::int64_t bucketMs) {
    std::string out;
    for (std::size_t i = 0; i < bucket.fields.size(); ++i) {
        if (i > 0) {
            out.push_back(',');
        }
        if (layout.roles[i] == Role::TimeOpen) {
            out += std::to_string(bucket.start);
        } else if (layout.roles[i] == Role::TimeClose) {
            std::int64_t closeTime{};
            if (!checkedAdd(bucket.start, bucketMs, closeTime) ||
                !checkedSubtract(closeTime, 1, closeTime)) {
                throw std::overflow_error("aggregate close timestamp is outside int64 range");
            }
            out += std::to_string(closeTime);
        } else if (layout.roles[i] == Role::Sum) {
            out += formatSum(bucket.sums[i]);
        } else {
            out += bucket.fields[i];
        }
    }
    return out;
}

std::int64_t currentUnixMilliseconds() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool sourceTimestampIsSafe(const std::int64_t ts, const Layout &layout,
                           const std::int64_t sourceMs,
                           const std::int64_t maximumAcceptedTimestamp) {
    if (ts < 0 || ts > maximumAcceptedTimestamp) {
        return false;
    }
    const auto remainder = ts % sourceMs;
    return layout.primaryTimeIsClose ? remainder == sourceMs - 1 : remainder == 0;
}

bool bucketStartForTimestamp(const std::int64_t ts, const std::int64_t bucketMs,
                             std::int64_t &bucketStart) {
    if (ts < 0 || bucketMs <= 0) {
        return false;
    }
    bucketStart = ts - ts % bucketMs;
    return true;
}

bool targetTimestampToBucketStart(const std::int64_t ts, const Role firstRole,
                                  const std::int64_t bucketMs,
                                  const std::int64_t maximumAcceptedTimestamp,
                                  std::int64_t &bucketStart) {
    if (ts < 0 || ts > maximumAcceptedTimestamp) {
        return false;
    }
    if (firstRole == Role::TimeClose) {
        std::int64_t adjustment{};
        if (!checkedSubtract(bucketMs, 1, adjustment) ||
            !checkedSubtract(ts, adjustment, bucketStart)) {
            return false;
        }
    } else {
        bucketStart = ts;
    }
    return bucketStart >= 0 && bucketStart % bucketMs == 0;
}

struct AppendBase {
    std::int64_t resumeAfter{-1};
    std::uintmax_t validBytes{};
    bool needsHeader{true};
};

std::vector<std::int64_t> readTargetBucketStarts(
    const std::filesystem::path &path,
    const std::string &expectedHeader,
    const std::size_t expectedFields,
    const Role firstRole,
    const std::int64_t bucketMs,
    const std::int64_t maximumAcceptedTimestamp) {
    std::vector<std::int64_t> buckets;
    std::error_code ec;
    const bool exists = std::filesystem::exists(path, ec);
    if (ec) {
        throw std::filesystem::filesystem_error("cannot inspect aggregate target", path, ec);
    }
    if (!exists) {
        return buckets;
    }

    std::ifstream input(path);
    if (!input.is_open()) {
        throw std::runtime_error("cannot open existing aggregate target");
    }

    std::string line;
    if (!std::getline(input, line)) {
        return buckets;
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    if (line != expectedHeader) {
        throw std::runtime_error("existing aggregate target header differs from its source");
    }
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        const auto fields = splitString(line, ',');
        if (fields.size() != expectedFields) {
            throw std::runtime_error("existing aggregate target contains a malformed row");
        }
        std::int64_t timestamp{};
        std::int64_t bucketStart{};
        if (!parseInt64(fields.front(), timestamp) ||
            !targetTimestampToBucketStart(timestamp, firstRole, bucketMs,
                                          maximumAcceptedTimestamp, bucketStart)) {
            throw std::runtime_error("existing aggregate target contains an invalid timestamp");
        }
        buckets.push_back(bucketStart);
    }
    if (input.bad()) {
        throw std::runtime_error("failed while reading existing aggregate target");
    }
    std::ranges::sort(buckets);
    buckets.erase(std::unique(buckets.begin(), buckets.end()), buckets.end());
    return buckets;
}

AppendBase inspectAppendBase(const std::filesystem::path &path,
                             const std::string &expectedHeader,
                             const std::size_t expectedFields,
                             const Role firstRole,
                             const std::int64_t bucketMs,
                             const std::int64_t maximumAcceptedTimestamp) {
    AppendBase base;
    std::error_code ec;
    const bool exists = std::filesystem::exists(path, ec);
    if (ec) {
        throw std::filesystem::filesystem_error("cannot inspect aggregate append target", path, ec);
    }
    if (!exists) {
        return base;
    }
    const auto fileSize = std::filesystem::file_size(path, ec);
    if (ec) {
        throw std::filesystem::filesystem_error("cannot determine aggregate append target size", path, ec);
    }
    if (fileSize == 0) {
        return base;
    }

    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        throw std::runtime_error("cannot open existing aggregate append target");
    }

    std::string line;
    if (!std::getline(input, line) || input.eof()) {
        throw std::runtime_error("existing aggregate target has an unterminated header");
    }
    const auto headerEnd = input.tellg();
    if (headerEnd < 0) {
        throw std::runtime_error("cannot locate existing aggregate target header boundary");
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    if (line != expectedHeader) {
        throw std::runtime_error("existing aggregate target header differs from its source");
    }
    base.needsHeader = false;
    base.validBytes = static_cast<std::uintmax_t>(headerEnd);

    while (std::getline(input, line)) {
        // getline sets eofbit when it consumes an unterminated final fragment.
        // Such a fragment and any invalid terminated lines after the last valid
        // record stay outside validBytes and are removed only by a successful
        // atomic commit.
        if (input.eof()) {
            break;
        }
        const auto recordEnd = input.tellg();
        if (recordEnd < 0) {
            throw std::runtime_error("cannot locate aggregate target record boundary");
        }
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        const auto fields = splitString(line, ',');
        std::int64_t timestamp{};
        std::int64_t bucketStart{};
        if (fields.size() != expectedFields || !parseInt64(fields.front(), timestamp) ||
            !targetTimestampToBucketStart(timestamp, firstRole, bucketMs,
                                          maximumAcceptedTimestamp, bucketStart) ||
            bucketStart <= base.resumeAfter) {
            break;
        }
        base.resumeAfter = bucketStart;
        base.validBytes = static_cast<std::uintmax_t>(recordEnd);
    }
    if (input.bad()) {
        throw std::runtime_error("failed while inspecting aggregate append target");
    }
    return base;
}

void copyFilePrefix(const std::filesystem::path &path, const std::uintmax_t bytes,
                    std::ofstream &output) {
    if (bytes == 0) {
        return;
    }
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        throw std::runtime_error("cannot reopen aggregate append target");
    }

    std::array<char, 64 * 1024> buffer{};
    std::uintmax_t remaining = bytes;
    while (remaining > 0) {
        const auto chunk = static_cast<std::streamsize>(
            std::min<std::uintmax_t>(remaining, buffer.size()));
        input.read(buffer.data(), chunk);
        if (input.gcount() != chunk) {
            throw std::runtime_error("short read while copying aggregate append target");
        }
        output.write(buffer.data(), chunk);
        if (!output.good()) {
            throw std::runtime_error("failed while copying aggregate append target");
        }
        remaining -= static_cast<std::uintmax_t>(chunk);
    }
}

CandleAggregator::Report aggregateFile(const std::filesystem::path &sourcePath,
                                       const std::filesystem::path &targetPath,
                                       const std::int32_t sourceMinutes,
                                       const std::int32_t targetMinutes,
                                       const bool rewrite,
                                       const bool allowPartialBuckets) {
    CandleAggregator::Report report;
    report.symbol = sourcePath.stem().string();
    report.targetMinutes = targetMinutes;

    std::ifstream ifs(sourcePath);
    if (!ifs.is_open()) {
        report.failed = true;
        report.error = "cannot open source file";
        return report;
    }

    std::string header;
    if (!std::getline(ifs, header)) {
        report.failed = true;
        report.error = "source file is empty";
        return report;
    }
    if (!header.empty() && header.back() == '\r') {
        header.pop_back();
    }

    Layout layout;
    try {
        layout = resolveLayout(header);
    } catch (const std::exception &e) {
        report.failed = true;
        report.error = e.what();
        return report;
    }

    if (sourceMinutes <= 0 || targetMinutes <= sourceMinutes ||
        targetMinutes % sourceMinutes != 0) {
        report.failed = true;
        report.error = "invalid source/target aggregation interval";
        return report;
    }

    const std::int64_t bucketMs = static_cast<std::int64_t>(targetMinutes) * MS_PER_MINUTE;
    const std::int64_t sourceMs = static_cast<std::int64_t>(sourceMinutes) * MS_PER_MINUTE;
    const auto nowMs = currentUnixMilliseconds();
    std::int64_t maximumAcceptedTimestamp{};
    if (!checkedAdd(nowMs, bucketMs, maximumAcceptedTimestamp)) {
        maximumAcceptedTimestamp = std::numeric_limits<std::int64_t>::max();
    }
    const auto dataStart = ifs.tellg();
    if (dataStart < 0) {
        report.failed = true;
        report.error = "source header is not newline-terminated";
        return report;
    }
    std::error_code sourceStatError;
    const auto sourceSizeBefore = std::filesystem::file_size(sourcePath, sourceStatError);
    if (sourceStatError) {
        report.failed = true;
        report.error = "cannot determine source file size";
        return report;
    }
    const auto sourceWriteTimeBefore = std::filesystem::last_write_time(sourcePath, sourceStatError);
    if (sourceStatError) {
        report.failed = true;
        report.error = "cannot determine source file modification time";
        return report;
    }

    // Hold the target's kernel lock for the complete operation. Both rewrite
    // and append build a sibling file and commit by atomic replacement, so a
    // failed scan/write never leaves a half-appended target.
    // Runs are serialized per exchange by the update scripts' flock, so no
    // per-target lock file is created next to the data.
    AtomicFileWriter targetOutput(targetPath, std::ios::binary, AtomicFileWriter::Locking::None);
    if (!targetOutput.isOpen()) {
        report.failed = true;
        report.error = targetOutput.error();
        return report;
    }

    // Resume point: only buckets strictly after the target's last valid bar are
    // emitted. In append mode copy only the non-torn prefix into the temporary
    // sibling; the original remains byte-for-byte untouched unless commit wins.
    std::int64_t resumeAfter = -1;
    if (!rewrite) {
        const auto appendBase = inspectAppendBase(targetPath, header, layout.roles.size(),
                                                  layout.roles.front(), bucketMs,
                                                  maximumAcceptedTimestamp);
        resumeAfter = appendBase.resumeAfter;
        if (appendBase.needsHeader) {
            targetOutput.stream() << header << "\n";
        } else {
            copyFilePrefix(targetPath, appendBase.validBytes, targetOutput.stream());
        }
    } else {
        targetOutput.stream() << header << "\n";
    }
    if (!targetOutput.stream().good()) {
        report.failed = true;
        report.error = "failed while preparing aggregate transaction";
        return report;
    }

    Bucket bucket;
    std::string line;
    std::int64_t prevTs = -1;
    bool sawValidRow = false;
    std::int64_t barsToCommit = 0;
    std::int64_t partialBarsToCommit = 0;
    std::vector<std::int64_t> outputBucketStarts;
    std::vector<std::int64_t> observedKeptBucketStarts;
    const auto expectedRows = bucketMs / sourceMs;

    // Unsafe buckets contain malformed, duplicate or out-of-order source
    // rows. They must never be rendered, even with --allow-partial. The first
    // pass preclassifies every explicitly timestamped/rejected row, which lets
    // the second pass stream completed buckets without a later rogue timestamp
    // retroactively invalidating already-buffered output.
    std::set<std::int64_t> unsafeBucketStarts;
    std::set<std::int64_t> incompleteBucketStarts;
    std::vector<BucketRange> whollyMissingRanges;
    const auto bucketStartFor = [bucketMs](const std::int64_t ts) {
        std::int64_t bucketStart{};
        if (!bucketStartForTimestamp(ts, bucketMs, bucketStart)) {
            throw std::overflow_error("source timestamp cannot be assigned to a target bucket");
        }
        return bucketStart;
    };

    const auto markIncomplete = [&](const std::int64_t bucketStart, const bool unsafe) {
        // Incremental aggregation scans the whole source.  Historical damage
        // at or before the existing target tail was already reported by the
        // run which advanced that tail and must not make every resume exit 2.
        if (bucketStart <= resumeAfter) {
            return;
        }
        incompleteBucketStarts.insert(bucketStart);
        if (unsafe) {
            unsafeBucketStarts.insert(bucketStart);
        }
    };

    const auto addWhollyMissingRange = [&](const std::int64_t previousStart,
                                           const std::int64_t nextStart) {
        std::int64_t first{};
        if (!checkedAdd(previousStart, bucketMs, first)) {
            throw std::overflow_error("target bucket range exceeds int64");
        }
        if (nextStart <= first) {
            return;
        }

        std::int64_t last{};
        if (!checkedSubtract(nextStart, bucketMs, last)) {
            throw std::overflow_error("target bucket range exceeds int64");
        }
        if (first <= resumeAfter) {
            const auto skipped = (resumeAfter - first) / bucketMs + 1;
            if (skipped > (std::numeric_limits<std::int64_t>::max() - first) / bucketMs) {
                throw std::overflow_error("target bucket resume range exceeds int64");
            }
            first += skipped * bucketMs;
        }
        if (first <= last) {
            whollyMissingRanges.push_back({first, last});
        }
    };

    const auto isWhollyMissing = [&](const std::int64_t bucketStart) {
        const auto it = std::upper_bound(whollyMissingRanges.begin(), whollyMissingRanges.end(), bucketStart,
                                         [](const std::int64_t value, const BucketRange &range) {
                                             return value < range.first;
                                         });
        if (it == whollyMissingRanges.begin()) {
            return false;
        }
        return bucketStart <= std::prev(it)->last;
    };

    const auto finishIncompleteCount = [&] {
        report.incompleteBuckets = 0;
        for (const auto &range: whollyMissingRanges) {
            report.incompleteBuckets += (range.last - range.first) / bucketMs + 1;
        }
        for (const auto bucketStart: incompleteBucketStarts) {
            if (!isWhollyMissing(bucketStart)) {
                ++report.incompleteBuckets;
            }
        }
    };

    const auto emit = [&] {
        if (!bucket.empty && bucket.start > resumeAfter) {
            std::int64_t expectedFirst = bucket.start;
            std::int64_t expectedLast{};
            const auto firstAdjustment = layout.primaryTimeIsClose ? sourceMs - 1 : 0;
            const auto lastAdjustment = layout.primaryTimeIsClose ? 1 : sourceMs;
            if (!checkedAdd(bucket.start, firstAdjustment, expectedFirst) ||
                !checkedAdd(bucket.start, bucketMs, expectedLast) ||
                !checkedSubtract(expectedLast, lastAdjustment, expectedLast)) {
                throw std::overflow_error("aggregate bucket boundary exceeds int64");
            }
            const bool structurallyComplete = bucket.contiguous && bucket.firstTs == expectedFirst &&
                                              bucket.lastTs == expectedLast &&
                                              bucket.rows == expectedRows;
            const bool unsafe = !bucket.valid || !sumsFitBinary64(bucket, layout) ||
                                unsafeBucketStarts.contains(bucket.start);
            const bool complete = !unsafe && structurallyComplete;
            // Partial mode relaxes only missing-source-bar completeness. It
            // never permits malformed numeric content into the target file.
            if (!unsafe && (structurallyComplete || allowPartialBuckets)) {
                targetOutput.stream() << renderBucket(bucket, layout, bucketMs) << "\n";
                if (!targetOutput.stream().good()) {
                    throw std::runtime_error("failed while writing aggregate transaction");
                }
                outputBucketStarts.push_back(bucket.start);
                if (!structurallyComplete) {
                    if (partialBarsToCommit == std::numeric_limits<std::int64_t>::max()) {
                        throw std::overflow_error("partial aggregate bar counter overflowed int64");
                    }
                    ++partialBarsToCommit;
                }
                if (barsToCommit == std::numeric_limits<std::int64_t>::max()) {
                    throw std::overflow_error("aggregate bar counter overflowed int64");
                }
                ++barsToCommit;
            }
            if (!complete) {
                markIncomplete(bucket.start, unsafe);
            }
        }
        bucket = Bucket{};
    };

    const auto locateExplicitRowTimestamp = [&](const std::vector<std::string> &fields,
                                                std::int64_t &ts) {
        if (layout.timeIdx < fields.size() && parseInt64(fields[layout.timeIdx], ts) &&
            sourceTimestampIsSafe(ts, layout, sourceMs, maximumAcceptedTimestamp)) {
            return true;
        }

        // A torn Binance row may have lost its later `timestamp` column while
        // retaining the leading close_time.  The first column is guaranteed by
        // resolveLayout() to be a timestamp, so use it as a fallback.
        if (layout.timeIdx != 0 && !fields.empty() && parseInt64(fields.front(), ts)) {
            if (layout.roles.front() == Role::TimeClose) {
                std::int64_t adjustment{};
                if (!checkedSubtract(sourceMs, 1, adjustment) ||
                    !checkedSubtract(ts, adjustment, ts)) {
                    return false;
                }
            }
            if (sourceTimestampIsSafe(ts, layout, sourceMs, maximumAcceptedTimestamp)) {
                return true;
            }
        }

        return false;
    };

    const auto locateMalformedRow = [&](const std::vector<std::string> &fields,
                                        std::int64_t &ts) {
        if (locateExplicitRowTimestamp(fields, ts)) {
            return true;
        }

        // A row with an unreadable timestamp cannot be located exactly.  In a
        // chronological source its narrowest safe attribution is the next
        // expected source slot; this taints one target bucket rather than the
        // entire symbol.
        if (prevTs >= 0) {
            return checkedAdd(prevTs, sourceMs, ts) &&
                   sourceTimestampIsSafe(ts, layout, sourceMs, maximumAcceptedTimestamp);
        }
        return false;
    };

    // First of two sequential source passes: choose the largest chronological
    // subset before aggregating. A single forward outlier must not advance
    // prevTs and make a long suffix appear out-of-order. Exact-width rows with
    // a usable timestamp participate in the LIS; explicitly timestamped torn
    // rows are marked unsafe here so the output pass can stream completed
    // buckets without retaining every rendered CSV row in memory.
    std::vector<std::int64_t> timestampCandidates;
    while (std::getline(ifs, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        const auto fields = splitString(line, ',');
        if (fields.size() != layout.roles.size()) {
            std::int64_t malformedTs{};
            if (locateExplicitRowTimestamp(fields, malformedTs)) {
                markIncomplete(bucketStartFor(malformedTs), true);
            }
            continue;
        }
        std::int64_t ts{};
        if (parseInt64(fields[layout.timeIdx], ts) &&
            sourceTimestampIsSafe(ts, layout, sourceMs, maximumAcceptedTimestamp)) {
            timestampCandidates.push_back(ts);
        } else if (locateExplicitRowTimestamp(fields, ts)) {
            markIncomplete(bucketStartFor(ts), true);
        }
    }
    if (ifs.bad()) {
        report.failed = true;
        report.error = "failed during source ordering scan";
        return report;
    }
    const auto keepTimestampCandidates =
        selectLongestIncreasingTimestamps(timestampCandidates);
    for (std::size_t i = 0; i < timestampCandidates.size(); ++i) {
        if (!keepTimestampCandidates[i]) {
            markIncomplete(bucketStartFor(timestampCandidates[i]), true);
        }
    }
    ifs.clear();
    ifs.seekg(dataStart);
    if (!ifs.good()) {
        report.failed = true;
        report.error = "cannot rewind source file for aggregation";
        return report;
    }
    std::size_t timestampCandidateIndex = 0;

    while (std::getline(ifs, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }

        auto fields = splitString(line, ',');
        if (fields.size() != layout.roles.size()) {
            std::int64_t malformedTs{};
            if (locateMalformedRow(fields, malformedTs)) {
                markIncomplete(bucketStartFor(malformedTs), true);
            }
            continue;
        }

        std::int64_t ts = 0;
        if (!parseInt64(fields[layout.timeIdx], ts) ||
            !sourceTimestampIsSafe(ts, layout, sourceMs, maximumAcceptedTimestamp)) {
            if (locateMalformedRow(fields, ts)) {
                markIncomplete(bucketStartFor(ts), true);
            }
            continue;
        }
        if (timestampCandidateIndex >= timestampCandidates.size() ||
            timestampCandidates[timestampCandidateIndex] != ts) {
            report.failed = true;
            report.error = "source file changed during its ordering scan";
            return report;
        }
        const bool keepTimestamp = keepTimestampCandidates[timestampCandidateIndex++];
        if (!keepTimestamp) {
            markIncomplete(bucketStartFor(ts), true);
            continue;
        }
        if (ts <= prevTs) {
            markIncomplete(bucketStartFor(ts), true);
            continue; // keep scanning; only this row's target bucket is unsafe
        }
        prevTs = ts;

        const std::int64_t bucketStart = bucketStartFor(ts);
        if (observedKeptBucketStarts.empty() ||
            observedKeptBucketStarts.back() != bucketStart) {
            observedKeptBucketStarts.push_back(bucketStart);
        }
        if (!bucket.empty && bucketStart != bucket.start) {
            const auto previousBucketStart = bucket.start;
            emit();
            // A gap spanning complete target intervals is invisible from both
            // adjacent (internally complete) buckets. Record the absent range,
            // but never discard the valid data on either side of an exchange
            // outage.
            addWhollyMissingRange(previousBucketStart, bucketStart);
        }
        sawValidRow = applyRow(bucket, layout, fields, ts, bucketStart, sourceMs) || sawValidRow;
    }

    if (ifs.bad()) {
        report.failed = true;
        report.error = "failed while reading source file";
        return report;
    }
    sourceStatError.clear();
    const auto sourceSizeAfter = std::filesystem::file_size(sourcePath, sourceStatError);
    if (sourceStatError) {
        report.failed = true;
        report.error = "cannot recheck source file size";
        return report;
    }
    sourceStatError.clear();
    const auto sourceWriteTimeAfter = std::filesystem::last_write_time(sourcePath, sourceStatError);
    if (sourceStatError || timestampCandidateIndex != timestampCandidates.size() ||
        sourceSizeAfter != sourceSizeBefore || sourceWriteTimeAfter != sourceWriteTimeBefore) {
        report.failed = true;
        report.error = "source file changed during aggregation";
        return report;
    }

    if (!sawValidRow) {
        report.failed = true;
        report.error = "source file contains no valid data rows";
        return report;
    }

    // A bucket which ends in the past is closed even when the exchange omitted
    // its final source bars (common around outages and delistings). Count/omit
    // or explicitly render it like every other historical gap. Only the
    // wall-clock-current bucket is held back for a later run.
    if (!bucket.empty) {
        std::int64_t bucketEnd{};
        if (!checkedAdd(bucket.start, bucketMs, bucketEnd)) {
            report.failed = true;
            report.error = "trailing aggregate bucket exceeds int64 timestamp range";
            return report;
        }
        if (bucketEnd <= nowMs) {
            emit();
        }
    }

    finishIncompleteCount();
    report.partialBucketsWritten = partialBarsToCommit;
    report.omittedIncompleteBuckets = report.incompleteBuckets - report.partialBucketsWritten;

    // Never replace a target with a header-only file. No complete output is a
    // normal outcome for an in-progress source or for a source whose only
    // closed bucket is damaged; preserve any existing target and report the
    // latter through incompleteBuckets/CLI exit 2.
    if (barsToCommit == 0) {
        return report;
    }

    if (rewrite) {
        const auto oldBucketStarts = readTargetBucketStarts(
            targetPath, header, layout.roles.size(), layout.roles.front(), bucketMs,
            maximumAcceptedTimestamp);

        // Exact destructive-rewrite proof: every real row in the old target
        // must either survive in the new output, or belong to a bucket which a
        // LIS-kept source row explicitly observed and classified incomplete.
        // Empty timestamps inside the old min/max range are irrelevant: a gap
        // already absent from the target cannot block all future rewrites.
        for (const auto oldBucketStart: oldBucketStarts) {
            const bool retained = std::binary_search(outputBucketStarts.begin(),
                                                     outputBucketStarts.end(),
                                                     oldBucketStart);
            const bool observedDamaged =
                std::binary_search(observedKeptBucketStarts.begin(),
                                   observedKeptBucketStarts.end(), oldBucketStart) &&
                incompleteBucketStarts.contains(oldBucketStart);
            if (!retained && !observedDamaged) {
                report.failed = true;
                report.error = fmt::format(
                    "rebuilt target would discard unobserved existing bucket {}; existing file was preserved",
                    oldBucketStart);
                return report;
            }
        }
    }
    std::string commitError;
    if (!targetOutput.commit(commitError)) {
        report.failed = true;
        report.error = commitError;
        return report;
    }

    report.barsWritten = barsToCommit;
    return report;
}
} // namespace

std::vector<CandleAggregator::Report> CandleAggregator::aggregateDirectory(const std::string &pricesCsvDir,
                                                                          const Options &options) {
    std::vector<Report> reports;

    const auto addConfigurationFailure = [&](const std::int32_t target, std::string error) {
        Report report;
        report.symbol = "<configuration>";
        report.targetMinutes = target;
        report.failed = true;
        report.error = std::move(error);
        spdlog::error(report.error);
        reports.push_back(std::move(report));
    };

    std::string sourceLabel;
    try {
        if (options.sourceMinutes <= 0) {
            throw std::runtime_error("source interval must be positive");
        }
        sourceLabel = Downloader::minutesToString(options.sourceMinutes);
    } catch (const std::exception &e) {
        addConfigurationFailure(0, fmt::format("Invalid aggregation source interval {}: {}",
                                               options.sourceMinutes, e.what()));
        return reports;
    }

    std::filesystem::path sourceDir(pricesCsvDir);
    sourceDir.append(sourceLabel);

    if (!std::filesystem::exists(sourceDir)) {
        addConfigurationFailure(0, fmt::format("Source directory does not exist: {}", sourceDir.string()));
        return reports;
    }

    std::vector<std::filesystem::path> sourceFiles;
    for (const auto &entry: std::filesystem::directory_iterator(sourceDir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".csv") {
            sourceFiles.push_back(entry.path());
        }
    }
    std::ranges::sort(sourceFiles);

    Semaphore maxJobs{options.maxJobs > 0 ? options.maxJobs : 1};

    for (const auto target: options.targetMinutes) {
        if (target <= options.sourceMinutes || target % options.sourceMinutes != 0 || target == 43200) {
            addConfigurationFailure(
                target,
                fmt::format("Invalid target bar size {} m for fixed aggregation from {} m",
                            target, options.sourceMinutes));
            continue;
        }

        std::string targetLabel;
        try {
            targetLabel = Downloader::minutesToString(target);
        } catch (const std::exception &e) {
            addConfigurationFailure(target,
                                    fmt::format("Invalid aggregation target {} m: {}", target, e.what()));
            continue;
        }

        std::filesystem::path targetDir(pricesCsvDir);
        targetDir.append(targetLabel);

        if (const auto err = createDirectoryRecursively(targetDir.string())) {
            addConfigurationFailure(target, fmt::format("Failed to create {}, err: {}",
                                                        targetDir.string(), err.message()));
            continue;
        }

        spdlog::info(fmt::format("Aggregating {} symbols from {} to {}...", sourceFiles.size(),
                                 sourceLabel, targetLabel));

        std::vector<std::future<Report> > futures;
        futures.reserve(sourceFiles.size());

        for (const auto &sourceFile: sourceFiles) {
            std::filesystem::path targetFile = targetDir;
            targetFile.append(sourceFile.filename().string());

            futures.push_back(launchBounded(maxJobs,
                                         [&options, target](const std::filesystem::path &src,
                                                                      const std::filesystem::path &dst) -> Report {
                                             try {
                                                 return aggregateFile(src, dst, options.sourceMinutes, target,
                                                                      options.rewrite,
                                                                      options.allowPartialBuckets);
                                             } catch (const std::exception &e) {
                                                 Report report;
                                                 report.symbol = src.stem().string();
                                                 report.targetMinutes = target;
                                                 report.failed = true;
                                                 report.error = fmt::format("unexpected aggregation error: {}",
                                                                            e.what());
                                                 return report;
                                             } catch (...) {
                                                 Report report;
                                                 report.symbol = src.stem().string();
                                                 report.targetMinutes = target;
                                                 report.failed = true;
                                                 report.error = "unexpected non-standard aggregation error";
                                                 return report;
                                             }
                                         }, sourceFile, targetFile));
        }

        std::int64_t totalBars = 0;
        std::size_t failed = 0;
        for (std::size_t i = 0; i < futures.size(); ++i) {
            Report report;
            try {
                report = futures[i].get();
            } catch (const std::exception &e) {
                report.symbol = sourceFiles[i].stem().string();
                report.targetMinutes = target;
                report.failed = true;
                report.error = fmt::format("aggregation worker failed: {}", e.what());
            } catch (...) {
                report.symbol = sourceFiles[i].stem().string();
                report.targetMinutes = target;
                report.failed = true;
                report.error = "aggregation worker failed with a non-standard exception";
            }
            if (!report.failed &&
                (report.barsWritten < 0 ||
                 report.barsWritten > std::numeric_limits<std::int64_t>::max() - totalBars)) {
                report.failed = true;
                report.error = "aggregate bar counter overflowed int64";
                report.barsWritten = 0;
            }
            if (report.failed) {
                failed++;
                spdlog::error(fmt::format("Aggregation of {} to {} failed: {}", report.symbol,
                                          targetLabel, report.error));
            }
            if (!report.failed && report.incompleteBuckets > 0) {
                const auto outcome = report.barsWritten > 0
                                       ? "the remaining usable buckets were written"
                                       : "no aggregate rows were written";
                if (options.allowPartialBuckets) {
                    spdlog::warn(fmt::format(
                        "Aggregation of {} to {}: {} incomplete buckets ({} safe partial buckets emitted by "
                        "request, {} unsafe or wholly absent buckets omitted; {})",
                        report.symbol, targetLabel, report.incompleteBuckets,
                        report.partialBucketsWritten, report.omittedIncompleteBuckets, outcome));
                } else {
                    spdlog::warn(fmt::format(
                        "Aggregation of {} to {}: {} incomplete buckets omitted ({})",
                        report.symbol, targetLabel, report.incompleteBuckets, outcome));
                }
            }
            totalBars += report.barsWritten;
            reports.push_back(std::move(report));
        }

        spdlog::info(fmt::format("{}: {} bars written, {} symbols failed",
                                 targetLabel, totalBars, failed));
    }

    return reports;
}

} // namespace stonky
