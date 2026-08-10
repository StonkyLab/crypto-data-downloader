/**
Candle Aggregator - builds coarser bar files from a 1-minute CSV dataset

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/candle_aggregator.h"
#include "stonky/csv_data.h"
#include "stonky/csv_format.h"
#include "stonky/interface/exchange_enums.h" // downloader.h expects CandleInterval to be declared
#include "stonky/downloader.h"
#include "stonky/future_utils.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/semaphore.h"
#include <algorithm>
#include <boost/math/special_functions/fpclassify.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <future>
#include <set>
#include <spdlog/spdlog.h>
#include <system_error>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace stonky {

namespace {
constexpr std::int64_t MS_PER_MINUTE = 60000;

bool replaceFile(const std::filesystem::path &source,
                 const std::filesystem::path &destination,
                 std::string &error) {
#ifdef _WIN32
    if (!::MoveFileExW(source.c_str(), destination.c_str(),
                       MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        error = fmt::format("Windows error {}", static_cast<unsigned long>(::GetLastError()));
        return false;
    }
    return true;
#else
    std::error_code ec;
    std::filesystem::rename(source, destination, ec);
    if (ec) {
        error = ec.message();
        return false;
    }
    return true;
#endif
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
        return boost::math::isfinite(out);
    } catch (const std::exception &) {
        return false;
    }
}

/// Preserve the precision of additive decimal columns. Scientific notation is
/// valid CSV numeric syntax and is accepted by the project's CSV readers.
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

void applyRow(Bucket &bucket, const Layout &layout, const std::vector<std::string> &fields,
              const std::int64_t ts, const std::int64_t bucketStart, const std::int64_t sourceMs) {
    std::vector<Decimal> values(fields.size());
    const auto rowOpenTime = layout.primaryTimeIsClose ? ts - sourceMs + 1 : ts;
    for (std::size_t i = 0; i < layout.roles.size(); ++i) {
        if (layout.roles[i] == Role::TimeOpen || layout.roles[i] == Role::TimeClose) {
            std::int64_t fieldTime{};
            const auto expected = layout.roles[i] == Role::TimeOpen
                                      ? rowOpenTime
                                      : rowOpenTime + sourceMs - 1;
            if (!parseInt64(fields[i], fieldTime) || fieldTime != expected) {
                bucket.valid = false;
            }
        } else if (!parseDecimal(fields[i], values[i])) {
            bucket.valid = false;
        }
    }

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
            out += std::to_string(bucket.start + bucketMs - 1);
        } else if (layout.roles[i] == Role::Sum) {
            out += formatSum(bucket.sums[i]);
        } else {
            out += bucket.fields[i];
        }
    }
    return out;
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

    const std::int64_t bucketMs = static_cast<std::int64_t>(targetMinutes) * MS_PER_MINUTE;
    const std::int64_t sourceMs = static_cast<std::int64_t>(sourceMinutes) * MS_PER_MINUTE;

    // Resume point: only buckets strictly after the target's last bar are
    // emitted, which makes re-running idempotent.
    std::int64_t resumeAfter = -1;
    if (!rewrite && std::filesystem::exists(targetPath)) {
        if (const auto tail = CsvData::lastValidRecord(targetPath.string(), layout.roles.size(), -1);
            tail.foundValid) {
            resumeAfter = tail.timestamp;
        }
    }

    std::vector<std::string> outputRows;
    Bucket bucket;
    std::string line;
    std::int64_t prevTs = -1;
    bool sawData = false;
    const auto expectedRows = bucketMs / sourceMs;

    const auto emit = [&] {
        if (!bucket.empty && bucket.start > resumeAfter) {
            const auto expectedFirst = bucket.start + (layout.primaryTimeIsClose ? sourceMs - 1 : 0);
            const auto expectedLast = bucket.start + bucketMs -
                                      (layout.primaryTimeIsClose ? 1 : sourceMs);
            const bool structurallyComplete = bucket.contiguous && bucket.firstTs == expectedFirst &&
                                              bucket.lastTs == expectedLast &&
                                              bucket.rows == expectedRows;
            const bool complete = bucket.valid && structurallyComplete;
            // Partial mode relaxes only missing-source-bar completeness. It
            // never permits malformed numeric content into the target file.
            if (bucket.valid && (structurallyComplete || allowPartialBuckets)) {
                outputRows.push_back(renderBucket(bucket, layout, bucketMs));
            }
            if (!complete) {
                ++report.incompleteBuckets;
                // Malformed numbers mean the source file itself is untrustworthy,
                // so that still aborts the symbol.
                if (!bucket.valid) {
                    report.failed = true;
                    report.error = "one or more source buckets contain invalid numeric values";
                }
                // A merely INCOMPLETE bucket does not. Missing source bars are
                // exchange outages: they never fill, so abandoning the whole file
                // over them means the target never gets written at all. Measured
                // on the OKX futures set, 140 of 567 symbols carry at least one
                // such gap — BTC-USDT-SWAP loses 3 minutes in 4.9 years and used
                // to produce zero 5m and zero 1h bars because of it. The bucket
                // is omitted, everything else is written, and the count is
                // reported so the run still exits non-zero.
            }
        }
        bucket = Bucket{};
    };

    while (std::getline(ifs, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }

        auto fields = splitString(line, ',');
        if (fields.size() != layout.roles.size()) {
            continue; // torn/malformed row — `-y`/`-r` is the tool that reports those
        }

        std::int64_t ts = 0;
        if (!parseInt64(fields[layout.timeIdx], ts)) {
            continue;
        }
        if (ts <= prevTs) {
            continue; // duplicate or out-of-order row
        }
        prevTs = ts;
        sawData = true;

        const std::int64_t bucketStart = ts - ts % bucketMs;
        if (!bucket.empty && bucketStart != bucket.start) {
            const auto missingBuckets = (bucketStart - bucket.start) / bucketMs - 1;
            emit();
            // A gap that spans one or more complete target intervals is not
            // visible from either adjacent bucket: both can contain all of
            // their own source rows.  Account for those absent buckets
            // explicitly so strict aggregation cannot publish a target with a
            // silent coarse-timeframe hole.
            if (missingBuckets > 0 && bucketStart > resumeAfter) {
                report.incompleteBuckets += static_cast<std::int64_t>(missingBuckets);
                if (!allowPartialBuckets) {
                    report.failed = true;
                    report.error = "one or more complete target buckets are absent from the source";
                }
            }
        }
        applyRow(bucket, layout, fields, ts, bucketStart, sourceMs);
    }

    if (ifs.bad()) {
        report.failed = true;
        report.error = "failed while reading source file";
        return report;
    }

    if (!sawData) {
        report.failed = true;
        report.error = "source file contains no data rows";
        return report;
    }

    // The trailing bucket is written only once the source reaches its final
    // sub-interval; otherwise it would freeze as a partial bar that the next
    // incremental run could no longer complete.
    if (!bucket.empty && bucket.lastTs + sourceMs >= bucket.start + bucketMs) {
        emit();
    }

    // Strict mode is fail-closed: never append later buckets past an invalid
    // or incomplete bucket, because their new tail would make the skipped
    // interval impossible to fill on a subsequent incremental run.
    if (report.failed) {
        return report;
    }

    // A rewrite must never publish a header-only target. This happens when the
    // source contains only the still-open trailing bucket (for example three
    // one-minute rows requested as a five-minute file). Treat that as no
    // usable output and preserve any existing target.
    if (rewrite && outputRows.empty()) {
        report.failed = true;
        report.error = "source contains no complete target buckets";
        return report;
    }

    if (outputRows.empty() && !rewrite) {
        return report; // already up to date
    }

    if (rewrite && std::filesystem::exists(targetPath)) {
        const auto oldTail = CsvData::lastValidRecord(targetPath.string(), layout.roles.size(), -1);
        if (oldTail.foundValid) {
            std::int64_t newTail = -1;
            if (!outputRows.empty()) {
                const auto lastFields = splitString(outputRows.back(), ',');
                (void) parseInt64(lastFields[0], newTail);
            }
            if (newTail < oldTail.timestamp) {
                report.failed = true;
                report.error = "rebuilt target would move backwards; existing file was preserved";
                return report;
            }
        }
    }

    const bool writeHeader = rewrite || !std::filesystem::exists(targetPath) ||
                             std::filesystem::file_size(targetPath) == 0;

    auto outputPath = targetPath;
    if (rewrite) {
        outputPath += ".aggregate.tmp";
    }
    std::ofstream ofs(outputPath, rewrite ? std::ios::trunc : std::ios::app);
    if (!ofs.is_open()) {
        report.failed = true;
        report.error = "cannot open target file";
        return report;
    }
    if (writeHeader) {
        ofs << header << "\n";
    }
    for (const auto &row: outputRows) {
        ofs << row << "\n";
    }
    ofs.flush();
    if (!ofs.good()) {
        report.failed = true;
        report.error = "write to target file failed";
        ofs.close();
        if (rewrite) {
            std::error_code ec;
            std::filesystem::remove(outputPath, ec);
        }
        return report;
    }
    ofs.close();
    if (!ofs.good()) {
        report.failed = true;
        report.error = "closing target file failed";
        if (rewrite) {
            std::error_code ec;
            std::filesystem::remove(outputPath, ec);
        }
        return report;
    }

    if (rewrite) {
        std::string replaceError;
        if (!replaceFile(outputPath, targetPath, replaceError)) {
            report.failed = true;
            report.error = fmt::format("cannot atomically replace target file: {}", replaceError);
            std::error_code ec;
            std::filesystem::remove(outputPath, ec);
            return report;
        }
    }

    report.barsWritten = static_cast<std::int64_t>(outputRows.size());
    return report;
}
} // namespace

std::vector<CandleAggregator::Report> CandleAggregator::aggregateDirectory(const std::string &pricesCsvDir,
                                                                          const Options &options) {
    std::vector<Report> reports;

    std::filesystem::path sourceDir(pricesCsvDir);
    sourceDir.append(Downloader::minutesToString(options.sourceMinutes));

    if (!std::filesystem::exists(sourceDir)) {
        spdlog::error(fmt::format("Source directory does not exist: {}", sourceDir.string()));
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
        if (target <= options.sourceMinutes || target % options.sourceMinutes != 0) {
            spdlog::error(fmt::format("Target bar size {} m is not a multiple of the source bar size {} m, skipping",
                                      target, options.sourceMinutes));
            continue;
        }

        std::filesystem::path targetDir(pricesCsvDir);
        targetDir.append(Downloader::minutesToString(target));

        if (const auto err = createDirectoryRecursively(targetDir.string())) {
            spdlog::error(fmt::format("Failed to create {}, err: {}", targetDir.string(), err.message()));
            continue;
        }

        spdlog::info(fmt::format("Aggregating {} symbols from {} to {}...", sourceFiles.size(),
                                 Downloader::minutesToString(options.sourceMinutes),
                                 Downloader::minutesToString(target)));

        std::vector<std::future<Report> > futures;
        futures.reserve(sourceFiles.size());

        for (const auto &sourceFile: sourceFiles) {
            std::filesystem::path targetFile = targetDir;
            targetFile.append(sourceFile.filename().string());

            futures.push_back(launchBounded(maxJobs,
                                         [&options, target](const std::filesystem::path &src,
                                                                      const std::filesystem::path &dst) -> Report {
                                             return aggregateFile(src, dst, options.sourceMinutes, target,
                                                                  options.rewrite, options.allowPartialBuckets);
                                         }, sourceFile, targetFile));
        }

        std::int64_t totalBars = 0;
        std::size_t failed = 0;
        for (auto &future: futures) {
            auto report = future.get();
            if (report.failed) {
                failed++;
                spdlog::error(fmt::format("Aggregation of {} to {} failed: {}", report.symbol,
                                          Downloader::minutesToString(target), report.error));
            }
            if (report.incompleteBuckets > 0) {
                spdlog::warn(fmt::format("Aggregation of {} to {}: {} incomplete buckets {} "
                                         "(the remaining complete buckets were written)",
                                         report.symbol, Downloader::minutesToString(target),
                                         report.incompleteBuckets,
                                         options.allowPartialBuckets ? "accepted by request" : "omitted"));
            }
            totalBars += report.barsWritten;
            reports.push_back(std::move(report));
        }

        spdlog::info(fmt::format("{}: {} bars written, {} symbols failed",
                                 Downloader::minutesToString(target), totalBars, failed));
    }

    return reports;
}

} // namespace stonky
