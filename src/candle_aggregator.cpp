/**
Candle Aggregator - builds coarser bar files from a 1-minute CSV dataset

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/candle_aggregator.h"
#include "stonky/csv_data.h"
#include "stonky/interface/exchange_enums.h" // downloader.h expects CandleInterval to be declared
#include "stonky/downloader.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/semaphore.h"
#include <algorithm>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <future>
#include <set>
#include <spdlog/spdlog.h>

namespace stonky {

namespace {
constexpr std::int64_t MS_PER_MINUTE = 60000;

/// Per-column handling inside a bucket
enum class Role { Time, Open, High, Low, Close, Sum };

/// Extra value columns known to be additive across a bucket. A source column
/// outside this set (and outside the OHLC set) is rejected rather than guessed
/// at — silently summing something like `close_time` would corrupt the output.
const std::set<std::string> &summableColumns() {
    static const std::set<std::string> cols{
        "volume", "vol", "vol_ccy", "vol_ccy_quote", "vol_quote",
        "quote_volume", "turnover", "trades", "num_trades"
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

bool parseDouble(const std::string &field, long double &out) {
    try {
        std::size_t pos = 0;
        out = std::stold(field, &pos);
        return pos == field.size();
    } catch (const std::exception &) {
        return false;
    }
}

/// Fixed-notation formatting with trailing zeros trimmed. Default float
/// formatting switches to scientific notation for small/large magnitudes,
/// which the CSV readers of this dataset do not expect.
std::string formatSum(const long double value) {
    auto s = fmt::format("{:.10f}", static_cast<double>(value));
    if (s.find('.') != std::string::npos) {
        while (!s.empty() && s.back() == '0') {
            s.pop_back();
        }
        if (!s.empty() && s.back() == '.') {
            s.push_back('0');
        }
    }
    return s;
}

struct Layout {
    std::vector<Role> roles;
    std::size_t timeIdx{};
    std::size_t highIdx{};
    std::size_t lowIdx{};
};

/// Resolve column roles from the source header. Throws when the header is not
/// an OHLCV layout this aggregator can reason about.
Layout resolveLayout(const std::string &header) {
    const auto names = splitString(header, ',');
    Layout layout;
    layout.roles.reserve(names.size());

    bool haveTime = false, haveOpen = false, haveHigh = false, haveLow = false, haveClose = false;

    for (std::size_t i = 0; i < names.size(); ++i) {
        auto name = names[i];
        if (!name.empty() && name.back() == '\r') {
            name.pop_back();
        }

        if (name == "open_time" || name == "time" || name == "timestamp") {
            layout.roles.push_back(Role::Time);
            layout.timeIdx = i;
            haveTime = true;
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
        } else {
            throw std::runtime_error(fmt::format("unsupported column '{}' in header '{}'", name, header));
        }
    }

    if (!haveTime || !haveOpen || !haveHigh || !haveLow || !haveClose) {
        throw std::runtime_error(fmt::format("header is not an OHLCV layout: '{}'", header));
    }
    return layout;
}

/// One bucket under construction. OHLC are the verbatim source strings of the
/// contributing rows (no parse/format round trip, so no precision is lost on
/// assets quoted with many decimals); only Sum columns are accumulated.
struct Bucket {
    std::int64_t start{};
    std::int64_t lastTs{-1};
    std::vector<std::string> fields; // output row, indexed like the source
    std::vector<long double> sums;
    long double high{};
    long double low{};
    bool empty{true};
};

void applyRow(Bucket &bucket, const Layout &layout, const std::vector<std::string> &fields,
              const std::int64_t ts, const std::int64_t bucketStart) {
    if (bucket.empty) {
        bucket.start = bucketStart;
        bucket.fields = fields;
        bucket.sums.assign(fields.size(), 0.0L);
        bucket.empty = false;
        parseDouble(fields[layout.highIdx], bucket.high);
        parseDouble(fields[layout.lowIdx], bucket.low);
    } else {
        for (std::size_t i = 0; i < layout.roles.size(); ++i) {
            switch (layout.roles[i]) {
                case Role::High: {
                    if (long double v = 0; parseDouble(fields[i], v) && v > bucket.high) {
                        bucket.high = v;
                        bucket.fields[i] = fields[i];
                    }
                    break;
                }
                case Role::Low: {
                    if (long double v = 0; parseDouble(fields[i], v) && v < bucket.low) {
                        bucket.low = v;
                        bucket.fields[i] = fields[i];
                    }
                    break;
                }
                case Role::Close:
                    bucket.fields[i] = fields[i]; // rows arrive in ascending ts order
                    break;
                default:
                    break;
            }
        }
    }

    for (std::size_t i = 0; i < layout.roles.size(); ++i) {
        if (layout.roles[i] == Role::Sum) {
            if (long double v = 0; parseDouble(fields[i], v)) {
                bucket.sums[i] += v;
            }
        }
    }
    bucket.lastTs = ts;
}

std::string renderBucket(const Bucket &bucket, const Layout &layout) {
    std::string out;
    for (std::size_t i = 0; i < bucket.fields.size(); ++i) {
        if (i > 0) {
            out.push_back(',');
        }
        if (i == layout.timeIdx) {
            out += std::to_string(bucket.start);
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
                                       const bool rewrite) {
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
        return report; // empty source — nothing to aggregate
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

    const auto emit = [&] {
        if (!bucket.empty && bucket.start > resumeAfter) {
            outputRows.push_back(renderBucket(bucket, layout));
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

        // A closed bucket is always emitted, even when source bars inside it
        // are missing: an exchange outage in the middle of the file would never
        // be filled by a later run, so dropping the bucket would lose it
        // permanently. Only the trailing bucket is held back (below).
        const std::int64_t bucketStart = ts - ts % bucketMs;
        if (!bucket.empty && bucketStart != bucket.start) {
            emit();
        }
        applyRow(bucket, layout, fields, ts, bucketStart);
    }

    // The trailing bucket is written only once the source reaches its final
    // sub-interval; otherwise it would freeze as a partial bar that the next
    // incremental run could no longer complete.
    if (!bucket.empty && bucket.lastTs + sourceMs >= bucket.start + bucketMs) {
        emit();
    }

    if (outputRows.empty() && !rewrite) {
        return report; // already up to date
    }

    const bool writeHeader = rewrite || !std::filesystem::exists(targetPath) ||
                             std::filesystem::file_size(targetPath) == 0;

    std::ofstream ofs(targetPath, rewrite ? std::ios::trunc : std::ios::app);
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
    ofs.close();

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

            futures.push_back(std::async(std::launch::async,
                                         [&options, &maxJobs, target](const std::filesystem::path &src,
                                                                      const std::filesystem::path &dst) -> Report {
                                             std::scoped_lock w(maxJobs);
                                             return aggregateFile(src, dst, options.sourceMinutes, target,
                                                                  options.rewrite);
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
            totalBars += report.barsWritten;
            reports.push_back(std::move(report));
        }

        spdlog::info(fmt::format("{}: {} bars written, {} symbols failed",
                                 Downloader::minutesToString(target), totalBars, failed));
    }

    return reports;
}

} // namespace stonky
