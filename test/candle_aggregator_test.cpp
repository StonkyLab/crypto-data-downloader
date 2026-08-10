#include "stonky/candle_aggregator.h"
#include "stonky/advisory_file_lock.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        const auto suffix = std::chrono::steady_clock::now().time_since_epoch().count();
        const auto entropy = std::random_device{}();
        path_ = std::filesystem::temp_directory_path() /
                ("crypto_data_downloader_aggregation_test_" + std::to_string(suffix) + "_" +
                 std::to_string(entropy));
        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    TemporaryDirectory(const TemporaryDirectory &) = delete;
    TemporaryDirectory &operator=(const TemporaryDirectory &) = delete;

    [[nodiscard]] const std::filesystem::path &path() const {
        return path_;
    }

private:
    std::filesystem::path path_;
};

bool writeSource(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    if (!out.is_open()) {
        return false;
    }

    out << "open_time,open,high,low,close,volume\n"
        << "0,10,11,9,10.5,1\n"
        << "60000,10.5,12,10,11,2\n"
        // 120000 is deliberately missing from the first closed 5m bucket.
        << "180000,11,13,10,12,3\n"
        << "240000,12,14,11,13,4\n"
        // The second 5m bucket is complete, including its final source bar.
        << "300000,20,21,19,20.5,0.1234567890123456789\n"
        << "360000,20.5,22,20,21,0.1234567890123456789\n"
        << "420000,21,23,20,22,0.1234567890123456789\n"
        << "480000,22,24,21,23,0.1234567890123456789\n"
        << "540000,23,25,22,24,0.1234567890123456789\n";
    out.flush();
    return out.good();
}

std::vector<std::string> readLines(const std::filesystem::path &path) {
    std::vector<std::string> lines;
    std::ifstream in(path);
    for (std::string line; std::getline(in, line);) {
        lines.push_back(std::move(line));
    }
    return lines;
}

std::string readBytes(const std::filesystem::path &path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

stonky::CandleAggregator::Report aggregate(const std::filesystem::path &pricesDir,
                                            const bool allowPartial,
                                            const bool rewrite = true) {
    stonky::CandleAggregator::Options options;
    options.sourceMinutes = 1;
    options.targetMinutes = {5};
    options.maxJobs = 1;
    options.rewrite = rewrite;
    options.allowPartialBuckets = allowPartial;

    auto reports = stonky::CandleAggregator::aggregateDirectory(pricesDir.string(), options);
    if (reports.size() != 1) {
        stonky::CandleAggregator::Report failure;
        failure.failed = true;
        failure.error = "expected exactly one aggregation report";
        return failure;
    }
    return reports.front();
}

bool checkStrictMode(const std::filesystem::path &pricesDir) {
    const auto report = aggregate(pricesDir, false);
    const auto lines = readLines(pricesDir / "5m" / "TEST.csv");

    if (report.failed || report.incompleteBuckets != 1 || report.partialBucketsWritten != 0 ||
        report.omittedIncompleteBuckets != 1 || report.barsWritten != 1) {
        std::cerr << "Strict aggregation did not localize exactly one incomplete bucket\n";
        return false;
    }
    if (lines.size() != 2 || !lines[1].starts_with("300000,")) {
        std::cerr << "Strict aggregation did not publish the complete bucket after a source gap\n";
        return false;
    }
    return true;
}

bool checkPartialMode(const std::filesystem::path &pricesDir) {
    const auto report = aggregate(pricesDir, true);
    const auto lines = readLines(pricesDir / "5m" / "TEST.csv");

    if (report.failed || report.incompleteBuckets != 1 || report.partialBucketsWritten != 1 ||
        report.omittedIncompleteBuckets != 0 || report.barsWritten != 2) {
        std::cerr << "Partial aggregation did not emit the incomplete bucket by request\n";
        return false;
    }
    if (lines.size() != 3 || !lines[1].starts_with("0,") || !lines[2].starts_with("300000,")) {
        std::cerr << "Partial aggregation output has unexpected bucket timestamps\n";
        return false;
    }
    const auto volumeText = lines[2].substr(lines[2].find_last_of(',') + 1);
    if (std::stod(volumeText) != 0.6172839450617283945) {
        std::cerr << "Partial aggregation did not preserve the expected binary64 volume sum\n";
        return false;
    }
    return true;
}

bool checkInvalidNumbersNeverEmit(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    out << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 15; ++minute) {
        out << minute * 60000 << ",10,"
            << (minute == 7 ? "1e1000" : "11")
            << ",9,10.5," << (minute == 7 ? "1e1000" : "1") << "\n";
    }
    out.close();

    const auto report = aggregate(pricesDir, true);
    const auto lines = readLines(pricesDir / "5m" / "TEST.csv");
    if (report.failed || report.incompleteBuckets != 1 || report.partialBucketsWritten != 0 ||
        report.omittedIncompleteBuckets != 1 || report.barsWritten != 2 ||
        lines.size() != 3 || !lines[1].starts_with("0,") || !lines[2].starts_with("600000,")) {
        std::cerr << "A non-binary64 numeric row affected buckets outside its own interval\n";
        return false;
    }
    return true;
}

bool checkEmptySourceFails(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream(sourceDir / "TEST.csv", std::ios::trunc).close();

    const auto report = aggregate(pricesDir, false);
    if (!report.failed || report.barsWritten != 0) {
        std::cerr << "Empty source CSV was treated as a successful aggregation\n";
        return false;
    }
    return true;
}

bool checkTrailingPartialDoesNotCreateHeaderOnlyTarget(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    // Use the next aligned bucket so the test cannot race the current 5m
    // boundary between fixture creation and aggregation.
    const auto currentBucket = nowMs - nowMs % 300000 + 300000;
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    out << "open_time,open,high,low,close,volume\n"
        << currentBucket << ",10,11,9,10.5,1\n";
    out.close();

    const auto report = aggregate(pricesDir, false);
    if (report.failed || report.incompleteBuckets != 0 || report.barsWritten != 0 ||
        std::filesystem::exists(pricesDir / "5m" / "TEST.csv")) {
        std::cerr << "Trailing in-progress bucket created a target or a false degradation\n";
        return false;
    }
    return true;
}

bool checkHistoricalTrailingGapIsReported(const std::filesystem::path &strictDir,
                                          const std::filesystem::path &partialDir) {
    for (const auto &pricesDir: {strictDir, partialDir}) {
        const auto sourceDir = pricesDir / "1m";
        std::filesystem::create_directories(sourceDir);
        std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
        out << "open_time,open,high,low,close,volume\n"
            << "0,10,11,9,10.5,1\n"
            << "60000,10.5,12,10,11,2\n"
            << "120000,11,13,10,12,3\n";
    }

    const auto strict = aggregate(strictDir, false);
    if (strict.failed || strict.incompleteBuckets != 1 ||
        strict.omittedIncompleteBuckets != 1 || strict.barsWritten != 0 ||
        std::filesystem::exists(strictDir / "5m" / "TEST.csv")) {
        std::cerr << "A historical trailing outage was mistaken for an in-progress bucket\n";
        return false;
    }

    const auto partial = aggregate(partialDir, true);
    const auto lines = readLines(partialDir / "5m" / "TEST.csv");
    if (partial.failed || partial.incompleteBuckets != 1 ||
        partial.partialBucketsWritten != 1 || partial.omittedIncompleteBuckets != 0 ||
        partial.barsWritten != 1 || lines.size() != 2 || !lines[1].starts_with("0,")) {
        std::cerr << "Explicit partial mode did not emit a closed historical trailing bucket\n";
        return false;
    }
    return true;
}

bool checkSumOverflowIsLocalized(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    out << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 15; ++minute) {
        out << minute * 60000 << ",10,11,9,10.5,"
            << (minute >= 5 && minute < 10 ? "1e308" : "1") << "\n";
    }
    out.close();

    const auto report = aggregate(pricesDir, true);
    const auto lines = readLines(pricesDir / "5m" / "TEST.csv");
    if (report.failed || report.incompleteBuckets != 1 ||
        report.partialBucketsWritten != 0 || report.omittedIncompleteBuckets != 1 ||
        report.barsWritten != 2 || lines.size() != 3 ||
        !lines[1].starts_with("0,") || !lines[2].starts_with("600000,")) {
        std::cerr << "An overflowing aggregate sum cascaded beyond its own bucket\n";
        return false;
    }
    return true;
}

bool checkMalformedOnlySourceFails(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    out << "open_time,open,high,low,close,volume\n"
        << "0,torn\n";
    out.close();

    const auto report = aggregate(pricesDir, false);
    if (!report.failed || report.barsWritten != 0 ||
        std::filesystem::exists(pricesDir / "5m" / "TEST.csv")) {
        std::cerr << "A source without one valid row was reported as usable\n";
        return false;
    }
    return true;
}

bool checkExtremeTimestampIsLocalized(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    out << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 15; ++minute) {
        out << minute * 60000 << ",10,11,9,10.5,1\n";
        if (minute == 4) {
            out << std::numeric_limits<std::int64_t>::max()
                << ",10,11,9,10.5,1\n";
        }
    }
    out.close();

    const auto report = aggregate(pricesDir, false);
    const auto lines = readLines(pricesDir / "5m" / "TEST.csv");
    if (report.failed || report.incompleteBuckets != 1 ||
        report.omittedIncompleteBuckets != 1 || report.barsWritten != 2 ||
        lines.size() != 3 || !lines[1].starts_with("0,") ||
        !lines[2].starts_with("600000,")) {
        std::cerr << "An extreme timestamp was not safely localized\n";
        return false;
    }
    return true;
}

bool writeWholeBucketGapSource(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    if (!out.is_open()) {
        return false;
    }

    out << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 5; ++minute) {
        out << minute * 60000 << ",10,11,9,10.5,1\n";
    }
    // The complete 5m bucket starting at 300000 is wholly absent. Both
    // adjacent buckets are internally complete, which used to evade strict
    // completeness checks and produce a silent gap in the derived CSV.
    for (std::int64_t minute = 10; minute < 15; ++minute) {
        out << minute * 60000 << ",20,21,19,20.5,2\n";
    }
    out.flush();
    return out.good();
}

bool checkWholeBucketGapPolicy(const std::filesystem::path &strictDir,
                               const std::filesystem::path &partialDir) {
    if (!writeWholeBucketGapSource(strictDir) || !writeWholeBucketGapSource(partialDir)) {
        std::cerr << "Could not create whole-bucket gap test data\n";
        return false;
    }

    const auto strictReport = aggregate(strictDir, false);
    const auto strictLines = readLines(strictDir / "5m" / "TEST.csv");
    if (strictReport.failed || strictReport.incompleteBuckets != 1 ||
        strictReport.partialBucketsWritten != 0 || strictReport.omittedIncompleteBuckets != 1 ||
        strictReport.barsWritten != 2 || strictLines.size() != 3 ||
        !strictLines[1].starts_with("0,") || !strictLines[2].starts_with("600000,")) {
        std::cerr << "Strict aggregation discarded valid data around a wholly missing target bucket\n";
        return false;
    }

    const auto partialReport = aggregate(partialDir, true);
    const auto partialLines = readLines(partialDir / "5m" / "TEST.csv");
    if (partialReport.failed || partialReport.incompleteBuckets != 1 ||
        partialReport.partialBucketsWritten != 0 || partialReport.omittedIncompleteBuckets != 1 ||
        partialReport.barsWritten != 2 || partialLines.size() != 3 ||
        !partialLines[1].starts_with("0,") || !partialLines[2].starts_with("600000,")) {
        std::cerr << "Partial aggregation did not preserve its whole-bucket gap policy\n";
        return false;
    }
    return true;
}

bool checkMalformedRowsAreLocalized(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    out << "open_time,open,high,low,close,volume\n";

    for (std::int64_t minute = 0; minute < 25; ++minute) {
        out << minute * 60000 << ",10,11,9,10.5,1\n";
        if (minute == 5) {
            // The unreadable timestamp is attributed to the next expected
            // source slot, so only the 5m bucket starting at 300000 is unsafe.
            out << "not-a-timestamp,10,11,9,10.5,1\n";
        }
        if (minute == 15) {
            // A torn row retains enough timestamp information to localize it.
            // The following valid row at the same minute is still consumed,
            // but --allow-partial must not publish a bucket containing damage.
            out << "960000,torn\n";
        }
    }
    out.close();

    const auto report = aggregate(pricesDir, true);
    const auto lines = readLines(pricesDir / "5m" / "TEST.csv");
    if (report.failed || report.incompleteBuckets != 2 || report.partialBucketsWritten != 0 ||
        report.omittedIncompleteBuckets != 2 || report.barsWritten != 3 ||
        lines.size() != 4 || !lines[1].starts_with("0,") ||
        !lines[2].starts_with("600000,") || !lines[3].starts_with("1200000,")) {
        std::cerr << "Malformed rows affected complete buckets outside their target intervals\n";
        return false;
    }
    return true;
}

bool checkDuplicateAndOutOfOrderRowsAreLocalized(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    out << "open_time,open,high,low,close,volume\n";

    for (std::int64_t minute = 0; minute < 20; ++minute) {
        out << minute * 60000 << ",10,11,9,10.5,1\n";
        if (minute == 6) {
            out << minute * 60000 << ",10,11,9,10.5,1\n"; // duplicate in bucket 300000
        }
        if (minute == 14) {
            // This arrives after bucket 0 was already rendered. The final
            // filtering pass must retract only that old bucket.
            out << "120000,10,11,9,10.5,1\n";
        }
    }
    out.close();

    const auto report = aggregate(pricesDir, false);
    const auto lines = readLines(pricesDir / "5m" / "TEST.csv");
    if (report.failed || report.incompleteBuckets != 2 || report.partialBucketsWritten != 0 ||
        report.omittedIncompleteBuckets != 2 || report.barsWritten != 2 ||
        lines.size() != 3 || !lines[1].starts_with("600000,") ||
        !lines[2].starts_with("900000,")) {
        std::cerr << "Duplicate/out-of-order rows caused non-local aggregation damage\n";
        return false;
    }
    return true;
}

bool checkForwardJumpDoesNotSuppressChronologicalSuffix(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    out << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 100; ++minute) {
        out << minute * 60000 << ",10,11,9,10.5,1\n";
        if (minute == 4) {
            // A single early row used to advance prevTs to minute 100 and make
            // every legitimate row 5..99 look out-of-order.
            out << 100 * 60000 << ",20,21,19,20.5,1\n";
        }
    }
    out.close();

    const auto report = aggregate(pricesDir, false);
    const auto lines = readLines(pricesDir / "5m" / "TEST.csv");
    if (report.failed || report.incompleteBuckets != 1 ||
        report.omittedIncompleteBuckets != 1 || report.barsWritten != 20 ||
        lines.size() != 21 || !lines[1].starts_with("0,") ||
        !lines.back().starts_with("5700000,")) {
        std::cerr << "A forward timestamp outlier suppressed the chronological suffix\n";
        return false;
    }
    for (std::size_t bucket = 0; bucket < 20; ++bucket) {
        const auto expectedPrefix = std::to_string(bucket * 300000) + ",";
        if (!lines[bucket + 1].starts_with(expectedPrefix)) {
            std::cerr << "Forward-jump recovery left a gap in a legitimate complete bucket\n";
            return false;
        }
    }
    return true;
}

bool checkIncrementalResumeAcrossGap(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    const auto sourcePath = sourceDir / "TEST.csv";
    {
        std::ofstream out(sourcePath, std::ios::trunc);
        out << "open_time,open,high,low,close,volume\n";
        for (std::int64_t minute = 0; minute < 5; ++minute) {
            out << minute * 60000 << ",10,11,9,10.5,1\n";
        }
    }

    const auto initial = aggregate(pricesDir, false, true);
    if (initial.failed || initial.incompleteBuckets != 0 || initial.barsWritten != 1) {
        std::cerr << "Could not create the initial aggregation tail\n";
        return false;
    }

    {
        std::ofstream out(sourcePath, std::ios::app);
        // The entire 5m bucket at 300000 is absent.
        for (std::int64_t minute = 10; minute < 15; ++minute) {
            out << minute * 60000 << ",20,21,19,20.5,2\n";
        }
    }

    const auto acrossGap = aggregate(pricesDir, false, false);
    auto lines = readLines(pricesDir / "5m" / "TEST.csv");
    if (acrossGap.failed || acrossGap.incompleteBuckets != 1 ||
        acrossGap.partialBucketsWritten != 0 || acrossGap.omittedIncompleteBuckets != 1 ||
        acrossGap.barsWritten != 1 ||
        lines.size() != 3 || !lines[1].starts_with("0,") || !lines[2].starts_with("600000,")) {
        std::cerr << "Incremental aggregation did not append data after a whole-bucket outage\n";
        return false;
    }

    const auto idempotent = aggregate(pricesDir, false, false);
    lines = readLines(pricesDir / "5m" / "TEST.csv");
    if (idempotent.failed || idempotent.incompleteBuckets != 0 || idempotent.barsWritten != 0 ||
        lines.size() != 3) {
        std::cerr << "Incremental resume re-reported an outage behind its existing tail\n";
        return false;
    }

    {
        std::ofstream out(sourcePath, std::ios::app);
        for (std::int64_t minute = 15; minute < 20; ++minute) {
            out << minute * 60000 << ",30,31,29,30.5,3\n";
        }
    }

    const auto resumed = aggregate(pricesDir, false, false);
    lines = readLines(pricesDir / "5m" / "TEST.csv");
    if (resumed.failed || resumed.incompleteBuckets != 0 || resumed.barsWritten != 1 ||
        lines.size() != 4 || !lines[3].starts_with("900000,")) {
        std::cerr << "Incremental aggregation did not keep advancing after an earlier outage\n";
        return false;
    }
    return true;
}

bool checkAppendIsAtomicAndRepairsTornTail(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    const auto targetDir = pricesDir / "5m";
    std::filesystem::create_directories(sourceDir);
    std::filesystem::create_directories(targetDir);

    std::ofstream source(sourceDir / "TEST.csv", std::ios::trunc);
    source << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 15; ++minute) {
        source << minute * 60000 << ",10,11,9,10.5,1\n";
    }
    source.close();

    const auto targetPath = targetDir / "TEST.csv";
    std::ofstream target(targetPath, std::ios::binary | std::ios::trunc);
    target << "open_time,open,high,low,close,volume\n"
           << "0,10,11,9,10.5,5\n"
           << "300000,torn\n"
           // A valid-looking later record must not make the garbage a
           // preservable middle row; append restarts at the valid prefix.
           << "600000,10,11,9,10.5,5\n"
           << "900000,torn"; // no newline: interrupted old append
    target.close();
    const auto tornBytes = readBytes(targetPath);

    auto lockPath = targetPath;
    lockPath += ".lock";
    {
        stonky::AdvisoryFileLock heldLock(lockPath);
        if (!heldLock.ownsLock()) {
            std::cerr << "Could not acquire append atomicity test lock\n";
            return false;
        }
        const auto blocked = aggregate(pricesDir, false, false);
        if (!blocked.failed || blocked.barsWritten != 0 || readBytes(targetPath) != tornBytes) {
            std::cerr << "A failed append modified its previous target\n";
            return false;
        }
    }

    {
        std::ofstream malformed(sourceDir / "TEST.csv", std::ios::trunc);
        malformed << "open_time,open,high,low,close,volume\n"
                  << "0,torn\n";
    }
    const auto failedAfterOpen = aggregate(pricesDir, false, false);
    if (!failedAfterOpen.failed || failedAfterOpen.barsWritten != 0 ||
        readBytes(targetPath) != tornBytes) {
        std::cerr << "An append failure after opening its transaction changed the target\n";
        return false;
    }

    source.open(sourceDir / "TEST.csv", std::ios::trunc);
    source << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 15; ++minute) {
        source << minute * 60000 << ",10,11,9,10.5,1\n";
    }
    source.close();

    const auto repaired = aggregate(pricesDir, false, false);
    const auto lines = readLines(targetPath);
    if (repaired.failed || repaired.incompleteBuckets != 0 || repaired.barsWritten != 2 ||
        lines.size() != 4 || !lines[1].starts_with("0,") ||
        !lines[2].starts_with("300000,") || !lines[3].starts_with("600000,") ||
        readBytes(targetPath).find("torn") != std::string::npos) {
        std::cerr << "Successful atomic append did not replace a torn tail cleanly\n";
        return false;
    }

    const auto afterRepair = readBytes(targetPath);
    const auto repeated = aggregate(pricesDir, false, false);
    if (repeated.failed || repeated.barsWritten != 0 || readBytes(targetPath) != afterRepair) {
        std::cerr << "Append writer retained its lock or changed an idempotent target\n";
        return false;
    }
    return true;
}

bool checkRewritePastDamagedFinalBucket(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    const auto targetDir = pricesDir / "5m";
    std::filesystem::create_directories(sourceDir);
    std::filesystem::create_directories(targetDir);

    std::ofstream source(sourceDir / "TEST.csv", std::ios::trunc);
    source << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 10; ++minute) {
        source << minute * 60000 << ",10,"
               << (minute == 7 ? "nan" : "11")
               << ",9,10.5,1\n";
    }
    source.close();

    // Simulate an older target whose tail is the bucket now known to contain
    // bad input. A rewrite must publish the clean prefix, not abort the symbol
    // merely because the new tail is earlier after omitting that bucket.
    std::ofstream target(targetDir / "TEST.csv", std::ios::trunc);
    target << "open_time,open,high,low,close,volume\n"
           << "0,10,11,9,10.5,5\n"
           << "300000,10,11,9,10.5,5\n";
    target.close();

    const auto report = aggregate(pricesDir, true, true);
    const auto lines = readLines(targetDir / "TEST.csv");
    if (report.failed || report.incompleteBuckets != 1 || report.partialBucketsWritten != 0 ||
        report.omittedIncompleteBuckets != 1 || report.barsWritten != 1 ||
        lines.size() != 2 || !lines[1].starts_with("0,")) {
        std::cerr << "Rewrite aborted instead of publishing data before a damaged final bucket\n";
        return false;
    }
    return true;
}

bool checkRewritePreservesMissingHistoricalPrefix(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    const auto targetDir = pricesDir / "5m";
    std::filesystem::create_directories(sourceDir);
    std::filesystem::create_directories(targetDir);

    std::ofstream source(sourceDir / "TEST.csv", std::ios::trunc);
    source << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 5; minute < 15; ++minute) {
        source << minute * 60000 << ",20,21,19,20.5,1\n";
    }
    source.close();

    const auto targetPath = targetDir / "TEST.csv";
    std::ofstream target(targetPath, std::ios::trunc);
    target << "open_time,open,high,low,close,volume\n"
           << "0,10,11,9,10.5,5\n"
           << "300000,20,21,19,20.5,5\n"
           << "600000,30,31,29,30.5,5\n"
           << "900000,40,41,39,40.5,5\n";
    target.close();
    const auto before = readLines(targetPath);

    const auto report = aggregate(pricesDir, false, true);
    if (!report.failed || report.barsWritten != 0 || readLines(targetPath) != before) {
        std::cerr << "Rewrite silently discarded an existing historical prefix\n";
        return false;
    }
    return true;
}

bool checkIncompleteFutureRowCannotBypassTailGuard(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    const auto targetDir = pricesDir / "5m";
    std::filesystem::create_directories(sourceDir);
    std::filesystem::create_directories(targetDir);

    std::ofstream source(sourceDir / "TEST.csv", std::ios::trunc);
    source << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 5; ++minute) {
        source << minute * 60000 << ",10,11,9,10.5,1\n";
    }
    // A lone row in a later closed bucket must not prove continuous tail
    // coverage. It is omitted, while the old target remains intact.
    source << "1140000,40,41,39,40.5,1\n";
    source.close();

    const auto targetPath = targetDir / "TEST.csv";
    std::ofstream target(targetPath, std::ios::trunc);
    target << "open_time,open,high,low,close,volume\n"
           << "0,10,11,9,10.5,5\n"
           << "300000,20,21,19,20.5,5\n"
           << "600000,30,31,29,30.5,5\n";
    target.close();
    const auto before = readLines(targetPath);

    const auto report = aggregate(pricesDir, false, true);
    if (!report.failed || report.barsWritten != 0 || readLines(targetPath) != before) {
        std::cerr << "An isolated later row bypassed the target tail coverage guard\n";
        return false;
    }
    const auto partialReport = aggregate(pricesDir, true, true);
    if (!partialReport.failed || partialReport.barsWritten != 0 || readLines(targetPath) != before) {
        std::cerr << "A partial isolated later row bypassed the target tail coverage guard\n";
        return false;
    }

    {
        std::ofstream complete(sourceDir / "TEST.csv", std::ios::trunc);
        complete << "open_time,open,high,low,close,volume\n";
        for (std::int64_t minute = 0; minute < 5; ++minute) {
            complete << minute * 60000 << ",10,11,9,10.5,1\n";
        }
        // This bucket reaches the old target tail and is internally complete,
        // but buckets 5 and 10 are wholly absent.
        for (std::int64_t minute = 15; minute < 20; ++minute) {
            complete << minute * 60000 << ",40,41,39,40.5,1\n";
        }
    }
    const auto completeReport = aggregate(pricesDir, false, true);
    if (!completeReport.failed || completeReport.barsWritten != 0 ||
        readLines(targetPath) != before) {
        std::cerr << "A complete isolated later bucket bypassed the target tail coverage guard\n";
        return false;
    }
    return true;
}

bool checkRejectedRogueCannotAuthorizeTailTruncation(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    const auto targetDir = pricesDir / "5m";
    std::filesystem::create_directories(sourceDir);
    std::filesystem::create_directories(targetDir);

    std::ofstream source(sourceDir / "TEST.csv", std::ios::trunc);
    source << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 55; ++minute) {
        source << minute * 60000 << ",10,11,9,10.5,1\n";
        if (minute == 4) {
            // LIS must reject this forward rogue. Its timestamp equals the old
            // target tail, but it proves no coverage of buckets 55..90.
            source << 95 * 60000 << ",20,21,19,20.5,1\n";
        }
    }
    source.close();

    const auto targetPath = targetDir / "TEST.csv";
    std::ofstream target(targetPath, std::ios::trunc);
    target << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute <= 95; minute += 5) {
        target << minute * 60000 << ",10,11,9,10.5,5\n";
    }
    target.close();
    const auto before = readBytes(targetPath);

    const auto report = aggregate(pricesDir, false, true);
    if (!report.failed || report.barsWritten != 0 || readBytes(targetPath) != before) {
        std::cerr << "A rejected rogue timestamp authorized destructive tail truncation\n";
        return false;
    }
    return true;
}

bool checkIsolatedDamagedHeadCannotAuthorizePrefixTruncation(
    const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    const auto targetDir = pricesDir / "5m";
    std::filesystem::create_directories(sourceDir);
    std::filesystem::create_directories(targetDir);

    std::ofstream source(sourceDir / "TEST.csv", std::ios::trunc);
    source << "open_time,open,high,low,close,volume\n"
           // The old first bucket is observed only by one damaged row.
           << "0,10,nan,9,10.5,1\n";
    for (std::int64_t minute = 50; minute < 100; ++minute) {
        source << minute * 60000 << ",20,21,19,20.5,1\n";
    }
    source.close();

    const auto targetPath = targetDir / "TEST.csv";
    std::ofstream target(targetPath, std::ios::trunc);
    target << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute <= 95; minute += 5) {
        target << minute * 60000 << ",10,11,9,10.5,5\n";
    }
    target.close();
    const auto before = readBytes(targetPath);

    const auto report = aggregate(pricesDir, false, true);
    if (!report.failed || report.barsWritten != 0 || readBytes(targetPath) != before) {
        std::cerr << "An isolated damaged head authorized destructive prefix truncation\n";
        return false;
    }
    return true;
}

bool checkExistingKnownGapDoesNotBlockNewSuffix(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    const auto targetDir = pricesDir / "5m";
    std::filesystem::create_directories(sourceDir);
    std::filesystem::create_directories(targetDir);

    std::ofstream source(sourceDir / "TEST.csv", std::ios::trunc);
    source << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 5; ++minute) {
        source << minute * 60000 << ",10,11,9,10.5,1\n";
    }
    // Bucket 5 is a known historical outage and is already absent from target.
    for (std::int64_t minute = 10; minute < 20; ++minute) {
        source << minute * 60000 << ",20,21,19,20.5,1\n";
    }
    source.close();

    const auto targetPath = targetDir / "TEST.csv";
    std::ofstream target(targetPath, std::ios::trunc);
    target << "open_time,open,high,low,close,volume\n"
           << "0,10,11,9,10.5,5\n"
           << "600000,20,21,19,20.5,5\n";
    target.close();

    const auto report = aggregate(pricesDir, false, true);
    const auto lines = readLines(targetPath);
    if (report.failed || report.incompleteBuckets != 1 || report.barsWritten != 3 ||
        lines.size() != 4 || !lines[1].starts_with("0,") ||
        !lines[2].starts_with("600000,") || !lines[3].starts_with("900000,")) {
        std::cerr << "A gap already absent from target permanently blocked a valid new suffix\n";
        return false;
    }
    return true;
}

bool checkConcurrentRewriteIsRejected(const std::filesystem::path &pricesDir) {
    if (!writeSource(pricesDir)) {
        return false;
    }
    const auto targetDir = pricesDir / "5m";
    std::filesystem::create_directories(targetDir);
    const auto targetPath = targetDir / "TEST.csv";
    std::ofstream(targetPath, std::ios::trunc)
        << "open_time,open,high,low,close,volume\n"
        << "0,10,11,9,10.5,5\n";
    const auto before = readLines(targetPath);

    auto lockPath = targetPath;
    lockPath += ".lock";
    stonky::AdvisoryFileLock heldLock(lockPath);
    if (!heldLock.ownsLock()) {
        std::cerr << "Could not acquire aggregate test lock\n";
        return false;
    }

    const auto report = aggregate(pricesDir, false, true);
    if (!report.failed || report.barsWritten != 0 || readLines(targetPath) != before) {
        std::cerr << "Concurrent rewrite was not rejected without touching the target\n";
        return false;
    }
    return true;
}

bool checkInvalidTargetProducesFailureReport(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream source(sourceDir / "TEST.csv", std::ios::trunc);
    source << "open_time,open,high,low,close,volume\n";
    for (std::int64_t minute = 0; minute < 5; ++minute) {
        source << minute * 60000 << ",10,11,9,10.5,1\n";
    }
    source.close();

    stonky::CandleAggregator::Options options;
    options.sourceMinutes = 1;
    options.targetMinutes = {5, 7};
    options.maxJobs = 1;
    options.rewrite = true;
    const auto reports = stonky::CandleAggregator::aggregateDirectory(pricesDir.string(), options);
    if (reports.size() != 2 || reports[0].failed || !reports[1].failed ||
        reports[1].targetMinutes != 7) {
        std::cerr << "A skipped invalid target was not returned as a failure report\n";
        return false;
    }
    return true;
}

bool checkVenueSchemas(const std::filesystem::path &binanceDir,
                       const std::filesystem::path &mexcDir) {
    std::filesystem::create_directories(binanceDir / "1m");
    std::ofstream binance(binanceDir / "1m" / "TEST.csv", std::ios::trunc);
    binance << "close_time,open,high,low,close,volume,timestamp,quote_av,trades,tb_base_av,tb_quote_av,ignore\n"
             << "59999,10,11,9,10.5,1,0,2,3,4,5,0\n"
             << "119999,10.5,12,10,11,1,60000,2,3,4,5,0\n"
             << "179999,11,13,10,12,1,120000,2,3,4,5,0\n"
             << "239999,12,13.5,11,12.5,1,180000,2,3,4,5,0\n"
             << "299999,12.5,14,12,13,1,240000,2,3,4,5,0\n";
    binance.close();

    const auto binanceReport = aggregate(binanceDir, false);
    const auto binanceLines = readLines(binanceDir / "5m" / "TEST.csv");
    if (binanceReport.failed || binanceReport.barsWritten != 1 || binanceLines.size() != 2 ||
        binanceLines[1] != "299999,10,14,9,13,5,0,10,15,20,25,0") {
        std::cerr << "Binance 12-column schema was not aggregated correctly\n";
        return false;
    }

    std::filesystem::create_directories(mexcDir / "1m");
    std::ofstream mexc(mexcDir / "1m" / "TEST.csv", std::ios::trunc);
    mexc << "open_time,open,high,low,close,volume,quote_asset_volume\n"
         << "0,10,11,9,10.5,1,2\n"
         << "60000,10.5,12,10,11,1,2\n"
         << "120000,11,13,10,12,1,2\n"
         << "180000,12,13.5,11,12.5,1,2\n"
         << "240000,12.5,14,12,13,1,2\n";
    mexc.close();

    const auto mexcReport = aggregate(mexcDir, false);
    const auto mexcLines = readLines(mexcDir / "5m" / "TEST.csv");
    if (mexcReport.failed || mexcReport.barsWritten != 1 || mexcLines.size() != 2 ||
        mexcLines[1] != "0,10,14,9,13,5,10") {
        std::cerr << "MEXC seven-column schema was not aggregated correctly\n";
        return false;
    }
    return true;
}

} // namespace

int main() {
    TemporaryDirectory temporaryDirectory;
    const auto strictDir = temporaryDirectory.path() / "strict";
    const auto partialDir = temporaryDirectory.path() / "partial";
    const auto invalidDir = temporaryDirectory.path() / "invalid";
    const auto sumOverflowDir = temporaryDirectory.path() / "sum-overflow";
    const auto emptyDir = temporaryDirectory.path() / "empty";
    const auto malformedOnlyDir = temporaryDirectory.path() / "malformed-only";
    const auto trailingDir = temporaryDirectory.path() / "trailing";
    const auto historicalTrailingStrictDir = temporaryDirectory.path() / "historical-trailing-strict";
    const auto historicalTrailingPartialDir = temporaryDirectory.path() / "historical-trailing-partial";
    const auto extremeTimestampDir = temporaryDirectory.path() / "extreme-timestamp";
    const auto wholeGapStrictDir = temporaryDirectory.path() / "whole-gap-strict";
    const auto wholeGapPartialDir = temporaryDirectory.path() / "whole-gap-partial";
    const auto malformedDir = temporaryDirectory.path() / "malformed";
    const auto orderingDir = temporaryDirectory.path() / "ordering";
    const auto forwardJumpDir = temporaryDirectory.path() / "forward-jump";
    const auto resumeDir = temporaryDirectory.path() / "resume";
    const auto atomicAppendDir = temporaryDirectory.path() / "atomic-append";
    const auto rewriteDir = temporaryDirectory.path() / "rewrite";
    const auto prefixGuardDir = temporaryDirectory.path() / "prefix-guard";
    const auto tailGuardDir = temporaryDirectory.path() / "tail-guard";
    const auto rogueTailGuardDir = temporaryDirectory.path() / "rogue-tail-guard";
    const auto isolatedHeadGuardDir = temporaryDirectory.path() / "isolated-head-guard";
    const auto knownGapRewriteDir = temporaryDirectory.path() / "known-gap-rewrite";
    const auto concurrentRewriteDir = temporaryDirectory.path() / "concurrent-rewrite";
    const auto invalidTargetDir = temporaryDirectory.path() / "invalid-target";
    const auto binanceDir = temporaryDirectory.path() / "binance";
    const auto mexcDir = temporaryDirectory.path() / "mexc";

    if (!writeSource(strictDir) || !writeSource(partialDir)) {
        std::cerr << "Could not create aggregation test data\n";
        return 1;
    }

    return checkStrictMode(strictDir) && checkPartialMode(partialDir) &&
           checkInvalidNumbersNeverEmit(invalidDir) &&
           checkSumOverflowIsLocalized(sumOverflowDir) &&
           checkEmptySourceFails(emptyDir) && checkMalformedOnlySourceFails(malformedOnlyDir) &&
           checkTrailingPartialDoesNotCreateHeaderOnlyTarget(trailingDir) &&
           checkHistoricalTrailingGapIsReported(historicalTrailingStrictDir,
                                                historicalTrailingPartialDir) &&
           checkExtremeTimestampIsLocalized(extremeTimestampDir) &&
           checkWholeBucketGapPolicy(wholeGapStrictDir, wholeGapPartialDir) &&
           checkMalformedRowsAreLocalized(malformedDir) &&
           checkDuplicateAndOutOfOrderRowsAreLocalized(orderingDir) &&
           checkForwardJumpDoesNotSuppressChronologicalSuffix(forwardJumpDir) &&
           checkIncrementalResumeAcrossGap(resumeDir) &&
           checkAppendIsAtomicAndRepairsTornTail(atomicAppendDir) &&
           checkRewritePastDamagedFinalBucket(rewriteDir) &&
           checkRewritePreservesMissingHistoricalPrefix(prefixGuardDir) &&
           checkIncompleteFutureRowCannotBypassTailGuard(tailGuardDir) &&
           checkRejectedRogueCannotAuthorizeTailTruncation(rogueTailGuardDir) &&
           checkIsolatedDamagedHeadCannotAuthorizePrefixTruncation(isolatedHeadGuardDir) &&
           checkExistingKnownGapDoesNotBlockNewSuffix(knownGapRewriteDir) &&
           checkConcurrentRewriteIsRejected(concurrentRewriteDir) &&
           checkInvalidTargetProducesFailureReport(invalidTargetDir) &&
           checkVenueSchemas(binanceDir, mexcDir) ? 0 : 1;
}
