#include "stonky/candle_aggregator.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        const auto suffix = std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() /
                ("crypto_data_downloader_aggregation_test_" + std::to_string(suffix));
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

stonky::CandleAggregator::Report aggregate(const std::filesystem::path &pricesDir,
                                            const bool allowPartial) {
    stonky::CandleAggregator::Options options;
    options.sourceMinutes = 1;
    options.targetMinutes = {5};
    options.maxJobs = 1;
    options.rewrite = true;
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

    if (!report.failed || report.incompleteBuckets != 1 || report.barsWritten != 0) {
        std::cerr << "Strict aggregation did not reject exactly one incomplete bucket\n";
        return false;
    }
    if (!lines.empty()) {
        std::cerr << "Strict aggregation modified its target despite a source gap\n";
        return false;
    }
    return true;
}

bool checkPartialMode(const std::filesystem::path &pricesDir) {
    const auto report = aggregate(pricesDir, true);
    const auto lines = readLines(pricesDir / "5m" / "TEST.csv");

    if (report.failed || report.incompleteBuckets != 1 || report.barsWritten != 2) {
        std::cerr << "Partial aggregation did not emit the incomplete bucket by request\n";
        return false;
    }
    if (lines.size() != 3 || !lines[1].starts_with("0,") || !lines[2].starts_with("300000,") ||
        !lines[2].ends_with(",0.6172839450617283945")) {
        std::cerr << "Partial aggregation output has unexpected bucket timestamps\n";
        return false;
    }
    return true;
}

bool checkInvalidNumbersNeverEmit(const std::filesystem::path &pricesDir) {
    const auto sourceDir = pricesDir / "1m";
    std::filesystem::create_directories(sourceDir);
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    out << "open_time,open,high,low,close,volume\n"
        << "0,10,11,9,10.5,1\n"
        << "60000,10,12,9,11,1\n"
        << "120000,11,nan,10,12,1\n"
        << "180000,12,13,11,12.5,1\n"
        << "240000,12.5,14,12,13,1\n";
    out.close();

    const auto report = aggregate(pricesDir, true);
    const auto lines = readLines(pricesDir / "5m" / "TEST.csv");
    if (!report.failed || report.incompleteBuckets != 1 || report.barsWritten != 0 || !lines.empty()) {
        std::cerr << "Partial aggregation emitted a non-finite numeric row\n";
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
    std::ofstream out(sourceDir / "TEST.csv", std::ios::trunc);
    out << "open_time,open,high,low,close,volume\n"
        << "0,10,11,9,10.5,1\n"
        << "60000,10.5,12,10,11,2\n"
        << "120000,11,13,10,12,3\n";
    out.close();

    const auto report = aggregate(pricesDir, false);
    if (!report.failed || report.barsWritten != 0 ||
        std::filesystem::exists(pricesDir / "5m" / "TEST.csv")) {
        std::cerr << "Trailing partial bucket created a successful header-only target\n";
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
    if (!strictReport.failed || strictReport.incompleteBuckets != 1 ||
        strictReport.barsWritten != 0 ||
        std::filesystem::exists(strictDir / "5m" / "TEST.csv")) {
        std::cerr << "Strict aggregation published data across a wholly missing target bucket\n";
        return false;
    }

    const auto partialReport = aggregate(partialDir, true);
    const auto partialLines = readLines(partialDir / "5m" / "TEST.csv");
    if (partialReport.failed || partialReport.incompleteBuckets != 1 ||
        partialReport.barsWritten != 2 || partialLines.size() != 3 ||
        !partialLines[1].starts_with("0,") || !partialLines[2].starts_with("600000,")) {
        std::cerr << "Partial aggregation did not preserve its whole-bucket gap policy\n";
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
    const auto emptyDir = temporaryDirectory.path() / "empty";
    const auto trailingDir = temporaryDirectory.path() / "trailing";
    const auto wholeGapStrictDir = temporaryDirectory.path() / "whole-gap-strict";
    const auto wholeGapPartialDir = temporaryDirectory.path() / "whole-gap-partial";
    const auto binanceDir = temporaryDirectory.path() / "binance";
    const auto mexcDir = temporaryDirectory.path() / "mexc";

    if (!writeSource(strictDir) || !writeSource(partialDir)) {
        std::cerr << "Could not create aggregation test data\n";
        return 1;
    }

    return checkStrictMode(strictDir) && checkPartialMode(partialDir) &&
           checkInvalidNumbersNeverEmit(invalidDir) && checkEmptySourceFails(emptyDir) &&
           checkTrailingPartialDoesNotCreateHeaderOnlyTarget(trailingDir) &&
           checkWholeBucketGapPolicy(wholeGapStrictDir, wholeGapPartialDir) &&
           checkVenueSchemas(binanceDir, mexcDir) ? 0 : 1;
}
