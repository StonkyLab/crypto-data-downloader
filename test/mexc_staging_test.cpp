#include "mexc_staging.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        path_ = std::filesystem::temp_directory_path() /
                ("crypto_data_downloader_mexc_staging_" + std::to_string(
                    std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path_);
    }
    ~TemporaryDirectory() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }
    [[nodiscard]] const std::filesystem::path &path() const { return path_; }

private:
    std::filesystem::path path_;
};

std::int64_t utcMs(const int year, const unsigned month, const unsigned day) {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        sys_days{std::chrono::year{year} / std::chrono::month{month} /
                 std::chrono::day{day}}.time_since_epoch()).count();
}

bool writeFile(const std::filesystem::path &path, const std::string &contents) {
    std::ofstream out(path, std::ios::trunc | std::ios::binary);
    out << contents;
    out.flush();
    return out.good();
}

std::vector<std::string> readLines(const std::filesystem::path &path) {
    std::ifstream in(path);
    std::vector<std::string> lines;
    for (std::string line; std::getline(in, line);) {
        lines.push_back(line);
    }
    return lines;
}

} // namespace

int main() {
    using namespace stonky::mexc_staging;
    bool ok = true;
    const std::string header = "open_time,open,high,low,close,volume,amount";

    constexpr std::int64_t minute = 60000;
    constexpr std::int64_t week = 7LL * 24 * 60 * 60 * 1000;
    if (currentPeriodOpen(utcMs(2026, 8, 9), week, Alignment::WeekMonday) !=
        utcMs(2026, 8, 3)) {
        std::cerr << "Weekly MEXC boundary is not Monday UTC\n";
        ok = false;
    }
    if (currentPeriodOpen(utcMs(2026, 8, 29), 30LL * 24 * 60 * 60 * 1000,
                          Alignment::CalendarMonth) != utcMs(2026, 8, 1) ||
        nextTimestamp(utcMs(2024, 2, 1), 30LL * 24 * 60 * 60 * 1000,
                      Alignment::CalendarMonth) != utcMs(2024, 3, 1)) {
        std::cerr << "Calendar-month MEXC boundary is incorrect\n";
        ok = false;
    }

    TemporaryDirectory tmp;
    CsvTail absentTail;
    std::string error;
    if (!inspectCsvTail(tmp.path() / "missing.csv", header, absentTail, error, minute,
                        Alignment::Fixed) || absentTail.hasData || absentTail.size != 0) {
        std::cerr << "Missing CSV was not treated as an empty base: " << error << '\n';
        ok = false;
    }

    const auto lockPath = tmp.path() / "TEST.lock";
    try {
        {
            DirectoryLock first(lockPath);
            try {
                DirectoryLock second(lockPath);
                std::cerr << "Concurrent process acquired an existing symbol lock\n";
                ok = false;
            } catch (const std::runtime_error &) {
                // Expected: a second writer must fail closed.
            }
        }
        DirectoryLock afterRelease(lockPath);
    } catch (const std::exception &lockError) {
        std::cerr << "Symbol lock lifecycle failed: " << lockError.what() << '\n';
        ok = false;
    }

    const auto staging = tmp.path() / "temp_TEST";
    const auto csv = tmp.path() / "TEST.csv";
    std::filesystem::create_directories(staging);
    ok &= writeFile(csv,
                    header + "\n"
                    "0,1,1,1,1,1,1\n");
    ok &= writeFile(batchPath(staging, 1),
                    "60000,1,1,1,1,1,1\n"
                    "120000,1,1,1,1,1,1\n");
    ok &= writeFile(batchPath(staging, 2), "180000,1,1,1,1,1,1\n");

    Manifest manifest;
    manifest.batchCount = 2;
    manifest.intervalMs = minute;
    manifest.baseTimestamp = 0;
    manifest.baseHasData = true;
    manifest.requestedStart = minute;
    manifest.expectedEnd = 3 * minute;
    manifest.firstTimestamp = minute;
    manifest.lastTimestamp = 3 * minute;

    if (!writeManifest(staging, manifest, error)) {
        std::cerr << "Could not publish complete staging: " << error << '\n';
        return 1;
    }
    if (!readManifest(staging)) {
        std::cerr << "Complete staging manifest cannot be read back\n";
        return 1;
    }
    if (!writeManifest(staging, manifest, error)) {
        std::cerr << "Could not atomically replace an existing manifest: " << error << '\n';
        return 1;
    }

    // A compare-tail check inside commit closes the TOCTOU window between
    // staging and publication, even if a caller forgot the process lock.
    ok &= writeFile(csv,
                    header + "\n"
                    "0,1,1,1,1,1,1\n"
                    "60000,1,1,1,1,1,1\n");
    if (commit(staging, manifest, csv, header, error)) {
        std::cerr << "Commit accepted a CSV tail changed after staging\n";
        ok = false;
    }
    ok &= writeFile(csv,
                    header + "\n"
                    "0,1,1,1,1,1,1\n");
    if (!commit(staging, manifest, csv, header, error)) {
        std::cerr << "Could not commit complete staging: " << error << '\n';
        return 1;
    }

    const auto lines = readLines(csv);
    if (lines.size() != 5 || !lines.back().starts_with("180000,")) {
        std::cerr << "Committed CSV does not contain the complete contiguous transaction\n";
        ok = false;
    }

    if (!truncateAfter(csv, 2 * minute, error, header, minute, Alignment::Fixed)) {
        std::cerr << "Could not transactionally remove an old open tail: " << error << '\n';
        ok = false;
    } else {
        const auto repaired = readLines(csv);
        if (repaired.size() != 4 || !repaired.back().starts_with("120000,")) {
            std::cerr << "Tail repair retained an open/future candle\n";
            ok = false;
        }
    }

    std::filesystem::remove(batchPath(staging, 2));
    if (validate(staging, manifest, error)) {
        std::cerr << "Validation accepted a transaction with a missing batch\n";
        ok = false;
    }

    ok &= writeFile(batchPath(staging, 2), "180000,1,1,1,1,1,1");
    if (validate(staging, manifest, error)) {
        std::cerr << "Validation accepted a batch without a terminating newline\n";
        ok = false;
    }

    ok &= writeFile(batchPath(staging, 2), "180000,1,1,1,1,NaN,1\n");
    if (validate(staging, manifest, error)) {
        std::cerr << "Validation accepted a non-finite numeric staging field\n";
        ok = false;
    }

    const auto gapped = tmp.path() / "GAPPED.csv";
    ok &= writeFile(gapped, header + "\n"
                            "0,1,1,1,1,1,1\n"
                            "120000,1,1,1,1,1,1\n");
    CsvTail gappedTail;
    if (inspectCsvTail(gapped, header, gappedTail, error, minute, Alignment::Fixed)) {
        std::cerr << "Existing CSV inspection accepted an interval gap\n";
        ok = false;
    }

    const auto wrongHeader = tmp.path() / "WRONG.csv";
    const std::string wrongContents = "timestamp,open,high,low,close,volume,amount\n"
                                      "0,1,1,1,1,1,1\n";
    ok &= writeFile(wrongHeader, wrongContents);
    auto wrongHeaderRepair = wrongHeader;
    wrongHeaderRepair += ".tail-repair";
    if (truncateAfter(wrongHeader, 0, error, header, minute, Alignment::Fixed) ||
        readLines(wrongHeader) != std::vector<std::string>{
                                      "timestamp,open,high,low,close,volume,amount",
                                      "0,1,1,1,1,1,1"} ||
        std::filesystem::exists(wrongHeaderRepair)) {
        std::cerr << "Tail repair accepted or modified a CSV with the wrong header\n";
        ok = false;
    }

    return ok ? 0 : 1;
}
