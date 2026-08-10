/**
CSV Data Verifier / Repairer

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/csv_verifier.h"
#include "stonky/future_utils.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/semaphore.h"
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <spdlog/spdlog.h>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace stonky {

namespace {
bool parseTimestamp(const std::string &field, std::int64_t &out) {
    if (field.empty() || field.size() > 19 || field[0] == '-') {
        return false;
    }
    std::size_t pos = 0;
    try {
        out = std::stoll(field, &pos);
    } catch (const std::exception &) {
        return false;
    }
    return pos == field.size();
}

bool parseFiniteNumber(const std::string &field) {
    if (field.empty()) {
        return false;
    }
    char *end = nullptr;
    errno = 0;
    const auto value = std::strtold(field.c_str(), &end);
    return end == field.c_str() + field.size() && errno != ERANGE && std::isfinite(value);
}

struct Record {
    std::int64_t ts{};
    std::string line;
};

bool matchesHeader(const std::vector<std::string> &fields, const CsvVerifier::Options &options) {
    static const std::vector<std::string> funding{"funding_time", "funding_rate"};
    static const std::vector<std::string> ohlcv{"open_time", "open", "high", "low", "close", "volume"};
    static const std::vector<std::string> legacyBybit{
        "open_time", "open", "high", "low", "close", "volume", "turnover"};
    static const std::vector<std::string> mexcSpot{
        "open_time", "open", "high", "low", "close", "volume", "quote_asset_volume"};
    static const std::vector<std::string> mexcFutures{
        "open_time", "open", "high", "low", "close", "volume", "amount"};
    static const std::vector<std::string> okx{
        "open_time", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote"};
    static const std::vector<std::string> binance{
        "close_time", "open", "high", "low", "close", "volume", "timestamp", "quote_av",
        "trades", "tb_base_av", "tb_quote_av", "ignore"};

    switch (options.expectedFields) {
        case 2:
            return fields == funding;
        case 6:
            return fields == ohlcv || (options.salvageExtraField && fields == legacyBybit);
        case 7:
            return fields == mexcSpot || fields == mexcFutures;
        case 8:
            return fields == okx;
        case 12:
            return fields == binance;
        default:
            return false;
    }
}

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

std::int64_t nextCalendarTimestamp(const std::int64_t timestamp,
                                   const bool timestampIsClose,
                                   const std::int32_t utcOffsetMinutes) {
    using namespace std::chrono;
    const auto offset = minutes{utcOffsetMinutes};
    const auto localBase = milliseconds{timestamp + (timestampIsClose ? 1 : 0)} + offset;
    const year_month_day current{floor<days>(sys_time<milliseconds>{localBase})};
    const year_month_day next = current.year() / current.month() / day{1} + months{1};
    const auto nextOpen = duration_cast<milliseconds>((sys_days{next} - offset).time_since_epoch()).count();
    if (!timestampIsClose) {
        return nextOpen;
    }
    return nextOpen - 1;
}
} // namespace

CsvVerifier::FileReport CsvVerifier::verifyFile(const std::string &path, const Options &options) {
    FileReport report;
    report.path = path;

    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        report.readFailed = true;
        spdlog::error(fmt::format("Couldn't open file: {}", path));
        return report;
    }

    std::string header;
    if (!std::getline(ifs, header)) {
        report.readFailed = true;
        spdlog::error(fmt::format("CSV file is empty: {}", path));
        return report;
    }
    if (!header.empty() && header.back() == '\r') {
        header.pop_back();
    }
    const auto headerFields = splitString(header, ',');
    if (!matchesHeader(headerFields, options)) {
        report.readFailed = true;
        spdlog::error(fmt::format("Invalid CSV header in {}", path));
        return report;
    }

    std::vector<Record> records;
    std::string line;
    std::int64_t prevTs = -1;

    while (std::getline(ifs, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }

        const auto fields = splitString(line, ',');
        std::int64_t ts = 0;

        // Fixed-width layout. Accept exactly one known legacy trailing Bybit
        // turnover field when requested. Arbitrary extra fields are never
        // accepted: they may be the suffix of a glued record.
        const bool exact = fields.size() == options.expectedFields;
        const bool legacyExtraHeader = options.salvageExtraField &&
                                       headerFields.size() == options.expectedFields + 1 &&
                                       headerFields.back() == "turnover";
        const bool extra = legacyExtraHeader &&
                           fields.size() == options.expectedFields + 1;
        if ((!exact && !extra) || !parseTimestamp(fields[0], ts)) {
            report.malformed++;
            continue;
        }
        const bool trailingDelim = !line.empty() && line.back() == ',';
        if (!exact || trailingDelim) {
            std::string canonical = fields[0];
            for (std::size_t i = 1; i < options.expectedFields; ++i) {
                canonical += ',';
                canonical += fields[i];
            }
            line = std::move(canonical);
            report.salvaged++;
        }

        // All downloader schemas put OHLCV/funding values after the timestamp.
        // Reject NaN/Inf, empty cells and glued text even when the field count
        // happens to look plausible.
        const auto numericEnd = std::min(fields.size(), options.expectedFields);
        bool numeric = true;
        for (std::size_t i = 1; i < numericEnd; ++i) {
            if (!parseFiniteNumber(fields[i])) {
                numeric = false;
                break;
            }
        }
        if (!numeric) {
            report.malformed++;
            continue;
        }

        if (prevTs >= 0 && ts < prevTs) {
            report.outOfOrder++;
        }
        prevTs = ts;
        records.push_back({ts, std::move(line)});
        line.clear();
    }
    if (ifs.bad()) {
        report.readFailed = true;
        spdlog::error(fmt::format("Failed while reading CSV file: {}", path));
        return report;
    }
    ifs.close();

    report.totalRecords = records.size();
    if (records.empty()) {
        report.readFailed = true;
        spdlog::error(fmt::format("CSV file contains no valid data rows: {}", path));
        return report;
    }

    // Sort + dedup (keep the FIRST occurrence — the original download; later
    // occurrences come from historical re-appends)
    std::stable_sort(records.begin(), records.end(),
                     [](const Record &a, const Record &b) { return a.ts < b.ts; });

    std::vector<Record> unique;
    unique.reserve(records.size());
    for (auto &rec: records) {
        if (!unique.empty() && unique.back().ts == rec.ts) {
            report.duplicates++;
            continue;
        }
        unique.push_back(std::move(rec));
    }

    // Gap analysis (report only)
    if (options.calendarMonth) {
        const bool timestampIsClose = headerFields[0] == "close_time";
        for (std::size_t i = 1; i < unique.size(); ++i) {
            auto expected = nextCalendarTimestamp(unique[i - 1].ts, timestampIsClose,
                                                  options.calendarUtcOffsetMinutes);
            if (unique[i].ts != expected) {
                if (report.gaps == 0) {
                    report.firstGapTs = unique[i - 1].ts;
                }
                ++report.gaps;
                std::size_t guard = 0;
                while (expected < unique[i].ts && guard++ < 1200) {
                    ++report.missingBars;
                    expected = nextCalendarTimestamp(expected, timestampIsClose,
                                                     options.calendarUtcOffsetMinutes);
                }
            }
        }
    } else if (options.intervalMs > 0) {
        for (std::size_t i = 1; i < unique.size(); ++i) {
            const auto delta = unique[i].ts - unique[i - 1].ts;
            if (delta != options.intervalMs) {
                if (report.gaps == 0) {
                    report.firstGapTs = unique[i - 1].ts;
                }
                report.gaps++;
                if (delta > options.intervalMs) {
                    report.missingBars += static_cast<std::size_t>(delta / options.intervalMs - 1);
                }
            }
        }
    }

    if (options.repair && report.needsRepair()) {
        // Canonical header: first expectedFields names of the original header
        // (drops a legacy trailing column name when salvaging)
        std::string outHeader = header;
        if (const auto headerFields = splitString(header, ','); headerFields.size() > options.expectedFields) {
            outHeader = headerFields[0];
            for (std::size_t i = 1; i < options.expectedFields; ++i) {
                outHeader += ',';
                outHeader += headerFields[i];
            }
        }

        const std::string tmpPath = path + ".repair.tmp";
        std::ofstream ofs(tmpPath, std::ios::trunc);
        if (!ofs.is_open()) {
            spdlog::error(fmt::format("Couldn't open temp file: {}", tmpPath));
            report.readFailed = true;
            return report;
        }

        ofs << outHeader << '\n';
        for (const auto &rec: unique) {
            ofs << rec.line << '\n';
        }
        ofs.flush();
        if (!ofs.good()) {
            spdlog::error(fmt::format("Write to temp file failed: {}", tmpPath));
            ofs.close();
            std::error_code ec;
            std::filesystem::remove(tmpPath, ec);
            report.readFailed = true;
            return report;
        }
        ofs.close();
        if (!ofs.good()) {
            spdlog::error(fmt::format("Closing temp file failed: {}", tmpPath));
            std::error_code ec;
            std::filesystem::remove(tmpPath, ec);
            report.readFailed = true;
            return report;
        }

        std::string replaceError;
        if (!replaceFile(tmpPath, path, replaceError)) {
            spdlog::error(fmt::format("Failed to replace {}: {}", path, replaceError));
            std::error_code ec;
            std::filesystem::remove(tmpPath, ec);
            report.readFailed = true;
            return report;
        }
        report.repaired = true;
        report.totalRecords = unique.size();
    }

    return report;
}

std::vector<CsvVerifier::FileReport> CsvVerifier::verifyDirectory(const std::string &dirPath,
                                                                  const Options &options) {
    std::vector<FileReport> reports;

    if (!std::filesystem::exists(dirPath)) {
        spdlog::error(fmt::format("Directory does not exist: {}", dirPath));
        return reports;
    }

    std::vector<std::filesystem::path> files;
    for (const auto &entry: std::filesystem::directory_iterator(dirPath)) {
        if (entry.is_regular_file() && entry.path().extension() == ".csv") {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());

    spdlog::info(fmt::format("Verifying {} CSV files in {}{}...", files.size(), dirPath,
                             options.repair ? " (repair mode)" : ""));

    Semaphore maxJobs{options.maxJobs > 0 ? options.maxJobs : 1};
    std::vector<std::future<FileReport> > futures;
    futures.reserve(files.size());

    for (const auto &file: files) {
        futures.push_back(launchBounded(maxJobs,
                                     [&options](const std::filesystem::path &p) -> FileReport {
                                         return verifyFile(p.string(), options);
                                     }, file));
    }

    std::size_t issueFiles = 0;
    for (auto &future: futures) {
        auto report = future.get();
        if (report.needsRepair() || report.gaps > 0 || report.readFailed) {
            issueFiles++;
            const auto fileName = std::filesystem::path(report.path).filename().string();
            std::string gapInfo;
            if (report.gaps > 0) {
                gapInfo = fmt::format(", gaps: {} ({} missing bars, first after {} UTC — short gaps at "
                                      "identical times across symbols are exchange outages (not repairable); "
                                      "large blocks may be refillable by delete + re-download)",
                                      report.gaps, report.missingBars,
                                      getDateTimeStringFromTimeStamp(report.firstGapTs, "%Y-%m-%d %H:%M", true));
            }
            spdlog::warn(fmt::format(
                "{}: records: {}, malformed: {}, salvaged: {}, duplicates: {}, out-of-order: {}{}{}",
                fileName, report.totalRecords, report.malformed, report.salvaged,
                report.duplicates, report.outOfOrder, gapInfo,
                report.repaired ? " -> REPAIRED" : ""));
        }
        reports.push_back(std::move(report));
    }

    spdlog::info(fmt::format("Verification finished: {} files OK, {} files with issues{}",
                             reports.size() - issueFiles, issueFiles,
                             options.repair ? " (repaired where possible)" : ""));
    return reports;
}

} // namespace stonky
