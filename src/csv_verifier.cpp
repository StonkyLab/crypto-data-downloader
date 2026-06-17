/**
CSV Data Verifier / Repairer

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/csv_verifier.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/semaphore.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <future>
#include <spdlog/spdlog.h>

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

struct Record {
    std::int64_t ts{};
    std::string line;
};
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
        return report; // empty file — nothing to verify
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

        if (options.allowMoreFields) {
            // Variable-width layout (MEXC): require at least expectedFields, keep verbatim.
            if (fields.size() < options.expectedFields || !parseTimestamp(fields[0], ts)) {
                report.malformed++;
                continue;
            }
        } else {
            // Fixed-width layout. Accept rows with exactly expectedFields, plus
            // (when salvaging) rows with extra value columns. Normalise every
            // accepted row to the canonical first-expectedFields form so a stray
            // trailing delimiter — an empty legacy turnover column that
            // splitString silently drops, leaving a 6-token row whose raw text
            // still has 7 comma-separated fields and breaks downstream pandas —
            // is rewritten cleanly.
            const bool exact = fields.size() == options.expectedFields;
            const bool extra = options.salvageExtraField && fields.size() > options.expectedFields;
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
        }

        if (prevTs >= 0 && ts < prevTs) {
            report.outOfOrder++;
        }
        prevTs = ts;
        records.push_back({ts, std::move(line)});
        line.clear();
    }
    ifs.close();

    report.totalRecords = records.size();

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
    if (options.intervalMs > 0) {
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
        if (const auto headerFields = splitString(header, ','); headerFields.size() > options.expectedFields &&
                                                                !options.allowMoreFields) {
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

        std::error_code ec;
        std::filesystem::rename(tmpPath, path, ec);
        if (ec) {
            spdlog::error(fmt::format("Failed to replace {}: {}", path, ec.message()));
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
        futures.push_back(std::async(std::launch::async,
                                     [&options, &maxJobs](const std::filesystem::path &p) -> FileReport {
                                         std::scoped_lock w(maxJobs);
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
