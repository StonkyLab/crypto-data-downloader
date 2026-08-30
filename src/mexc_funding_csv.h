/**
Transactional CSV persistence for MEXC funding-rate history.

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/
#ifndef STONKY_MEXC_FUNDING_CSV_H
#define STONKY_MEXC_FUNDING_CSV_H

#include "stonky/atomic_file.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <fmt/format.h>

namespace stonky::mexc_funding_csv {

inline constexpr std::string_view Header = "funding_time,funding_rate";

struct Tail {
    bool hasData{};
    std::int64_t firstTimestamp{};
    std::int64_t timestamp{};
    std::uintmax_t size{};
    bool newlineTerminated{};

    friend bool operator==(const Tail &, const Tail &) = default;
};

struct Record {
    std::int64_t timestamp{};
    std::string rate;

    friend bool operator==(const Record &, const Record &) = default;
};

/**
 * --since is a fresh-file floor, never a way to jump over an existing tail.
 * MEXC funding is deliberately full-scanned and unioned on every run, so an
 * established CSV uses a boundary no later than either the venue default or
 * its actual tail.  A fresh file raises the venue boundary to the configured
 * inclusive floor.
 */
inline std::int64_t downloadCutoff(const bool existingData,
                                   const std::int64_t existingTail,
                                   const std::int64_t exchangeDefault,
                                   const std::int64_t configuredFloor) {
    return existingData ? std::min(exchangeDefault, existingTail)
                        : std::max(exchangeDefault, configuredFloor);
}

inline bool atOrAfterDownloadCutoff(const std::int64_t timestamp,
                                    const std::int64_t cutoff) {
    return timestamp >= cutoff;
}

/** Empty filtered history is a safe success only when a CSV already exists. */
inline bool emptyDownloadIsNoOp(const bool existingData) {
    return existingData;
}

inline bool parseFiniteBinary64(std::string_view value, double &parsed) {
    if (value.empty()) {
        return false;
    }
    if (value.front() == '+') {
        value.remove_prefix(1); // floating from_chars rejects leading '+'
    }
    if (value.empty()) {
        return false;
    }
    const auto *begin = value.data();
    const auto *end = begin + value.size();
    const auto [ptr, ec] = std::from_chars(begin, end, parsed, std::chars_format::general);
    return ec == std::errc{} && ptr == end && std::isfinite(parsed);
}

inline bool isDecimalNumber(const std::string_view value) {
    if (value.empty()) {
        return false;
    }

    std::size_t offset = 0;
    if (value[offset] == '+' || value[offset] == '-') {
        ++offset;
    }

    bool hasDigit = false;
    while (offset < value.size() && value[offset] >= '0' && value[offset] <= '9') {
        hasDigit = true;
        ++offset;
    }
    if (offset < value.size() && value[offset] == '.') {
        ++offset;
        while (offset < value.size() && value[offset] >= '0' && value[offset] <= '9') {
            hasDigit = true;
            ++offset;
        }
    }
    if (!hasDigit) {
        return false;
    }

    if (offset < value.size() && (value[offset] == 'e' || value[offset] == 'E')) {
        ++offset;
        if (offset < value.size() && (value[offset] == '+' || value[offset] == '-')) {
            ++offset;
        }
        const auto exponentStart = offset;
        while (offset < value.size() && value[offset] >= '0' && value[offset] <= '9') {
            ++offset;
        }
        if (offset == exponentStart) {
            return false;
        }
    }
    if (offset != value.size()) {
        return false;
    }

    // The repository's numeric storage contract is binary64.  A syntactically
    // valid decimal such as 1e1000 must not pass validation only to become
    // infinity when a backtest loads it as double.
    double parsed{};
    return parseFiniteBinary64(value, parsed);
}

inline bool parseRecord(const std::string_view line, Record &record) {
    const auto comma = line.find(',');
    if (comma == 0 || comma == std::string_view::npos ||
        line.find(',', comma + 1) != std::string_view::npos) {
        return false;
    }

    const char *begin = line.data();
    const char *end = begin + comma;
    const auto [ptr, ec] = std::from_chars(begin, end, record.timestamp);
    if (ec != std::errc{} || ptr != end || record.timestamp < 0) {
        return false;
    }

    const auto rate = line.substr(comma + 1);
    if (!isDecimalNumber(rate)) {
        return false;
    }
    record.rate.assign(rate);
    return true;
}

/**
 * Validate the complete existing file.  Funding intervals may change at the
 * venue, so continuity here means a strictly increasing timestamp sequence;
 * imposing a fixed 8-hour delta would reject legitimate history.
 */
inline bool inspectImpl(const std::filesystem::path &path, Tail &tail,
                        std::vector<Record> *records, std::string &error) {
    tail = {};
    if (records) {
        records->clear();
    }
    error.clear();

    std::error_code ec;
    const auto status = std::filesystem::symlink_status(path, ec);
    if (ec) {
        if (ec == std::errc::no_such_file_or_directory) {
            return true;
        }
        error = fmt::format("cannot stat funding CSV {}: {}", path.string(), ec.message());
        return false;
    }
    if (!std::filesystem::exists(status)) {
        return true;
    }
    if (!std::filesystem::is_regular_file(status)) {
        error = fmt::format("funding CSV path is not a regular file: {}", path.string());
        return false;
    }

    tail.size = std::filesystem::file_size(path, ec);
    if (ec) {
        error = fmt::format("cannot determine funding CSV size {}: {}", path.string(), ec.message());
        return false;
    }
    if (tail.size == 0) {
        return true;
    }

    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        error = fmt::format("cannot open funding CSV {}", path.string());
        return false;
    }

    input.seekg(-1, std::ios::end);
    const auto finalByte = input.get();
    if (finalByte == std::char_traits<char>::eof() || input.bad()) {
        error = fmt::format("cannot inspect funding CSV ending {}", path.string());
        return false;
    }
    tail.newlineTerminated = finalByte == '\n';
    input.clear();
    input.seekg(0, std::ios::beg);
    if (!input.good()) {
        error = fmt::format("cannot rewind funding CSV {}", path.string());
        return false;
    }

    std::string line;
    if (!std::getline(input, line)) {
        error = fmt::format("cannot read funding CSV header {}", path.string());
        return false;
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    if (line != Header) {
        error = fmt::format("unexpected funding CSV header in {}", path.string());
        return false;
    }

    std::optional<std::int64_t> previous;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        Record record;
        if (!parseRecord(line, record)) {
            error = fmt::format("invalid funding CSV record in {}", path.string());
            return false;
        }
        if (previous && record.timestamp <= *previous) {
            error = fmt::format("non-increasing funding timestamps in {}: {} followed by {}",
                                path.string(), *previous, record.timestamp);
            return false;
        }
        if (!previous) {
            tail.firstTimestamp = record.timestamp;
        }
        previous = record.timestamp;
        if (records) {
            records->push_back(std::move(record));
        }
    }
    if (input.bad()) {
        error = fmt::format("failed while reading funding CSV {}", path.string());
        return false;
    }

    if (previous) {
        tail.hasData = true;
        tail.timestamp = *previous;
    }
    return true;
}

inline bool inspect(const std::filesystem::path &path, Tail &tail, std::string &error) {
    return inspectImpl(path, tail, nullptr, error);
}

inline bool readRecords(const std::filesystem::path &path, Tail &tail,
                        std::vector<Record> &records, std::string &error) {
    return inspectImpl(path, tail, &records, error);
}

inline constexpr std::string_view ProvisionalMarkerContents =
    "mexc-funding-prefix-provisional-v1\n";

inline std::filesystem::path provisionalMarkerPath(const std::filesystem::path &csvPath) {
    auto path = csvPath;
    path += ".prefix-provisional";
    return path;
}

inline bool inspectProvisionalMarker(const std::filesystem::path &csvPath, bool &exists,
                                     std::string &error) {
    exists = false;
    error.clear();
    const auto marker = provisionalMarkerPath(csvPath);
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(marker, ec);
    if (ec) {
        if (ec == std::errc::no_such_file_or_directory) {
            return true;
        }
        error = fmt::format("cannot stat MEXC funding prefix marker {}: {}", marker.string(),
                            ec.message());
        return false;
    }
    if (!std::filesystem::exists(status)) {
        return true;
    }
    if (!std::filesystem::is_regular_file(status)) {
        error = fmt::format("MEXC funding prefix marker is not a regular file: {}",
                            marker.string());
        return false;
    }

    std::ifstream input(marker, std::ios::binary);
    if (!input.is_open()) {
        error = fmt::format("cannot open MEXC funding prefix marker {}", marker.string());
        return false;
    }
    const std::string contents{std::istreambuf_iterator<char>{input},
                               std::istreambuf_iterator<char>{}};
    if (input.bad()) {
        error = fmt::format("cannot read MEXC funding prefix marker {}", marker.string());
        return false;
    }
    if (contents != ProvisionalMarkerContents) {
        error = fmt::format("invalid MEXC funding prefix marker {}", marker.string());
        return false;
    }
    exists = true;
    return true;
}

inline bool ensureProvisionalMarker(const std::filesystem::path &csvPath, std::string &error) {
    bool exists = false;
    if (!inspectProvisionalMarker(csvPath, exists, error)) {
        return false;
    }
    if (exists) {
        return true;
    }

    const auto marker = provisionalMarkerPath(csvPath);
    AtomicFileWriter output(marker, std::ios::binary);
    if (!output.isOpen()) {
        error = output.error();
        return false;
    }
    output.stream() << ProvisionalMarkerContents;
    if (!output.stream().good()) {
        error = fmt::format("cannot write MEXC funding prefix marker {}", marker.string());
        return false;
    }
    if (!output.commit(error)) {
        return false;
    }
    return true;
}

inline bool validateRecordSequence(const std::span<const Record> records, std::string &error) {
    std::optional<std::int64_t> previous;
    for (const auto &record : records) {
        if (record.timestamp < 0 || !isDecimalNumber(record.rate)) {
            error = "invalid record in MEXC funding transaction";
            return false;
        }
        if (previous && record.timestamp <= *previous) {
            error = fmt::format("non-increasing MEXC funding transaction: {} followed by {}",
                                *previous, record.timestamp);
            return false;
        }
        previous = record.timestamp;
    }
    return true;
}

inline bool mergeRecords(const std::span<const Record> existing,
                         const std::span<const Record> downloaded,
                         std::vector<Record> &merged, std::string &error) {
    if (!validateRecordSequence(existing, error) || !validateRecordSequence(downloaded, error)) {
        return false;
    }

    merged.clear();
    merged.reserve(existing.size() + downloaded.size());
    std::size_t oldIndex = 0;
    std::size_t newIndex = 0;
    while (oldIndex < existing.size() || newIndex < downloaded.size()) {
        if (newIndex >= downloaded.size() ||
            (oldIndex < existing.size() &&
             existing[oldIndex].timestamp < downloaded[newIndex].timestamp)) {
            merged.push_back(existing[oldIndex++]);
            continue;
        }
        if (oldIndex >= existing.size() ||
            downloaded[newIndex].timestamp < existing[oldIndex].timestamp) {
            merged.push_back(downloaded[newIndex++]);
            continue;
        }

        double oldValue{};
        double newValue{};
        if (!parseFiniteBinary64(existing[oldIndex].rate, oldValue) ||
            !parseFiniteBinary64(downloaded[newIndex].rate, newValue) || oldValue != newValue) {
            error = fmt::format("conflicting MEXC funding values at timestamp {}",
                                existing[oldIndex].timestamp);
            return false;
        }
        // Prefer the freshly normalized representation when both values map to
        // the same supported binary64 value.
        merged.push_back(downloaded[newIndex]);
        ++oldIndex;
        ++newIndex;
    }
    return true;
}

inline bool replaceAtomically(const std::filesystem::path &path, const Tail &expectedBase,
                              const std::span<const Record> records, std::string &error) {
    error.clear();
    bool provisional = false;
    if (!inspectProvisionalMarker(path, provisional, error)) {
        return false;
    }
    if (!provisional) {
        error = "refusing to replace MEXC funding history without a valid provisional marker";
        return false;
    }
    if (records.empty()) {
        error = "refusing to publish an empty MEXC funding transaction";
        return false;
    }
    if (!validateRecordSequence(records, error)) {
        return false;
    }

    Tail current;
    if (!inspect(path, current, error)) {
        return false;
    }
    if (current != expectedBase) {
        error = "funding CSV changed after its records were inspected";
        return false;
    }

    AtomicFileWriter replacement(path, std::ios::binary);
    if (!replacement.isOpen()) {
        error = replacement.error();
        return false;
    }
    auto &output = replacement.stream();
    output << Header << '\n';
    for (const auto &record : records) {
        output << record.timestamp << ',' << record.rate << '\n';
        if (!output.good()) {
            error = fmt::format("failed to write MEXC funding replacement {}", path.string());
            return false;
        }
    }
    if (!replacement.commit(error)) {
        return false;
    }
    return true;
}

inline bool copyExactly(std::ifstream &input, std::ofstream &output,
                        const std::uintmax_t expected, const std::filesystem::path &source,
                        std::string &error) {
    std::array<char, 64 * 1024> buffer{};
    std::uintmax_t copied = 0;
    while (input) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const auto count = input.gcount();
        if (count > 0) {
            output.write(buffer.data(), count);
            copied += static_cast<std::uintmax_t>(count);
        }
    }
    if (input.bad() || copied != expected || !output.good()) {
        error = fmt::format("failed to copy funding CSV {} completely ({} of {} bytes)",
                            source.string(), copied, expected);
        return false;
    }
    return true;
}

/**
 * Atomically publish existing data plus a validated append transaction.
 *
 * Runs are serialized per exchange by the update scripts' flock, so no per-file
 * lock is taken around this.  Re-inspecting and comparing the base before
 * publishing still closes the TOCTOU window, and that check is what actually
 * protects the file.
 */
inline bool appendAtomically(const std::filesystem::path &path, const Tail &expectedBase,
                             const std::span<const Record> records, std::string &error) {
    error.clear();
    if (records.empty()) {
        return true;
    }

    std::optional<std::int64_t> previous = expectedBase.hasData
        ? std::optional<std::int64_t>{expectedBase.timestamp}
        : std::nullopt;
    for (const auto &record : records) {
        if (record.timestamp < 0 || !isDecimalNumber(record.rate)) {
            error = "invalid record in funding append transaction";
            return false;
        }
        if (previous && record.timestamp <= *previous) {
            error = fmt::format("funding append is not strictly newer: {} followed by {}",
                                *previous, record.timestamp);
            return false;
        }
        previous = record.timestamp;
    }

    Tail current;
    if (!inspect(path, current, error)) {
        return false;
    }
    if (current != expectedBase) {
        error = "funding CSV changed after its tail was inspected";
        return false;
    }

    AtomicFileWriter replacement(path, std::ios::binary);
    if (!replacement.isOpen()) {
        error = replacement.error();
        return false;
    }
    auto &output = replacement.stream();

    if (current.size > 0) {
        std::ifstream existing(path, std::ios::binary);
        if (!existing.is_open()) {
            error = fmt::format("cannot reopen funding CSV {}", path.string());
            return false;
        }
        if (!copyExactly(existing, output, current.size, path, error)) {
            return false;
        }
        if (!current.newlineTerminated) {
            output.put('\n');
        }
    } else {
        output << Header << '\n';
    }
    if (!output.good()) {
        error = fmt::format("failed to prepare funding CSV output {}", path.string());
        return false;
    }

    for (const auto &record : records) {
        output << record.timestamp << ',' << record.rate << '\n';
        if (!output.good()) {
            error = fmt::format("failed to write funding CSV output {}", path.string());
            return false;
        }
    }

    if (!replacement.commit(error)) {
        return false;
    }
    return true;
}

} // namespace stonky::mexc_funding_csv

#endif // STONKY_MEXC_FUNDING_CSV_H
