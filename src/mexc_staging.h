/**
Transactional staging helpers shared by the MEXC spot and futures downloaders.

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.

A staging directory is intentionally disposable until complete.manifest has
been flushed. A process interrupted while downloading or writing batches
therefore cannot append a newest-first fragment across an unvisited gap.
*/
#ifndef STONKY_MEXC_STAGING_H
#define STONKY_MEXC_STAGING_H

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <fmt/format.h>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace stonky::mexc_staging {

enum class Alignment : std::int32_t {
    Fixed = 0,
    WeekMonday = 1,
    CalendarMonth = 2,
};

struct Manifest {
    std::int32_t version{1};
    std::int32_t batchCount{};
    std::int64_t intervalMs{};
    Alignment alignment{Alignment::Fixed};
    std::int64_t baseTimestamp{};
    bool baseHasData{};
    std::int64_t requestedStart{};
    std::int64_t expectedEnd{};
    std::int64_t firstTimestamp{};
    std::int64_t lastTimestamp{};
};

/**
 * A deliberately fail-closed inter-process lock. Directory creation is atomic
 * on the supported filesystems. A crashed process leaves a stale lock behind;
 * the next run reports its exact path instead of guessing that it is safe to
 * break the lock and risking concurrent writes.
 */
class DirectoryLock {
public:
    explicit DirectoryLock(std::filesystem::path path) : path_(std::move(path)) {
        std::error_code ec;
        const bool created = std::filesystem::create_directory(path_, ec);
        if (!created) {
            if (ec) {
                throw std::runtime_error(fmt::format("cannot acquire lock {}: {}", path_.string(),
                                                     ec.message()));
            }
            throw std::runtime_error(fmt::format(
                "lock {} already exists (another process is active, or remove this stale lock after verification)",
                path_.string()));
        }
    }

    DirectoryLock(const DirectoryLock &) = delete;
    DirectoryLock &operator=(const DirectoryLock &) = delete;
    DirectoryLock(DirectoryLock &&) = delete;
    DirectoryLock &operator=(DirectoryLock &&) = delete;

    ~DirectoryLock() {
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }

private:
    std::filesystem::path path_;
};

class RemoveUnlessReleased {
public:
    explicit RemoveUnlessReleased(std::filesystem::path path) : path_(std::move(path)) {}
    RemoveUnlessReleased(const RemoveUnlessReleased &) = delete;
    RemoveUnlessReleased &operator=(const RemoveUnlessReleased &) = delete;
    ~RemoveUnlessReleased() {
        if (active_) {
            std::error_code ec;
            std::filesystem::remove(path_, ec);
        }
    }
    void release() noexcept { active_ = false; }

private:
    std::filesystem::path path_;
    bool active_{true};
};

inline std::filesystem::path batchPath(const std::filesystem::path &dir, const std::int32_t index) {
    return dir / fmt::format("batch_{:05d}.tmp", index);
}

inline std::filesystem::path manifestPath(const std::filesystem::path &dir) {
    return dir / "complete.manifest";
}

inline bool replaceAtomically(const std::filesystem::path &source,
                              const std::filesystem::path &destination, std::string &error) {
#ifdef _WIN32
    if (!::MoveFileExW(source.c_str(), destination.c_str(),
                       MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        error = fmt::format("failed to atomically replace {} (Windows error {})",
                            destination.string(), static_cast<unsigned long>(::GetLastError()));
        return false;
    }
    return true;
#else
    std::error_code ec;
    std::filesystem::rename(source, destination, ec);
    if (ec) {
        error = fmt::format("failed to atomically replace {}: {}", destination.string(),
                            ec.message());
        return false;
    }
    return true;
#endif
}

inline std::int64_t nextTimestamp(const std::int64_t timestampMs, const std::int64_t intervalMs,
                                  const Alignment alignment) {
    if (alignment != Alignment::CalendarMonth) {
        if (timestampMs > std::numeric_limits<std::int64_t>::max() - intervalMs) {
            throw std::overflow_error("MEXC candle timestamp overflow");
        }
        return timestampMs + intervalMs;
    }

    using namespace std::chrono;
    const auto dayPoint = floor<days>(sys_time<milliseconds>{milliseconds{timestampMs}});
    const year_month_day ymd{dayPoint};
    const year_month_day next = ymd.year() / ymd.month() / std::chrono::day{1} + months{1};
    return duration_cast<milliseconds>(sys_days{next}.time_since_epoch()).count();
}

inline std::int64_t currentPeriodOpen(const std::int64_t timestampMs, const std::int64_t intervalMs,
                                      const Alignment alignment) {
    using namespace std::chrono;

    if (alignment == Alignment::CalendarMonth) {
        const auto day = floor<days>(sys_time<milliseconds>{milliseconds{timestampMs}});
        const year_month_day ymd{day};
        return duration_cast<milliseconds>(
            sys_days{ymd.year() / ymd.month() / std::chrono::day{1}}.time_since_epoch()).count();
    }

    if (alignment == Alignment::WeekMonday) {
        constexpr std::int64_t mondayOffsetMs = 4LL * 24 * 60 * 60 * 1000;
        return ((timestampMs - mondayOffsetMs) / intervalMs) * intervalMs + mondayOffsetMs;
    }

    return (timestampMs / intervalMs) * intervalMs;
}

inline bool isAlignedTimestamp(const std::int64_t timestampMs, const std::int64_t intervalMs,
                               const Alignment alignment) {
    if (timestampMs < 0 || intervalMs <= 0) {
        return false;
    }
    try {
        return currentPeriodOpen(timestampMs, intervalMs, alignment) == timestampMs;
    } catch (...) {
        return false;
    }
}

inline bool isNextTimestamp(const std::int64_t previous, const std::int64_t timestamp,
                            const std::int64_t intervalMs, const Alignment alignment) {
    try {
        return timestamp == nextTimestamp(previous, intervalMs, alignment);
    } catch (...) {
        return false;
    }
}

inline std::int64_t previousPeriodOpen(const std::int64_t periodOpenMs, const std::int64_t intervalMs,
                                       const Alignment alignment) {
    if (alignment != Alignment::CalendarMonth) {
        return periodOpenMs - intervalMs;
    }

    using namespace std::chrono;
    const auto day = floor<days>(sys_time<milliseconds>{milliseconds{periodOpenMs}});
    const year_month_day ymd{day};
    const year_month_day previous = ymd.year() / ymd.month() / std::chrono::day{1} - months{1};
    return duration_cast<milliseconds>(sys_days{previous}.time_since_epoch()).count();
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
    return offset == value.size();
}

inline bool parseTimestamp(const std::string &line, std::int64_t &timestamp) {
    // MEXC candle files have exactly seven fields.  Requiring the exact schema
    // prevents a glued/torn line with a valid timestamp prefix being accepted.
    if (std::count(line.begin(), line.end(), ',') != 6) {
        return false;
    }
    const auto comma = line.find(',');
    if (comma == 0 || comma == std::string::npos) {
        return false;
    }
    const char *begin = line.data();
    const char *end = begin + comma;
    const auto [ptr, ec] = std::from_chars(begin, end, timestamp);
    if (ec != std::errc{} || ptr != end || timestamp < 0) {
        return false;
    }

    std::size_t fieldStart = comma + 1;
    for (std::int32_t field = 1; field < 7; ++field) {
        const auto fieldEnd = field == 6 ? line.size() : line.find(',', fieldStart);
        if (fieldEnd == std::string::npos ||
            !isDecimalNumber(std::string_view{line}.substr(fieldStart, fieldEnd - fieldStart))) {
            return false;
        }
        fieldStart = fieldEnd + 1;
    }
    return true;
}

struct CsvTail {
    bool hasData{};
    std::int64_t timestamp{};
    std::uintmax_t size{};
};

inline bool inspectCsvTail(const std::filesystem::path &path, const std::string &expectedHeader,
                           CsvTail &tail, std::string &error, const std::int64_t intervalMs = 0,
                           const Alignment alignment = Alignment::Fixed) {
    tail = {};
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(path, ec);
    if (ec) {
        if (ec == std::errc::no_such_file_or_directory) {
            return true;
        }
        error = fmt::format("cannot stat existing CSV {}: {}", path.string(), ec.message());
        return false;
    }
    if (!std::filesystem::exists(status)) {
        return true;
    }
    if (!std::filesystem::is_regular_file(status)) {
        error = fmt::format("existing CSV path is not a regular file: {}", path.string());
        return false;
    }
    tail.size = std::filesystem::file_size(path, ec);
    if (ec) {
        error = fmt::format("cannot determine existing CSV size {}: {}", path.string(), ec.message());
        return false;
    }
    if (tail.size == 0) {
        return true;
    }

    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        error = fmt::format("cannot open existing CSV {}", path.string());
        return false;
    }
    std::string line;
    if (!std::getline(input, line)) {
        error = fmt::format("cannot read existing CSV header {}", path.string());
        return false;
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    if (line != expectedHeader) {
        error = fmt::format("unexpected existing CSV header in {}", path.string());
        return false;
    }

    std::optional<std::int64_t> previous;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        std::int64_t timestamp{};
        if (!parseTimestamp(line, timestamp)) {
            error = fmt::format("invalid existing CSV record in {}", path.string());
            return false;
        }
        if (intervalMs > 0 && !isAlignedTimestamp(timestamp, intervalMs, alignment)) {
            error = fmt::format("misaligned existing CSV timestamp in {}", path.string());
            return false;
        }
        if (previous && ((intervalMs > 0 && !isNextTimestamp(*previous, timestamp, intervalMs,
                                                              alignment)) ||
                         (intervalMs <= 0 && timestamp <= *previous))) {
            error = fmt::format("non-contiguous existing CSV timestamps in {}", path.string());
            return false;
        }
        previous = timestamp;
    }
    if (input.bad()) {
        error = fmt::format("failed while reading existing CSV {}", path.string());
        return false;
    }
    if (previous) {
        tail.hasData = true;
        tail.timestamp = *previous;
    }
    return true;
}

inline bool copyExactly(std::ifstream &input, std::ofstream &output, const std::uintmax_t expected,
                        const std::filesystem::path &source, std::string &error) {
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
        error = fmt::format("failed to copy {} completely ({} of {} bytes)", source.string(),
                            copied, expected);
        return false;
    }
    return true;
}

inline bool validate(const std::filesystem::path &dir, const Manifest &manifest, std::string &error) {
    const auto alignmentValue = static_cast<std::int32_t>(manifest.alignment);
    if (manifest.version != 1 || manifest.batchCount <= 0 || manifest.batchCount > 1000000 ||
        manifest.intervalMs <= 0 || alignmentValue < static_cast<std::int32_t>(Alignment::Fixed) ||
        alignmentValue > static_cast<std::int32_t>(Alignment::CalendarMonth)) {
        error = "invalid staging manifest header";
        return false;
    }
    if (manifest.requestedStart < 0 || manifest.expectedEnd < manifest.requestedStart ||
        manifest.firstTimestamp < manifest.requestedStart ||
        manifest.firstTimestamp > manifest.lastTimestamp ||
        manifest.lastTimestamp != manifest.expectedEnd) {
        error = "staged timestamp bounds do not match the requested range";
        return false;
    }
    try {
        if (manifest.baseHasData &&
            (manifest.requestedStart != manifest.firstTimestamp ||
             manifest.firstTimestamp != nextTimestamp(manifest.baseTimestamp, manifest.intervalMs,
                                                       manifest.alignment))) {
            error = "staging does not begin immediately after the existing CSV tail";
            return false;
        }
    } catch (const std::exception &e) {
        error = fmt::format("invalid staging timestamp arithmetic: {}", e.what());
        return false;
    }

    std::optional<std::int64_t> first;
    std::optional<std::int64_t> previous;
    std::size_t rowCount = 0;

    // New transactions always number files in chronological order.
    for (std::int32_t i = 1; i <= manifest.batchCount; ++i) {
        const auto path = batchPath(dir, i);
        if (!std::filesystem::is_regular_file(path)) {
            error = fmt::format("missing staging batch {}", path.string());
            return false;
        }

        std::ifstream ifs(path, std::ios::binary);
        if (!ifs.is_open()) {
            error = fmt::format("cannot open staging batch {}", path.string());
            return false;
        }

        std::error_code sizeError;
        const auto batchSize = std::filesystem::file_size(path, sizeError);
        if (sizeError || batchSize == 0) {
            error = fmt::format("empty or unreadable staging batch {}", path.string());
            return false;
        }
        ifs.seekg(-1, std::ios::end);
        const auto finalByte = ifs.get();
        if (finalByte != '\n') {
            error = fmt::format("staging batch is not newline-terminated: {}", path.string());
            return false;
        }
        ifs.clear();
        ifs.seekg(0, std::ios::beg);

        std::string line;
        std::size_t batchRows = 0;
        while (std::getline(ifs, line)) {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            std::int64_t timestamp{};
            if (!parseTimestamp(line, timestamp)) {
                error = fmt::format("invalid CSV record in staging batch {}", path.string());
                return false;
            }
            if (!isAlignedTimestamp(timestamp, manifest.intervalMs, manifest.alignment)) {
                error = fmt::format("misaligned timestamp {} in staging batch {}", timestamp,
                                    path.string());
                return false;
            }
            if (previous) {
                try {
                    if (!isNextTimestamp(*previous, timestamp, manifest.intervalMs,
                                         manifest.alignment)) {
                        error = fmt::format("non-contiguous staged candles: {} followed by {}",
                                            *previous, timestamp);
                        return false;
                    }
                } catch (const std::exception &e) {
                    error = fmt::format("invalid staged timestamp arithmetic: {}", e.what());
                    return false;
                }
            }
            if (!first) {
                first = timestamp;
            }
            previous = timestamp;
            ++batchRows;
            ++rowCount;
        }
        if (!ifs.eof() || batchRows == 0) {
            error = fmt::format("empty or unreadable staging batch {}", path.string());
            return false;
        }
    }

    if (rowCount == 0 || !first || !previous || *first != manifest.firstTimestamp ||
        *previous != manifest.lastTimestamp) {
        error = "staging manifest does not match staged candle data";
        return false;
    }
    return true;
}

inline bool writeManifest(const std::filesystem::path &dir, const Manifest &manifest, std::string &error) {
    if (!validate(dir, manifest, error)) {
        return false;
    }

    const auto partial = dir / "complete.manifest.writing";
    RemoveUnlessReleased partialGuard(partial);
    std::ofstream ofs(partial, std::ios::trunc | std::ios::binary);
    if (!ofs.is_open()) {
        error = fmt::format("cannot open staging manifest {}", partial.string());
        return false;
    }
    ofs << manifest.version << ' ' << manifest.batchCount << ' ' << manifest.intervalMs << ' '
        << static_cast<std::int32_t>(manifest.alignment) << ' ' << manifest.baseTimestamp << ' '
        << (manifest.baseHasData ? 1 : 0) << ' ' << manifest.requestedStart << ' '
        << manifest.expectedEnd << ' ' << manifest.firstTimestamp << ' ' << manifest.lastTimestamp
        << '\n';
    ofs.flush();
    if (!ofs.good()) {
        error = fmt::format("failed to flush staging manifest {}", partial.string());
        return false;
    }
    ofs.close();
    if (!ofs.good()) {
        error = fmt::format("failed to close staging manifest {}", partial.string());
        return false;
    }

    if (!replaceAtomically(partial, manifestPath(dir), error)) {
        error = "failed to publish staging manifest: " + error;
        return false;
    }
    partialGuard.release();
    return true;
}

inline std::optional<Manifest> readManifest(const std::filesystem::path &dir) {
    std::ifstream ifs(manifestPath(dir), std::ios::binary);
    if (!ifs.is_open()) {
        return std::nullopt;
    }

    Manifest manifest;
    std::int32_t alignment{};
    int baseHasData{};
    if (!(ifs >> manifest.version >> manifest.batchCount >> manifest.intervalMs >> alignment >>
          manifest.baseTimestamp >> baseHasData >> manifest.requestedStart >> manifest.expectedEnd >>
          manifest.firstTimestamp >> manifest.lastTimestamp)) {
        return std::nullopt;
    }
    ifs >> std::ws;
    if (!ifs.eof() || alignment < static_cast<std::int32_t>(Alignment::Fixed) ||
        alignment > static_cast<std::int32_t>(Alignment::CalendarMonth) ||
        (baseHasData != 0 && baseHasData != 1)) {
        return std::nullopt;
    }
    manifest.alignment = static_cast<Alignment>(alignment);
    manifest.baseHasData = baseHasData != 0;
    return manifest;
}

inline bool commit(const std::filesystem::path &dir, const Manifest &manifest,
                   const std::filesystem::path &csvPath, const std::string &header,
                   std::string &error) {
    if (!validate(dir, manifest, error)) {
        return false;
    }

    CsvTail currentTail;
    if (!inspectCsvTail(csvPath, header, currentTail, error, manifest.intervalMs,
                        manifest.alignment)) {
        return false;
    }
    if (currentTail.hasData != manifest.baseHasData ||
        (currentTail.hasData && currentTail.timestamp != manifest.baseTimestamp)) {
        error = "existing CSV tail changed after the transaction was staged";
        return false;
    }

    const auto replacement = dir / "committed.csv";
    RemoveUnlessReleased replacementGuard(replacement);
    std::ofstream ofs(replacement, std::ios::trunc | std::ios::binary);
    if (!ofs.is_open()) {
        error = fmt::format("cannot create transaction output {}", replacement.string());
        return false;
    }

    if (currentTail.size > 0) {
        std::ifstream existing(csvPath, std::ios::binary);
        if (!existing.is_open()) {
            error = fmt::format("cannot open existing CSV {}", csvPath.string());
            return false;
        }
        existing.seekg(-1, std::ios::end);
        const auto last = existing.get();
        if (last == std::char_traits<char>::eof() || !existing.good()) {
            error = fmt::format("cannot inspect existing CSV ending {}", csvPath.string());
            return false;
        }
        existing.clear();
        existing.seekg(0, std::ios::beg);
        if (!existing.good()) {
            error = fmt::format("cannot rewind existing CSV {}", csvPath.string());
            return false;
        }
        if (!copyExactly(existing, ofs, currentTail.size, csvPath, error)) {
            return false;
        }
        if (last != '\n') {
            ofs.put('\n');
            if (!ofs.good()) {
                error = fmt::format("failed to terminate existing CSV {}", csvPath.string());
                return false;
            }
        }
    } else {
        ofs << header << '\n';
    }

    for (std::int32_t i = 1; i <= manifest.batchCount; ++i) {
        const auto path = batchPath(dir, i);
        std::error_code sizeError;
        const auto expectedSize = std::filesystem::file_size(path, sizeError);
        if (sizeError) {
            error = fmt::format("cannot stat staging batch {}: {}", path.string(),
                                sizeError.message());
            return false;
        }
        std::ifstream batch(path, std::ios::binary);
        if (!batch.is_open()) {
            error = fmt::format("cannot reopen staging batch {}", path.string());
            return false;
        }
        if (!copyExactly(batch, ofs, expectedSize, path, error)) {
            return false;
        }
    }

    ofs.flush();
    if (!ofs.good()) {
        error = fmt::format("failed to flush transaction output {}", replacement.string());
        return false;
    }
    ofs.close();
    if (!ofs.good()) {
        error = fmt::format("failed to close transaction output {}", replacement.string());
        return false;
    }

    // The staging directory is next to the destination, so this is a same-
    // filesystem atomic replacement (rename on POSIX, MoveFileEx on Windows).
    if (!replaceAtomically(replacement, csvPath, error)) {
        return false;
    }
    replacementGuard.release();
    return true;
}

inline bool truncateAfter(const std::filesystem::path &csvPath, const std::int64_t maxTimestamp,
                          std::string &error, const std::string &expectedHeader = {},
                          const std::int64_t intervalMs = 0,
                          const Alignment alignment = Alignment::Fixed) {
    std::ifstream input(csvPath, std::ios::binary);
    if (!input.is_open()) {
        error = fmt::format("cannot open CSV {} for tail repair", csvPath.string());
        return false;
    }

    auto replacement = csvPath;
    replacement += ".tail-repair";
    RemoveUnlessReleased replacementGuard(replacement);
    std::ofstream output(replacement, std::ios::trunc | std::ios::binary);
    if (!output.is_open()) {
        error = fmt::format("cannot create tail repair file {}", replacement.string());
        return false;
    }

    std::string line;
    if (!std::getline(input, line)) {
        error = fmt::format("CSV {} has no header", csvPath.string());
        return false;
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    if (!expectedHeader.empty() && line != expectedHeader) {
        error = fmt::format("unexpected CSV header while repairing {}", csvPath.string());
        return false;
    }
    output << line << '\n';

    std::optional<std::int64_t> previous;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        std::int64_t timestamp{};
        if (!parseTimestamp(line, timestamp)) {
            error = fmt::format("invalid record while repairing CSV {}", csvPath.string());
            return false;
        }
        if (intervalMs > 0 && !isAlignedTimestamp(timestamp, intervalMs, alignment)) {
            error = fmt::format("misaligned timestamp while repairing CSV {}", csvPath.string());
            return false;
        }
        if (previous && ((intervalMs > 0 && !isNextTimestamp(*previous, timestamp, intervalMs,
                                                              alignment)) ||
                         (intervalMs <= 0 && timestamp <= *previous))) {
            error = fmt::format("non-contiguous timestamps while repairing CSV {}", csvPath.string());
            return false;
        }
        previous = timestamp;
        if (timestamp <= maxTimestamp) {
            output << line << '\n';
        }
    }
    if (!input.eof()) {
        error = fmt::format("failed to read CSV {} during tail repair", csvPath.string());
        return false;
    }
    input.clear();
    input.close();
    if (!input.good()) {
        error = fmt::format("failed to close CSV {} during tail repair", csvPath.string());
        return false;
    }

    output.flush();
    if (!output.good()) {
        error = fmt::format("failed to flush tail repair file {}", replacement.string());
        return false;
    }
    output.close();
    if (!output.good()) {
        error = fmt::format("failed to close tail repair file {}", replacement.string());
        return false;
    }

    if (!replaceAtomically(replacement, csvPath, error)) {
        return false;
    }
    replacementGuard.release();
    return true;
}

inline void discard(const std::filesystem::path &dir) {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

} // namespace stonky::mexc_staging

#endif // STONKY_MEXC_STAGING_H
