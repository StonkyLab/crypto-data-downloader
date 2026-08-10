/**
Transactional staging helpers shared by the MEXC spot and futures downloaders.

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.

A staging directory is intentionally disposable until complete.manifest has
been flushed. MEXC/venue outages may legitimately leave missing candle slots,
so aligned strictly-increasing rows may contain gaps. Pagination completion and
the persistent fresh-prefix marker distinguish those visited-range outages
from an unsafe newest-only fragment.
*/
#ifndef STONKY_MEXC_STAGING_H
#define STONKY_MEXC_STAGING_H

#include "stonky/advisory_file_lock.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
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

enum class DelistedProbeAction {
    Download,
    NoOpExisting,
    RefuseFresh
};

struct DelistedProbeDecision {
    DelistedProbeAction action{DelistedProbeAction::RefuseFresh};
    std::int64_t authoritativeLastOpen{};
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
 * Persistent proof that a freshly published CSV still has an unresolved older
 * prefix.  The marker deliberately survives successful appends and negative
 * API probes.  It is removed only after a later rebuild positively reaches
 * requestedStart.
 */
struct PrefixMarker {
    std::int32_t version{1};
    std::int64_t requestedStart{};
    std::int64_t intervalMs{};
    Alignment alignment{Alignment::Fixed};

    friend bool operator==(const PrefixMarker &, const PrefixMarker &) = default;
};

/**
 * A fail-closed inter-process lock whose ownership is released by the kernel
 * when a process exits, including SIGKILL.  The regular lock file intentionally
 * persists and must not be deleted: only its OS advisory lock denotes an
 * active owner.  A directory left by the pre-advisory implementation is
 * intentionally rejected rather than guessed stale; it requires one-time
 * manual removal after verifying that no old downloader process is running.
 */
class DirectoryLock {
public:
    explicit DirectoryLock(std::filesystem::path path) : lock_(std::move(path)) {
        if (!lock_.ownsLock()) {
            throw std::runtime_error(lock_.error());
        }
    }

    DirectoryLock(const DirectoryLock &) = delete;
    DirectoryLock &operator=(const DirectoryLock &) = delete;
    DirectoryLock(DirectoryLock &&) = delete;
    DirectoryLock &operator=(DirectoryLock &&) = delete;

    ~DirectoryLock() = default;

private:
    AdvisoryFileLock lock_;
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

inline std::filesystem::path prefixMarkerPath(const std::filesystem::path &csvPath) {
    auto path = csvPath;
    path += ".prefix.pending";
    return path;
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

inline bool validPrefixMarker(const PrefixMarker &marker) {
    const auto alignment = static_cast<std::int32_t>(marker.alignment);
    return marker.version == 1 && marker.requestedStart >= 0 && marker.intervalMs > 0 &&
           alignment >= static_cast<std::int32_t>(Alignment::Fixed) &&
           alignment <= static_cast<std::int32_t>(Alignment::CalendarMonth);
}

inline bool writePrefixMarker(const std::filesystem::path &csvPath,
                              const PrefixMarker &marker, std::string &error) {
    error.clear();
    if (!validPrefixMarker(marker)) {
        error = "invalid MEXC unresolved-prefix marker";
        return false;
    }

    const auto destination = prefixMarkerPath(csvPath);
    auto partial = destination;
    partial += ".writing";
    RemoveUnlessReleased partialGuard(partial);
    std::ofstream output(partial, std::ios::trunc | std::ios::binary);
    if (!output.is_open()) {
        error = fmt::format("cannot create unresolved-prefix marker {}", partial.string());
        return false;
    }
    output << marker.version << ' ' << marker.requestedStart << ' ' << marker.intervalMs << ' '
           << static_cast<std::int32_t>(marker.alignment) << '\n';
    output.flush();
    if (!output.good()) {
        error = fmt::format("failed to flush unresolved-prefix marker {}", partial.string());
        return false;
    }
    output.close();
    if (!output.good()) {
        error = fmt::format("failed to close unresolved-prefix marker {}", partial.string());
        return false;
    }
    if (!replaceAtomically(partial, destination, error)) {
        error = "failed to publish unresolved-prefix marker: " + error;
        return false;
    }
    partialGuard.release();
    return true;
}

inline bool readPrefixMarker(const std::filesystem::path &csvPath,
                             std::optional<PrefixMarker> &marker, std::string &error) {
    marker.reset();
    error.clear();
    const auto path = prefixMarkerPath(csvPath);
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(path, ec);
    if (ec) {
        if (ec == std::errc::no_such_file_or_directory) {
            return true;
        }
        error = fmt::format("cannot stat unresolved-prefix marker {}: {}", path.string(),
                            ec.message());
        return false;
    }
    if (!std::filesystem::exists(status)) {
        return true;
    }
    if (!std::filesystem::is_regular_file(status)) {
        error = fmt::format("unresolved-prefix marker is not a regular file: {}", path.string());
        return false;
    }

    std::ifstream input(path, std::ios::binary);
    PrefixMarker parsed;
    std::int32_t alignment{};
    if (!(input >> parsed.version >> parsed.requestedStart >> parsed.intervalMs >> alignment)) {
        error = fmt::format("cannot parse unresolved-prefix marker {}", path.string());
        return false;
    }
    input >> std::ws;
    if (!input.eof() || alignment < static_cast<std::int32_t>(Alignment::Fixed) ||
        alignment > static_cast<std::int32_t>(Alignment::CalendarMonth)) {
        error = fmt::format("invalid unresolved-prefix marker {}", path.string());
        return false;
    }
    parsed.alignment = static_cast<Alignment>(alignment);
    if (!validPrefixMarker(parsed)) {
        error = fmt::format("invalid unresolved-prefix marker values in {}", path.string());
        return false;
    }
    marker = parsed;
    return true;
}

inline bool removePrefixMarker(const std::filesystem::path &csvPath, std::string &error) {
    error.clear();
    std::error_code ec;
    std::filesystem::remove(prefixMarkerPath(csvPath), ec);
    if (ec) {
        error = fmt::format("cannot remove resolved-prefix marker {}: {}",
                            prefixMarkerPath(csvPath).string(), ec.message());
        return false;
    }
    return true;
}

inline bool requirePrefixMarkerForPublication(const std::filesystem::path &csvPath,
                                              const Manifest &manifest,
                                              std::string &error) {
    // An existing base makes an outage at the append boundary local and
    // recoverable by downstream gap-aware aggregation.  Only a fresh/full
    // replacement can permanently lose an unknown older prefix.
    if (manifest.baseHasData || manifest.firstTimestamp <= manifest.requestedStart) {
        return true;
    }
    std::optional<PrefixMarker> marker;
    if (!readPrefixMarker(csvPath, marker, error)) {
        return false;
    }
    if (!marker || marker->requestedStart != manifest.requestedStart ||
        marker->intervalMs != manifest.intervalMs || marker->alignment != manifest.alignment) {
        error = "refusing to publish an unresolved MEXC prefix without its matching persistent marker";
        return false;
    }
    return true;
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

/**
 * Convert a bounded delisted-symbol availability probe into a download
 * decision.  A negative probe never advances persistent state: an existing
 * CSV is a safe no-op and is retried on the next run, while a fresh symbol
 * produces no output.  A prefix rebuild already has a positively known CSV
 * suffix, so its existing tail is the authoritative end and avoids traversing
 * an arbitrarily long post-delisting empty range.
 */
inline DelistedProbeDecision decideDelistedProbe(
        const bool rebuildPrefix, const std::optional<std::int64_t> existingTail,
        const std::optional<std::int64_t> newestProbeTimestamp) {
    if (rebuildPrefix) {
        if (!existingTail) {
            throw std::invalid_argument("MEXC prefix rebuild requires an existing CSV tail");
        }
        return {DelistedProbeAction::Download, *existingTail};
    }
    if (newestProbeTimestamp) {
        if (existingTail && *newestProbeTimestamp <= *existingTail) {
            throw std::invalid_argument("MEXC delisted probe did not advance the existing tail");
        }
        return {DelistedProbeAction::Download, *newestProbeTimestamp};
    }
    if (existingTail) {
        return {DelistedProbeAction::NoOpExisting, *existingTail};
    }
    return {DelistedProbeAction::RefuseFresh, 0};
}

/** Number of aligned candle slots after lastPresent through expectedLast. */
inline std::uint64_t missingCandleSlotsAfter(const std::int64_t lastPresent,
                                             const std::int64_t expectedLast,
                                             const std::int64_t intervalMs,
                                             const Alignment alignment) {
    if (expectedLast <= lastPresent) {
        return 0;
    }
    if (!isAlignedTimestamp(lastPresent, intervalMs, alignment) ||
        !isAlignedTimestamp(expectedLast, intervalMs, alignment)) {
        throw std::invalid_argument("cannot count misaligned MEXC candle slots");
    }
    if (alignment != Alignment::CalendarMonth) {
        const auto difference = expectedLast - lastPresent;
        if (difference % intervalMs != 0) {
            throw std::invalid_argument("MEXC candle bounds are on different interval grids");
        }
        return static_cast<std::uint64_t>(difference / intervalMs);
    }

    std::uint64_t missing = 0;
    auto cursor = lastPresent;
    while (cursor < expectedLast) {
        cursor = nextTimestamp(cursor, intervalMs, alignment);
        ++missing;
    }
    if (cursor != expectedLast) {
        throw std::invalid_argument("MEXC calendar-month bounds are not contiguous period opens");
    }
    return missing;
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

inline bool hasDecimalSyntax(const std::string_view value) {
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

inline bool parseFiniteBinary64(std::string_view value, double &parsed) {
    if (!hasDecimalSyntax(value)) {
        return false;
    }
    if (value.front() == '+') {
        value.remove_prefix(1); // floating from_chars rejects a leading plus
    }
    const auto *begin = value.data();
    const auto *end = begin + value.size();
    const auto [ptr, ec] = std::from_chars(begin, end, parsed, std::chars_format::general);
    // result_out_of_range covers both overflow and non-representable
    // underflow.  Market-data CSV is explicitly a finite binary64 contract;
    // silently turning a non-zero decimal into Inf/0 would violate it.
    return ec == std::errc{} && ptr == end && std::isfinite(parsed);
}

inline bool isDecimalNumber(const std::string_view value) {
    double parsed{};
    return parseFiniteBinary64(value, parsed);
}

struct CandleCsvRow {
    std::int64_t timestamp{};
    std::array<double, 6> values{};
    std::string text;
};

inline bool parseCandleCsvFields(const std::string_view line, std::int64_t &timestamp,
                                 std::array<double, 6> *values = nullptr) {
    // MEXC candle files have exactly seven fields.  Requiring the exact schema
    // prevents a glued/torn line with a valid timestamp prefix being accepted.
    if (std::count(line.begin(), line.end(), ',') != 6) {
        return false;
    }
    const auto comma = line.find(',');
    if (comma == 0 || comma == std::string_view::npos) {
        return false;
    }
    const char *begin = line.data();
    const char *end = begin + comma;
    const auto [ptr, ec] = std::from_chars(begin, end, timestamp);
    if (ec != std::errc{} || ptr != end || timestamp < 0) {
        return false;
    }

    std::size_t fieldStart = comma + 1;
    for (std::size_t field = 0; field < 6; ++field) {
        const auto fieldEnd = field == 5 ? line.size() : line.find(',', fieldStart);
        double parsed{};
        if (fieldEnd == std::string_view::npos ||
            !parseFiniteBinary64(line.substr(fieldStart, fieldEnd - fieldStart), parsed)) {
            return false;
        }
        if (values) {
            (*values)[field] = parsed;
        }
        fieldStart = fieldEnd + 1;
    }
    return true;
}

inline bool parseTimestamp(const std::string &line, std::int64_t &timestamp) {
    return parseCandleCsvFields(line, timestamp);
}

struct CsvTail {
    bool hasData{};
    std::int64_t firstTimestamp{};
    std::int64_t timestamp{};
    std::uintmax_t size{};

    friend bool operator==(const CsvTail &, const CsvTail &) = default;
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
        if (previous && timestamp <= *previous) {
            error = fmt::format("non-increasing existing CSV timestamps in {}", path.string());
            return false;
        }
        if (!previous) {
            tail.firstTimestamp = timestamp;
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
        if (manifest.baseHasData) {
            const auto expectedStart = nextTimestamp(manifest.baseTimestamp, manifest.intervalMs,
                                                     manifest.alignment);
            if (manifest.requestedStart != expectedStart ||
                manifest.firstTimestamp < manifest.requestedStart) {
                error = "staging begins before, or was requested away from, the existing CSV tail";
                return false;
            }
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
            if (previous && timestamp <= *previous) {
                error = fmt::format("non-increasing staged candles: {} followed by {}",
                                    *previous, timestamp);
                return false;
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
    if (!requirePrefixMarkerForPublication(csvPath, manifest, error)) {
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

/**
 * Atomically replace an existing CSV with the timestamp union of a fully
 * validated fresh staging transaction and every previously stored row.  This
 * is used only when a persistent prefix marker discovers older history.  A
 * temporary venue outage during the rebuild must never erase a row already on
 * disk.  Equal timestamps must carry binary64-equivalent OHLCV values or the
 * replacement fails without touching the old CSV.  expectedCurrent closes the
 * read/probe/rebuild TOCTOU window; callers additionally hold the per-symbol
 * process lock.
 */
inline bool replaceCsv(const std::filesystem::path &dir, const Manifest &manifest,
                       const std::filesystem::path &csvPath, const std::string &header,
                       const CsvTail &expectedCurrent, std::string &error) {
    if (!validate(dir, manifest, error)) {
        return false;
    }
    // Unlike a fresh definitive commit, this operation is specifically a
    // rebuild of an already published provisional suffix.  The marker must
    // therefore still exist until after the union replacement succeeds.
    std::optional<PrefixMarker> replacementMarker;
    if (!readPrefixMarker(csvPath, replacementMarker, error)) {
        return false;
    }
    if (!replacementMarker || replacementMarker->requestedStart != manifest.requestedStart ||
        replacementMarker->intervalMs != manifest.intervalMs ||
        replacementMarker->alignment != manifest.alignment) {
        error = "refusing MEXC prefix replacement without its matching persistent marker";
        return false;
    }
    if (manifest.baseHasData) {
        error = "prefix rebuild staging must not append to an existing base";
        return false;
    }

    CsvTail current;
    if (!inspectCsvTail(csvPath, header, current, error, manifest.intervalMs,
                        manifest.alignment)) {
        return false;
    }
    if (current != expectedCurrent) {
        error = "existing CSV changed while its historical prefix was rebuilt";
        return false;
    }

    std::ifstream existing;
    std::optional<CandleCsvRow> existingRow;
    if (current.size > 0) {
        existing.open(csvPath, std::ios::binary);
        if (!existing.is_open()) {
            error = fmt::format("cannot reopen existing CSV {} for prefix union",
                                csvPath.string());
            return false;
        }
        std::string existingHeader;
        if (!std::getline(existing, existingHeader)) {
            error = fmt::format("cannot read existing CSV header {} for prefix union",
                                csvPath.string());
            return false;
        }
        if (!existingHeader.empty() && existingHeader.back() == '\r') {
            existingHeader.pop_back();
        }
        if (existingHeader != header) {
            error = fmt::format("unexpected existing CSV header in prefix union {}",
                                csvPath.string());
            return false;
        }
    }

    const auto readRow = [&error](std::istream &input, const std::filesystem::path &source,
                                  std::optional<CandleCsvRow> &row) {
        std::string line;
        if (!std::getline(input, line)) {
            if (input.bad()) {
                error = fmt::format("failed while reading candle rows from {}", source.string());
                return false;
            }
            row.reset();
            return true;
        }
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        CandleCsvRow parsed;
        if (!parseCandleCsvFields(line, parsed.timestamp, &parsed.values)) {
            error = fmt::format("invalid candle row while merging prefix from {}", source.string());
            return false;
        }
        parsed.text = std::move(line);
        row = std::move(parsed);
        return true;
    };

    if (existing.is_open() && !readRow(existing, csvPath, existingRow)) {
        return false;
    }

    std::int32_t stagedBatchIndex = 0;
    std::ifstream stagedBatch;
    std::filesystem::path stagedBatchSource;
    const auto readNextStagedRow = [&](std::optional<CandleCsvRow> &row) {
        while (true) {
            if (!stagedBatch.is_open()) {
                ++stagedBatchIndex;
                if (stagedBatchIndex > manifest.batchCount) {
                    row.reset();
                    return true;
                }
                stagedBatchSource = batchPath(dir, stagedBatchIndex);
                stagedBatch.clear();
                stagedBatch.open(stagedBatchSource, std::ios::binary);
                if (!stagedBatch.is_open()) {
                    error = fmt::format("cannot reopen prefix-rebuild batch {}",
                                        stagedBatchSource.string());
                    return false;
                }
            }

            if (!readRow(stagedBatch, stagedBatchSource, row)) {
                return false;
            }
            if (row) {
                return true;
            }
            stagedBatch.close();
        }
    };

    std::optional<CandleCsvRow> stagedRow;
    if (!readNextStagedRow(stagedRow)) {
        return false;
    }

    const auto replacement = dir / "committed.csv";
    RemoveUnlessReleased replacementGuard(replacement);
    std::ofstream output(replacement, std::ios::trunc | std::ios::binary);
    if (!output.is_open()) {
        error = fmt::format("cannot create prefix-rebuild output {}", replacement.string());
        return false;
    }
    output << header << '\n';
    if (!output.good()) {
        error = fmt::format("failed to write prefix-rebuild header {}", replacement.string());
        return false;
    }

    std::optional<std::int64_t> unionFirst;
    std::optional<std::int64_t> unionPrevious;
    const auto emit = [&](const CandleCsvRow &row) {
        if (unionPrevious && row.timestamp <= *unionPrevious) {
            error = fmt::format("non-increasing timestamp {} in MEXC prefix union",
                                row.timestamp);
            return false;
        }
        output << row.text << '\n';
        if (!output.good()) {
            error = fmt::format("failed to write prefix-rebuild union {}",
                                replacement.string());
            return false;
        }
        if (!unionFirst) {
            unionFirst = row.timestamp;
        }
        unionPrevious = row.timestamp;
        return true;
    };

    while (existingRow || stagedRow) {
        if (!stagedRow || (existingRow && existingRow->timestamp < stagedRow->timestamp)) {
            if (!emit(*existingRow) || !readRow(existing, csvPath, existingRow)) {
                return false;
            }
            continue;
        }
        if (!existingRow || stagedRow->timestamp < existingRow->timestamp) {
            if (!emit(*stagedRow) || !readNextStagedRow(stagedRow)) {
                return false;
            }
            continue;
        }

        // Equal timestamp: a different decimal spelling is harmless only when
        // all six stored market values map to the same finite double.
        if (existingRow->values != stagedRow->values) {
            error = fmt::format(
                "conflicting MEXC candle values at timestamp {}; refusing prefix replacement",
                existingRow->timestamp);
            return false;
        }
        if (!emit(*stagedRow) || !readRow(existing, csvPath, existingRow) ||
            !readNextStagedRow(stagedRow)) {
            return false;
        }
    }

    const auto expectedFirst = current.hasData
        ? std::min(current.firstTimestamp, manifest.firstTimestamp)
        : manifest.firstTimestamp;
    const auto expectedLast = current.hasData
        ? std::max(current.timestamp, manifest.lastTimestamp)
        : manifest.lastTimestamp;
    if (!unionFirst || !unionPrevious || *unionFirst != expectedFirst ||
        *unionPrevious != expectedLast) {
        error = "prefix-rebuild union does not cover both staged and existing CSV bounds";
        return false;
    }

    output.flush();
    if (!output.good()) {
        error = fmt::format("failed to flush prefix-rebuild output {}", replacement.string());
        return false;
    }
    output.close();
    if (!output.good()) {
        error = fmt::format("failed to close prefix-rebuild output {}", replacement.string());
        return false;
    }
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
        if (previous && timestamp <= *previous) {
            error = fmt::format("non-increasing timestamps while repairing CSV {}", csvPath.string());
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
