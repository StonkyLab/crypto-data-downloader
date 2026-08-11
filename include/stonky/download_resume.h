/**
Download Resume Policy

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_DOWNLOAD_RESUME_H
#define INCLUDE_STONKY_DOWNLOAD_RESUME_H

#include "stonky/csv_data.h"
#include <algorithm>
#include <cstdint>
#include <limits>

namespace stonky {

/**
 * A timestamp returned from a CSV tail inspection has two distinct meanings:
 * a real persisted tail is exclusive, while the fallback used for a fresh
 * file is an inclusive lower bound. Keeping that bit of state avoids dropping
 * a candle or funding event that starts exactly at --since.
 */
struct DownloadResume {
    std::int64_t timestamp{};
    bool hasSavedRecord{false};
};

inline DownloadResume downloadResume(const CsvData::TailCheck &tail) {
    return {tail.timestamp, tail.foundValid};
}

inline bool shouldPersistTimestamp(const std::int64_t timestamp, const DownloadResume resume) {
    return timestamp > resume.timestamp || (!resume.hasSavedRecord && timestamp == resume.timestamp);
}

/** Return an inclusive REST query start without re-requesting a saved tail. */
inline std::int64_t requestStartTimestamp(const DownloadResume resume) {
    if (!resume.hasSavedRecord) {
        return resume.timestamp;
    }
    if (resume.timestamp == std::numeric_limits<std::int64_t>::max()) {
        return resume.timestamp;
    }
    return resume.timestamp + 1;
}

/**
 * Apply a venue retention bound only to a fresh download. Existing files must
 * always resume from their actual tail, even when that tail is unusually old.
 */
inline std::int64_t freshDownloadStart(const DownloadResume resume,
                                       const std::int64_t earliestAvailable) {
    return resume.hasSavedRecord
               ? resume.timestamp
               : std::max(resume.timestamp, earliestAvailable);
}

inline std::int64_t floorTimestamp(const std::int64_t timestamp,
                                   const std::int64_t intervalMs) {
    if (intervalMs <= 0) {
        return timestamp;
    }
    const auto remainder = timestamp % intervalMs;
    return remainder >= 0 ? timestamp - remainder : timestamp - remainder - intervalMs;
}

} // namespace stonky

#endif // INCLUDE_STONKY_DOWNLOAD_RESUME_H
