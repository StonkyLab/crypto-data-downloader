/**
Process-wide floor on how far back history is fetched

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_HISTORY_FLOOR_H
#define INCLUDE_STONKY_HISTORY_FLOOR_H

#include <algorithm>
#include <cstdint>

namespace stonky {

namespace detail {
inline std::int64_t &historyFloorStorage() {
    static std::int64_t floorMs = 0; // 0 = no override
    return floorMs;
}
} // namespace detail

/**
 * Set the oldest timestamp any downloader may reach for, in ms since epoch.
 * Zero clears the override. Must be called from main() before any worker
 * starts; it is read-only afterwards, so no synchronisation is needed.
 */
inline void setHistoryFloorMs(const std::int64_t floorMs) {
    detail::historyFloorStorage() = floorMs;
}

inline std::int64_t historyFloorMs() {
    return detail::historyFloorStorage();
}

/**
 * Raise an exchange's own oldest-history constant to the configured floor.
 *
 * This value is only ever used where there is NO usable local data: it is the
 * fallback CsvData::lastValidRecord() returns for a missing, empty or
 * header-only file, the lower bound of a listing-date probe, and the cutoff of
 * a first funding scan. A file that already holds records always resumes from
 * its own tail, so raising the floor can never skip forward over stored data
 * and open a gap — the invariant the whole append-only design rests on.
 *
 * Intended use: once old years are archived away and deleted from the live
 * CSVs, `--since` stops every fresh symbol from pulling the full history back
 * in.
 */
inline std::int64_t historyFloor(const std::int64_t exchangeDefault) {
    return std::max(exchangeDefault, historyFloorMs());
}

} // namespace stonky

#endif // INCLUDE_STONKY_HISTORY_FLOOR_H
