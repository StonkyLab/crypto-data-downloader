/**
Process-wide floor on how far back history is fetched

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_HISTORY_FLOOR_H
#define INCLUDE_STONKY_HISTORY_FLOOR_H

#include <algorithm>
#include <atomic>
#include <cstdint>

namespace stonky {

namespace detail {
inline std::atomic<std::int64_t> &historyFloorStorage() {
    static std::atomic<std::int64_t> floorMs{0}; // 0 = no override
    return floorMs;
}
} // namespace detail

/**
 * Set the oldest timestamp any downloader may reach for, in ms since epoch.
 * Zero clears the override. Production code sets it once in main() before any
 * worker starts; atomic storage also keeps the public helper race-free if it is
 * inspected concurrently. Changing the policy during an active run is not a
 * supported workflow because different requests could observe different floors.
 */
inline void setHistoryFloorMs(const std::int64_t floorMs) {
    detail::historyFloorStorage().store(floorMs, std::memory_order_relaxed);
}

inline std::int64_t historyFloorMs() {
    return detail::historyFloorStorage().load(std::memory_order_relaxed);
}

/**
 * Raise an exchange's own oldest-history constant to the configured floor.
 *
 * This is the inclusive lower bound for a missing, empty or header-only CSV,
 * listing-date probes and fresh funding scans. Callers keep the accompanying
 * `foundValid` state so a real persisted tail remains an exclusive resume
 * cursor and always wins over a later configured floor.
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
