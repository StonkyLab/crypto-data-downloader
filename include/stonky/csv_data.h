/**
CSV Data File Integrity Helpers

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_CSV_DATA_H
#define INCLUDE_STONKY_CSV_DATA_H

#include <cstdint>
#include <string>

namespace stonky {

class CsvData {
public:
    struct TailCheck {
        /// Timestamp (first CSV field) of the last valid data record, or `fallback`
        std::int64_t timestamp{};
        /// True when a valid data record was found
        bool foundValid{false};
        /// True when trailing torn/invalid bytes were truncated away
        bool repairedTail{false};
    };

    /**
     * Return the timestamp of the last VALID data record of a CSV file and
     * self-heal a torn tail.
     *
     * A record is valid when it is newline-terminated, splits into exactly
     * `expectedFields` comma-separated fields (or at least that many when
     * `allowMoreFields` is set) and its first field parses as int64.
     *
     * An interrupted write (process kill, disk full) leaves a partial last
     * line without a trailing newline; a later append-mode write then glues
     * its first row onto that fragment, producing one unparseable line.
     * Historically the last-line check answered such files with the
     * oldest-date fallback, silently triggering a full-history re-download
     * appended onto the corrupt file. This helper instead truncates the file
     * back to the end of the last valid record (the file header on offset 0
     * is always preserved) so the resume timestamp stays correct and the next
     * append starts on a clean newline boundary.
     *
     * @param path           CSV file path
     * @param expectedFields number of comma-separated fields of a valid record
     * @param fallback       returned when the file is missing/empty/header-only
     * @param allowMoreFields accept records with more than expectedFields fields
     */
    static TailCheck lastValidRecord(const std::string& path, std::size_t expectedFields,
                                     std::int64_t fallback, bool allowMoreFields = false);
};

} // namespace stonky

#endif // INCLUDE_STONKY_CSV_DATA_H
