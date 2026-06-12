/**
CSV Data Verifier / Repairer

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_CSV_VERIFIER_H
#define INCLUDE_STONKY_CSV_VERIFIER_H

#include <cstdint>
#include <string>
#include <vector>

namespace stonky {

/**
 * Offline integrity check + repair of downloaded CSV market data.
 *
 * Detects (and with `repair` enabled, fixes) the damage classes known to occur
 * in practice:
 *  - torn/glued lines (interrupted write + append, wrong field count)
 *  - duplicate timestamps (historical full-history re-appends, page-boundary
 *    duplicates)
 *  - out-of-order blocks (history re-appended after a corrupt tail)
 *  - legacy rows with one extra trailing column (salvaged by dropping it)
 *  - gaps (reported only — missing data cannot be repaired locally; delete the
 *    file or truncate before the gap and re-download where the exchange still
 *    serves the range)
 */
class CsvVerifier {
public:
    struct Options {
        /// Canonical number of comma-separated fields of a data record
        std::size_t expectedFields = 6;
        /// Accept records with more fields than expectedFields (MEXC layout)
        bool allowMoreFields = false;
        /// Salvage rows with exactly expectedFields+1 fields by dropping the
        /// last column (legacy Bybit 1h layout with a trailing turnover field)
        bool salvageExtraField = false;
        /// Bar interval in ms for gap analysis; 0 disables gap reporting
        std::int64_t intervalMs = 0;
        /// Rewrite the file (atomic replace) when issues are found
        bool repair = false;
        /// Parallel file jobs for directory verification
        std::uint32_t maxJobs = 4;
    };

    struct FileReport {
        std::string path;
        std::size_t totalRecords = 0;
        std::size_t malformed = 0;   ///< dropped on repair
        std::size_t salvaged = 0;    ///< extra-column rows converted
        std::size_t duplicates = 0;  ///< duplicate timestamps, first kept
        std::size_t outOfOrder = 0;  ///< backward timestamp jumps, sorted on repair
        std::size_t gaps = 0;        ///< report-only
        std::int64_t firstGapTs = 0;
        bool repaired = false;
        bool readFailed = false;

        [[nodiscard]] bool needsRepair() const {
            return malformed > 0 || salvaged > 0 || duplicates > 0 || outOfOrder > 0;
        }
    };

    /// Verify (and optionally repair) a single CSV file.
    static FileReport verifyFile(const std::string &path, const Options &options);

    /// Verify (and optionally repair) every *.csv file in a directory.
    /// Reports are logged via spdlog; the returned vector holds one report per file.
    static std::vector<FileReport> verifyDirectory(const std::string &dirPath, const Options &options);
};

} // namespace stonky

#endif // INCLUDE_STONKY_CSV_VERIFIER_H
