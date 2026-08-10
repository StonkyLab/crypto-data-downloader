#include "mexc_staging.h"
#include "mexc_funding_csv.h"
#include "mexc_funding_pagination.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        path_ = std::filesystem::temp_directory_path() /
                ("crypto_data_downloader_mexc_staging_" + std::to_string(
                    std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path_);
    }
    ~TemporaryDirectory() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }
    [[nodiscard]] const std::filesystem::path &path() const { return path_; }

private:
    std::filesystem::path path_;
};

std::int64_t utcMs(const int year, const unsigned month, const unsigned day) {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        sys_days{std::chrono::year{year} / std::chrono::month{month} /
                 std::chrono::day{day}}.time_since_epoch()).count();
}

bool writeFile(const std::filesystem::path &path, const std::string &contents) {
    std::ofstream out(path, std::ios::trunc | std::ios::binary);
    out << contents;
    out.flush();
    return out.good();
}

std::vector<std::string> readLines(const std::filesystem::path &path) {
    std::ifstream in(path);
    std::vector<std::string> lines;
    for (std::string line; std::getline(in, line);) {
        lines.push_back(line);
    }
    return lines;
}

struct FakeFundingPage {
    std::vector<int> resultList;
    std::int32_t totalPage{};
    std::int32_t totalCount{};
    std::int32_t pageSize{1000};
    std::int32_t currentPage{1};
};

} // namespace

int main() {
    using namespace stonky::mexc_staging;
    bool ok = true;
    const std::string header = "open_time,open,high,low,close,volume,amount";

    constexpr std::int64_t minute = 60000;
    constexpr std::int64_t week = 7LL * 24 * 60 * 60 * 1000;
    if (currentPeriodOpen(utcMs(2026, 8, 9), week, Alignment::WeekMonday) !=
        utcMs(2026, 8, 3)) {
        std::cerr << "Weekly MEXC boundary is not Monday UTC\n";
        ok = false;
    }
    if (currentPeriodOpen(utcMs(2026, 8, 29), 30LL * 24 * 60 * 60 * 1000,
                          Alignment::CalendarMonth) != utcMs(2026, 8, 1) ||
        nextTimestamp(utcMs(2024, 2, 1), 30LL * 24 * 60 * 60 * 1000,
                      Alignment::CalendarMonth) != utcMs(2024, 3, 1)) {
        std::cerr << "Calendar-month MEXC boundary is incorrect\n";
        ok = false;
    }
    if (missingCandleSlotsAfter(2 * minute, 5 * minute, minute, Alignment::Fixed) != 3 ||
        missingCandleSlotsAfter(utcMs(2024, 1, 1), utcMs(2024, 4, 1),
                                30LL * 24 * 60 * 60 * 1000,
                                Alignment::CalendarMonth) != 3) {
        std::cerr << "Trailing MEXC candle-gap slot count is incorrect\n";
        ok = false;
    }

    const auto delistedExistingNoOp =
        decideDelistedProbe(false, 10 * minute, std::nullopt);
    const auto delistedExistingDownload =
        decideDelistedProbe(false, 10 * minute, 12 * minute);
    const auto delistedFreshFailure =
        decideDelistedProbe(false, std::nullopt, std::nullopt);
    const auto delistedFreshDownload =
        decideDelistedProbe(false, std::nullopt, 12 * minute);
    const auto delistedPrefixRebuild =
        decideDelistedProbe(true, 10 * minute, std::nullopt);
    if (delistedExistingNoOp.action != DelistedProbeAction::NoOpExisting ||
        delistedExistingDownload.action != DelistedProbeAction::Download ||
        delistedExistingDownload.authoritativeLastOpen != 12 * minute ||
        delistedFreshFailure.action != DelistedProbeAction::RefuseFresh ||
        delistedFreshDownload.action != DelistedProbeAction::Download ||
        delistedPrefixRebuild.action != DelistedProbeAction::Download ||
        delistedPrefixRebuild.authoritativeLastOpen != 10 * minute) {
        std::cerr << "Bounded delisted-symbol probe selected an unsafe action/end\n";
        ok = false;
    }

    TemporaryDirectory tmp;
    CsvTail absentTail;
    std::string error;
    if (!inspectCsvTail(tmp.path() / "missing.csv", header, absentTail, error, minute,
                        Alignment::Fixed) || absentTail.hasData || absentTail.size != 0) {
        std::cerr << "Missing CSV was not treated as an empty base: " << error << '\n';
        ok = false;
    }

    const auto lockPath = tmp.path() / "TEST.lock";
    try {
        {
            DirectoryLock first(lockPath);
            try {
                DirectoryLock second(lockPath);
                std::cerr << "Concurrent process acquired an existing symbol lock\n";
                ok = false;
            } catch (const std::runtime_error &) {
                // Expected: a second writer must fail closed.
            }
        }
        DirectoryLock afterRelease(lockPath);
    } catch (const std::exception &lockError) {
        std::cerr << "Symbol lock lifecycle failed: " << lockError.what() << '\n';
        ok = false;
    }
    if (!std::filesystem::is_regular_file(lockPath)) {
        std::cerr << "Advisory symbol lock file should persist between owners\n";
        ok = false;
    }

    const auto legacyDirectoryLock = tmp.path() / "LEGACY.lock";
    std::filesystem::create_directory(legacyDirectoryLock);
    try {
        DirectoryLock mustNotGuessStale(legacyDirectoryLock);
        std::cerr << "Advisory lock replaced a possibly-live legacy directory lock\n";
        ok = false;
    } catch (const std::runtime_error &) {
        // Migration is deliberately fail-closed: only an operator can verify
        // that no old binary still owns this directory-style lock.
    }
    if (!std::filesystem::is_directory(legacyDirectoryLock)) {
        std::cerr << "Failed legacy lock acquisition modified the old lock directory\n";
        ok = false;
    }

#ifndef _WIN32
    const auto crashLockPath = tmp.path() / "CRASH.lock";
    int lockReadyPipe[2]{};
    if (::pipe(lockReadyPipe) != 0) {
        std::cerr << "Could not create symbol-lock crash-test pipes\n";
        return 1;
    }
    const auto lockChild = ::fork();
    if (lockChild < 0) {
        std::cerr << "Could not fork symbol-lock crash-test process\n";
        return 1;
    }
    if (lockChild == 0) {
        ::close(lockReadyPipe[0]);
        try {
            DirectoryLock crashed(crashLockPath);
            const char ready = 'R';
            if (::write(lockReadyPipe[1], &ready, 1) != 1) {
                ::_exit(2);
            }
            for (;;) {
                ::pause();
            }
        } catch (...) {
            ::_exit(4);
        }
    }

    ::close(lockReadyPipe[1]);
    char lockReady{};
    if (::read(lockReadyPipe[0], &lockReady, 1) != 1 || lockReady != 'R') {
        std::cerr << "Crash-test child did not acquire the symbol lock\n";
        return 1;
    }
    try {
        DirectoryLock mustFail(crashLockPath);
        std::cerr << "Live child symbol lock was not excluded\n";
        ok = false;
    } catch (const std::runtime_error &) {
        // Expected while the child owns the kernel lock.
    }
    if (::kill(lockChild, SIGKILL) != 0) {
        std::cerr << "Could not SIGKILL symbol-lock crash-test child\n";
        return 1;
    }
    ::close(lockReadyPipe[0]);
    int lockChildStatus{};
    if (::waitpid(lockChild, &lockChildStatus, 0) != lockChild ||
        !WIFSIGNALED(lockChildStatus) || WTERMSIG(lockChildStatus) != SIGKILL) {
        std::cerr << "Symbol-lock crash-test child did not terminate through SIGKILL\n";
        return 1;
    }
    try {
        DirectoryLock recoveredAfterCrash(crashLockPath);
    } catch (const std::exception &lockError) {
        std::cerr << "Kernel did not release symbol lock after process exit: "
                  << lockError.what() << '\n';
        ok = false;
    }
#endif

    const auto staging = tmp.path() / "temp_TEST";
    const auto csv = tmp.path() / "TEST.csv";
    std::filesystem::create_directories(staging);
    ok &= writeFile(csv,
                    header + "\n"
                    "0,1,1,1,1,1,1\n");
    ok &= writeFile(batchPath(staging, 1),
                    "60000,1,1,1,1,1,1\n"
                    "120000,1,1,1,1,1,1\n");
    ok &= writeFile(batchPath(staging, 2), "180000,1,1,1,1,1,1\n");

    Manifest manifest;
    manifest.batchCount = 2;
    manifest.intervalMs = minute;
    manifest.baseTimestamp = 0;
    manifest.baseHasData = true;
    manifest.requestedStart = minute;
    manifest.expectedEnd = 3 * minute;
    manifest.firstTimestamp = minute;
    manifest.lastTimestamp = 3 * minute;

    if (!writeManifest(staging, manifest, error)) {
        std::cerr << "Could not publish complete staging: " << error << '\n';
        return 1;
    }
    if (!readManifest(staging)) {
        std::cerr << "Complete staging manifest cannot be read back\n";
        return 1;
    }
    if (!writeManifest(staging, manifest, error)) {
        std::cerr << "Could not atomically replace an existing manifest: " << error << '\n';
        return 1;
    }

    // A compare-tail check inside commit closes the TOCTOU window between
    // staging and publication, even if a caller forgot the process lock.
    ok &= writeFile(csv,
                    header + "\n"
                    "0,1,1,1,1,1,1\n"
                    "60000,1,1,1,1,1,1\n");
    if (commit(staging, manifest, csv, header, error)) {
        std::cerr << "Commit accepted a CSV tail changed after staging\n";
        ok = false;
    }
    ok &= writeFile(csv,
                    header + "\n"
                    "0,1,1,1,1,1,1\n");
    if (!commit(staging, manifest, csv, header, error)) {
        std::cerr << "Could not commit complete staging: " << error << '\n';
        return 1;
    }

    const auto lines = readLines(csv);
    if (lines.size() != 5 || !lines.back().starts_with("180000,")) {
        std::cerr << "Committed CSV does not contain the complete chronological transaction\n";
        ok = false;
    }

    if (!truncateAfter(csv, 2 * minute, error, header, minute, Alignment::Fixed)) {
        std::cerr << "Could not transactionally remove an old open tail: " << error << '\n';
        ok = false;
    } else {
        const auto repaired = readLines(csv);
        if (repaired.size() != 4 || !repaired.back().starts_with("120000,")) {
            std::cerr << "Tail repair retained an open/future candle\n";
            ok = false;
        }
    }

    std::filesystem::remove(batchPath(staging, 2));
    if (validate(staging, manifest, error)) {
        std::cerr << "Validation accepted a transaction with a missing batch\n";
        ok = false;
    }

    ok &= writeFile(batchPath(staging, 2), "180000,1,1,1,1,1,1");
    if (validate(staging, manifest, error)) {
        std::cerr << "Validation accepted a batch without a terminating newline\n";
        ok = false;
    }

    const std::vector<std::string> invalidCandleNumbers{
        "NaN", "1e1000", "-1e1000", "1e-1000"};
    for (const auto &invalidNumber : invalidCandleNumbers) {
        ok &= writeFile(batchPath(staging, 2),
                        "180000,1,1,1,1," + invalidNumber + ",1\n");
        if (validate(staging, manifest, error)) {
            std::cerr << "Validation accepted a non-binary64 candle field: "
                      << invalidNumber << '\n';
            ok = false;
        }
    }

    const auto gapped = tmp.path() / "GAPPED.csv";
    ok &= writeFile(gapped, header + "\n"
                            "0,1,1,1,1,1,1\n"
                            "120000,1,1,1,1,1,1\n");
    CsvTail gappedTail;
    if (!inspectCsvTail(gapped, header, gappedTail, error, minute, Alignment::Fixed) ||
        !gappedTail.hasData || gappedTail.firstTimestamp != 0 ||
        gappedTail.timestamp != 2 * minute) {
        std::cerr << "Existing CSV inspection rejected a legitimate venue outage: "
                  << error << '\n';
        ok = false;
    }

    const auto unordered = tmp.path() / "UNORDERED.csv";
    ok &= writeFile(unordered, header + "\n"
                              "120000,1,1,1,1,1,1\n"
                              "60000,1,1,1,1,1,1\n");
    if (inspectCsvTail(unordered, header, gappedTail, error, minute, Alignment::Fixed)) {
        std::cerr << "Existing CSV inspection accepted out-of-order timestamps\n";
        ok = false;
    }

    const auto duplicated = tmp.path() / "DUPLICATED.csv";
    ok &= writeFile(duplicated, header + "\n"
                               "60000,1,1,1,1,1,1\n"
                               "60000,1,1,1,1,1,1\n");
    if (inspectCsvTail(duplicated, header, gappedTail, error, minute, Alignment::Fixed)) {
        std::cerr << "Existing CSV inspection accepted duplicate timestamps\n";
        ok = false;
    }

    const auto misaligned = tmp.path() / "MISALIGNED.csv";
    ok &= writeFile(misaligned, header + "\n30000,1,1,1,1,1,1\n");
    if (inspectCsvTail(misaligned, header, gappedTail, error, minute, Alignment::Fixed)) {
        std::cerr << "Existing CSV inspection accepted a misaligned timestamp\n";
        ok = false;
    }

    // A venue outage may begin exactly at a resume boundary and may also span
    // rows inside the downloaded transaction.  Both gaps remain explicit in
    // raw CSV so downstream aggregation can omit only affected coarse buckets.
    const auto outageCsv = tmp.path() / "OUTAGE.csv";
    const auto outageStaging = tmp.path() / "temp_OUTAGE";
    std::filesystem::create_directories(outageStaging);
    ok &= writeFile(outageCsv, header + "\n0,1,1,1,1,1,1\n");
    const std::string outageRows =
        "120000,1,1,1,1,1,1\n"  // missing 60000 at the append boundary
        "240000,1,1,1,1,1,1\n"; // missing 180000 inside staging
    ok &= writeFile(batchPath(outageStaging, 1), outageRows);
    Manifest outageManifest;
    outageManifest.batchCount = 1;
    outageManifest.intervalMs = minute;
    outageManifest.alignment = Alignment::Fixed;
    outageManifest.baseTimestamp = 0;
    outageManifest.baseHasData = true;
    outageManifest.requestedStart = minute;
    outageManifest.expectedEnd = 4 * minute;
    outageManifest.firstTimestamp = 2 * minute;
    outageManifest.lastTimestamp = 4 * minute;
    if (!writeManifest(outageStaging, outageManifest, error) ||
        !commit(outageStaging, outageManifest, outageCsv, header, error)) {
        std::cerr << "Legitimate boundary/internal candle gaps blocked publication: "
                  << error << '\n';
        ok = false;
    }
    const auto outageLines = readLines(outageCsv);
    if (outageLines.size() != 4 || !outageLines[1].starts_with("0,") ||
        !outageLines[2].starts_with("120000,") ||
        !outageLines[3].starts_with("240000,") ||
        std::filesystem::exists(prefixMarkerPath(outageCsv))) {
        std::cerr << "Gap-tolerant append changed or mislabeled the raw outage\n";
        ok = false;
    }

    // A missing latest closed candle is also a legitimate venue outage.  The
    // transaction's authoritative end is its last returned row; the next run
    // starts there again and can fill or explicitly preserve the trailing gap.
    const auto trailingCsv = tmp.path() / "TRAILING.csv";
    const auto trailingStaging = tmp.path() / "temp_TRAILING";
    std::filesystem::create_directories(trailingStaging);
    ok &= writeFile(trailingCsv, header + "\n0,1,1,1,1,1,1\n");
    ok &= writeFile(batchPath(trailingStaging, 1),
                    "60000,1,1,1,1,1,1\n"
                    "120000,1,1,1,1,1,1\n");
    Manifest trailingManifest;
    trailingManifest.batchCount = 1;
    trailingManifest.intervalMs = minute;
    trailingManifest.alignment = Alignment::Fixed;
    trailingManifest.baseTimestamp = 0;
    trailingManifest.baseHasData = true;
    trailingManifest.requestedStart = minute;
    trailingManifest.expectedEnd = 2 * minute;
    trailingManifest.firstTimestamp = minute;
    trailingManifest.lastTimestamp = 2 * minute;
    if (!writeManifest(trailingStaging, trailingManifest, error) ||
        !commit(trailingStaging, trailingManifest, trailingCsv, header, error) ||
        !readLines(trailingCsv).back().starts_with("120000,")) {
        std::cerr << "Trailing candle outage blocked an otherwise valid transaction: "
                  << error << '\n';
        ok = false;
    }

    ok &= writeFile(batchPath(outageStaging, 1),
                    "120000,1,1,1,1,1,1\n"
                    "120000,1,1,1,1,1,1\n"
                    "240000,1,1,1,1,1,1\n");
    if (validate(outageStaging, outageManifest, error)) {
        std::cerr << "Staging accepted duplicate timestamps while allowing gaps\n";
        ok = false;
    }
    ok &= writeFile(batchPath(outageStaging, 1),
                    "120000,1,1,1,1,1,1\n"
                    "60000,1,1,1,1,1,1\n"
                    "240000,1,1,1,1,1,1\n");
    if (validate(outageStaging, outageManifest, error)) {
        std::cerr << "Staging accepted out-of-order timestamps while allowing gaps\n";
        ok = false;
    }
    ok &= writeFile(batchPath(outageStaging, 1),
                    "120000,1,1,1,1,1,1\n"
                    "210000,1,1,1,1,1,1\n"
                    "240000,1,1,1,1,1,1\n");
    if (validate(outageStaging, outageManifest, error)) {
        std::cerr << "Staging accepted a misaligned timestamp while allowing gaps\n";
        ok = false;
    }

    const auto wrongHeader = tmp.path() / "WRONG.csv";
    const std::string wrongContents = "timestamp,open,high,low,close,volume,amount\n"
                                      "0,1,1,1,1,1,1\n";
    ok &= writeFile(wrongHeader, wrongContents);
    auto wrongHeaderRepair = wrongHeader;
    wrongHeaderRepair += ".tail-repair";
    if (truncateAfter(wrongHeader, 0, error, header, minute, Alignment::Fixed) ||
        readLines(wrongHeader) != std::vector<std::string>{
                                      "timestamp,open,high,low,close,volume,amount",
                                      "0,1,1,1,1,1,1"} ||
        std::filesystem::exists(wrongHeaderRepair)) {
        std::cerr << "Tail repair accepted or modified a CSV with the wrong header\n";
        ok = false;
    }

    // The commit layer independently enforces marker-before-publication, so a
    // recoverable manifest left by an older/crashed caller cannot bypass the
    // downloader's ordering protocol.
    const auto unmarkedCsv = tmp.path() / "UNMARKED.csv";
    const auto unmarkedStaging = tmp.path() / "temp_UNMARKED";
    std::filesystem::create_directories(unmarkedStaging);
    ok &= writeFile(batchPath(unmarkedStaging, 1),
                    "60000,1,1,1,1,1,1\n"
                    "120000,1,1,1,1,1,1\n");
    Manifest unmarkedManifest;
    unmarkedManifest.batchCount = 1;
    unmarkedManifest.intervalMs = minute;
    unmarkedManifest.baseTimestamp = 0;
    unmarkedManifest.baseHasData = false;
    unmarkedManifest.requestedStart = 0;
    unmarkedManifest.expectedEnd = 2 * minute;
    unmarkedManifest.firstTimestamp = minute;
    unmarkedManifest.lastTimestamp = 2 * minute;
    if (!writeManifest(unmarkedStaging, unmarkedManifest, error)) {
        std::cerr << "Could not prepare unmarked-prefix regression staging: " << error << '\n';
        return 1;
    }
    if (commit(unmarkedStaging, unmarkedManifest, unmarkedCsv, header, error) ||
        std::filesystem::exists(unmarkedCsv)) {
        std::cerr << "Fresh suffix was published without a persistent prefix marker\n";
        ok = false;
    }

    // A negative listing-boundary response is never definitive.  Persist the
    // marker before exposing its suffix so a crash at either side of the CSV
    // publication cannot make the missing prefix permanent.
    const auto prefixCsv = tmp.path() / "PREFIX.csv";
    const PrefixMarker unresolvedPrefix{1, 0, minute, Alignment::Fixed};
    if (!writePrefixMarker(prefixCsv, unresolvedPrefix, error) ||
        std::filesystem::exists(prefixCsv) ||
        !std::filesystem::is_regular_file(prefixMarkerPath(prefixCsv))) {
        std::cerr << "Unresolved-prefix marker was not durable before suffix publication: "
                  << error << '\n';
        ok = false;
    }
    std::optional<PrefixMarker> readPrefix;
    if (!readPrefixMarker(prefixCsv, readPrefix, error) || !readPrefix ||
        *readPrefix != unresolvedPrefix) {
        std::cerr << "Unresolved-prefix marker did not round-trip: " << error << '\n';
        ok = false;
    }

    ok &= writeFile(prefixCsv, header + "\n"
                               "120000,1,1,1,1,1,1\n"
                               "180000,1,1,1,1,1,1\n"
                               "240000,1,1,1,1,1,1\n");
    CsvTail prefixBase;
    if (!inspectCsvTail(prefixCsv, header, prefixBase, error, minute, Alignment::Fixed) ||
        !prefixBase.hasData || prefixBase.firstTimestamp != 2 * minute ||
        prefixBase.timestamp != 4 * minute) {
        std::cerr << "Could not inspect provisional suffix before rebuild: " << error << '\n';
        ok = false;
    }

    const auto prefixStaging = tmp.path() / "temp_PREFIX";
    std::filesystem::create_directories(prefixStaging);
    const std::string rebuiltRows =
        "0,1,1,1,1,1,1\n"
        "60000,1,1,1,1,1,1\n"
        "120000,1e0,1.0,1,1,1,1\n" // binary64-equivalent overlap
        "300000,1,1,1,1,1,1\n";    // old 180000/240000 are absent
    ok &= writeFile(batchPath(prefixStaging, 1), rebuiltRows);
    Manifest prefixManifest;
    prefixManifest.batchCount = 1;
    prefixManifest.intervalMs = minute;
    prefixManifest.alignment = Alignment::Fixed;
    prefixManifest.baseTimestamp = 0;
    prefixManifest.baseHasData = false;
    prefixManifest.requestedStart = 0;
    prefixManifest.expectedEnd = 5 * minute;
    prefixManifest.firstTimestamp = 0;
    prefixManifest.lastTimestamp = 5 * minute;
    if (!writeManifest(prefixStaging, prefixManifest, error)) {
        std::cerr << "Could not validate prefix-rebuild staging: " << error << '\n';
        return 1;
    }

    const auto provisionalSuffix = readLines(prefixCsv);
    std::filesystem::remove(batchPath(prefixStaging, 1));
    if (replaceCsv(prefixStaging, prefixManifest, prefixCsv, header, prefixBase, error) ||
        readLines(prefixCsv) != provisionalSuffix ||
        !std::filesystem::is_regular_file(prefixMarkerPath(prefixCsv))) {
        std::cerr << "Failed prefix rebuild damaged the usable suffix or lost its marker\n";
        ok = false;
    }
    ok &= writeFile(batchPath(prefixStaging, 1), rebuiltRows);

    // Even under an accidental missing caller lock, the full-base snapshot
    // prevents an obsolete rebuild from replacing a concurrently changed CSV.
    ok &= writeFile(prefixCsv, header + "\n"
                               "120000,1,1,1,1,1,1\n"
                               "180000,1,1,1,1,1,1\n"
                               "240000,1,1,1,1,1,1\n"
                               "300000,1,1,1,1,1,1\n");
    const auto externallyExtendedSuffix = readLines(prefixCsv);
    if (replaceCsv(prefixStaging, prefixManifest, prefixCsv, header, prefixBase, error) ||
        readLines(prefixCsv) != externallyExtendedSuffix) {
        std::cerr << "Prefix rebuild accepted a changed base or damaged it\n";
        ok = false;
    }

    ok &= writeFile(prefixCsv, header + "\n"
                               "120000,1,1,1,1,1,1\n"
                               "180000,1,1,1,1,1,1\n"
                               "240000,1,1,1,1,1,1\n");
    if (!inspectCsvTail(prefixCsv, header, prefixBase, error, minute, Alignment::Fixed) ||
        !replaceCsv(prefixStaging, prefixManifest, prefixCsv, header, prefixBase, error)) {
        std::cerr << "Could not atomically publish the extended prefix: " << error << '\n';
        ok = false;
    }
    const auto rebuilt = readLines(prefixCsv);
    if (rebuilt.size() != 7 || !rebuilt[1].starts_with("0,") ||
        !rebuilt[4].starts_with("180000,") ||
        !rebuilt[5].starts_with("240000,") ||
        !rebuilt.back().starts_with("300000,") ||
        !std::filesystem::is_regular_file(prefixMarkerPath(prefixCsv))) {
        std::cerr << "Prefix union lost existing rows or removed its marker too early\n";
        ok = false;
    }
    if (!removePrefixMarker(prefixCsv, error) ||
        std::filesystem::exists(prefixMarkerPath(prefixCsv))) {
        std::cerr << "Positively resolved prefix marker could not be removed: " << error << '\n';
        ok = false;
    }

    // A conflicting same-timestamp row must fail before atomic replacement;
    // both the old CSV and its recovery marker remain available for retry.
    const auto conflictCsv = tmp.path() / "CONFLICT_PREFIX.csv";
    const auto conflictStaging = tmp.path() / "temp_CONFLICT_PREFIX";
    std::filesystem::create_directories(conflictStaging);
    ok &= writeFile(conflictCsv, header + "\n"
                                 "120000,1,1,1,1,1,1\n"
                                 "180000,1,1,1,1,1,1\n");
    ok &= writePrefixMarker(conflictCsv, unresolvedPrefix, error);
    CsvTail conflictBase;
    ok &= inspectCsvTail(conflictCsv, header, conflictBase, error, minute, Alignment::Fixed);
    ok &= writeFile(batchPath(conflictStaging, 1),
                    "0,1,1,1,1,1,1\n"
                    "120000,2,1,1,1,1,1\n"
                    "180000,1,1,1,1,1,1\n");
    Manifest conflictManifest;
    conflictManifest.batchCount = 1;
    conflictManifest.intervalMs = minute;
    conflictManifest.alignment = Alignment::Fixed;
    conflictManifest.baseHasData = false;
    conflictManifest.requestedStart = 0;
    conflictManifest.expectedEnd = 3 * minute;
    conflictManifest.firstTimestamp = 0;
    conflictManifest.lastTimestamp = 3 * minute;
    const auto conflictBefore = readLines(conflictCsv);
    if (!writeManifest(conflictStaging, conflictManifest, error) ||
        replaceCsv(conflictStaging, conflictManifest, conflictCsv, header,
                   conflictBase, error) ||
        readLines(conflictCsv) != conflictBefore ||
        !std::filesystem::is_regular_file(prefixMarkerPath(conflictCsv))) {
        std::cerr << "Conflicting prefix union modified old CSV or lost marker\n";
        ok = false;
    }

    const auto malformedMarkerCsv = tmp.path() / "MALFORMED_PREFIX.csv";
    ok &= writeFile(prefixMarkerPath(malformedMarkerCsv), "not a marker\n");
    if (readPrefixMarker(malformedMarkerCsv, readPrefix, error)) {
        std::cerr << "Malformed unresolved-prefix marker was accepted\n";
        ok = false;
    }

    // Funding updates use a separate per-symbol transaction because funding
    // timestamps are strictly increasing but their interval is not fixed.
    // Start with a valid legacy file lacking its final newline; the atomic
    // replacement must preserve the old row and safely terminate it.
    const auto fundingCsv = tmp.path() / "TEST_fr.csv";
    const std::string fundingHeader{stonky::mexc_funding_csv::Header};
    ok &= writeFile(fundingCsv, fundingHeader + "\n100,0.0001");
    stonky::mexc_funding_csv::Tail fundingBase;
    if (!stonky::mexc_funding_csv::inspect(fundingCsv, fundingBase, error) ||
        !fundingBase.hasData || fundingBase.timestamp != 100 ||
        fundingBase.newlineTerminated) {
        std::cerr << "Valid funding CSV tail was not inspected correctly: " << error << '\n';
        ok = false;
    }

    const std::vector<stonky::mexc_funding_csv::Record> fundingAppend{
        {200, "-0.0002"},
        {300, "3e-4"},
    };
    if (!stonky::mexc_funding_csv::appendAtomically(
            fundingCsv, fundingBase, fundingAppend, error)) {
        std::cerr << "Could not commit funding transaction: " << error << '\n';
        ok = false;
    }
    const std::vector<std::string> committedFunding{
        fundingHeader,
        "100,0.0001",
        "200,-0.0002",
        "300,3e-4",
    };
    if (readLines(fundingCsv) != committedFunding) {
        std::cerr << "Funding transaction did not preserve and append complete rows\n";
        ok = false;
    }

    stonky::mexc_funding_csv::Tail committedFundingTail;
    if (!stonky::mexc_funding_csv::inspect(fundingCsv, committedFundingTail, error) ||
        !committedFundingTail.newlineTerminated || committedFundingTail.timestamp != 300) {
        std::cerr << "Committed funding CSV did not validate: " << error << '\n';
        ok = false;
    }

    // Duplicate/out-of-order transactions are rejected before opening a
    // replacement; the prior file must remain byte-for-byte equivalent.
    const std::vector<stonky::mexc_funding_csv::Record> duplicateFunding{
        {400, "0.0004"},
        {400, "0.0004"},
    };
    if (stonky::mexc_funding_csv::appendAtomically(
            fundingCsv, committedFundingTail, duplicateFunding, error) ||
        readLines(fundingCsv) != committedFunding) {
        std::cerr << "Funding transaction accepted a duplicate or modified the old CSV\n";
        ok = false;
    }

    // The compare-tail guard rejects a transaction staged from an obsolete
    // view even without relying on the caller's process lock.
    const auto staleFundingBase = committedFundingTail;
    ok &= writeFile(fundingCsv, fundingHeader +
                                    "\n100,0.0001\n200,-0.0002\n300,3e-4\n400,0.0004\n");
    const auto externallyChangedFunding = readLines(fundingCsv);
    const std::vector<stonky::mexc_funding_csv::Record> staleAppend{{500, "0.0005"}};
    if (stonky::mexc_funding_csv::appendAtomically(
            fundingCsv, staleFundingBase, staleAppend, error) ||
        readLines(fundingCsv) != externallyChangedFunding) {
        std::cerr << "Funding commit accepted a changed base or damaged existing data\n";
        ok = false;
    }

    const auto invalidFunding = tmp.path() / "INVALID_fr.csv";
    ok &= writeFile(invalidFunding, fundingHeader + "\n100,0.1\n100,0.2\n");
    stonky::mexc_funding_csv::Tail invalidFundingTail;
    if (stonky::mexc_funding_csv::inspect(invalidFunding, invalidFundingTail, error)) {
        std::cerr << "Funding validation accepted duplicate existing timestamps\n";
        ok = false;
    }

    const auto tornFunding = tmp.path() / "TORN_fr.csv";
    ok &= writeFile(tornFunding, fundingHeader + "\n100,0.1\n200,");
    if (stonky::mexc_funding_csv::inspect(tornFunding, invalidFundingTail, error)) {
        std::cerr << "Funding validation accepted a torn record\n";
        ok = false;
    }

    const std::vector<std::string> nonBinary64FundingValues{"1e1000", "NaN", "Inf", "-Inf"};
    for (std::size_t i = 0; i < nonBinary64FundingValues.size(); ++i) {
        const auto invalidValueFunding =
            tmp.path() / ("NON_BINARY64_" + std::to_string(i) + "_fr.csv");
        ok &= writeFile(invalidValueFunding,
                        fundingHeader + "\n100," + nonBinary64FundingValues[i] + "\n");
        if (stonky::mexc_funding_csv::inspect(
                invalidValueFunding, invalidFundingTail, error)) {
            std::cerr << "Funding validation accepted non-finite binary64 value: "
                      << nonBinary64FundingValues[i] << '\n';
            ok = false;
        }
    }

    // Semantic emptiness is retried independently of the network client.  A
    // transient pair of empty responses must not become a false-success
    // callback when the third attempt contains data.
    using stonky::mexc_funding_pagination::EmptyFirstPageDecision;
    using stonky::mexc_funding_pagination::RetryPolicy;
    const RetryPolicy retryPolicy{3, std::chrono::milliseconds{7}};
    const std::vector<FakeFundingPage> scriptedFundingPages{
        {{}, 0, 0},
        {{}, 0, 0},
        {{1}, 1, 1},
    };
    std::size_t fundingFetches = 0;
    std::vector<std::pair<std::chrono::milliseconds, std::int32_t>> fundingSleeps;
    const auto recoveredFundingPage = stonky::mexc_funding_pagination::fetchFirstPage(
        [&] { return scriptedFundingPages.at(fundingFetches++); },
        [&](const std::chrono::milliseconds delay, const std::int32_t nextAttempt) {
            fundingSleeps.emplace_back(delay, nextAttempt);
        },
        retryPolicy);
    if (recoveredFundingPage.resultList.size() != 1 || fundingFetches != 3 ||
        fundingSleeps !=
            std::vector<std::pair<std::chrono::milliseconds, std::int32_t>>{
                {std::chrono::milliseconds{7}, 2},
                {std::chrono::milliseconds{7}, 3},
            }) {
        std::cerr << "Funding page-1 retry did not recover deterministically\n";
        ok = false;
    }

    fundingFetches = 0;
    fundingSleeps.clear();
    try {
        std::ignore = stonky::mexc_funding_pagination::fetchFirstPage(
            [&] {
                ++fundingFetches;
                return FakeFundingPage{{}, 0, 0};
            },
            [&](const std::chrono::milliseconds delay, const std::int32_t nextAttempt) {
                fundingSleeps.emplace_back(delay, nextAttempt);
            },
            retryPolicy);
        std::cerr << "Repeatedly empty funding page 1 was reported as success\n";
        ok = false;
    } catch (const std::runtime_error &) {
        if (fundingFetches != 3 || fundingSleeps.size() != 2) {
            std::cerr << "Funding page-1 exhaustion used the wrong retry bound\n";
            ok = false;
        }
    }

    fundingFetches = 0;
    fundingSleeps.clear();
    try {
        std::ignore = stonky::mexc_funding_pagination::fetchFirstPage(
            [&] {
                ++fundingFetches;
                return FakeFundingPage{{}, 1, 0};
            },
            [&](const std::chrono::milliseconds delay, const std::int32_t nextAttempt) {
                fundingSleeps.emplace_back(delay, nextAttempt);
            },
            retryPolicy);
        std::cerr << "Empty funding page with nonzero totalPage was accepted\n";
        ok = false;
    } catch (const std::runtime_error &) {
        if (fundingFetches != 1 || !fundingSleeps.empty()) {
            std::cerr << "Inconsistent empty funding page was retried instead of rejected\n";
            ok = false;
        }
    }

    if (stonky::mexc_funding_pagination::decideEmptyFirstPage(
            0, 0, retryPolicy.maxAttempts, retryPolicy, true) !=
        EmptyFirstPageDecision::AcceptAuthoritativeNoOp) {
        std::cerr << "Authoritatively current funding base could not select safe no-op\n";
        ok = false;
    }

    // A page must identify the exact requested page and retain one stable,
    // internally consistent metadata snapshot throughout pagination.
    std::optional<stonky::mexc_funding_pagination::SnapshotMetadata> fundingSnapshot;
    std::string fundingPaginationError;
    if (!stonky::mexc_funding_pagination::validatePageMetadata(
            1, 20, 2, 1, 2, 3, 2, fundingSnapshot, fundingPaginationError) ||
        !stonky::mexc_funding_pagination::validatePageMetadata(
            2, 20, 1, 2, 2, 3, 2, fundingSnapshot, fundingPaginationError)) {
        std::cerr << "Consistent funding pagination metadata was rejected: "
                  << fundingPaginationError << '\n';
        ok = false;
    }
    std::optional<stonky::mexc_funding_pagination::SnapshotMetadata> wrongPageSnapshot;
    if (stonky::mexc_funding_pagination::validatePageMetadata(
            1, 2, 2, 2, 2, 2, 1, wrongPageSnapshot, fundingPaginationError)) {
        std::cerr << "Funding pagination accepted a response for the wrong currentPage\n";
        ok = false;
    }

    using stonky::mexc_funding_pagination::ScanDecision;
    if (stonky::mexc_funding_pagination::decideScanProgress(true, false, 1, 1) !=
            ScanDecision::RejectMissingBaseOverlap ||
        stonky::mexc_funding_pagination::decideScanProgress(true, true, 1, 3) !=
            ScanDecision::Complete) {
        std::cerr << "Funding scan could commit a truncated totalPage without exact tail overlap\n";
        ok = false;
    }

    // A fresh suffix is marked provisional before publication.  Every later
    // scan can atomically union an older prefix with it, while the persistent
    // marker prevents the suffix tail from becoming an incremental resume
    // point that would permanently hide the missing history.
    const auto provisionalFundingCsv = tmp.path() / "PROVISIONAL_fr.csv";
    stonky::mexc_funding_csv::Tail missingFundingBase;
    std::vector<stonky::mexc_funding_csv::Record> noFundingRecords;
    const std::vector<stonky::mexc_funding_csv::Record> provisionalFundingSuffix{
        {200, "0.0002"},
        {300, "0.0003"},
    };
    if (!stonky::mexc_funding_csv::readRecords(
            provisionalFundingCsv, missingFundingBase, noFundingRecords, error)) {
        std::cerr << "Could not inspect missing provisional funding CSV: " << error << '\n';
        ok = false;
    }
    if (stonky::mexc_funding_csv::replaceAtomically(
            provisionalFundingCsv, missingFundingBase, provisionalFundingSuffix, error) ||
        std::filesystem::exists(provisionalFundingCsv)) {
        std::cerr << "Unmarked fresh funding replacement was published\n";
        ok = false;
    }
    if (!stonky::mexc_funding_csv::ensureProvisionalMarker(provisionalFundingCsv, error) ||
        std::filesystem::exists(provisionalFundingCsv) ||
        !std::filesystem::is_regular_file(
            stonky::mexc_funding_csv::provisionalMarkerPath(provisionalFundingCsv))) {
        std::cerr << "Fresh funding prefix was not marked before CSV publication: " << error << '\n';
        ok = false;
    }
    if (!stonky::mexc_funding_csv::replaceAtomically(
            provisionalFundingCsv, missingFundingBase, provisionalFundingSuffix, error)) {
        std::cerr << "Could not publish provisional funding suffix: " << error << '\n';
        ok = false;
    }

    stonky::mexc_funding_csv::Tail provisionalTail;
    std::vector<stonky::mexc_funding_csv::Record> storedProvisional;
    if (!stonky::mexc_funding_csv::readRecords(
            provisionalFundingCsv, provisionalTail, storedProvisional, error)) {
        std::cerr << "Could not read provisional funding suffix: " << error << '\n';
        ok = false;
    }
    const std::vector<stonky::mexc_funding_csv::Record> widerFundingScan{
        {100, "0.0001"},
        {200, "2e-4"}, // same binary64 value, normalized representation may differ
        {300, "0.0003"},
        {400, "0.0004"},
    };
    std::vector<stonky::mexc_funding_csv::Record> convergedFunding;
    if (!stonky::mexc_funding_csv::mergeRecords(
            storedProvisional, widerFundingScan, convergedFunding, error) ||
        !stonky::mexc_funding_csv::replaceAtomically(
            provisionalFundingCsv, provisionalTail, convergedFunding, error)) {
        std::cerr << "Provisional funding prefix did not converge: " << error << '\n';
        ok = false;
    }

    bool stillProvisional = false;
    stonky::mexc_funding_csv::Tail convergedFundingTail;
    std::vector<stonky::mexc_funding_csv::Record> convergedStoredFunding;
    if (!stonky::mexc_funding_csv::inspectProvisionalMarker(
            provisionalFundingCsv, stillProvisional, error) || !stillProvisional ||
        !stonky::mexc_funding_csv::readRecords(
            provisionalFundingCsv, convergedFundingTail, convergedStoredFunding, error) ||
        convergedFundingTail.firstTimestamp != 100 || convergedFundingTail.timestamp != 400 ||
        convergedStoredFunding != widerFundingScan) {
        std::cerr << "Converged funding prefix lost data or its provisional marker: "
                  << error << '\n';
        ok = false;
    }

    // A legacy CSV cannot become an established append base merely because a
    // truncated but self-consistent snapshot contains its exact tail.  Migrate
    // it to provisional before the first replacement, retain the marker, and
    // let a later wider full scan fill the missing middle timestamp.
    const auto legacyFundingCsv = tmp.path() / "LEGACY_fr.csv";
    ok &= writeFile(legacyFundingCsv, fundingHeader + "\n100,0.0001\n");
    stonky::mexc_funding_csv::Tail legacyBase;
    std::vector<stonky::mexc_funding_csv::Record> legacyRecords;
    if (!stonky::mexc_funding_csv::readRecords(
            legacyFundingCsv, legacyBase, legacyRecords, error) ||
        !stonky::mexc_funding_csv::ensureProvisionalMarker(legacyFundingCsv, error)) {
        std::cerr << "Could not migrate legacy funding CSV to provisional mode: "
                  << error << '\n';
        ok = false;
    }

    std::vector<stonky::mexc_funding_csv::Record> truncatedRemoteSnapshot{
        {300, "0.0003"},
        {100, "0.0001"},
    };
    std::ranges::sort(truncatedRemoteSnapshot, {},
                      &stonky::mexc_funding_csv::Record::timestamp);
    std::vector<stonky::mexc_funding_csv::Record> firstLegacyUnion;
    if (!stonky::mexc_funding_csv::mergeRecords(
            legacyRecords, truncatedRemoteSnapshot, firstLegacyUnion, error) ||
        firstLegacyUnion != truncatedRemoteSnapshot ||
        !stonky::mexc_funding_csv::replaceAtomically(
            legacyFundingCsv, legacyBase, firstLegacyUnion, error)) {
        std::cerr << "Could not retain a truncated legacy funding snapshot safely: "
                  << error << '\n';
        ok = false;
    }

    stonky::mexc_funding_csv::Tail truncatedLegacyTail;
    std::vector<stonky::mexc_funding_csv::Record> truncatedLegacyRecords;
    bool legacyStillProvisional = false;
    if (!stonky::mexc_funding_csv::inspectProvisionalMarker(
            legacyFundingCsv, legacyStillProvisional, error) || !legacyStillProvisional ||
        !stonky::mexc_funding_csv::readRecords(
            legacyFundingCsv, truncatedLegacyTail, truncatedLegacyRecords, error) ||
        truncatedLegacyRecords != firstLegacyUnion) {
        std::cerr << "Legacy funding migration lost data or its provisional marker: "
                  << error << '\n';
        ok = false;
    }

    std::vector<stonky::mexc_funding_csv::Record> restoredRemoteSnapshot{
        {300, "0.0003"},
        {200, "0.0002"},
        {100, "0.0001"},
    };
    std::ranges::sort(restoredRemoteSnapshot, {},
                      &stonky::mexc_funding_csv::Record::timestamp);
    std::vector<stonky::mexc_funding_csv::Record> restoredLegacyUnion;
    if (!stonky::mexc_funding_csv::mergeRecords(
            truncatedLegacyRecords, restoredRemoteSnapshot, restoredLegacyUnion, error) ||
        !stonky::mexc_funding_csv::replaceAtomically(
            legacyFundingCsv, truncatedLegacyTail, restoredLegacyUnion, error)) {
        std::cerr << "Later funding snapshot did not fill the legacy middle gap: "
                  << error << '\n';
        ok = false;
    }

    stonky::mexc_funding_csv::Tail restoredLegacyTail;
    std::vector<stonky::mexc_funding_csv::Record> restoredLegacyRecords;
    if (!stonky::mexc_funding_csv::inspectProvisionalMarker(
            legacyFundingCsv, legacyStillProvisional, error) || !legacyStillProvisional ||
        !stonky::mexc_funding_csv::readRecords(
            legacyFundingCsv, restoredLegacyTail, restoredLegacyRecords, error) ||
        restoredLegacyRecords != restoredRemoteSnapshot) {
        std::cerr << "Legacy funding history failed to converge after the wider snapshot: "
                  << error << '\n';
        ok = false;
    }

    return ok ? 0 : 1;
}
