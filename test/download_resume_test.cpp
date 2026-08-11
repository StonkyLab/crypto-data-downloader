#include "stonky/csv_archive.h"
#include "stonky/download_resume.h"
#include "stonky/history_floor.h"

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <optional>
#include <string>

int main() {
    bool ok = true;
    constexpr std::int64_t floor = 1'750'000'000'123LL;

    const stonky::DownloadResume fresh{floor, false};
    if (stonky::shouldPersistTimestamp(floor - 1, fresh) ||
        !stonky::shouldPersistTimestamp(floor, fresh) ||
        !stonky::shouldPersistTimestamp(floor + 1, fresh)) {
        std::cerr << "Fresh --since floor is not inclusive\n";
        ok = false;
    }
    if (stonky::requestStartTimestamp(fresh) != floor ||
        stonky::freshDownloadStart(fresh, floor - 1000) != floor ||
        stonky::freshDownloadStart(fresh, floor + 1000) != floor + 1000) {
        std::cerr << "Fresh request/retention start is incorrect\n";
        ok = false;
    }

    const stonky::DownloadResume saved{floor, true};
    if (stonky::shouldPersistTimestamp(floor, saved) ||
        !stonky::shouldPersistTimestamp(floor + 1, saved) ||
        stonky::requestStartTimestamp(saved) != floor + 1 ||
        stonky::freshDownloadStart(saved, floor + 1000) != floor) {
        std::cerr << "Persisted CSV tail is not exclusive\n";
        ok = false;
    }
    const stonky::DownloadResume maximum{std::numeric_limits<std::int64_t>::max(), true};
    if (stonky::requestStartTimestamp(maximum) != maximum.timestamp) {
        std::cerr << "Resume request start overflowed\n";
        ok = false;
    }

    stonky::CsvData::TailCheck freshTail{floor, false, false};
    stonky::CsvData::TailCheck savedTail{floor, true, true};
    if (stonky::downloadResume(freshTail).hasSavedRecord ||
        !stonky::downloadResume(savedTail).hasSavedRecord) {
        std::cerr << "CSV tail state was lost while building resume point\n";
        ok = false;
    }

    stonky::setHistoryFloorMs(floor);
    if (stonky::historyFloorMs() != floor ||
        stonky::historyFloor(floor - 1) != floor ||
        stonky::historyFloor(floor + 1) != floor + 1) {
        std::cerr << "Process-wide history floor policy is incorrect\n";
        ok = false;
    }
    stonky::setHistoryFloorMs(0);

    constexpr std::int64_t dayMs = 86'400'000;
    if (stonky::floorTimestamp(3 * dayMs + 12'345, dayMs) != 3 * dayMs) {
        std::cerr << "Non-midnight Lighter listing probe was not rounded down\n";
        ok = false;
    }

    const auto expectStem = [&ok](const std::string &name, const std::optional<std::string> &expected) {
        const auto actual = stonky::csvStemFromArchivePath(std::filesystem::path{name});
        if (actual != expected) {
            std::cerr << "Unexpected CSV archive stem for " << name << '\n';
            ok = false;
        }
    };
    expectStem("BTC-USDT-SWAP.csv", "BTC-USDT-SWAP");
    expectStem("BTC-USDT-SWAP.csv.gz", "BTC-USDT-SWAP");
    expectStem("BTC-USDT-SWAP_fr.csv.zst", "BTC-USDT-SWAP_fr");
    expectStem("all-symbols.tar.gz", std::nullopt);
    expectStem("unrelated.zip", std::nullopt);

    return ok ? 0 : 1;
}
