#include "stonky/csv_data.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

namespace {
class TemporaryDirectory {
public:
    TemporaryDirectory()
        : path_(std::filesystem::temp_directory_path() /
                ("crypto_data_downloader_csv_data_" + std::to_string(
                    std::chrono::steady_clock::now().time_since_epoch().count()))) {
        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    [[nodiscard]] const std::filesystem::path &path() const { return path_; }

private:
    std::filesystem::path path_;
};

std::string readAll(const std::filesystem::path &path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}
} // namespace

int main() {
    TemporaryDirectory temporary;
    constexpr std::int64_t fallback = 123;

    const auto missing = stonky::CsvData::lastValidRecord(
        (temporary.path() / "missing.csv").string(), 6, fallback);
    if (missing.timestamp != fallback || missing.foundValid || missing.repairedTail) {
        std::cerr << "Missing CSV did not return an untouched fallback\n";
        return 1;
    }

    const auto torn = temporary.path() / "torn.csv";
    const std::string validPrefix =
        "open_time,open,high,low,close,volume\n"
        "0,1,2,0.5,1.5,10\n"
        "60000,1,2,0.5,1.5,10\n";
    {
        std::ofstream output(torn, std::ios::binary | std::ios::trunc);
        output << validPrefix << "120000,1,2";
    }
    const auto repaired = stonky::CsvData::lastValidRecord(torn.string(), 6, fallback);
    if (!repaired.foundValid || !repaired.repairedTail || repaired.timestamp != 60000 ||
        readAll(torn) != validPrefix) {
        std::cerr << "Torn CSV tail was not safely truncated to the last complete record\n";
        return 1;
    }

    const auto corrupt = temporary.path() / "corrupt.csv";
    const std::string header = "open_time,open,high,low,close,volume\n";
    {
        std::ofstream output(corrupt, std::ios::binary | std::ios::trunc);
        output << header << "not-a-record\n";
    }
    const auto headerOnly = stonky::CsvData::lastValidRecord(corrupt.string(), 6, fallback);
    if (headerOnly.foundValid || !headerOnly.repairedTail || headerOnly.timestamp != fallback ||
        readAll(corrupt) != header) {
        std::cerr << "Fully corrupt CSV body was not reduced to its header\n";
        return 1;
    }

    bool directoryReadFailed = false;
    try {
        (void) stonky::CsvData::lastValidRecord(temporary.path().string(), 6, fallback);
    } catch (const std::exception &) {
        directoryReadFailed = true;
    }
    if (!directoryReadFailed) {
        std::cerr << "Unreadable/non-regular CSV input silently returned a fallback\n";
        return 1;
    }

    return 0;
}
