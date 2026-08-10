#include "stonky/csv_verifier.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        path_ = std::filesystem::temp_directory_path() /
                ("crypto_data_downloader_verifier_" + std::to_string(
                    std::chrono::steady_clock::now().time_since_epoch().count()));
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

bool write(const std::filesystem::path &path, const std::string &contents) {
    std::ofstream output(path, std::ios::trunc);
    output << contents;
    output.flush();
    return output.good();
}

std::int64_t utcMs(const int year, const unsigned month, const unsigned day) {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        sys_days{std::chrono::year{year} / std::chrono::month{month} /
                 std::chrono::day{day}}.time_since_epoch()).count();
}

} // namespace

int main() {
    TemporaryDirectory temporary;
    stonky::CsvVerifier::Options options;
    options.expectedFields = 7;
    options.intervalMs = 60000;

    const auto valid = temporary.path() / "valid.csv";
    if (!write(valid,
               "open_time,open,high,low,close,volume,amount\n"
               "0,1,2,0.5,1.5,10,15\n"
               "60000,1.5,2.5,1,2,11,22\n")) {
        return 1;
    }
    const auto validReport = stonky::CsvVerifier::verifyFile(valid.string(), options);
    if (validReport.readFailed || validReport.needsRepair() || validReport.hasGaps()) {
        std::cerr << "Verifier rejected a valid fixed-width CSV\n";
        return 1;
    }

    const auto damaged = temporary.path() / "damaged.csv";
    if (!write(damaged,
               "open_time,open,high,low,close,volume,amount\n"
               "0,1,2,0.5,1.5,10,15\n"
               // A glued MEXC record has a valid timestamp prefix but 13 fields.
               "60000,1,2,0.5,1.5,10,15\n120000,1,2,0.5,1.5,10,15,180000,1,2,0.5,1.5,10,15\n"
               "240000,1,nan,0.5,1.5,10,15\n")) {
        return 1;
    }
    const auto damagedReport = stonky::CsvVerifier::verifyFile(damaged.string(), options);
    if (damagedReport.readFailed || damagedReport.malformed != 2 || !damagedReport.needsRepair()) {
        std::cerr << "Verifier did not reject glued/non-finite MEXC rows\n";
        return 1;
    }

    const auto headerOnly = temporary.path() / "header-only.csv";
    if (!write(headerOnly, "open_time,open,high,low,close,volume,amount\n")) {
        return 1;
    }
    if (!stonky::CsvVerifier::verifyFile(headerOnly.string(), options).readFailed) {
        std::cerr << "Verifier accepted a header-only CSV\n";
        return 1;
    }

    const auto wrongHeader = temporary.path() / "wrong-header.csv";
    if (!write(wrongHeader,
               "not_time,open,high,low,close,volume,amount,extra\n"
               "0,1,2,0.5,1.5,10,15\n")) {
        return 1;
    }
    if (!stonky::CsvVerifier::verifyFile(wrongHeader.string(), options).readFailed) {
        std::cerr << "Verifier accepted an invalid header\n";
        return 1;
    }

    const auto wrongMexcSchema = temporary.path() / "wrong-mexc-schema.csv";
    if (!write(wrongMexcSchema,
               "open_time,open,high,low,close,volume,mystery\n"
               "0,1,2,0.5,1.5,10,15\n")) {
        return 1;
    }
    if (!stonky::CsvVerifier::verifyFile(wrongMexcSchema.string(), options).readFailed) {
        std::cerr << "Verifier accepted an unknown MEXC column with canonical width\n";
        return 1;
    }

    const auto legacyBybit = temporary.path() / "legacy-bybit.csv";
    if (!write(legacyBybit,
               "open_time,open,high,low,close,volume,turnover\n"
               "0,1,2,0.5,1.5,10,15\n"
               // More than one extra field is glued/corrupt, not legacy data.
               "60000,1,2,0.5,1.5,10,15,120000,1,2,0.5,1.5\n")) {
        return 1;
    }
    stonky::CsvVerifier::Options bybitOptions;
    bybitOptions.expectedFields = 6;
    bybitOptions.salvageExtraField = true;
    bybitOptions.repair = true;
    const auto bybitReport = stonky::CsvVerifier::verifyFile(legacyBybit.string(), bybitOptions);
    if (bybitReport.readFailed || !bybitReport.repaired || bybitReport.salvaged != 1 ||
        bybitReport.malformed != 1 || bybitReport.totalRecords != 1) {
        std::cerr << "Verifier did not safely canonicalize the legacy Bybit schema\n";
        return 1;
    }
    std::ifstream repairedBybit(legacyBybit);
    const std::string repairedContents{std::istreambuf_iterator<char>{repairedBybit},
                                       std::istreambuf_iterator<char>{}};
    if (repairedContents !=
        "open_time,open,high,low,close,volume\n0,1,2,0.5,1.5,10\n") {
        std::cerr << "Legacy Bybit repair produced an unexpected CSV\n";
        return 1;
    }
    const auto unknownExtraSchema = temporary.path() / "unknown-extra.csv";
    if (!write(unknownExtraSchema,
               "open_time,open,high,low,close,volume,mystery\n"
               "0,1,2,0.5,1.5,10,15\n")) {
        return 1;
    }
    if (!stonky::CsvVerifier::verifyFile(unknownExtraSchema.string(), bybitOptions).readFailed) {
        std::cerr << "Verifier discarded an unknown extra column as if it were legacy turnover\n";
        return 1;
    }

    const auto extraRowUnderCurrentHeader = temporary.path() / "extra-row-current-header.csv";
    if (!write(extraRowUnderCurrentHeader,
               "open_time,open,high,low,close,volume\n"
               "0,1,2,0.5,1.5,10,unexpected\n")) {
        return 1;
    }
    const auto extraRowReport =
        stonky::CsvVerifier::verifyFile(extraRowUnderCurrentHeader.string(), bybitOptions);
    if (!extraRowReport.readFailed || extraRowReport.malformed != 1) {
        std::cerr << "Verifier salvaged an extra field without the known legacy header\n";
        return 1;
    }

    const auto monthly = temporary.path() / "monthly.csv";
    if (!write(monthly,
               "open_time,open,high,low,close,volume\n" +
               std::to_string(utcMs(2024, 1, 1)) + ",1,2,0.5,1.5,10\n" +
               std::to_string(utcMs(2024, 2, 1)) + ",1,2,0.5,1.5,10\n" +
               std::to_string(utcMs(2024, 3, 1)) + ",1,2,0.5,1.5,10\n")) {
        return 1;
    }
    stonky::CsvVerifier::Options monthOptions;
    monthOptions.expectedFields = 6;
    monthOptions.calendarMonth = true;
    const auto monthReport = stonky::CsvVerifier::verifyFile(monthly.string(), monthOptions);
    if (monthReport.readFailed || monthReport.hasGaps()) {
        std::cerr << "Verifier treated a valid leap-year month sequence as fixed 30-day bars\n";
        return 1;
    }

    const auto monthlyClose = temporary.path() / "monthly-close.csv";
    if (!write(monthlyClose,
               "close_time,open,high,low,close,volume,timestamp,quote_av,trades,tb_base_av,tb_quote_av,ignore\n" +
               std::to_string(utcMs(2024, 2, 1) - 1) + ",1,2,0.5,1.5,10," +
               std::to_string(utcMs(2024, 1, 1)) + ",20,3,4,5,0\n" +
               std::to_string(utcMs(2024, 3, 1) - 1) + ",1,2,0.5,1.5,10," +
               std::to_string(utcMs(2024, 2, 1)) + ",20,3,4,5,0\n")) {
        return 1;
    }
    stonky::CsvVerifier::Options closeMonthOptions;
    closeMonthOptions.expectedFields = 12;
    closeMonthOptions.calendarMonth = true;
    const auto closeMonthReport =
        stonky::CsvVerifier::verifyFile(monthlyClose.string(), closeMonthOptions);
    if (closeMonthReport.readFailed || closeMonthReport.hasGaps()) {
        std::cerr << "Verifier rejected valid inclusive monthly close timestamps\n";
        return 1;
    }

    const auto okxMonthly = temporary.path() / "okx-monthly.csv";
    constexpr std::int64_t utc8Ms = 8LL * 60 * 60 * 1000;
    if (!write(okxMonthly,
               "open_time,open,high,low,close,volume,vol_ccy,vol_ccy_quote\n" +
               std::to_string(utcMs(2024, 1, 1) - utc8Ms) + ",1,2,0.5,1.5,10,11,12\n" +
               std::to_string(utcMs(2024, 2, 1) - utc8Ms) + ",1,2,0.5,1.5,10,11,12\n" +
               std::to_string(utcMs(2024, 3, 1) - utc8Ms) + ",1,2,0.5,1.5,10,11,12\n")) {
        return 1;
    }
    stonky::CsvVerifier::Options okxMonthOptions;
    okxMonthOptions.expectedFields = 8;
    okxMonthOptions.calendarMonth = true;
    okxMonthOptions.calendarUtcOffsetMinutes = 8 * 60;
    const auto okxMonthReport = stonky::CsvVerifier::verifyFile(okxMonthly.string(), okxMonthOptions);
    if (okxMonthReport.readFailed || okxMonthReport.hasGaps()) {
        std::cerr << "Verifier rejected valid OKX UTC+8 monthly boundaries\n";
        return 1;
    }

    return 0;
}
