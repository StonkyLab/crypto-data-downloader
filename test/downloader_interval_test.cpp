#include "stonky/interface/exchange_enums.h"
#include "stonky/downloader.h"

#include <iostream>
#include <chrono>
#include <stdexcept>
#include <string>
#include <type_traits>

int main() {
    bool ok = true;

    if (stonky::Downloader::minutesToString(43200) != "1M") {
        std::cerr << "43200 minutes is not mapped to the 1M directory\n";
        ok = false;
    }

    const auto interval = stonky::Downloader::minutesToCandleInterval(43200);
    if (interval != stonky::CandleInterval::_1M) {
        std::cerr << "43200 minutes is not mapped to CandleInterval::_1M\n";
        ok = false;
    }

    const auto intervalMinutes =
        static_cast<std::underlying_type_t<stonky::CandleInterval>>(interval) / 60;
    if (intervalMinutes != 43200) {
        std::cerr << "CandleInterval::_1M does not round-trip to 43200 minutes\n";
        ok = false;
    }

    try {
        (void) stonky::Downloader::minutesToCandleInterval(40320);
        std::cerr << "Legacy 28-day value 40320 was unexpectedly accepted as a month\n";
        ok = false;
    } catch (const std::runtime_error &) {
        // Expected: the exchange-facing Month value is the existing 30-day enum.
    }

    using namespace std::chrono;
    const auto februaryOpen = duration_cast<milliseconds>(
        sys_days{year{2024} / February / day{1}}.time_since_epoch()).count();
    const auto marchOpen = duration_cast<milliseconds>(
        sys_days{year{2024} / March / day{1}}.time_since_epoch()).count();
    if (stonky::Downloader::candleCloseTimestampMs(
            februaryOpen, stonky::CandleInterval::_1M) != marchOpen) {
        std::cerr << "Leap-year calendar month was treated as a fixed duration\n";
        ok = false;
    }

    // OKX calendar candles roll at midnight UTC+8 (16:00 UTC the prior day).
    const auto utc8Offset = duration_cast<milliseconds>(hours{8}).count();
    const auto okxFebruaryOpen = februaryOpen - utc8Offset;
    const auto okxMarchOpen = marchOpen - utc8Offset;
    if (stonky::Downloader::candleCloseTimestampMs(
            okxFebruaryOpen, stonky::CandleInterval::_1M, 8 * 60) != okxMarchOpen) {
        std::cerr << "UTC+8 calendar month close timestamp is incorrect\n";
        ok = false;
    }

    return ok ? 0 : 1;
}
