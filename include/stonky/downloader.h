/**
Common Definitions for Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_DEFINITIONS_H
#define INCLUDE_STONKY_DEFINITIONS_H
#include "stonky/interface/exchange_enums.h"
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <thread>
#include <spdlog/fmt/ostr.h>

namespace stonky {
static auto CSV_FUT_DIR = "futures/prices/csv";
static auto CSV_FUT_FR_DIR = "futures/funding_rates/csv";
static auto CSV_SPOT_DIR = "spot/prices/csv";
static auto T6_FUT_DIR = "futures/prices/t6";
static auto T6_SPOT_DIR = "spot/prices/t6";

/// OKX X-Perps live in their own tree: they are USD-settled while the swaps
/// next to them are USDT-settled, so mixing both products into one directory
/// would leave nothing but the file name to tell them apart.
static auto CSV_XPERP_DIR = "xperp/prices/csv";
static auto CSV_XPERP_FR_DIR = "xperp/funding_rates/csv";
static auto T6_XPERP_DIR = "xperp/prices/t6";

class Downloader {
public:
    static std::string minutesToString(const std::int32_t minutes) {
        switch (minutes) {
        case 1:
            return "1m";
        case 3:
            return "3m";
        case 5:
            return "5m";
        case 15:
            return "15m";
        case 30:
            return "30m";
        case 60:
            return "1h";
        case 120:
            return "2h";
        case 240:
            return "4h";
        case 360:
            return "6h";
        case 480:
            return "8h";
        case 720:
            return "12h";
        case 1440:
            return "1d";
        case 4320:
            return "3d";
        case 10080:
            return "1w";
        case 43200:
            return "1M";
        default:
            throw std::runtime_error(fmt::format("Invalid minutes number: {} ", minutes));
        }
    }

    static CandleInterval minutesToCandleInterval(const std::int32_t minutes) {
        switch (minutes) {
        case 1:
            return CandleInterval::_1m;
        case 3:
            return CandleInterval::_3m;
        case 5:
            return CandleInterval::_5m;
        case 15:
            return CandleInterval::_15m;
        case 30:
            return CandleInterval::_30m;
        case 60:
            return CandleInterval::_1h;
        case 120:
            return CandleInterval::_2h;
        case 240:
            return CandleInterval::_4h;
        case 360:
            return CandleInterval::_6h;
        case 480:
            return CandleInterval::_8h;
        case 720:
            return CandleInterval::_12h;
        case 1440:
            return CandleInterval::_1d;
        case 4320:
            return CandleInterval::_3d;
        case 10080:
            return CandleInterval::_1w;
        case 43200:
            return CandleInterval::_1M;
        default:
            throw std::runtime_error(fmt::format("Invalid minutes number: {} ", minutes));
        }
    }

    /// T6 timestamps represent the end boundary. Month bars need calendar
    /// arithmetic; adding a fixed 30 days is wrong for 28/29/31-day months.
    static std::int64_t candleCloseTimestampMs(const std::int64_t openTimestampMs,
                                               const CandleInterval interval,
                                               const std::int32_t utcOffsetMinutes = 0) {
        if (interval != CandleInterval::_1M) {
            return openTimestampMs + static_cast<std::int64_t>(interval) * 1000;
        }

        using namespace std::chrono;
        const auto offset = minutes{utcOffsetMinutes};
        const sys_time<milliseconds> localOpen{milliseconds{openTimestampMs} + offset};
        const year_month_day current{floor<days>(localOpen)};
        const year_month_day next = current.year() / current.month() / day{1} + months{1};
        return duration_cast<milliseconds>((sys_days{next} - offset).time_since_epoch()).count();
    }

    static std::int32_t determineMaxJobs() {
        auto jobs = static_cast<std::int32_t>(std::thread::hardware_concurrency() * 0.75);

        if (jobs < 1) {
            jobs = 1;
        }
        return jobs;
    }
};
}

#endif //INCLUDE_STONKY_DEFINITIONS_H
