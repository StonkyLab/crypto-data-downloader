/**
 * Strict parser for the command-line history floor.
 *
 * Licensed under the MIT License <http://opensource.org/licenses/MIT>.
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
 */

#pragma once

#include <charconv>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <system_error>

namespace stonky {

class SinceParseError final : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

namespace detail {

inline bool isAsciiDigit(const char value) noexcept {
    return value >= '0' && value <= '9';
}

inline bool allAsciiDigits(const std::string_view value) noexcept {
    for (const char character: value) {
        if (!isAsciiDigit(character)) {
            return false;
        }
    }
    return !value.empty();
}

inline unsigned twoDigitNumber(const std::string_view value, const std::size_t offset) noexcept {
    return static_cast<unsigned>((value[offset] - '0') * 10 + value[offset + 1] - '0');
}

} // namespace detail

/**
 * Parse a --since value as an exact UTC calendar date or epoch milliseconds.
 *
 * Calendar input must be exactly YYYY-MM-DD. Numeric input must contain only
 * decimal digits and fit in a signed 64-bit millisecond timestamp. The zero
 * value remains reserved for "no configured floor", and future values are
 * rejected.
 *
 * @throws SinceParseError if the value is malformed, outside the supported
 * timestamp range, at/before the Unix epoch, or later than nowMs.
 */
inline std::int64_t parseSinceMs(const std::string_view value, const std::int64_t nowMs) {
    std::int64_t parsedMs = 0;

    const bool dateShape = value.size() == 10 && value[4] == '-' && value[7] == '-';
    if (dateShape) {
        if (!detail::allAsciiDigits(value.substr(0, 4)) ||
            !detail::allAsciiDigits(value.substr(5, 2)) ||
            !detail::allAsciiDigits(value.substr(8, 2))) {
            throw SinceParseError("expected exactly YYYY-MM-DD or decimal milliseconds");
        }

        int yearNumber = 0;
        const auto [yearEnd, yearError] = std::from_chars(value.data(), value.data() + 4, yearNumber);
        if (yearError != std::errc{} || yearEnd != value.data() + 4) {
            throw SinceParseError("invalid calendar year");
        }

        const auto date = std::chrono::year_month_day{
            std::chrono::year{yearNumber},
            std::chrono::month{detail::twoDigitNumber(value, 5)},
            std::chrono::day{detail::twoDigitNumber(value, 8)}
        };
        if (!date.ok()) {
            throw SinceParseError("invalid calendar date");
        }

        parsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::sys_days{date}.time_since_epoch()).count();
    } else {
        if (!detail::allAsciiDigits(value)) {
            throw SinceParseError("expected exactly YYYY-MM-DD or decimal milliseconds");
        }

        const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsedMs);
        if (error == std::errc::result_out_of_range) {
            throw SinceParseError("millisecond timestamp is outside the signed 64-bit range");
        }
        if (error != std::errc{} || end != value.data() + value.size()) {
            throw SinceParseError("invalid millisecond timestamp");
        }
    }

    if (parsedMs <= 0) {
        throw SinceParseError("timestamp must be after 1970-01-01T00:00:00Z");
    }
    if (parsedMs > nowMs) {
        throw SinceParseError("timestamp must not be in the future");
    }
    return parsedMs;
}

} // namespace stonky
