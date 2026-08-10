/**
Numeric formatting for CSV output

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_CSV_FORMAT_H
#define INCLUDE_STONKY_CSV_FORMAT_H

#include <boost/multiprecision/cpp_dec_float.hpp>
#include <boost/math/special_functions/fpclassify.hpp>
#include <spdlog/fmt/fmt.h>
#include <cmath>
#include <ios>
#include <limits>
#include <stdexcept>
#include <string>

namespace stonky {

/**
 * Format a binary64 number for a CSV cell without losing any of its bits.
 *
 * Streaming a value with `ofs << value` uses the stream's default precision of
 * SIX SIGNIFICANT DIGITS. That silently truncated every value needing more:
 * BTC-USDT-SWAP above $100 000 lost its 0.1 tick (105635.8 was stored as
 * 105636) across 310 000 bars, and a volume of 1 205 829 became 1.20583e+06.
 * Values that fit in six digits — most alt prices — came through unharmed,
 * which is why the damage stayed invisible.
 *
 * The shortest representation that parses back to the same double is used
 * instead. Besides being lossless for binary64 it also normalises the noise
 * exchanges put in their own archives: OKX ships
 * 19557.900000000001455192, which round-trips to a clean 19557.9.
 */
inline std::string csvNumber(const double value) {
    if (!std::isfinite(value)) {
        throw std::domain_error("non-finite double cannot be serialized to market-data CSV");
    }
    return fmt::format("{}", value);
}

/**
 * Same for the decimal type the OKX and MEXC models use while parsing and
 * aggregating.
 *
 * This is deliberately a NORMALISATION to the project's storage contract, not
 * a general lossless serialisation of a 50-digit decimal. Market prices,
 * quantities and derived sums are persisted as binary64 values because the
 * downstream backtest stack consumes them as float64. Converting here and
 * writing the shortest round-tripping representation guarantees that loading
 * the CSV produces exactly that same double, regardless of whether the venue
 * supplied the source number as JSON numeric data or as a decimal string.
 *
 * This also removes binary64 expansions found in exchange archives. For
 * example, 47415.400000000001455192 and 47415.4 map to the same double; keeping
 * the extra digits would increase file size without changing a supported
 * consumer's value.
 *
 * Timestamps and identifiers must use their integer/string serializers and
 * must never pass through this overload.
 */
inline std::string csvNumber(const boost::multiprecision::cpp_dec_float_50 &value) {
    if (!boost::math::isfinite(value)) {
        throw std::domain_error("non-finite decimal cannot be serialized to market-data CSV");
    }

    using Decimal = boost::multiprecision::cpp_dec_float_50;
    const auto absoluteValue = boost::multiprecision::abs(value);
    static const Decimal maxBinary64{std::numeric_limits<double>::max()};
    if (absoluteValue > maxBinary64) {
        throw std::domain_error("decimal is outside the finite binary64 range");
    }

    const double narrowed = value.convert_to<double>();
    if (!std::isfinite(narrowed) || (value != 0 && narrowed == 0)) {
        throw std::domain_error("decimal cannot be represented by finite non-zero binary64");
    }

    // Normal binary64 rounding is far below the project's 0.001 % market-data
    // tolerance. Near the subnormal boundary, however, a finite conversion can
    // still have a large relative error, so validate that exceptional range.
    if (value != 0 && std::abs(narrowed) < std::numeric_limits<double>::min()) {
        static const Decimal maxRelativeError{"0.00001"}; // 0.001 %
        const Decimal restored{narrowed};
        const auto relativeError = boost::multiprecision::abs((restored - value) / value);
        if (relativeError > maxRelativeError) {
            throw std::domain_error("decimal loses more than 0.001 percent in binary64");
        }
    }
    return csvNumber(narrowed);
}

} // namespace stonky

#endif // INCLUDE_STONKY_CSV_FORMAT_H
