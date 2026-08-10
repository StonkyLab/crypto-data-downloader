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
#include <stdexcept>
#include <string>

namespace stonky {

/**
 * Format a number for a CSV cell without losing information.
 *
 * Streaming a value with `ofs << value` uses the stream's default precision of
 * SIX SIGNIFICANT DIGITS. That silently truncated every value needing more:
 * BTC-USDT-SWAP above $100 000 lost its 0.1 tick (105635.8 was stored as
 * 105636) across 310 000 bars, and a volume of 1 205 829 became 1.20583e+06.
 * Values that fit in six digits — most alt prices — came through unharmed,
 * which is why the damage stayed invisible.
 *
 * The shortest representation that parses back to the same double is used
 * instead. Besides being lossless it also normalises the noise exchanges put
 * in their own archives: OKX ships 19557.900000000001455192, which round-trips
 * to a clean 19557.9.
 */
inline std::string csvNumber(const double value) {
    if (!std::isfinite(value)) {
        throw std::domain_error("non-finite double cannot be serialized to market-data CSV");
    }
    return fmt::format("{}", value);
}

/**
 * Same for the arbitrary-precision type the OKX and MEXC models use. Keep the
 * value in decimal form throughout: narrowing to double here would silently
 * discard every significant digit beyond the double's precision. With zero
 * requested digits Boost emits the backend's full available precision in its
 * general format and suppresses insignificant trailing zeroes.
 */
inline std::string csvNumber(const boost::multiprecision::cpp_dec_float_50 &value) {
    if (!boost::math::isfinite(value)) {
        throw std::domain_error("non-finite decimal cannot be serialized to market-data CSV");
    }
    return value.str(0, std::ios_base::fmtflags{});
}

} // namespace stonky

#endif // INCLUDE_STONKY_CSV_FORMAT_H
