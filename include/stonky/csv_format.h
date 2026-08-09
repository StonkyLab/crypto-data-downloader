/**
Numeric formatting for CSV output

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_CSV_FORMAT_H
#define INCLUDE_STONKY_CSV_FORMAT_H

#include <boost/multiprecision/cpp_dec_float.hpp>
#include <spdlog/fmt/fmt.h>
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
    return fmt::format("{}", value);
}

/**
 * Same for the arbitrary-precision type the OKX and MEXC models use. Market
 * data carries far fewer than the 15 significant digits a double holds
 * exactly, so the round trip is safe and yields the cleanest true value.
 */
inline std::string csvNumber(const boost::multiprecision::cpp_dec_float_50 &value) {
    return csvNumber(value.convert_to<double>());
}

} // namespace stonky

#endif // INCLUDE_STONKY_CSV_FORMAT_H
