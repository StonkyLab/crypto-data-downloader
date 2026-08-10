#include "stonky/csv_format.h"

#include <boost/multiprecision/cpp_dec_float.hpp>
#include <iostream>
#include <limits>
#include <string>

namespace {

using boost::multiprecision::cpp_dec_float_50;

bool expectDecimalRoundTrip(const std::string &input) {
    const cpp_dec_float_50 value(input);
    const std::string serialized = stonky::csvNumber(value);
    const cpp_dec_float_50 reparsed(serialized);

    if (reparsed == value) {
        return true;
    }

    std::cerr << "Decimal round trip failed: " << input << " -> " << serialized << '\n';
    return false;
}

} // namespace

int main() {
    bool ok = true;

    // Both values contain significant digits that a double conversion loses.
    ok &= expectDecimalRoundTrip("12345678901234567890.12345678901234567890123456789");
    ok &= expectDecimalRoundTrip("0.00000000000000028101000000000000000000000000000001");

    const auto normalized = stonky::csvNumber(cpp_dec_float_50("19557.90000000000000000000"));
    if (normalized != "19557.9") {
        std::cerr << "Trailing-zero normalization failed: " << normalized << '\n';
        ok = false;
    }

    if (stonky::csvNumber(105635.8) != "105635.8") {
        std::cerr << "Double round-trip formatting failed\n";
        ok = false;
    }

    try {
        (void) stonky::csvNumber(std::numeric_limits<double>::infinity());
        std::cerr << "Non-finite double was serialized\n";
        ok = false;
    } catch (const std::domain_error &) {
    }

    try {
        (void) stonky::csvNumber(cpp_dec_float_50("nan"));
        std::cerr << "Non-finite decimal was serialized\n";
        ok = false;
    } catch (const std::domain_error &) {
    }

    return ok ? 0 : 1;
}
