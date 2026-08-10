#include "stonky/csv_format.h"

#include <bit>
#include <boost/multiprecision/cpp_dec_float.hpp>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>

namespace {

using boost::multiprecision::cpp_dec_float_50;

bool expectBinary64RoundTrip(const std::string &input) {
    const cpp_dec_float_50 value(input);
    const double expected = value.convert_to<double>();
    const std::string serialized = stonky::csvNumber(value);
    const double reparsed = std::stod(serialized);

    if (std::bit_cast<std::uint64_t>(reparsed) == std::bit_cast<std::uint64_t>(expected)) {
        return true;
    }

    std::cerr << "Binary64 round trip failed: " << input << " -> " << serialized << '\n';
    return false;
}

bool expectRejected(const std::string &input) {
    try {
        (void) stonky::csvNumber(cpp_dec_float_50(input));
    } catch (const std::domain_error &) {
        return true;
    }
    std::cerr << "Out-of-range decimal was accepted: " << input << '\n';
    return false;
}

} // namespace

int main() {
    bool ok = true;

    // Decimal model values intentionally use the project's binary64 storage
    // contract. The written text must recover the exact same double.
    ok &= expectBinary64RoundTrip("12345678901234567890.12345678901234567890123456789");
    ok &= expectBinary64RoundTrip("0.00000000000000028101000000000000000000000000000001");
    ok &= expectBinary64RoundTrip("0.123456789012345678901234567890");
    ok &= expectBinary64RoundTrip("-9876543210987654.32");
    ok &= expectRejected("1e1000");
    ok &= expectRejected("1e-1000");

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
