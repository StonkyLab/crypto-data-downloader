#include "stonky/csv_format.h"
#include "stonky/mexc/mexc_models.h"

#include <bit>
#include <cstdint>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>

namespace {

bool sameBinary64Value(const boost::multiprecision::cpp_dec_float_50 &parsed,
                       const std::string &source) {
    const double expected = std::stod(source);
    const double modelValue = parsed.convert_to<double>();
    const double csvValue = std::stod(stonky::csvNumber(parsed));
    return std::bit_cast<std::uint64_t>(modelValue) == std::bit_cast<std::uint64_t>(expected) &&
           std::bit_cast<std::uint64_t>(csvValue) == std::bit_cast<std::uint64_t>(expected);
}

} // namespace

int main() {
    const auto payload = nlohmann::json::parse(R"({
        "success": true,
        "code": 0,
        "data": {
            "time": [1700000000, 1700000060],
            "open": [0.0000028101, "0.123456789012345678901234567890"],
            "high": [0.0000029, "0.223456789012345678901234567890"],
            "low": [0.0000027, "0.023456789012345678901234567890"],
            "close": [0.00000285, "0.133456789012345678901234567890"],
            "vol": [1234567.89, "1234567890123456.78"],
            "amount": [3.456789, "9876543210987654.32"]
        }
    })");

    stonky::mexc::futures::Candles response;
    response.fromJson(payload);
    if (response.candles.size() != 2) {
        std::cerr << "MEXC candle fixture did not parse\n";
        return 1;
    }
    if (stonky::csvNumber(response.candles[0].open) != "2.8101e-06") {
        std::cerr << "Small numeric MEXC price was rounded through six decimal places\n";
        return 1;
    }
    if (!sameBinary64Value(response.candles[1].open,
                           "0.123456789012345678901234567890") ||
        !sameBinary64Value(response.candles[1].volume, "1234567890123456.78")) {
        std::cerr << "String-valued MEXC decimal changed its binary64 market value\n";
        return 1;
    }

    return 0;
}
