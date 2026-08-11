#include "stonky/since_parser.h"

#include <cstdint>
#include <iostream>
#include <string_view>

namespace {

constexpr std::int64_t NOW_MS = 1893456000000; // 2030-01-01T00:00:00Z

bool rejects(const std::string_view input) {
    try {
        (void) stonky::parseSinceMs(input, NOW_MS);
        return false;
    } catch (const stonky::SinceParseError &) {
        return true;
    }
}

} // namespace

int main() {
    bool ok = true;

    const auto expectValue = [&ok](const std::string_view input, const std::int64_t expected) {
        try {
            const auto actual = stonky::parseSinceMs(input, NOW_MS);
            if (actual != expected) {
                std::cerr << input << " parsed as " << actual << ", expected " << expected << '\n';
                ok = false;
            }
        } catch (const std::exception &error) {
            std::cerr << input << " was unexpectedly rejected: " << error.what() << '\n';
            ok = false;
        }
    };

    expectValue("2024-02-29", 1709164800000);
    expectValue("1970-01-02", 86400000);
    expectValue("1709164800000", 1709164800000);
    expectValue("1", 1);
    expectValue("1893456000000", NOW_MS);

    constexpr std::string_view invalidValues[] = {
        "",
        " ",
        " 1709164800000",
        "+1709164800000",
        "-1709164800000",
        "1709164800000junk",
        "2026-01-01junk",
        "2026-1-01",
        "2026-01-1",
        "2026/01/01",
        "2026-00-01",
        "2026-13-01",
        "2026-01-00",
        "2026-01-32",
        "2026-02-29",
        "2026-02-31",
        "2024-02-30",
        "1970-01-01",
        "0",
        "9223372036854775808",
        "2030-01-02",
        "1893456000001"
    };

    for (const auto input: invalidValues) {
        if (!rejects(input)) {
            std::cerr << "Invalid value was unexpectedly accepted: " << input << '\n';
            ok = false;
        }
    }

    return ok ? 0 : 1;
}
