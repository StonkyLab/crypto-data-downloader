#include "stonky/kraken/kraken_funding_archive.h"

#include <iostream>
#include <string>
#include <string_view>
#include <tuple>

// Three rows copied from Kraken's export (exports/PI_XBTUSD.csv, PF_LOOMUSD.csv,
// PF_XBTUSD.csv), deliberately out of order and with a CRLF and a blank line.
constexpr std::string_view SAMPLE =
    "timestamp,tradeable,absolute_rate,relative_rate\r\n"
    "2026-02-01 00:00:00,PF_XBTUSD,-0.664212646570953183,-0.000008445163888889\n"
    "\n"
    "2018-08-31 16:00:00,PI_XBTUSD,1.0327058177E-8,0.000071824070000000\n"
    "2024-09-05 14:00:00,PF_LOOMUSD,0E-18,0E-18\n";

int main() {
    const auto rates = stonky::kraken::FundingArchive::parseCsv(SAMPLE);
    bool ok = rates.size() == 3;
    if (ok) {
        ok = rates[0].time == 1535731200000LL && rates[0].relativeRate == std::stod("0.000071824070000000") &&
             rates[0].absoluteRate == std::stod("1.0327058177E-8") &&
             rates[1].time == 1725544800000LL && rates[1].relativeRate == 0.0 && rates[1].absoluteRate == 0.0 &&
             rates[2].time == 1769904000000LL && rates[2].relativeRate == std::stod("-0.000008445163888889") &&
             rates[2].absoluteRate == std::stod("-0.664212646570953183");
    }
    if (!ok) {
        std::cerr << "parsed " << rates.size() << " rows\n";
        for (const auto &rate: rates) {
            std::cerr << "  " << rate.time << ' ' << rate.relativeRate << ' ' << rate.absoluteRate << '\n';
        }
        return 1;
    }

    bool threw = false;
    try {
        std::ignore = stonky::kraken::FundingArchive::parseCsv("timestamp,tradeable,absolute_rate,relative_rate\n"
                                                               "2026-02-01 00:00:00,PF_XBTUSD,-0.66\n");
    } catch (const std::exception &) {
        threw = true;
    }
    if (!threw) {
        std::cerr << "a short row was accepted\n";
        return 1;
    }
    std::cout << "kraken funding archive: ok\n";
    return 0;
}
