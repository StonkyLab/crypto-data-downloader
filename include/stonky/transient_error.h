/**
Transient transport error classification

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_TRANSIENT_ERROR_H
#define INCLUDE_STONKY_TRANSIENT_ERROR_H

#include <string_view>

namespace stonky {

/**
 * True for a failure that a later attempt of the same request can be expected
 * to clear: a connection the peer reset, a DNS answer that did not arrive, a
 * TLS handshake that broke, a socket that timed out.
 *
 * The per-symbol retry loops used to retry only rate-limit rejections, so every
 * network fault ended the symbol on its first attempt and left the CSV cut at
 * that point: Bybit reset six symbols mid-1m-history, one HL run lost 73
 * symbols to "Host not found", Lighter three. A retry is safe because every
 * loop re-reads the CSV tail before each attempt and resumes from it.
 *
 * A venue's own rejection (unknown symbol, bad parameter, "not supported") is
 * deliberately not matched: repeating it cannot change the answer.
 */
inline bool isTransientTransportError(const std::string_view message) {
    constexpr std::string_view markers[] = {
        "Transport failure",       // Bybit/Binance async transport wrapper
        "Connection reset",        // ECONNRESET, with or without "by peer"
        "Broken pipe",             // EPIPE on write
        "timed out",               // deadline / ETIMEDOUT
        "Timeout",                 // boost/beast spelling
        "Host not found",          // resolver, transient variant included
        "Temporary failure in name resolution",
        "handshake:",              // TLS handshake interrupted
        "End of file",             // peer closed mid-response
        "Operation canceled",      // cancelled by a deadline timer
        "Connection refused",      // venue edge briefly unavailable
        "Network is unreachable",
        "asio.",                   // any other boost.asio category error
    };
    for (const auto marker: markers) {
        if (message.find(marker) != std::string_view::npos) {
            return true;
        }
    }
    return false;
}

} // namespace stonky

#endif // INCLUDE_STONKY_TRANSIENT_ERROR_H
