#include "stonky/transient_error.h"

#include <iostream>
#include <string_view>

int main() {
    bool ok = true;
    // Verbatim messages from the logs that ended a symbol on its first attempt.
    constexpr std::string_view transient[] = {
        "Transport failure for /v5/market/kline?category=spot&interval=1&start=1783281540000&symbol=MEWUSDT: Connection reset by peer [system:104 at /usr/local/include/boost/asio/detail/socket_ops.ipp]",
        "resolve: Host not found (non-authoritative), try again later [asio.netdb:2 at /usr/local/include/boost/asio/detail/resolver_service.hpp:83:5]",
        "handshake: Connection reset by peer [system:104]",
        "Connection reset by peer [system:104]",
        "Transport failure for /fapi/v1/klines: The socket was closed due to a timeout [asio.misc:...] timed out",
        "handshake: tlsv1 alert internal error (SSL routines) [asio.ssl:...]",
    };
    // Venue answers: repeating the request cannot change them.
    constexpr std::string_view permanent[] = {
        "Bybit API error, code: 10001, msg: Not supported symbols",
        "Bybit API error, code: 10016, msg: internal error",
        "Bad response, code 400, msg: {\"msg\":\"Invalid symbol.\",\"code\":-1121,\"_extend\":null}",
        "CSV write failed for symbol: BTCUSDT",
        "MEXC Spot returned no candles for a non-empty range",
        "Old data format for symbol BTCUSDT, delete file before retrying",
    };
    for (const auto m: transient) {
        if (!stonky::isTransientTransportError(m)) {
            std::cerr << "Not classified as transient: " << m << '\n';
            ok = false;
        }
    }
    for (const auto m: permanent) {
        if (stonky::isTransientTransportError(m)) {
            std::cerr << "Venue rejection classified as transient: " << m << '\n';
            ok = false;
        }
    }
    return ok ? 0 : 1;
}
