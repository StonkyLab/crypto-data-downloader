#include "okx_retry.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#ifndef OKX_DOWNLOADER_CPP
#error "OKX_DOWNLOADER_CPP must name src/okx_downloader.cpp (CMakeLists.txt sets it)"
#endif

using namespace std::chrono_literals;
using Pauses = std::vector<std::chrono::milliseconds>;

namespace {
bool ok = true;

void check(const bool condition, const std::string_view what) {
    if (!condition) {
        std::cerr << "FAIL: " << what << '\n';
        ok = false;
    }
}

struct Outcome {
    int calls = 0;
    Pauses pauses;
    bool succeeded = false;
    std::string error;
};

/// A request that fails with `error` on its first `failures` calls and succeeds after.
/// The pauses are recorded, never slept.
Outcome request(const std::string &error, const int failures) {
    Outcome outcome;
    try {
        const int value = stonky::okx_retry::withRetry(
            "request", [&] {
                if (++outcome.calls <= failures) {
                    throw std::runtime_error(error);
                }
                return 42;
            }, [&](const std::chrono::milliseconds pause) { outcome.pauses.push_back(pause); });
        outcome.succeeded = value == 42;
    } catch (const std::exception &e) {
        outcome.error = e.what();
    }
    return outcome;
}
} // namespace

int main() {
    // Verbatim from the downloader log, 2026-11-20 20:24:50 UTC: the market-data-history
    // listing of MYRIA-USDT that failed the OKX spot step after four such answers.
    const std::string rateLimited = R"(Bad response, code 429, msg: {"msg":"Too Many Requests","code":"50011"})";

    {
        // The incident: four 429 answers, then the venue answers. The old policy gave up
        // after the fourth.
        const auto o = request(rateLimited, 4);
        check(o.succeeded && o.calls == 5, "four 429 answers in a row are waited out");
        check(o.pauses == Pauses{1s, 2s, 4s, 8s}, "a rate limit doubles the pause from 1 s");
    }
    {
        // A rate limit that does not lift: eight attempts, pauses capped at 60 s, about
        // two minutes of pauses, then the failure says how many attempts it made.
        const auto o = request(rateLimited, 100);
        check(!o.succeeded && o.calls == 8, "a lasting rate limit ends after eight attempts");
        check(o.pauses == Pauses{1s, 2s, 4s, 8s, 16s, 32s, 60s}, "no pause is longer than 60 s");
        check(o.error == "request failed after 8 attempts: " + rateLimited, "the failure names the attempts");
    }

    // The same class on every OKX request: the API host's and the archive's statuses,
    // OKX's try-again-later codes in an HTTP 200 envelope or in a body (texts of the
    // docs-v5 error table), a fault on the way. Each one is waited out.
    for (const std::string transient: {
             std::string("Bad response, code 429, msg: <html><body>Too Many Requests</body></html>"),
             std::string(R"(Bad response, code 400, msg: {"msg":"Too Many Requests","code":"50011"})"),
             std::string(R"(Bad response, code 400, msg: {"msg":"API endpoint request timeout. (does not mean that the request was successful or failed, please check the request result).","code":"50004"})"),
             std::string("Bad response, code 502, msg: <html>Bad Gateway</html>"),
             std::string("OKX API error, code: 50011, msg: Rate limit reached. Please refer to API documentation and throttle requests accordingly."),
             std::string("OKX API error, code: 50001, msg: Service temporarily unavailable. Please try again later."),
             std::string("OKX API error, code: 50013, msg: Systems are busy. Please try again later."),
             std::string("OKX API error, code: 50026, msg: System error. Try again later"),
             std::string("Failed to download file, HTTP status: 429"),
             std::string("Failed to download file, HTTP status: 503"),
             std::string("Connection reset by peer [system:104]"),
             std::string("end of stream [beast.http:1]"),
         }) {
        const auto o = request(transient, 6);
        check(o.succeeded && o.calls == 7, "waited out: " + transient);
    }

    // Anything else keeps the short budget: four attempts, 3 s of pauses. A run that
    // OKX refuses on every call (its range limits once changed, 50077) fails quickly.
    // A body's words never decide (a 403 page with a script), digits of a timestamp or
    // a longer code are not OKX's code, and a refused certificate stays refused.
    for (const std::string other: {
             std::string("OKX API error, code: 50077, msg: range too long, end=1750011200000"),
             std::string("OKX API error, code: 500110, msg: a code that only starts with 50011"),
             std::string(R"(Bad response, code 400, msg: {"msg":"Parameter end error","code":"50014"})"),
             std::string("Bad response, code 403, msg: <html><script>setTimeout(function(){location.reload()},5000)</script></html>"),
             std::string("Failed to download file, HTTP status: 404"),
             std::string("handshake: certificate verify failed (SSL routines) [asio.ssl:167772294]"),
         }) {
        const auto o = request(other, 100);
        check(!o.succeeded && o.calls == 4, "four attempts for: " + other);
        check(o.pauses == Pauses{500ms, 1000ms, 1500ms}, "short pauses for: " + other);
    }

    // A delisted or unknown instrument (51001) and a parameter OKX rejects (51000) are
    // final: thrown at once, as they came.
    for (const std::string terminal: {
             std::string("OKX API error, code: 51001, msg: Instrument ID, Instrument ID code, or Spread ID doesn't exist."),
             std::string("OKX API error, code: 51000, msg: Parameter instId error"),
         }) {
        const auto o = request(terminal, 100);
        check(o.calls == 1 && o.pauses.empty() && o.error == terminal, "not repeated: " + terminal);
    }

    {
        // Every OKX request in the downloader goes through withRetry: instruments, REST
        // candles and REST funding rates had no retry at all. A request line without a
        // withRetry( within the five lines above it is a bare call.
        std::ifstream source(OKX_DOWNLOADER_CPP);
        check(source.is_open(), "okx_downloader.cpp is readable");
        std::vector<std::string> lines;
        for (std::string line; std::getline(source, line);) {
            lines.push_back(line);
        }
        int requests = 0;
        for (std::size_t i = 0; i < lines.size(); ++i) {
            const auto &line = lines[i];
            if (line.find("okxClient->") == std::string::npos && line.find("client.get") == std::string::npos &&
                line.find("downloadMarketDataFile(") == std::string::npos) {
                continue;
            }
            ++requests;
            bool wrapped = false;
            for (std::size_t j = i >= 5 ? i - 5 : 0; j <= i; ++j) {
                wrapped = wrapped || lines[j].find("withRetry(") != std::string::npos;
            }
            check(wrapped, "OKX request without withRetry at okx_downloader.cpp:" + std::to_string(i + 1));
        }
        check(requests == 9, "nine OKX request sites, found " + std::to_string(requests));
    }

    if (ok) {
        std::cout << "okx_retry_test passed\n";
    }
    return ok ? 0 : 1;
}
