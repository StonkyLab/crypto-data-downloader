#include "okx_retry.h"

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

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
        // two minutes in all, then the failure says how many attempts it made.
        const auto o = request(rateLimited, 100);
        check(!o.succeeded && o.calls == 8, "a lasting rate limit ends after eight attempts");
        check(o.pauses == Pauses{1s, 2s, 4s, 8s, 16s, 32s, 60s}, "no pause is longer than 60 s");
        check(o.error == "request failed after 8 attempts: " + rateLimited, "the failure names the attempts");
    }

    // The same class on every OKX request: archive files, the gateway, OKX's own code,
    // a fault on the way. Each one is waited out like the rate limit.
    for (const std::string transient: {
             std::string("OKX API error, code: 50011, msg: Too Many Requests"),
             std::string("Failed to download file, HTTP status: 429"),
             std::string("Failed to download file, HTTP status: 503"),
             std::string("Bad response, code 502, msg: <html>Bad Gateway</html>"),
             std::string("Connection reset by peer [system:104]"),
             std::string("end of stream [beast.http:1]"),
         }) {
        const auto o = request(transient, 6);
        check(o.succeeded && o.calls == 7, "waited out: " + transient);
    }

    // Anything else keeps the short budget: four attempts, 3 s of pauses. A run that
    // OKX refuses on every call (its range limits once changed, 50077) fails quickly.
    for (const std::string other: {
             std::string("OKX API error, code: 50077, msg: range too long, end=1750011200000"),
             std::string("Bad response, code 400, msg: {\"msg\":\"Parameter end error\",\"code\":\"50014\"}"),
             std::string("Failed to download file, HTTP status: 404"),
         }) {
        const auto o = request(other, 100);
        check(!o.succeeded && o.calls == 4, "four attempts for: " + other);
        check(o.pauses == Pauses{500ms, 1000ms, 1500ms}, "short pauses for: " + other);
    }

    {
        // A delisted or unknown instrument is final: thrown at once, as it came.
        const std::string unknown =
            "OKX API error, code: 51001, msg: Instrument ID, Instrument ID code, or Spread ID doesn't exist.";
        const auto o = request(unknown, 100);
        check(o.calls == 1 && o.pauses.empty() && o.error == unknown, "51001 is not repeated");
    }

    if (ok) {
        std::cout << "okx_retry_test passed\n";
    }
    return ok ? 0 : 1;
}
