/**
OKX request retry policy

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef SRC_OKX_RETRY_H
#define SRC_OKX_RETRY_H

#include "stonky/transient_error.h"
#include <algorithm>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <spdlog/spdlog.h>

namespace stonky::okx_retry {

/// Attempts of a request that keeps failing with anything but a transient failure.
constexpr int MAX_REQUEST_ATTEMPTS = 4;

/// Attempts of a request the venue rate-limits or a fault cuts, with pauses of
/// 1 s doubling to at most 60 s: about two minutes of pauses in all, as the Bybit
/// loops wait since 2026-11-18. On 2026-11-20 OKX answered one market-data-history
/// request for MYRIA-USDT with 429/50011 four times within 5.5 s; four attempts
/// with 3 s of pauses ran out and the whole OKX spot step failed. Each request
/// waits on its own and an attempt may itself wait for its connection, so a venue
/// down for hours makes a step run for hours before it fails.
constexpr int MAX_TRANSIENT_ATTEMPTS = 8;

/// OKX's codes that ask to try again later (docs-v5 error table, read 2026-11-21):
/// 50001 service temporarily unavailable (HTTP 503), 50004 endpoint request timeout
/// (400), 50011 rate limit (200 by the table, 429 in practice), 50013 systems busy
/// (429), 50026 system error (500). The execution gateway takes 50011, 50013 and
/// 50026 for throttling too.
constexpr std::string_view RETRY_LATER_CODES[] = {"50001", "50004", "50011", "50013", "50026"};

/// The venue's state rather than its answer to the request, so a later attempt can
/// be expected to clear it: a rate limit, a gateway error, a fault on the way.
inline bool isTransientFailure(const std::string_view message) {
    using namespace std::string_view_literals;

    // An OKX code in an HTTP 200 answer (handleOKXResponse): "OKX API error, code: 50013, msg: ...".
    if (constexpr auto prefix = "OKX API error, code: "sv; message.starts_with(prefix)) {
        const auto rest = message.substr(prefix.size());
        return std::ranges::any_of(RETRY_LATER_CODES, [&](const std::string_view code) {
            return rest.starts_with(code) && rest.substr(code.size()).starts_with(",");
        });
    }
    // Any other answer: its HTTP status or the OKX code in its body decides, never a
    // word of the body, which is the venue's text (a 403 page may well say setTimeout).
    for (const auto prefix: {"Bad response, code "sv,                      // API host (checkResponse)
                             "Failed to download file, HTTP status: "sv}) { // archive (downloadBinary)
        if (message.starts_with(prefix)) {
            const auto status = message.substr(prefix.size());
            return status.starts_with("429") || status.starts_with("5") ||
                   std::ranges::any_of(RETRY_LATER_CODES, [&](const std::string_view code) {
                       return message.find(std::string(R"("code":")").append(code).append(R"(")")) !=
                              std::string_view::npos;
                   });
        }
    }
    // No answer came back: a reset, a timeout, a lost DNS answer, a broken TLS session.
    return isTransientTransportError(message);
}

/// A delisted or unknown instrument: a final answer, never repeated.
inline bool isTerminal(const std::string_view message) {
    return message.find("code: 51001") != std::string_view::npos ||
           message.find("code: 51000") != std::string_view::npos;
}

/// Pause before the next attempt once attempt `attempt` (1-based) failed with
/// `message`, or nothing when the request has failed for good.
inline std::optional<std::chrono::milliseconds> pauseAfter(const int attempt, const std::string_view message) {
    if (isTerminal(message)) {
        return std::nullopt;
    }
    if (isTransientFailure(message)) {
        if (attempt >= MAX_TRANSIENT_ATTEMPTS) {
            return std::nullopt;
        }
        return std::chrono::milliseconds{std::min(1000LL << (attempt - 1), 60'000LL)};
    }
    if (attempt >= MAX_REQUEST_ATTEMPTS) {
        return std::nullopt;
    }
    return std::chrono::milliseconds{500LL * attempt};
}

/// Run a request with bounded retries. A transient failure must not turn into a
/// skipped archive file or a failed step: a skipped file becomes a permanent
/// hole, because the resume logic only ever appends after the last stored record.
template<typename Fn, typename Sleep>
auto withRetry(const std::string &what, Fn &&fn, Sleep &&sleep) -> decltype(fn()) {
    for (int attempt = 1;; ++attempt) {
        try {
            return fn();
        } catch (const std::exception &e) {
            const std::string error = e.what();
            if (isTerminal(error)) {
                throw;
            }
            const auto pause = pauseAfter(attempt, error);
            if (!pause) {
                throw std::runtime_error(fmt::format("{} failed after {} attempts: {}", what, attempt, error));
            }
            spdlog::warn("{}: attempt {} failed, next in {} ms: {}", what, attempt, pause->count(), error);
            sleep(*pause);
        }
    }
}

template<typename Fn>
auto withRetry(const std::string &what, Fn &&fn) -> decltype(fn()) {
    return withRetry(what, std::forward<Fn>(fn), [](const std::chrono::milliseconds pause) {
        std::this_thread::sleep_for(pause);
    });
}

} // namespace stonky::okx_retry

#endif // SRC_OKX_RETRY_H
