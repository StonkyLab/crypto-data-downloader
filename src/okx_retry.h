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

/// Attempts of a request the venue rate-limits or a fault cuts: pauses of 1 s
/// doubling to at most 60 s, about two minutes in all, as the Bybit loops wait
/// since 2026-11-18. On 2026-11-20 OKX answered one market-data-history request
/// for MYRIA-USDT with 429/50011 four times within 5.5 s; four attempts with 3 s
/// of pauses ran out and the whole OKX spot step failed. Each request waits on
/// its own, so a venue down for hours makes a step run for hours before it fails.
constexpr int MAX_TRANSIENT_ATTEMPTS = 8;

/// The venue's state rather than its answer, so a later attempt of the same
/// request can be expected to clear it: a rate limit (HTTP 429, OKX 50011), a
/// gateway error (HTTP 5xx), a fault on the way (reset, timeout, lost DNS answer).
inline bool isTransientFailure(const std::string_view message) {
    constexpr std::string_view markers[] = {
        "Bad response, code 429", "Bad response, code 5", // API host (RESTClient checkResponse)
        "HTTP status: 429", "HTTP status: 5",             // archive file (HTTPSession::downloadBinary)
        "code: 50011", "\"code\":\"50011\"",              // OKX's rate-limit code, as an API error or a body
    };
    for (const auto marker: markers) {
        if (message.find(marker) != std::string_view::npos) {
            return true;
        }
    }
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
