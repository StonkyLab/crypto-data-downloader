/**
Bounded asynchronous task helpers

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_FUTURE_UTILS_H
#define INCLUDE_STONKY_FUTURE_UTILS_H

#include "stonky/utils/semaphore.h"
#include <spdlog/spdlog.h>
#include <cstdint>
#include <exception>
#include <future>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

namespace stonky {

constexpr std::uint32_t normalizedJobCount(const std::uint32_t requested) {
    return requested == 0 ? 1 : requested;
}

constexpr std::uint32_t boundedJobCount(const std::uint32_t requested, const std::uint32_t venueCap) {
    const auto jobs = normalizedJobCount(requested);
    const auto cap = normalizedJobCount(venueCap);
    return jobs < cap ? jobs : cap;
}

/**
 * Acquire a launch slot before creating the std::async worker. Unlike placing
 * the semaphore inside the worker, this bounds the number of live OS threads,
 * not merely the number of threads doing useful work.
 */
template<typename F, typename... Args>
auto launchBounded(Semaphore &launchSlots, F &&function, Args &&...args)
    -> std::future<std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...> > {
    using Result = std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>;

    launchSlots.lock();
    try {
        return std::async(
            std::launch::async,
            [&launchSlots, fn = std::forward<F>(function),
             params = std::make_tuple(std::forward<Args>(args)...)]() mutable -> Result {
                struct SlotGuard {
                    Semaphore &slots;
                    ~SlotGuard() { slots.unlock(); }
                } guard{launchSlots};

                if constexpr (std::is_void_v<Result>) {
                    std::apply(std::move(fn), std::move(params));
                    return;
                } else {
                    return std::apply(std::move(fn), std::move(params));
                }
            });
    } catch (...) {
        launchSlots.unlock();
        throw;
    }
}

/** Wait for every task, retain all failures, then fail the enclosing command. */
template<typename T, typename OnSuccess>
std::vector<T> waitAllOrThrow(std::vector<std::future<T> > &futures, OnSuccess &&onSuccess) {
    std::vector<T> results;
    results.reserve(futures.size());
    std::vector<std::string> errors;

    for (auto &future: futures) {
        try {
            auto result = future.get();
            std::invoke(onSuccess, std::as_const(result));
            results.push_back(std::move(result));
        } catch (const std::exception &e) {
            errors.emplace_back(e.what());
        } catch (...) {
            errors.emplace_back("unknown worker exception");
        }
    }

    if (!errors.empty()) {
        std::ostringstream message;
        message << errors.size() << " worker task(s) failed";
        for (const auto &error: errors) {
            message << "\n - " << error;
        }
        throw std::runtime_error(message.str());
    }
    return results;
}

template<typename T>
std::vector<T> waitAllOrThrow(std::vector<std::future<T> > &futures) {
    return waitAllOrThrow(futures, [](const T &) {});
}

/** Remove duplicate work items while retaining the caller's order. */
template<typename T>
void deduplicatePreserveOrder(std::vector<T> &values) {
    std::unordered_set<T> seen;
    auto out = values.begin();
    for (auto current = values.begin(); current != values.end(); ++current) {
        if (seen.insert(*current).second) {
            // Avoid self-move: std::string is permitted to become empty after
            // `value = std::move(value)` (and libstdc++ does exactly that).
            if (out != current) {
                *out = std::move(*current);
            }
            ++out;
        }
    }
    values.erase(out, values.end());
}

/**
 * A symbol is unusable as a file-name component only when it could escape the
 * data directory or break the filesystem: empty, "." / "..", path separators,
 * ASCII control bytes, Windows-reserved punctuation, or an absurd length.
 * Anything else — including non-ASCII UTF-8 — is legitimate. An ASCII-only
 * whitelist here once aborted a whole Binance run over the perpetual
 * 币安人生USDT, whose files had been part of the dataset for years.
 */
inline bool isSafeSymbolFileComponent(const std::string_view symbol) {
    if (symbol.empty() || symbol == "." || symbol == "..") {
        return false;
    }
    if (symbol.size() > 240) {
        return false; // leaves room for suffixes within the usual 255-byte file name limit
    }
    for (const unsigned char c: symbol) {
        if (c < 0x20 || c == 0x7f) {
            return false; // control bytes
        }
        switch (c) {
            case '/':
            case '\\':
            case '<':
            case '>':
            case ':':
            case '"':
            case '|':
            case '?':
            case '*':
                return false; // path separators and Windows-reserved punctuation
            default:
                break;
        }
    }
    return true;
}

/**
 * Drop symbols unusable as file names, logging each. One hostile or broken
 * entry must not abort the run for the hundreds of valid symbols around it —
 * a skipped symbol is never used to build a path, which is all the protection
 * the caller needs. The one exception: filtering a non-empty list down to
 * nothing throws, because an empty symbol list means "the whole exchange"
 * further down, and that silent flip would be worse than stopping.
 */
inline std::size_t removeUnsafeSymbolFileComponents(std::vector<std::string> &symbols) {
    const bool hadSymbols = !symbols.empty();
    const auto removed = std::erase_if(symbols, [](const std::string &symbol) {
        if (isSafeSymbolFileComponent(symbol)) {
            return false;
        }
        spdlog::error("Skipping exchange symbol '{}': not usable as a file name component", symbol);
        return true;
    });
    if (hadSymbols && symbols.empty()) {
        throw std::invalid_argument("every requested symbol was rejected as unsafe for file names");
    }
    return removed;
}

} // namespace stonky

#endif // INCLUDE_STONKY_FUTURE_UTILS_H
