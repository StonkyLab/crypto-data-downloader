#include "stonky/future_utils.h"

#include <iostream>
#include <string>
#include <vector>

bool checkRetryTransient() {
    int calls = 0;
    const auto flaky = stonky::retryTransient([&calls](const int x) {
        if (++calls == 1) {
            throw std::runtime_error("ROBINCATUSDT: handshake: tlsv1 alert internal error (SSL routines)");
        }
        return x * 2;
    }, 3, std::chrono::milliseconds(1));
    if (flaky(21) != 42 || calls != 2) {
        std::cerr << "A transient fault was not retried\n";
        return false;
    }
    calls = 0;
    const auto rejected = stonky::retryTransient([&calls](const int) -> int {
        ++calls;
        throw std::runtime_error("Bad HTTP response: 400, API Code: -1121, message: Invalid symbol.");
    }, 3, std::chrono::milliseconds(1));
    try {
        rejected(1);
    } catch (const std::exception &) {
        if (calls == 1) {
            return true;
        }
    }
    std::cerr << "A venue rejection was retried or swallowed\n";
    return false;
}

int main() {
    if (!checkRetryTransient()) {
        return 1;
    }
    std::vector<std::string> unique{"BTCUSDT", "ETHUSDT", "SOLUSDT"};
    stonky::deduplicatePreserveOrder(unique);
    if (unique != std::vector<std::string>{"BTCUSDT", "ETHUSDT", "SOLUSDT"}) {
        std::cerr << "Deduplication corrupted an already unique symbol list\n";
        return 1;
    }

    std::vector<std::string> repeated{"BTCUSDT", "ETHUSDT", "BTCUSDT", "SOLUSDT", "ETHUSDT"};
    stonky::deduplicatePreserveOrder(repeated);
    if (repeated != std::vector<std::string>{"BTCUSDT", "ETHUSDT", "SOLUSDT"}) {
        std::cerr << "Deduplication did not preserve first-occurrence order\n";
        return 1;
    }

    // Non-ASCII symbols are real: Binance lists perpetuals such as 币安人生USDT.
    std::vector<std::string> safe{"BTCUSDT", "BTC-USD-SWAP", "ETH_USDT", "币安人生USDT", "10000SATSUSDT"};
    if (stonky::removeUnsafeSymbolFileComponents(safe) != 0 || safe.size() != 5) {
        std::cerr << "A safe exchange symbol was rejected\n";
        return 1;
    }
    // A list reduced to nothing must throw: an empty list means "the whole
    // exchange" downstream, and that silent flip would be worse than stopping.
    for (const std::string invalid: {"../BTC", "BTC/USDT", "/tmp/owned", ".", "..", ""}) {
        std::vector<std::string> lone{invalid};
        try {
            stonky::removeUnsafeSymbolFileComponents(lone);
            std::cerr << "Unsafe symbol was accepted: " << invalid << '\n';
            return 1;
        } catch (const std::invalid_argument &) {
            // Expected.
        }
    }
    // One bad entry must drop only itself, never abort the surrounding run.
    std::vector<std::string> mixed{"BTCUSDT", "../evil", "龙虾USDT"};
    if (stonky::removeUnsafeSymbolFileComponents(mixed) != 1 ||
        mixed != std::vector<std::string>{"BTCUSDT", "龙虾USDT"}) {
        std::cerr << "Filtering a mixed list did not keep exactly the safe symbols\n";
        return 1;
    }

    Semaphore slots{2};
    std::vector<std::future<int>> futures;
    futures.push_back(stonky::launchBounded(slots, [] { return 1; }));
    futures.push_back(stonky::launchBounded(slots, []() -> int {
        throw std::runtime_error("expected worker failure");
    }));
    futures.push_back(stonky::launchBounded(slots, [] { return 3; }));
    int successfulCallbackSum = 0;
    try {
        (void) stonky::waitAllOrThrow(futures, [&successfulCallbackSum](const int result) {
            successfulCallbackSum += result;
        });
        std::cerr << "Aggregated worker failure was not propagated\n";
        return 1;
    } catch (const std::runtime_error &) {
        if (successfulCallbackSum != 4) {
            std::cerr << "Successful workers were not reported when a peer failed\n";
            return 1;
        }
    }

    return 0;
}
