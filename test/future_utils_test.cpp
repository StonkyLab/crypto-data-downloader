#include "stonky/future_utils.h"

#include <iostream>
#include <string>
#include <vector>

int main() {
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

    try {
        stonky::validateSymbolFileComponents({"BTCUSDT", "BTC-USD-SWAP", "ETH_USDT"});
    } catch (const std::exception &error) {
        std::cerr << "Safe exchange symbol was rejected: " << error.what() << '\n';
        return 1;
    }
    for (const std::string invalid: {"../BTC", "BTC/USDT", "/tmp/owned", ".", ""}) {
        try {
            stonky::validateSymbolFileComponents({invalid});
            std::cerr << "Unsafe symbol was accepted: " << invalid << '\n';
            return 1;
        } catch (const std::invalid_argument &) {
            // Expected.
        }
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
