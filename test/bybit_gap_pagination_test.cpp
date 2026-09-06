#include "stonky/bybit/bybit_http_session.h"
#include "stonky/bybit/bybit_rest_client.h"

#include <nlohmann/json.hpp>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace {
constexpr std::int64_t minute = 60'000;
constexpr std::int64_t day = 1440 * minute;
constexpr std::int64_t tail = 1'700'000'040'000;
std::vector<std::int64_t> history;
int probes = 0;
int failOnProbe = 0;
std::int64_t lastProbeEnd = 0;

void require(const bool condition, const char *message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}
} // namespace

// Link this offline transport instead of HTTPSession's implementation. The
// production RESTClient still builds/parses requests and performs all pagination.
namespace stonky::bybit {
struct HTTPSession::P {};
HTTPSession::HTTPSession(const std::string &, const std::string &, const std::string &)
    : m_p(std::make_unique<P>()) {}
HTTPSession::~HTTPSession() = default;
std::int64_t HTTPSession::lastSuccessfulResponseMs() const { return 0; }
void HTTPSession::setRequestTimeout(int) const {}

http::response<http::string_body> HTTPSession::post(const std::string &, const nlohmann::json &) const {
    throw std::runtime_error("Unexpected POST in candle pagination");
}

http::response<http::string_body> HTTPSession::get(
    const std::string &path, const std::map<std::string, std::string> &parameters) const {
    require(path == "/v5/market/kline", "Unexpected endpoint");
    const auto start = std::stoll(parameters.at("start"));
    const auto limit = parameters.contains("limit") ? std::stoi(parameters.at("limit")) : 200;
    auto first = std::lower_bound(history.begin(), history.end(), start);
    auto last = first;
    if (parameters.contains("end")) {
        const auto end = std::stoll(parameters.at("end"));
        require(start <= end, "Gap probe moved backwards");
        lastProbeEnd = end;
        if (++probes == failOnProbe) {
            throw TransportError("simulated reset during gap probe");
        }
        // Bybit anchors at end, returning its newest candles <= end, even if
        // they all predate start. It does not fabricate candles inside the gap.
        last = std::upper_bound(history.begin(), history.end(), end);
        first = last - std::min<std::ptrdiff_t>(limit, last - history.begin());
    } else if (first != history.end() && *first == start) {
        last = std::upper_bound(history.begin(), history.end(), start + (limit - 1) * minute);
    } else {
        // A start inside the relisting gap returns the old, pre-gap window.
        last = first;
        first = last - std::min<std::ptrdiff_t>(limit, last - history.begin());
    }

    auto list = nlohmann::json::array();
    while (last != first) {
        list.push_back({std::to_string(*--last), "1", "1", "1", "1", "1", "1"});
    }
    http::response<http::string_body> response{http::status::ok, 11};
    // No wall-clock rate-limit sleeps in this offline test.
    response.set("X-Bapi-Limit-Status", "100");
    response.set("X-Bapi-Limit-Reset", "0");
    response.body() = nlohmann::json{
        {"retCode", 0}, {"retMsg", "OK"}, {"time", tail},
        {"result", {{"category", parameters.at("category")},
                    {"symbol", parameters.at("symbol")}, {"list", list}}}}.dump();
    response.prepare_payload();
    return response;
}
} // namespace stonky::bybit

int main() {
    using namespace stonky::bybit;
    try {
        RESTClient client("", "");
        for (const int gapDays : {14, 300}) {
            history = {tail - minute, tail};
            for (int i = 0; i < 500; ++i) {
                history.push_back(tail + gapDays * day + i * minute);
            }
            probes = 0;
            std::vector<std::int64_t> written;
            const auto candles = client.getHistoricalPrices(
                Category::spot, "RELISTUSDT", CandleInterval::_1, tail, history.back() + minute, 200,
                [&written](const std::vector<Candle> &batch) {
                    for (const auto &candle : batch) {
                        written.push_back(candle.startTime);
                    }
                });
            const std::vector<std::int64_t> expected(history.begin() + 1, history.end());
            std::vector<std::int64_t> returned;
            for (const auto &candle : candles) {
                returned.push_back(candle.startTime);
            }
            require(returned == expected && written == expected,
                    "Relisting gap truncated, skipped or duplicated candles");
            require(gapDays < 300 || probes > 2000, "Long-gap case did not cross the old cap");
        }

        history = {tail - minute, tail};
        probes = 0;
        const auto end = tail + 300 * day;
        const auto absent = client.getHistoricalPrices(
            Category::linear, "DELISTUSDT", CandleInterval::_1, tail + minute, end);
        require(absent.empty() && probes > 2000 && lastProbeEnd == end,
                "Empty history did not stop exactly at the requested end");

        probes = 0;
        failOnProbe = 2100;
        bool failed = false;
        try {
            (void) client.getHistoricalPrices(
                Category::spot, "RELISTUSDT", CandleInterval::_1, tail + minute, end);
        } catch (const TransportError &) {
            failed = true;
        }
        require(failed, "Failed gap probe was treated as successful completion");
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
    return 0;
}
