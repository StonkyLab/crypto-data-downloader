/**
Kraken Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/kraken/kraken_downloader.h"
#include "stonky/kraken_trades_aggregator.h"
#include "stonky/history_floor.h"
#include "stonky/atomic_file.h"
#include "stonky/csv_data.h"
#include "stonky/csv_format.h"
#include "stonky/download_resume.h"
#include "stonky/transient_error.h"
#include "stonky/downloader.h"
#include "stonky/future_utils.h"
#include "stonky/kraken/kraken_rest_client.h"
#include "stonky/kraken/kraken_funding_archive.h"
#include "stonky/kraken/kraken.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/semaphore.h"
#include "csv.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <future>
#include <optional>
#include <ranges>
#include <set>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/ranges.h>
#include <thread>
#include <tuple>

using namespace stonky::kraken;

namespace stonky {
struct KrakenDownloader::P {
    std::unique_ptr<RESTClient> client;
    MarketCategory category;
    mutable Semaphore maxConcurrentConvertJobs;
    Semaphore maxConcurrentDownloadJobs;
    bool deleteDelistedData = false;

    /// A symbol the venue lists for the selected market, delisted ones flagged.
    struct Listed {
        std::string symbol;
        bool delisted;
    };

    explicit P(const std::uint32_t maxJobs, const MarketCategory category, const bool deleteDelistedData)
        : client(std::make_unique<RESTClient>()),
          category(category),
          maxConcurrentConvertJobs(normalizedJobCount(maxJobs)),
          // Spot public calls share one ~1 req/s budget per IP, so a second
          // worker would only queue on the client's throttle. The futures
          // endpoints carry no documented cost; four workers keep the charts
          // API well inside anything its edge has pushed back on.
          maxConcurrentDownloadJobs(boundedJobCount(maxJobs, category == MarketCategory::Spot ? 1 : 4)),
          deleteDelistedData(deleteDelistedData) {
    }

    [[nodiscard]] const char *csvDir() const {
        return category == MarketCategory::Spot ? CSV_SPOT_DIR : CSV_FUT_DIR;
    }

    [[nodiscard]] const char *t6Dir() const {
        return category == MarketCategory::Spot ? T6_SPOT_DIR : T6_FUT_DIR;
    }

    [[nodiscard]] std::vector<Listed> listSymbols() const;

    [[nodiscard]] std::pair<std::vector<std::string>, std::vector<std::string> > selectSymbols(
        const std::vector<Listed> &listed, const std::vector<std::string> &requested,
        const std::filesystem::path &filesDir, const std::string &fileSuffix) const;

    static DownloadResume checkSymbolCSVFile(const std::string &path, CsvData::TailCheck *tailOut = nullptr);

    static DownloadResume checkFundingRatesCSVFile(const std::string &path);

    static bool writeCandlesToCSVFile(const std::vector<kraken::Candle> &candles, const std::string &path,
                                      DownloadResume resume);

    static bool writeFundingRatesToCSVFile(const std::vector<kraken::FundingRate> &rates, const std::string &path,
                                           DownloadResume resume);

    static bool readCandlesFromCSVFile(const std::string &path, std::vector<kraken::Candle> &candles);

    static bool writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path,
                                             stonky::CandleInterval interval);

    void convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths, const std::string &outDirPath,
                            stonky::CandleInterval interval) const;

    void downloadFuturesCandles(const std::string &symbol, const std::string &csvPath,
                                kraken::CandleInterval interval, bool delisted) const;

    void downloadSpotCandles(const std::string &symbol, const std::string &csvPath) const;

    /**
     * Seed a file that starts from nothing with the venue's own export, then
     * append the REST window. `venueListed` false means the contract exists
     * only in the archive: the REST endpoint rejects its symbol.
     */
    void downloadFundingRates(const std::string &symbol, const std::string &csvPath,
                              const FundingArchive *archive, bool venueListed) const;

    /**
     * Run `fn` up to five times. A rate-limit answer and a transient transport
     * fault are retried with backoff; `fn` re-reads the CSV tail when it
     * starts, so a retry resumes where the previous attempt was cut.
     */
    template<typename Fn>
    static void withRetries(const std::string &symbol, const char *what, Fn &&fn) {
        constexpr int maxRetries = 5;
        for (int attempt = 0; attempt < maxRetries; ++attempt) {
            try {
                fn();
                return;
            } catch (const std::exception &e) {
                const std::string errMsg = e.what();
                const bool rateLimited = errMsg.find("Too many requests") != std::string::npos ||
                                         errMsg.find("Rate limit") != std::string::npos ||
                                         errMsg.find("429") != std::string::npos;
                if (rateLimited && attempt < maxRetries - 1) {
                    const int waitMs = 2000 * (1 << attempt);
                    spdlog::warn(fmt::format("Rate limit for symbol: {}, retry {}/{} in {} ms: {}",
                                             symbol, attempt + 1, maxRetries - 1, waitMs, errMsg));
                    std::this_thread::sleep_for(std::chrono::milliseconds(waitMs));
                } else if (isTransientTransportError(errMsg) && attempt < maxRetries - 1) {
                    const int waitMs = 2000 * (1 << attempt);
                    spdlog::warn(fmt::format("Transient network error for symbol: {}, retry {}/{} in {} ms: {}",
                                             symbol, attempt + 1, maxRetries - 1, waitMs, errMsg));
                    std::this_thread::sleep_for(std::chrono::milliseconds(waitMs));
                } else {
                    throw std::runtime_error(fmt::format("Updating {} for symbol {} failed (attempt {}/{}): {}",
                                                         what, symbol, attempt + 1, maxRetries, errMsg));
                }
            }
        }
    }
};

KrakenDownloader::KrakenDownloader(const std::uint32_t maxJobs, const MarketCategory marketCategory,
                                   const bool deleteDelistedData)
    : m_p(std::make_unique<P>(maxJobs, marketCategory, deleteDelistedData)) {
}

KrakenDownloader::~KrakenDownloader() = default;

std::vector<KrakenDownloader::P::Listed> KrakenDownloader::P::listSymbols() const {
    std::vector<Listed> listed;
    if (category == MarketCategory::Spot) {
        // altname ("XBTUSD") is the name every public endpoint accepts and the
        // one people use; the primary name ("XXBTZUSD") stays inside the API.
        // Pairs the venue removed are simply not listed.
        for (const auto &pair: client->getAssetPairs()) {
            listed.push_back({pair.altname, false});
        }
    } else {
        for (const auto &instrument: client->getInstruments(true)) {
            if (instrument.perpetual) {
                listed.push_back({instrument.symbol, instrument.isDelisted});
            }
        }
    }
    return listed;
}

std::pair<std::vector<std::string>, std::vector<std::string> > KrakenDownloader::P::selectSymbols(
    const std::vector<Listed> &listed, const std::vector<std::string> &requested,
    const std::filesystem::path &filesDir, const std::string &fileSuffix) const {
    std::vector<std::string> symbolsToUpdate;
    std::vector<std::string> symbolsToDelete;

    std::set<std::string> known;
    for (const auto &entry: listed) {
        known.insert(entry.symbol);
    }

    if (requested.empty()) {
        for (const auto &entry: listed) {
            if (entry.delisted && deleteDelistedData) {
                symbolsToDelete.push_back(entry.symbol);
            } else {
                symbolsToUpdate.push_back(entry.symbol);
            }
        }
        if (deleteDelistedData && std::filesystem::exists(filesDir)) {
            for (const auto &file: std::filesystem::directory_iterator(filesDir)) {
                if (!file.is_regular_file() || file.path().extension() != ".csv") {
                    continue;
                }
                auto stem = file.path().stem().string();
                if (!fileSuffix.empty() && stem.ends_with(fileSuffix)) {
                    stem.resize(stem.size() - fileSuffix.size());
                }
                if (!known.contains(stem)) {
                    symbolsToDelete.push_back(stem);
                }
            }
        }
    } else {
        for (const auto &symbol: requested) {
            const auto it = std::ranges::find_if(listed, [&symbol](const Listed &entry) {
                return entry.symbol == symbol;
            });
            if (it == listed.end()) {
                if (deleteDelistedData) {
                    symbolsToDelete.push_back(symbol);
                }
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", symbol));
            } else if (it->delisted && deleteDelistedData) {
                symbolsToDelete.push_back(symbol);
            } else {
                symbolsToUpdate.push_back(symbol);
            }
        }
    }

    deduplicatePreserveOrder(symbolsToUpdate);
    removeUnsafeSymbolFileComponents(symbolsToUpdate);
    return {symbolsToUpdate, symbolsToDelete};
}

DownloadResume KrakenDownloader::P::checkSymbolCSVFile(const std::string &path, CsvData::TailCheck *tailOut) {
    // Kraken keeps the whole history: spot trades from 2013, futures candles
    // from listing. A fresh file starts at the beginning unless --since says
    // otherwise. Self-healing read: a torn tail is truncated instead of
    // resetting the resume point.
    const auto tail = CsvData::lastValidRecord(path, 6, historyFloor(0));
    if (tailOut != nullptr) {
        *tailOut = tail;
    }
    return downloadResume(tail);
}

DownloadResume KrakenDownloader::P::checkFundingRatesCSVFile(const std::string &path) {
    return downloadResume(CsvData::lastValidRecord(path, 2, historyFloor(0)));
}

bool KrakenDownloader::P::readCandlesFromCSVFile(const std::string &path, std::vector<kraken::Candle> &candles) {
    try {
        io::CSVReader<6> in(path);
        in.read_header(io::ignore_extra_column, "open_time", "open", "high", "low", "close", "volume");

        kraken::Candle candle;
        while (in.read_row(candle.openTime, candle.open, candle.high, candle.low, candle.close, candle.volume)) {
            candles.push_back(candle);
        }
    } catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }
    return true;
}

bool KrakenDownloader::P::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path,
                                                       const stonky::CandleInterval interval) {
    const std::filesystem::path pathToT6File{t6Path};

    AtomicFileWriter output(pathToT6File, std::ios::binary, AtomicFileWriter::Locking::None);
    if (!output.isOpen()) {
        spdlog::error(fmt::format("Couldn't prepare file {}: {}", t6Path, output.error()));
        return false;
    }
    auto &ofs = output.stream();

    std::vector<kraken::Candle> candles;
    if (!readCandlesFromCSVFile(csvPath, candles) || candles.empty()) {
        spdlog::error(fmt::format("Couldn't read candles from csv file: {}", csvPath));
        return false;
    }

    for (const auto &candle: std::ranges::reverse_view(candles)) {
        T6 t6;
        t6.fOpen = static_cast<float>(candle.open);
        t6.fHigh = static_cast<float>(candle.high);
        t6.fLow = static_cast<float>(candle.low);
        t6.fClose = static_cast<float>(candle.close);
        t6.fVal = 0.0;
        t6.fVol = static_cast<float>(candle.volume);
        t6.time = convertTimeMs(Downloader::candleCloseTimestampMs(candle.openTime, interval));
        ofs.write(reinterpret_cast<char *>(&t6), sizeof(T6));
    }

    std::string error;
    if (!output.commit(error)) {
        spdlog::error(fmt::format("Couldn't commit T6 file {}: {}", t6Path, error));
        return false;
    }
    return true;
}

void KrakenDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
                                              const std::string &outDirPath,
                                              const stonky::CandleInterval interval) const {
    std::vector<std::future<std::pair<std::string, bool> > > futures;

    for (const auto &path: filePaths) {
        if (path.empty()) {
            continue;
        }
        std::filesystem::path t6FilePath = outDirPath;
        t6FilePath.append(path.filename().replace_extension("t6").string());

        spdlog::info(fmt::format("Converting symbol: {}...", path.stem().string()));

        futures.push_back(
            launchBounded(maxConcurrentConvertJobs,
                          [interval](const std::filesystem::path &csvPath,
                                     const std::filesystem::path &t6Path) -> std::pair<std::string, bool> {
                              return {csvPath.stem().string(),
                                      writeCSVCandlesToZorroT6File(csvPath.string(), t6Path.string(), interval)};
                          }, path, t6FilePath));
    }

    for (const auto &[symbol, converted]: waitAllOrThrow(futures)) {
        if (converted) {
            spdlog::info(fmt::format("Symbol: {} converted", symbol));
        } else {
            throw std::runtime_error(fmt::format("Symbol: {} conversion failed", symbol));
        }
    }
}

bool KrakenDownloader::P::writeCandlesToCSVFile(const std::vector<kraken::Candle> &candles, const std::string &path,
                                                DownloadResume resume) {
    std::ofstream ofs(path, std::ios::app);
    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open file: {}", path));
        return false;
    }

    std::error_code ec;
    const auto fileSize = std::filesystem::file_size(path, ec);
    if (ec || fileSize == 0) {
        ofs << "open_time,open,high,low,close,volume\n";
    }

    for (const auto &candle: candles) {
        if (!shouldPersistTimestamp(candle.openTime, resume)) {
            continue;
        }
        ofs << candle.openTime << ','
            << csvNumber(candle.open) << ','
            << csvNumber(candle.high) << ','
            << csvNumber(candle.low) << ','
            << csvNumber(candle.close) << ','
            << csvNumber(candle.volume) << '\n';
        resume = {candle.openTime, true};
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Write to file failed (disk full?): {}", path));
        return false;
    }
    ofs.close();
    return ofs.good();
}

bool KrakenDownloader::P::writeFundingRatesToCSVFile(const std::vector<kraken::FundingRate> &rates,
                                                     const std::string &path, DownloadResume resume) {
    std::ofstream ofs(path, std::ios::app);
    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open file: {}", path));
        return false;
    }

    std::error_code ec;
    const auto fileSize = std::filesystem::file_size(path, ec);
    if (ec || fileSize == 0) {
        ofs << "funding_time,funding_rate\n";
    }

    for (const auto &rate: rates) {
        if (!shouldPersistTimestamp(rate.time, resume)) {
            continue;
        }
        // The relative rate: a fraction of the mark price per one-hour period,
        // positive when longs pay — the convention the other venues' files use.
        ofs << rate.time << ',' << csvNumber(rate.relativeRate) << '\n';
        resume = {rate.time, true};
    }

    ofs.flush();
    if (!ofs.good()) {
        spdlog::error(fmt::format("Write to file failed (disk full?): {}", path));
        return false;
    }
    ofs.close();
    return ofs.good();
}

void KrakenDownloader::P::downloadFuturesCandles(const std::string &symbol, const std::string &csvPath,
                                                 const kraken::CandleInterval interval, const bool delisted) const {
    const auto resume = checkSymbolCSVFile(csvPath);
    const auto nowMs = getMsTimestamp(currentTime()).count();

    // A delisted contract's chart does not end: it keeps producing a flat
    // zero-volume bar per interval indefinitely (PF_LOOMUSD, delisted in
    // 2025, still gets one every minute). Hold the zero-volume run back and
    // write it only when a traded bar follows; the run after the last trade
    // is dropped. Live contracts keep every bar — their empty minutes are
    // real data, as for Hyperliquid and Lighter.
    std::vector<kraken::Candle> heldZero;
    const auto writer = [&](const std::vector<kraken::Candle> &page) {
        std::vector<kraken::Candle> out;
        if (delisted) {
            for (const auto &candle: page) {
                if (candle.volume == 0.0) {
                    heldZero.push_back(candle);
                    continue;
                }
                out.insert(out.end(), heldZero.begin(), heldZero.end());
                heldZero.clear();
                out.push_back(candle);
            }
        } else {
            out = page;
        }
        if (out.empty()) {
            return;
        }
        const auto persisted = checkSymbolCSVFile(csvPath);
        if (!writeCandlesToCSVFile(out, csvPath, persisted)) {
            // Abort pagination — continuing after a failed batch write
            // would leave a permanent gap inside the CSV.
            throw std::runtime_error(fmt::format("CSV write failed for symbol: {}", symbol));
        }
    };

    std::ignore = client->getHistoricalPrices(symbol, interval, requestStartTimestamp(resume), nowMs, writer);
}

void KrakenDownloader::P::downloadSpotCandles(const std::string &symbol, const std::string &csvPath) const {
    constexpr std::int64_t MINUTE_MS = KrakenTradesAggregator::MINUTE_MS;
    constexpr int PAGE = 1000;

    CsvData::TailCheck tail;
    const auto resume = checkSymbolCSVFile(csvPath, &tail);

    // Resume one minute after the persisted bar; a fresh file starts at the
    // first whole minute at or after the floor, so no bar is built from a
    // partial minute of trades.
    const std::int64_t startMs = resume.hasSavedRecord
                                     ? resume.timestamp + MINUTE_MS
                                     : floorTimestamp(resume.timestamp + MINUTE_MS - 1, MINUTE_MS);
    std::optional<double> lastClose;
    if (resume.hasSavedRecord) {
        if (const auto fields = splitString(tail.record, ','); fields.size() >= 5) {
            try {
                lastClose = std::stod(fields[4]);
            } catch (const std::exception &) {
                lastClose.reset();
            }
        }
    }

    KrakenTradesAggregator fold;
    fold.resume(startMs - MINUTE_MS, lastClose);

    const auto write = [&](const std::vector<kraken::Candle> &bars) {
        if (bars.empty()) {
            return;
        }
        const auto persisted = checkSymbolCSVFile(csvPath);
        if (!writeCandlesToCSVFile(bars, csvPath, persisted)) {
            throw std::runtime_error(fmt::format("CSV write failed for symbol: {}", symbol));
        }
    };

    // The tape is walked by the venue's nanosecond cursor; a page shorter
    // than the maximum means the present has been reached.
    std::int64_t sinceNs = startMs * 1'000'000;
    for (;;) {
        const auto page = client->getTrades(symbol, sinceNs, PAGE);
        write(fold.feed(page.trades));
        if (page.trades.size() < static_cast<std::size_t>(PAGE) || page.last <= sinceNs) {
            break;
        }
        sinceNs = page.last;
    }

    // Everything before the previous whole minute is closed; the one just
    // closed waits for the next run as a margin against clock skew.
    const auto nowMs = getMsTimestamp(currentTime()).count();
    write(fold.finish(floorTimestamp(nowMs - MINUTE_MS, MINUTE_MS)));
}

void KrakenDownloader::P::downloadFundingRates(const std::string &symbol, const std::string &csvPath,
                                               const FundingArchive *archive, const bool venueListed) const {
    auto resume = checkFundingRatesCSVFile(csvPath);

    if (!resume.hasSavedRecord && archive != nullptr) {
        // The archive ends inside the REST window, so the REST rows below
        // continue it without a gap; any overlap is dropped by timestamp.
        const auto history = archive->rates(symbol);
        if (!history.empty()) {
            if (!writeFundingRatesToCSVFile(history, csvPath, resume)) {
                throw std::runtime_error(fmt::format("CSV funding-rate write failed for symbol {}", symbol));
            }
            resume = checkFundingRatesCSVFile(csvPath);
            spdlog::info(fmt::format("Seeded {} funding periods of symbol: {} from the Kraken archive",
                                     history.size(), symbol));
        }
    }

    if (!venueListed) {
        return;
    }
    const auto rates = client->getHistoricalFundingRates(symbol);
    if (rates.empty()) {
        return;
    }
    if (!writeFundingRatesToCSVFile(rates, csvPath, resume)) {
        throw std::runtime_error(fmt::format("CSV funding-rate write failed for symbol {}", symbol));
    }
}

void KrakenDownloader::updateMarketData(const std::string &dirPath,
                                        const std::vector<std::string> &symbols,
                                        const CandleInterval candleInterval,
                                        const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                        const onSymbolCompleted &onSymbolCompletedCB,
                                        const bool convertToT6) const {
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    const bool spot = m_p->category == MarketCategory::Spot;
    kraken::CandleInterval krakenInterval{};

    if (spot) {
        if (barSizeInMinutes != 1) {
            throw std::invalid_argument(
                "Kraken spot candles are folded from the public trade tape at 1 minute only (-b 1); "
                "build coarser bars from them with -g");
        }
    } else if (!Kraken::isValidCandleResolution(barSizeInMinutes, krakenInterval)) {
        throw std::invalid_argument("invalid Kraken Futures candle resolution: " + std::to_string(barSizeInMinutes) +
                                    " m");
    }

    const std::filesystem::path finalPath(dirPath);
    std::filesystem::path csvDirectory = finalPath;
    csvDirectory.append(m_p->csvDir());
    csvDirectory.append(Downloader::minutesToString(barSizeInMinutes));
    std::filesystem::path t6Directory = finalPath;
    t6Directory.append(m_p->t6Dir());
    t6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));
    if (symbols.empty()) {
        spdlog::info("Updating all symbols");
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto listed = m_p->listSymbols();
    std::set<std::string> delisted;
    for (const auto &entry: listed) {
        if (entry.delisted) {
            delisted.insert(entry.symbol);
        }
    }
    const auto selection = m_p->selectSymbols(listed, symbols, csvDirectory, "");
    const auto &symbolsToUpdate = selection.first;
    const auto &symbolsToDelete = selection.second;

    if (onSymbolsToUpdateCB) {
        onSymbolsToUpdateCB(symbolsToUpdate);
    }

    if (!symbolsToUpdate.empty()) {
        if (const auto err = createDirectoryRecursively(csvDirectory.string())) {
            throw std::runtime_error(fmt::format("Failed to create {}, err: {}", csvDirectory.string(),
                                                 err.message()));
        }
    }

    std::vector<std::future<std::filesystem::path> > futures;
    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            launchBounded(m_p->maxConcurrentDownloadJobs,
                          [this, csvDirectory, krakenInterval, spot, &delisted](
                      const std::string &symbol) -> std::filesystem::path {
                              std::filesystem::path symbolFilePathCsv = csvDirectory;
                              symbolFilePathCsv.append(symbol + ".csv");

                              spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));
                              P::withRetries(symbol, "candles", [&] {
                                  if (spot) {
                                      m_p->downloadSpotCandles(symbol, symbolFilePathCsv.string());
                                  } else {
                                      m_p->downloadFuturesCandles(symbol, symbolFilePathCsv.string(),
                                                                  krakenInterval, delisted.contains(symbol));
                                  }
                              });
                              spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                              return symbolFilePathCsv;
                          }, s));
    }

    std::ignore = waitAllOrThrow(futures, [&onSymbolCompletedCB](const std::filesystem::path &path) {
        if (onSymbolCompletedCB && !path.empty()) {
            onSymbolCompletedCB(path.stem().string());
        }
    });

    if (convertToT6) {
        std::vector<std::filesystem::path> allCsvFiles;
        if (std::filesystem::exists(csvDirectory)) {
            for (const auto &entry: std::filesystem::directory_iterator(csvDirectory)) {
                if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                    allCsvFiles.push_back(entry.path());
                }
            }
        }
        if (!allCsvFiles.empty()) {
            if (const auto err = createDirectoryRecursively(t6Directory.string())) {
                throw std::runtime_error(fmt::format("Failed to create {}, err: {}", t6Directory.string(),
                                                     err.message()));
            }
            spdlog::info("Converting from csv to t6...");
            m_p->convertFromCSVToT6(allCsvFiles, t6Directory.string(), candleInterval);
        }
    }

    if (m_p->deleteDelistedData) {
        for (const auto &symbol: symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = csvDirectory.lexically_normal();
            symbolFilePathCsv.append(symbol + ".csv");
            std::filesystem::path symbolFilePathT6 = t6Directory.lexically_normal();
            symbolFilePathT6.append(symbol + ".t6");

            if (std::filesystem::exists(symbolFilePathCsv)) {
                std::filesystem::remove(symbolFilePathCsv);
                spdlog::info(fmt::format("Removing csv file for delisted symbol: {}, file: {}...", symbol,
                                         symbolFilePathCsv.string()));
            }
            if (std::filesystem::exists(symbolFilePathT6)) {
                std::filesystem::remove(symbolFilePathT6);
                spdlog::info(fmt::format("Removing t6 file for delisted symbol: {}, file: {}...", symbol,
                                         symbolFilePathT6.string()));
            }
        }
    }
}

void KrakenDownloader::updateMarketData(const std::string &connectionString,
                                        const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                        const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: KrakenDownloader::updateMarketData");
}

void KrakenDownloader::updateFundingRateData(const std::string &dirPath,
                                             const std::vector<std::string> &symbols,
                                             const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                             const onSymbolCompleted &onSymbolCompletedCB) const {
    if (m_p->category == MarketCategory::Spot) {
        throw std::runtime_error("Kraken spot has no funding rates, use -c f for Kraken Futures");
    }

    const std::filesystem::path finalPath(dirPath);
    std::filesystem::path frDirectory = finalPath;
    frDirectory.append(CSV_FUT_FR_DIR);

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));
    if (symbols.empty()) {
        spdlog::info("Updating all symbols");
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto listed = m_p->listSymbols();
    std::set<std::string> venue;
    for (const auto &entry: listed) {
        venue.insert(entry.symbol);
    }
    const auto selection = m_p->selectSymbols(listed, symbols, frDirectory, "_fr");
    auto symbolsToUpdate = selection.first;
    const auto &symbolsToDelete = selection.second;

    const auto pathOf = [&frDirectory](const std::string &symbol) {
        std::filesystem::path path = frDirectory;
        path.append(symbol + "_fr.csv");
        return path;
    };

    // The REST endpoint answers roughly the last year. Kraken's own export
    // reaches back to the first perpetuals and still names contracts the venue
    // has dropped from every listing, but it is 110 MB, so it is fetched only
    // when some symbol starts from nothing or was asked for by name and is
    // unknown to the venue. A file that already holds records only needs the
    // REST window appended.
    std::vector<std::string> unknownRequested;
    for (const auto &symbol: symbols) {
        if (!venue.contains(symbol)) {
            unknownRequested.push_back(symbol);
        }
    }
    const bool anyFresh = std::ranges::any_of(symbolsToUpdate, [&pathOf](const std::string &symbol) {
        return !P::checkFundingRatesCSVFile(pathOf(symbol).string()).hasSavedRecord;
    });
    std::unique_ptr<FundingArchive> archive;
    if (anyFresh || !unknownRequested.empty()) {
        spdlog::info(fmt::format("Downloading the Kraken funding history archive: {}", FUNDING_ARCHIVE_URL));
        archive = std::make_unique<FundingArchive>(RESTClient::downloadFile(FUNDING_ARCHIVE_URL));
        // Archive-only contracts: every one of them for a whole-universe run,
        // else only those asked for by name.
        for (const auto &symbol: symbols.empty() ? archive->symbols() : unknownRequested) {
            if (!venue.contains(symbol) && archive->contains(symbol)) {
                symbolsToUpdate.push_back(symbol);
            }
        }
        deduplicatePreserveOrder(symbolsToUpdate);
        removeUnsafeSymbolFileComponents(symbolsToUpdate);
    }

    if (onSymbolsToUpdateCB) {
        onSymbolsToUpdateCB(symbolsToUpdate);
    }

    if (!symbolsToUpdate.empty()) {
        if (const auto err = createDirectoryRecursively(frDirectory.string())) {
            throw std::runtime_error(fmt::format("Failed to create {}, err: {}", frDirectory.string(),
                                                 err.message()));
        }
    }

    std::vector<std::future<std::filesystem::path> > futures;
    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            launchBounded(m_p->maxConcurrentDownloadJobs,
                          [this, &pathOf, archivePtr = archive.get(), &venue](
                      const std::string &symbol) -> std::filesystem::path {
                              const std::filesystem::path symbolFilePathCsv = pathOf(symbol);

                              spdlog::info(fmt::format("Updating FR for symbol: {}...", symbol));
                              P::withRetries(symbol, "funding rates", [&] {
                                  m_p->downloadFundingRates(symbol, symbolFilePathCsv.string(), archivePtr,
                                                            venue.contains(symbol));
                              });
                              spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                              return symbolFilePathCsv;
                          }, s));
    }

    std::ignore = waitAllOrThrow(futures, [&onSymbolCompletedCB](const std::filesystem::path &path) {
        if (onSymbolCompletedCB && !path.empty()) {
            auto symbol = path.stem().string();
            if (symbol.ends_with("_fr")) {
                symbol.resize(symbol.size() - 3);
            }
            onSymbolCompletedCB(symbol);
        }
    });

    if (m_p->deleteDelistedData) {
        for (const auto &symbol: symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = frDirectory.lexically_normal();
            symbolFilePathCsv.append(symbol + "_fr.csv");
            if (std::filesystem::exists(symbolFilePathCsv)) {
                std::filesystem::remove(symbolFilePathCsv);
                spdlog::info(fmt::format("Removing csv file for delisted symbol: {}, file: {}...", symbol,
                                         symbolFilePathCsv.string()));
            }
        }
    }
}

void KrakenDownloader::convertToT6(const std::string &dirPath, const CandleInterval candleInterval) const {
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    kraken::CandleInterval krakenInterval{};

    if (m_p->category == MarketCategory::Spot) {
        if (barSizeInMinutes != 1) {
            throw std::invalid_argument("Kraken spot data exist at 1 minute only (-b 1); coarser bars come from -g");
        }
    } else if (!Kraken::isValidCandleResolution(barSizeInMinutes, krakenInterval)) {
        throw std::invalid_argument("invalid Kraken Futures candle resolution: " + std::to_string(barSizeInMinutes) +
                                    " m");
    }

    const std::filesystem::path finalPath(dirPath);
    std::filesystem::path csvDirectory = finalPath;
    csvDirectory.append(m_p->csvDir());
    csvDirectory.append(Downloader::minutesToString(barSizeInMinutes));
    std::filesystem::path t6Directory = finalPath;
    t6Directory.append(m_p->t6Dir());
    t6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    std::vector<std::filesystem::path> allCsvFiles;
    if (std::filesystem::exists(csvDirectory)) {
        for (const auto &entry: std::filesystem::directory_iterator(csvDirectory)) {
            if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                allCsvFiles.push_back(entry.path());
            }
        }
    }

    if (!allCsvFiles.empty()) {
        if (const auto err = createDirectoryRecursively(t6Directory.string())) {
            throw std::runtime_error(fmt::format("Failed to create {}, err: {}", t6Directory.string(),
                                                 err.message()));
        }
        spdlog::info("Converting from csv to t6...");
        m_p->convertFromCSVToT6(allCsvFiles, t6Directory.string(), candleInterval);
    }
}
}
