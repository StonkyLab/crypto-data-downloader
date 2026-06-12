/**
Crypto Data Downloader main

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/binance/binance_futures_downloader.h"
#include "stonky/bybit/bybit_downloader.h"
#include "stonky/okx/okx_downloader.h"
#include "stonky/mexc/mexc_futures_downloader.h"
#include "stonky/mexc/mexc_spot_downloader.h"
#include "stonky/hyperliquid/hyperliquid_downloader.h"
#include "stonky/lighter/lighter_downloader.h"
#include "stonky/csv_verifier.h"
#include "stonky/downloader.h"
#include "stonky/binance/binance_spot_downloader.h"
#include <spdlog/spdlog.h>
#include <cxxopts.hpp>
#include <filesystem>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <algorithm>
#include "csv.h"
#include <iostream>
#include <memory>

#undef max

#define VERSION "2.4.0"

using namespace stonky;

std::vector<std::string> parseSymbolsFile(const std::string &path) {
    std::vector<std::string> retVal;

    // Detect format from the header row
    std::ifstream probe(path);
    if (!probe.is_open()) {
        spdlog::warn(fmt::format("Could not open symbols file: {}", path));
        return retVal;
    }
    std::string header;
    std::getline(probe, header);
    probe.close();

    std::string column;
    if (header.find("Symbol") != std::string::npos) {
        column = "Symbol";   // Zorro Assets file
    } else if (header.find("symbol") != std::string::npos) {
        column = "symbol";   // exchange info CSV (symbol,available_since,available_to)
    } else {
        spdlog::warn(fmt::format("Unrecognised CSV header in file: {}", path));
        return retVal;
    }

    try {
        io::CSVReader<1> in(path);
        std::string symbolStr;
        if (column == "Symbol") {
            in.read_header(io::ignore_extra_column, "Symbol");
        } else {
            in.read_header(io::ignore_extra_column, "symbol");
        }
        while (in.read_row(symbolStr)) {
            retVal.push_back(symbolStr);
        }
    } catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse symbols file: {}, reason: {}", path, e.what()));
    }
    return retVal;
}

int main(int argc, char **argv) {
    cxxopts::Options options("data_downloader",
                             "Utility for downloading historical data from crypto exchanges, currently Binance (bnb), Bybit (bybit), OKX (okx) and MEXC (mexc) exchanges are supported");
    std::vector<std::string> symbols;
    std::string outputDirectory;
    std::string dataType;
    std::string exchange;
    int32_t barSizeInMinutes = 1;
    auto marketCategory = MarketCategory::Futures;
    bool convertToT6 = false;
    bool keepDelistedData = true;
    bool verifyData = false;
    bool repairData = false;
    std::uint32_t maxJobs = static_cast<std::uint32_t>(std::max(std::floor(std::thread::hardware_concurrency() * 0.75),
                                                                1.0));

    options.add_options()
            ("e,exchange",
             R"(Exchange name: Binance (bnb), OKX (okx), Bybit (bybit), MEXC (mexc), Hyperliquid (hl) or Lighter (lt), example: -e bnb (default: bnb))",
             cxxopts::value<std::string>()->default_value({"bnb"}))
            ("o,output", R"(Output directory path, example: -o "C:\Users\UserName\BNBData")",
             cxxopts::value<std::string>())
            ("t,data_type",
             R"(Data type for download, either candles 'c' or funding rate 'fr', example -t c, default is candles)",
             cxxopts::value<std::string>()->default_value({"c"}))
            ("s,symbols",
             R"(Symbols of assets to download, example: -s "BTCUSDT,ETHUSDT", "all" means All symbols, mutually exclusive with parameter -a)",
             cxxopts::value<std::vector<std::string> >()->default_value({"all"}))
            ("a,assets_file", R"(Path to a symbols CSV file: either a Zorro Assets file (header: Symbol) or an exchange info CSV (header: symbol,available_since,available_to). Mutually exclusive with parameter -s)",
             cxxopts::value<std::string>()->default_value(""))
            ("j,jobs", R"(Maximum number of jobs to run in parallel, example -j 8)",
             cxxopts::value<std::uint32_t>()->default_value(std::to_string(maxJobs)))
            ("b,bar_size", R"(Bar size in minutes, example -b 5, default is 1)",
             cxxopts::value<int32_t>()->default_value("1"))
            ("c,category", R"(Market category, either Spot (s) or Futures (f), example -c f, default is Futures)",
             cxxopts::value<std::string>()->default_value("f"))
            ("d,delete_delisted", R"(Delete delisted symbols data files, if not specified delisted files will be preserved)")
            ("z,t6_conversion", R"(Convert existing CSV data to T6 format (Zorro Trader format) without downloading new data)")
            ("y,verify", R"(Verify CSV data integrity (torn lines, duplicates, ordering, gaps) without downloading, example: -e bybit -o /data/bybit -b 1 -y)")
            ("r,repair", R"(Verify and repair CSV data files in place (removes torn lines and duplicates, restores ordering), example: -e bybit -o /data/bybit -b 1 -r)")
            ("v,version", R"(Print version and quit)")
            ("h,help", R"(Print usage and quit)");
    try {
        cxxopts::ParseResult parseResult;
        parseResult = options.parse(argc, argv);

        if (parseResult["help"].as<bool>()) {
            spdlog::info(options.help());
            return 0;
        }

        if (parseResult["version"].as<bool>()) {
            spdlog::info(fmt::format("Version: {}", VERSION));
            return 0;
        }

        outputDirectory = parseResult["output"].as<std::string>();

        if (!std::filesystem::exists(outputDirectory)) {
            spdlog::critical("Output directory dost not exist!");
            return -1;
        }

        if (const auto assetFile = parseResult["assets_file"].as<std::string>(); assetFile.empty()) {
            symbols = parseResult["symbols"].as<std::vector<std::string> >();

            if (symbols.size() == 1 && symbols[0] == "all") {
                symbols.clear();
            }
        } else {
            symbols = parseSymbolsFile(assetFile);

            if (symbols.empty()) {
                spdlog::info("Symbols file is empty or unreadable, updating all exchange symbols");
            }
        }

        maxJobs = parseResult["jobs"].as<std::uint32_t>();

        if (maxJobs < 1) {
            maxJobs = 1;
        }

        if (maxJobs >= std::thread::hardware_concurrency()) {
            spdlog::warn(fmt::format("Number of concurrent jobs is {}, which is too high, system can experience performance issues",
                         std::thread::hardware_concurrency()));
        }

        dataType = parseResult["data_type"].as<std::string>();

        if (dataType != "c" && dataType != "fr") {
            spdlog::error(fmt::format("Wrong value of data_type parameter, must be 'c' or 'fr', is: {}", dataType));
            spdlog::info(options.help());
            return -1;
        }

        exchange = parseResult["exchange"].as<std::string>();

        if (exchange != "bnb" && exchange != "bybit" && exchange != "okx" && exchange != "mexc" && exchange != "hl" && exchange != "lt") {
            spdlog::error(fmt::format("Wrong value of exchange parameter, must be 'bnb', 'okx', 'bybit', 'mexc', 'hl' or 'lt', is: {}", exchange));
            spdlog::info(options.help());
            return -1;
        }

        std::string outputDirectoryLowerCase = outputDirectory;
        std::ranges::transform(outputDirectoryLowerCase,
                               outputDirectoryLowerCase.begin(),
                               [](const unsigned char c) { return std::tolower(c); });

        if (exchange == "bnb") {
            if (outputDirectoryLowerCase.find("bybit") != std::string::npos ||
                outputDirectoryLowerCase.find("okx") != std::string::npos ||
                outputDirectoryLowerCase.find("mexc") != std::string::npos ||
                outputDirectoryLowerCase.find("hyperliquid") != std::string::npos ||
                outputDirectoryLowerCase.find("lighter") != std::string::npos) {
                std::string response;
                std::cout
                        << "Seems that you are trying to save Binance data into another exchange folder, are you sure? Type y (yes) or n (no)"
                        << std::endl;
                std::cin >> response;

                if (response != "y") {
                    return -1;
                }
            }
        } else if (exchange == "bybit") {
            if (outputDirectoryLowerCase.find("bnb") != std::string::npos ||
                outputDirectoryLowerCase.find("binance") != std::string::npos ||
                outputDirectoryLowerCase.find("okx") != std::string::npos ||
                outputDirectoryLowerCase.find("mexc") != std::string::npos ||
                outputDirectoryLowerCase.find("hyperliquid") != std::string::npos ||
                outputDirectoryLowerCase.find("lighter") != std::string::npos) {
                std::string response;
                std::cout
                        << "Seems that you are trying to save Bybit data into another exchange folder, are you sure? Type y (yes) or n (no)"
                        << std::endl;
                std::cin >> response;

                if (response != "y") {
                    return -1;
                }
            }
        } else if (exchange == "okx") {
            if (outputDirectoryLowerCase.find("bnb") != std::string::npos ||
                outputDirectoryLowerCase.find("binance") != std::string::npos ||
                outputDirectoryLowerCase.find("bybit") != std::string::npos ||
                outputDirectoryLowerCase.find("mexc") != std::string::npos ||
                outputDirectoryLowerCase.find("hyperliquid") != std::string::npos ||
                outputDirectoryLowerCase.find("lighter") != std::string::npos) {
                std::string response;
                std::cout
                        << "Seems that you are trying to save OKX data into another exchange folder, are you sure? Type y (yes) or n (no)"
                        << std::endl;
                std::cin >> response;

                if (response != "y") {
                    return -1;
                }
            }
        } else if (exchange == "mexc") {
            if (outputDirectoryLowerCase.find("bnb") != std::string::npos ||
                outputDirectoryLowerCase.find("binance") != std::string::npos ||
                outputDirectoryLowerCase.find("bybit") != std::string::npos ||
                outputDirectoryLowerCase.find("okx") != std::string::npos ||
                outputDirectoryLowerCase.find("hyperliquid") != std::string::npos ||
                outputDirectoryLowerCase.find("lighter") != std::string::npos) {
                std::string response;
                std::cout
                        << "Seems that you are trying to save MEXC data into another exchange folder, are you sure? Type y (yes) or n (no)"
                        << std::endl;
                std::cin >> response;

                if (response != "y") {
                    return -1;
                }
            }
        } else if (exchange == "hl") {
            if (outputDirectoryLowerCase.find("bnb") != std::string::npos ||
                outputDirectoryLowerCase.find("binance") != std::string::npos ||
                outputDirectoryLowerCase.find("bybit") != std::string::npos ||
                outputDirectoryLowerCase.find("okx") != std::string::npos ||
                outputDirectoryLowerCase.find("mexc") != std::string::npos ||
                outputDirectoryLowerCase.find("lighter") != std::string::npos) {
                std::string response;
                std::cout
                        << "Seems that you are trying to save Hyperliquid data into another exchange folder, are you sure? Type y (yes) or n (no)"
                        << std::endl;
                std::cin >> response;

                if (response != "y") {
                    return -1;
                }
            }
        } else if (exchange == "lt") {
            if (outputDirectoryLowerCase.find("bnb") != std::string::npos ||
                outputDirectoryLowerCase.find("binance") != std::string::npos ||
                outputDirectoryLowerCase.find("bybit") != std::string::npos ||
                outputDirectoryLowerCase.find("okx") != std::string::npos ||
                outputDirectoryLowerCase.find("mexc") != std::string::npos ||
                outputDirectoryLowerCase.find("hyperliquid") != std::string::npos) {
                std::string response;
                std::cout
                        << "Seems that you are trying to save Lighter data into another exchange folder, are you sure? Type y (yes) or n (no)"
                        << std::endl;
                std::cin >> response;

                if (response != "y") {
                    return -1;
                }
            }
        }

        barSizeInMinutes = parseResult["bar_size"].as<int32_t>();

        if (dataType != "c" && dataType != "fr") {
            spdlog::error(fmt::format("Wrong value of data_type parameter, must be 'c' or 'fr', is: {}", dataType));
            spdlog::info(options.help());
            return -1;
        }

        const auto category = parseResult["category"].as<std::string>();

        if (category != "s" && category != "f") {
            spdlog::error(fmt::format("Wrong value of category parameter, must be 's' or 'f', is: {}", category));
            spdlog::info(options.help());
            return -1;
        }
        if (category == "s") {
            marketCategory = MarketCategory::Spot;
        } else {
            marketCategory = MarketCategory::Futures;
        }

        convertToT6 = parseResult["t6_conversion"].as<bool>();
        keepDelistedData = !parseResult["delete_delisted"].as<bool>();
        verifyData = parseResult["verify"].as<bool>();
        repairData = parseResult["repair"].as<bool>();
    } catch (const std::exception &) {
        spdlog::critical("Wrong parameters!");
        spdlog::info(options.help());
        return -1;
    }

    try {
        std::vector<spdlog::sink_ptr> sinks;
        sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_st>());
        sinks.push_back(std::make_shared<spdlog::sinks::basic_file_sink_st>("crypto_data_downloader.log"));
        auto combinedLogger = std::make_shared<spdlog::logger>("crypto_data_downloader", begin(sinks), end(sinks));
        register_logger(combinedLogger);
        set_default_logger(combinedLogger);
        spdlog::flush_on(spdlog::level::info);

        if (verifyData || repairData) {
            CsvVerifier::Options verifierOptions;
            verifierOptions.repair = repairData;
            verifierOptions.maxJobs = maxJobs;

            if (exchange == "bnb") {
                verifierOptions.expectedFields = 12;
            } else if (exchange == "okx") {
                verifierOptions.expectedFields = 8;
            } else if (exchange == "mexc") {
                verifierOptions.expectedFields = 7;
                verifierOptions.allowMoreFields = true;
            } else {
                verifierOptions.expectedFields = 6; // bybit, hl, lt
            }
            if (exchange == "bybit") {
                // Salvage legacy 7-column rows (trailing turnover) found in old 1h files
                verifierOptions.salvageExtraField = true;
            }

            std::filesystem::path verifyDir(outputDirectory);
            if (dataType == "fr") {
                verifyDir.append(CSV_FUT_FR_DIR);
                verifierOptions.expectedFields = 2;
                verifierOptions.allowMoreFields = false;
                verifierOptions.salvageExtraField = false;
                verifierOptions.intervalMs = 0; // funding cadence varies per symbol
            } else {
                verifyDir.append(marketCategory == MarketCategory::Spot ? CSV_SPOT_DIR : CSV_FUT_DIR);
                verifyDir.append(Downloader::minutesToString(barSizeInMinutes));
                // Hyperliquid/Lighter drop zero-volume bars, so gaps are expected there
                verifierOptions.intervalMs = (exchange == "hl" || exchange == "lt")
                                                 ? 0
                                                 : static_cast<std::int64_t>(barSizeInMinutes) * 60000;
            }

            const auto reports = CsvVerifier::verifyDirectory(verifyDir.string(), verifierOptions);

            bool anyUnresolvedIssue = false;
            for (const auto &report: reports) {
                if (report.readFailed || (report.needsRepair() && !report.repaired)) {
                    anyUnresolvedIssue = true;
                    break;
                }
            }
            return anyUnresolvedIssue ? 1 : 0;
        }

        std::unique_ptr<IExchangeDownloader> downloader;
        const auto candleInterval = Downloader::minutesToCandleInterval(barSizeInMinutes);
        const bool deleteDelistedData = !keepDelistedData;

        if (exchange == "bnb" && marketCategory == MarketCategory::Futures) {
            downloader = std::make_unique<BinanceFuturesDownloader>(maxJobs, deleteDelistedData);
        } else if (exchange == "bnb" && marketCategory == MarketCategory::Spot) {
            downloader = std::make_unique<BinanceSpotDownloader>(maxJobs, deleteDelistedData);
        } else if (exchange == "bybit") {
            downloader = std::make_unique<BybitDownloader>(maxJobs, marketCategory, deleteDelistedData);
        } else if (exchange == "okx") {
            downloader = std::make_unique<OKXDownloader>(maxJobs, marketCategory, deleteDelistedData);
        } else if (exchange == "mexc" && marketCategory == MarketCategory::Futures) {
            downloader = std::make_unique<MEXCFuturesDownloader>(maxJobs, deleteDelistedData);
        } else if (exchange == "mexc" && marketCategory == MarketCategory::Spot) {
            downloader = std::make_unique<MEXCSpotDownloader>(maxJobs, deleteDelistedData);
        } else if (exchange == "hl" && marketCategory == MarketCategory::Futures) {
            downloader = std::make_unique<HyperliquidDownloader>(maxJobs, deleteDelistedData);
        } else if (exchange == "hl" && marketCategory == MarketCategory::Spot) {
            spdlog::error("Hyperliquid does not support Spot market, use -c f for Futures");
            return -1;
        } else if (exchange == "lt" && marketCategory == MarketCategory::Futures) {
            downloader = std::make_unique<LighterDownloader>(maxJobs, deleteDelistedData);
        } else if (exchange == "lt" && marketCategory == MarketCategory::Spot) {
            spdlog::error("Lighter does not support Spot market, use -c f for Futures");
            return -1;
        }

        if (convertToT6) {
            downloader->convertToT6(outputDirectory, candleInterval);
        } else if (dataType == "c") {
            downloader->updateMarketData(outputDirectory, symbols, candleInterval, {}, {}, false);
        } else if (dataType == "fr") {
            downloader->updateFundingRateData(outputDirectory, symbols, {}, {});
        }
    } catch (std::exception &e) {
        spdlog::critical(e.what());
    }

    return 0;
}
