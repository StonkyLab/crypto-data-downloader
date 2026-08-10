cmake_minimum_required(VERSION 4.0)
project(crypto_data_downloader)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)

option(BUILD_DOWNLOADER "Build the exchange downloader executable" ON)
option(ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(ENABLE_COVERAGE "Enable compiler coverage instrumentation" OFF)

if (ENABLE_SANITIZERS AND ENABLE_COVERAGE)
    message(FATAL_ERROR "ENABLE_SANITIZERS and ENABLE_COVERAGE require separate build directories")
endif ()

if (ENABLE_SANITIZERS)
    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
        add_link_options(-fsanitize=address,undefined)
    else ()
        message(FATAL_ERROR "ENABLE_SANITIZERS is supported only with GCC or Clang")
    endif ()
endif ()

if (ENABLE_COVERAGE)
    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(--coverage -O0 -g)
        add_link_options(--coverage)
    else ()
        message(FATAL_ERROR "ENABLE_COVERAGE is supported only with GCC or Clang")
    endif ()
endif ()

if (MSVC)
    add_definitions(-D_WIN32_WINNT=0x0A00 /utf-8)
endif ()

if (POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif ()

find_package(spdlog CONFIG REQUIRED)

if (NOT TARGET stonky_common)
    add_subdirectory(stonky-cpp-common)
endif ()

if (BUILD_DOWNLOADER)
    find_package(Boost 1.88 REQUIRED)
    find_package(cxxopts CONFIG REQUIRED)
    find_package(magic_enum CONFIG REQUIRED)
    find_package(nlohmann_json CONFIG REQUIRED)
    get_target_property(MAGIC_ENUM_INCLUDE_DIRS magic_enum::magic_enum INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${Boost_INCLUDE_DIR} ${MAGIC_ENUM_INCLUDE_DIRS})
    add_subdirectory(binance-cpp-api)
    add_subdirectory(bybit-cpp-api)
    add_subdirectory(okx-cpp-api)
    add_subdirectory(mexc-cpp-api)
    add_subdirectory(hyperliquid-cpp-api)
    add_subdirectory(lighter-cpp-api)
endif ()

include_directories(
        include
        stonky-cpp-common/include
        binance-cpp-api/include
        bybit-cpp-api/include
        okx-cpp-api/include
        mexc-cpp-api/include
        hyperliquid-cpp-api/include
        lighter-cpp-api/include)

set(HEADERS
        include/stonky/downloader.h
        include/stonky/atomic_file.h
        include/stonky/candle_aggregator.h
        include/stonky/csv_data.h
        include/stonky/csv_format.h
        include/stonky/csv_verifier.h
        include/stonky/future_utils.h
        include/stonky/binance/binance_futures_downloader.h
        include/stonky/binance/binance_spot_downloader.h
        include/stonky/binance/binance_common.h
        include/stonky/bybit/bybit_downloader.h
        include/stonky/okx/okx_downloader.h
        include/stonky/mexc/mexc_futures_downloader.h
        include/stonky/mexc/mexc_spot_downloader.h
        include/stonky/hyperliquid/hyperliquid_downloader.h
        include/stonky/lighter/lighter_downloader.h
)

set(SOURCES
        src/candle_aggregator.cpp
        src/csv_data.cpp
        src/csv_verifier.cpp
        src/binance_futures_downloader.cpp
        src/binance_spot_downloader.cpp
        src/bybit_downloader.cpp
        src/okx_downloader.cpp
        src/binance_common.cpp
        src/mexc_futures_downloader.cpp
        src/mexc_spot_downloader.cpp
        src/hyperliquid_downloader.cpp
        src/lighter_downloader.cpp
)

if (BUILD_DOWNLOADER)
    add_executable(crypto_data_downloader main.cpp ${SOURCES} ${HEADERS})
    target_link_libraries(crypto_data_downloader PRIVATE spdlog::spdlog_header_only cxxopts::cxxopts magic_enum::magic_enum nlohmann_json::nlohmann_json binance_api bybit_api okx_api mexc_api hyperliquid_api lighter_api)
endif ()

if (BUILD_TESTING)
    add_executable(csv_format_test test/csv_format_test.cpp)
    target_include_directories(csv_format_test PRIVATE include)
    target_link_libraries(csv_format_test PRIVATE spdlog::spdlog_header_only)
    add_test(NAME csv_format_round_trip COMMAND csv_format_test)

    add_executable(csv_data_test test/csv_data_test.cpp src/csv_data.cpp)
    target_include_directories(csv_data_test PRIVATE include stonky-cpp-common/include)
    target_link_libraries(csv_data_test PRIVATE spdlog::spdlog_header_only stonky_common)
    add_test(NAME csv_data_tail_recovery COMMAND csv_data_test)

    add_executable(downloader_interval_test test/downloader_interval_test.cpp)
    target_include_directories(downloader_interval_test PRIVATE include stonky-cpp-common/include)
    target_link_libraries(downloader_interval_test PRIVATE spdlog::spdlog_header_only)
    add_test(NAME downloader_month_interval COMMAND downloader_interval_test)

    add_executable(candle_aggregator_test
            test/candle_aggregator_test.cpp
            src/candle_aggregator.cpp
            src/csv_data.cpp)
    target_include_directories(candle_aggregator_test PRIVATE include stonky-cpp-common/include)
    target_link_libraries(candle_aggregator_test PRIVATE spdlog::spdlog_header_only stonky_common)
    add_test(NAME aggregation_incomplete_bucket_policy COMMAND candle_aggregator_test)

    add_executable(mexc_staging_test test/mexc_staging_test.cpp)
    target_include_directories(mexc_staging_test PRIVATE src)
    target_link_libraries(mexc_staging_test PRIVATE spdlog::spdlog_header_only)
    add_test(NAME mexc_transactional_staging COMMAND mexc_staging_test)

    add_executable(csv_verifier_test test/csv_verifier_test.cpp src/csv_verifier.cpp)
    target_include_directories(csv_verifier_test PRIVATE include stonky-cpp-common/include)
    target_link_libraries(csv_verifier_test PRIVATE spdlog::spdlog_header_only stonky_common)
    add_test(NAME csv_verifier_rejects_corruption COMMAND csv_verifier_test)

    add_executable(future_utils_test test/future_utils_test.cpp)
    target_include_directories(future_utils_test PRIVATE include stonky-cpp-common/include)
    target_link_libraries(future_utils_test PRIVATE spdlog::spdlog_header_only stonky_common)
    add_test(NAME future_utils_preserves_symbols COMMAND future_utils_test)

    add_executable(atomic_file_test test/atomic_file_test.cpp)
    target_include_directories(atomic_file_test PRIVATE include)
    add_test(NAME atomic_file_preserves_previous_output COMMAND atomic_file_test)
endif ()

if (BUILD_TESTING AND BUILD_DOWNLOADER)
    add_executable(binance_pagination_test
            test/binance_pagination_test.cpp
            src/binance_common.cpp
            src/csv_data.cpp)
    target_include_directories(binance_pagination_test PRIVATE
            include
            stonky-cpp-common/include
            binance-cpp-api/include)
    target_link_libraries(binance_pagination_test PRIVATE
            spdlog::spdlog_header_only
            nlohmann_json::nlohmann_json
            stonky_common
            binance_api)
    add_test(NAME binance_pagination_preserves_progress COMMAND binance_pagination_test)

    add_executable(mexc_models_precision_test test/mexc_models_precision_test.cpp)
    target_include_directories(mexc_models_precision_test PRIVATE
            include
            stonky-cpp-common/include
            mexc-cpp-api/include)
    target_link_libraries(mexc_models_precision_test PRIVATE
            spdlog::spdlog_header_only
            nlohmann_json::nlohmann_json
            stonky_common
            mexc_api)
    add_test(NAME mexc_models_preserve_decimal_text COMMAND mexc_models_precision_test)
endif ()
