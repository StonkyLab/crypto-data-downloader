cmake_minimum_required(VERSION 4.0)
project(crypto_data_downloader)

set(CMAKE_CXX_STANDARD 20)

if (MSVC)
    add_definitions(-D_WIN32_WINNT=0x0A00 /utf-8)
endif ()

if (POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif ()

find_package(spdlog CONFIG REQUIRED)
find_package(cxxopts CONFIG REQUIRED)

add_subdirectory(binance-cpp-api)
add_subdirectory(bybit-cpp-api)
add_subdirectory(okx-cpp-api)
add_subdirectory(mexc-cpp-api)
set(MODULE_MANAGER 1)
add_subdirectory(hyperliquid-cpp-api)
add_subdirectory(lighter-cpp-api)
unset(MODULE_MANAGER)

if (NOT TARGET stonky_common)
    add_subdirectory(stonky-cpp-common)
endif ()

include_directories(include stonky-cpp-common/include binance-cpp-api/include bybit-cpp-api/include okx-cpp-api/include mexc-cpp-api/include hyperliquid-cpp-api/include lighter-cpp-api/include magic_enum)

set(HEADERS
        include/stonky/downloader.h
        include/stonky/csv_data.h
        include/stonky/csv_verifier.h
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

add_executable(crypto_data_downloader main.cpp ${SOURCES} ${HEADERS})
target_link_libraries(crypto_data_downloader PRIVATE spdlog::spdlog_header_only binance_api bybit_api okx_api mexc_api hyperliquid_api lighter_api)

# hyperliquid_api and lighter_api build as DLLs (MODULE_MANAGER). Windows has no
# rpath and vcpkg's applocal copies only vcpkg DLLs, so drop our own module DLLs
# next to the executable or it fails to start ("cannot find hyperliquid_api.dll").
# On Linux the build-tree rpath already resolves the .so from their subdirs.
if (WIN32)
    add_custom_command(TARGET crypto_data_downloader POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:hyperliquid_api> $<TARGET_FILE:lighter_api>
            $<TARGET_FILE_DIR:crypto_data_downloader>
            COMMAND_EXPAND_LISTS)
endif ()
