/**
Run Lock - one downloader per exchange and output tree

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_RUN_LOCK_H
#define INCLUDE_STONKY_RUN_LOCK_H

#include <cstdint>
#include <filesystem>
#include <string>
#include <fmt/format.h>

namespace stonky {

/**
 * Where the single-instance lock for one exchange and output tree lives.
 *
 * The identity is the exchange plus the canonical output directory, so two
 * spellings of the same tree resolve to one lock. The directory holding the
 * lock file must be the same for every process that can reach that tree, and
 * therefore must not come from the environment: XDG_RUNTIME_DIR differs
 * between a cron job, a login shell and sudo, and temp_directory_path()
 * follows TMPDIR. Two runs over the same data would then take two different
 * locks and both proceed, which is the whole thing this prevents. POSIX
 * guarantees /tmp exists and is writable by every user, which is exactly the
 * property needed.
 *
 * It is deliberately outside the data tree, so an HTTP server publishing that
 * tree never serves it.
 */
inline std::filesystem::path runLockPath(const std::string &exchange,
                                         const std::string &outputDirectory) {
    std::error_code ec;
    const auto canonical = std::filesystem::weakly_canonical(outputDirectory, ec);
    const auto identity = exchange + '\0' + (ec ? outputDirectory : canonical.string());

    // FNV-1a rather than std::hash: the name has to come out the same in every
    // process sharing this data, which the standard does not promise.
    std::uint64_t digest = 1469598103934665603ULL;
    for (const unsigned char byte: identity) {
        digest = (digest ^ byte) * 1099511628211ULL;
    }

#ifdef _WIN32
    auto directory = std::filesystem::temp_directory_path(ec);
    if (ec) {
        directory = ".";
    }
#else
    const std::filesystem::path directory{"/tmp"};
#endif

    return directory / fmt::format("crypto_data_downloader-{:016x}.lock", digest);
}

} // namespace stonky

#endif // INCLUDE_STONKY_RUN_LOCK_H
