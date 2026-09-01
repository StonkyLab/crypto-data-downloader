/**
Run Lock - one downloader per exchange and output tree

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_RUN_LOCK_H
#define INCLUDE_STONKY_RUN_LOCK_H

#include "stonky/advisory_file_lock.h"

#include <cstdint>
#include <filesystem>
#include <string>
#include <fmt/format.h>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace stonky {

/**
 * Stable identity of the single-instance lock for one exchange and output tree.
 *
 * The identity is the exchange plus the canonical output directory, so two
 * spellings of the same tree resolve to one lock. The directory holding the
 * lock file must be the same for every process that can reach that tree, and
 * therefore must not come from the environment: XDG_RUNTIME_DIR differs
 * between a cron job, a login shell and sudo, and temp_directory_path()
 * follows TMPDIR. Two runs over the same data would then take two different
 * locks and both proceed, which is the whole thing this prevents.
 *
 * It is deliberately outside the data tree, so an HTTP server publishing that
 * tree never serves it.
 */
inline std::string runLockKey(const std::string &exchange,
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

    return fmt::format("crypto_data_downloader-{:016x}", digest);
}

#ifndef _WIN32
inline std::filesystem::path runLockPath(const std::string &exchange,
                                         const std::string &outputDirectory) {
    return std::filesystem::path{"/tmp"} / (runLockKey(exchange, outputDirectory) + ".lock");
}
#endif

/** Process-lifetime run guard. POSIX uses flock in /tmp; Windows uses the
 * kernel's global named-object namespace, so neither identity follows a
 * process-specific temporary directory. */
class RunLock {
public:
    RunLock(const std::string &exchange, const std::string &outputDirectory) {
#ifdef _WIN32
        const auto asciiName = "Global\\" + runLockKey(exchange, outputDirectory);
        const std::wstring name(asciiName.begin(), asciiName.end());
        // Object lifetime, not recursive mutex ownership, is the guard: the
        // first handle creates the name and process teardown closes it.
        handle_ = ::CreateMutexW(nullptr, FALSE, name.c_str());
        if (!handle_) {
            error_ = fmt::format("cannot create global run mutex (Windows error {})",
                                 static_cast<unsigned long>(::GetLastError()));
            return;
        }
        if (::GetLastError() == ERROR_ALREADY_EXISTS) {
            contended_ = true;
            error_ = "run mutex is already held";
            ::CloseHandle(handle_);
            handle_ = nullptr;
            return;
        }
        ownsLock_ = true;
#else
        fileLock_.acquire(runLockPath(exchange, outputDirectory), true);
#endif
    }

    RunLock(const RunLock &) = delete;
    RunLock &operator=(const RunLock &) = delete;

    ~RunLock() {
#ifdef _WIN32
        if (handle_) {
            ::CloseHandle(handle_);
        }
#endif
    }

    [[nodiscard]] bool ownsLock() const noexcept {
#ifdef _WIN32
        return ownsLock_;
#else
        return fileLock_.ownsLock();
#endif
    }

    [[nodiscard]] bool contended() const noexcept {
#ifdef _WIN32
        return contended_;
#else
        return fileLock_.contended();
#endif
    }

    [[nodiscard]] const std::string &error() const noexcept {
#ifdef _WIN32
        return error_;
#else
        return fileLock_.error();
#endif
    }

private:
#ifdef _WIN32
    HANDLE handle_{};
    std::string error_;
    bool ownsLock_{};
    bool contended_{};
#else
    AdvisoryFileLock fileLock_;
#endif
};

} // namespace stonky

#endif // INCLUDE_STONKY_RUN_LOCK_H
