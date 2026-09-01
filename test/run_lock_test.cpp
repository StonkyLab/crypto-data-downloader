#include "stonky/run_lock.h"
#include "stonky/advisory_file_lock.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>

namespace {

#ifndef _WIN32
void setEnvironment(const char *name, const char *value) {
    if (value) {
        ::setenv(name, value, 1);
    } else {
        ::unsetenv(name);
    }
}
#endif

} // namespace

int main() {
    bool ok = true;
    const std::string exchange = "mexc";
    const std::string output = std::filesystem::temp_directory_path().string();

#ifndef _WIN32
    // The audit's reproduction: the same exchange and the same data, started
    // two different ways, must not end up locking two different files.
    setEnvironment("XDG_RUNTIME_DIR", "/run/user/1000");
    setEnvironment("TMPDIR", "/var/tmp");
    const auto fromSession = stonky::runLockPath(exchange, output);

    setEnvironment("XDG_RUNTIME_DIR", nullptr);
    setEnvironment("TMPDIR", nullptr);
    const auto fromCron = stonky::runLockPath(exchange, output);

    setEnvironment("XDG_RUNTIME_DIR", "/run/user/0");
    setEnvironment("TMPDIR", "/some/other/tmp");
    const auto fromSudo = stonky::runLockPath(exchange, output);

    if (fromSession != fromCron || fromCron != fromSudo) {
        std::cerr << "Run lock path depends on the environment: " << fromSession << " / " << fromCron
                  << " / " << fromSudo << '\n';
        ok = false;
    }
#endif

    // Two spellings of one tree are one lock; a different tree or exchange is not.
    if (stonky::runLockPath(exchange, output) !=
        stonky::runLockPath(exchange, output + "/./")) {
        std::cerr << "Equivalent output paths produced different run locks\n";
        ok = false;
    }
    if (stonky::runLockPath(exchange, output) == stonky::runLockPath("bybit", output)) {
        std::cerr << "Two exchanges shared one run lock\n";
        ok = false;
    }
    if (stonky::runLockPath(exchange, output) ==
        stonky::runLockPath(exchange, output + "/elsewhere")) {
        std::cerr << "Two output trees shared one run lock\n";
        ok = false;
    }

    // Contention has to be distinguishable from a lock that cannot be made at
    // all, because the two need different things done about them.
    const auto path = std::filesystem::temp_directory_path() / "crypto_data_downloader_run_lock_test.lock";
    {
        const stonky::AdvisoryFileLock first(path);
        if (!first.ownsLock()) {
            std::cerr << "Could not take the first run lock: " << first.error() << '\n';
            ok = false;
        }
        const stonky::AdvisoryFileLock second(path);
        if (second.ownsLock() || !second.contended()) {
            std::cerr << "A second run lock was not reported as contended\n";
            ok = false;
        }
    }
    const stonky::AdvisoryFileLock unusable(std::filesystem::temp_directory_path() /
                                            "crypto_data_downloader_absent_dir" / "run.lock");
    if (unusable.ownsLock() || unusable.contended()) {
        std::cerr << "An unusable lock path was reported as contention\n";
        ok = false;
    }

    std::error_code ec;
    std::filesystem::remove(path, ec);
    return ok ? 0 : 1;
}
