#include "stonky/run_lock.h"
#include "stonky/advisory_file_lock.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>

namespace {

#ifndef _WIN32
void setEnvironment(const char *name, const char *value) {
    if (value) {
        ::setenv(name, value, 1);
    } else {
        ::unsetenv(name);
    }
}

class EnvironmentGuard {
public:
    explicit EnvironmentGuard(const char *name) : name_(name) {
        if (const auto *value = std::getenv(name)) {
            original_ = value;
        }
    }
    ~EnvironmentGuard() { setEnvironment(name_, original_ ? original_->c_str() : nullptr); }

private:
    const char *name_;
    std::optional<std::string> original_;
};
#endif

} // namespace

int main() {
    bool ok = true;
    const std::string exchange = "mexc";
    const auto testRoot = std::filesystem::temp_directory_path() /
                          ("crypto_data_downloader_run_lock_test_" + std::to_string(
                              std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(testRoot);
    const std::string output = testRoot.string();

#ifndef _WIN32
    const EnvironmentGuard xdgGuard("XDG_RUNTIME_DIR");
    const EnvironmentGuard tmpGuard("TMPDIR");
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
    if (stonky::runLockKey(exchange, output) !=
        stonky::runLockKey(exchange, output + "/./")) {
        std::cerr << "Equivalent output paths produced different run locks\n";
        ok = false;
    }
    if (stonky::runLockKey(exchange, output) == stonky::runLockKey("bybit", output)) {
        std::cerr << "Two exchanges shared one run lock\n";
        ok = false;
    }
    if (stonky::runLockKey(exchange, output) ==
        stonky::runLockKey(exchange, output + "/elsewhere")) {
        std::cerr << "Two output trees shared one run lock\n";
        ok = false;
    }

    // Contention has to be distinguishable from a lock that cannot be made at
    // all, because the two need different things done about them.
    const auto path = testRoot / "generic.lock";
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
    const stonky::AdvisoryFileLock unusable(testRoot / "absent" / "run.lock");
    if (unusable.ownsLock() || unusable.contended()) {
        std::cerr << "An unusable lock path was reported as contention\n";
        ok = false;
    }

    std::error_code ec;

    // The actual process guard, not just its path helper, must reject a second
    // owner and become available again when the first owner exits.
    {
        const stonky::RunLock first(exchange, output);
        const stonky::RunLock second(exchange, output);
        if (!first.ownsLock() || second.ownsLock() || !second.contended()) {
            std::cerr << "Process run lock did not report contention\n";
            ok = false;
        }
    }
    {
        const stonky::RunLock afterRelease(exchange, output);
        if (!afterRelease.ownsLock()) {
            std::cerr << "Process run lock was not released with its owner\n";
            ok = false;
        }
    }

#ifndef _WIN32
    // A persistent /tmp lock created under sudo must remain usable by an
    // ordinary account after the privileged process exits.
    std::filesystem::permissions(
        path, std::filesystem::perms::owner_read | std::filesystem::perms::group_read |
                  std::filesystem::perms::others_read,
        std::filesystem::perm_options::replace, ec);
    if (ec) {
        std::cerr << "Could not prepare read-only shared-lock regression: "
                  << ec.message() << '\n';
        ok = false;
    } else {
        const stonky::AdvisoryFileLock readOnlyShared(path, true);
        if (!readOnlyShared.ownsLock()) {
            std::cerr << "Shared run-lock namespace required write permission: "
                      << readOnlyShared.error() << '\n';
            ok = false;
        }
    }
    std::filesystem::remove(stonky::runLockPath(exchange, output), ec);
#endif

    std::filesystem::remove_all(testRoot, ec);
    return ok ? 0 : 1;
}
