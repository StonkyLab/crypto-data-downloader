#include "stonky/atomic_file.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

#ifndef _WIN32
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {
std::string read(const std::filesystem::path &path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}
}

int main() {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("crypto_data_downloader_atomic_file_" + std::to_string(
                         std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);
    const auto target = dir / "TEST.t6";
    {
        std::ofstream initial(target, std::ios::binary);
        initial << "old";
    }

    {
        stonky::AtomicFileWriter abandoned(target);
        if (!abandoned.isOpen()) {
            std::cerr << abandoned.error() << '\n';
            return 1;
        }
        abandoned.stream() << "partial";
        // No commit: the old target must survive.
    }
    if (read(target) != "old") {
        std::cerr << "Abandoned transaction modified the existing target\n";
        return 1;
    }
    auto temporary = target;
    temporary += ".writing";
    auto lock = target;
    lock += ".lock";
    if (std::filesystem::exists(temporary) || !std::filesystem::is_regular_file(lock)) {
        std::cerr << "Abandoned transaction did not clean its output or retain its advisory lock file\n";
        return 1;
    }

    {
        stonky::AtomicFileWriter failedWrite(target);
        if (!failedWrite.isOpen()) {
            std::cerr << failedWrite.error() << '\n';
            return 1;
        }
        failedWrite.stream() << "disk-full-partial";
        // Model an ENOSPC/writeback failure observed by flush/close without
        // requiring a privileged tiny filesystem in the unit test.
        failedWrite.stream().setstate(std::ios::badbit);
        std::string error;
        if (failedWrite.commit(error) || error.find("failed to flush") == std::string::npos) {
            std::cerr << "Failed output stream was committed\n";
            return 1;
        }
    }
    if (read(target) != "old" || std::filesystem::exists(temporary)) {
        std::cerr << "Write/flush failure damaged the previous output\n";
        return 1;
    }

    {
        stonky::AtomicFileWriter first(target);
        if (!first.isOpen()) {
            std::cerr << first.error() << '\n';
            return 1;
        }
        first.stream() << "new";

        // Destroy the rejected writer while the owner is still active.  A
        // rejected instance used to unlink the owner's shared .writing file.
        {
            stonky::AtomicFileWriter concurrent(target);
            if (concurrent.isOpen()) {
                std::cerr << "Cross-process output lock did not exclude a second writer\n";
                return 1;
            }
        }
        if (!std::filesystem::is_regular_file(temporary)) {
            std::cerr << "Rejected writer removed the lock owner's temporary output\n";
            return 1;
        }

        std::string error;
        if (!first.commit(error)) {
            std::cerr << error << '\n';
            return 1;
        }
    }
    if (read(target) != "new") {
        std::cerr << "Committed transaction did not atomically replace the target\n";
        return 1;
    }
    if (std::filesystem::exists(temporary) || !std::filesystem::is_regular_file(lock)) {
        std::cerr << "Committed transaction left temporary output or lost its advisory lock file\n";
        return 1;
    }

    // Production writers are protected by the process-wide run guard and use
    // this mode so their published directories never retain sibling lock files.
    const auto noLockTarget = dir / "NO_LOCK.t6";
    {
        stonky::AtomicFileWriter noLock(
            noLockTarget, std::ios::binary, stonky::AtomicFileWriter::Locking::None);
        if (!noLock.isOpen()) {
            std::cerr << noLock.error() << '\n';
            return 1;
        }
        noLock.stream() << "without-lock-file";
        std::string error;
        if (!noLock.commit(error)) {
            std::cerr << error << '\n';
            return 1;
        }
    }
    auto noLockSibling = noLockTarget;
    noLockSibling += ".lock";
    if (read(noLockTarget) != "without-lock-file" ||
        std::filesystem::exists(noLockSibling)) {
        std::cerr << "Lock-free atomic publication created persistent lock state\n";
        return 1;
    }

#ifndef _WIN32
    // A hard process exit bypasses every C++ destructor.  The sibling output
    // stays partial, but the kernel releases the advisory lock.  A subsequent
    // writer must therefore be able to replace the stale sibling safely while
    // the committed target remains intact until that replacement commits.
    int readyPipe[2]{};
    if (::pipe(readyPipe) != 0) {
        std::cerr << "Could not create crash-test pipes\n";
        return 1;
    }
    const auto child = ::fork();
    if (child < 0) {
        std::cerr << "Could not fork crash-test process\n";
        return 1;
    }
    if (child == 0) {
        ::close(readyPipe[0]);
        stonky::AtomicFileWriter crashed(target);
        if (!crashed.isOpen()) {
            ::_exit(2);
        }
        crashed.stream() << "crash-partial";
        crashed.stream().flush();
        const char ready = 'R';
        if (::write(readyPipe[1], &ready, 1) != 1) {
            ::_exit(3);
        }
        for (;;) {
            ::pause();
        }
    }

    ::close(readyPipe[1]);
    char ready{};
    if (::read(readyPipe[0], &ready, 1) != 1 || ready != 'R') {
        std::cerr << "Crash-test child did not acquire the output lock\n";
        return 1;
    }
    bool liveProcessWasExcluded = true;
    {
        stonky::AtomicFileWriter blocked(target);
        if (blocked.isOpen()) {
            std::cerr << "Advisory lock did not exclude a live process\n";
            liveProcessWasExcluded = false;
        }
    }
    if (::kill(child, SIGKILL) != 0) {
        std::cerr << "Could not SIGKILL crash-test child\n";
        return 1;
    }
    ::close(readyPipe[0]);
    int childStatus{};
    if (::waitpid(child, &childStatus, 0) != child || !WIFSIGNALED(childStatus) ||
        WTERMSIG(childStatus) != SIGKILL) {
        std::cerr << "Crash-test child did not terminate through SIGKILL\n";
        return 1;
    }
    if (!liveProcessWasExcluded) {
        return 1;
    }
    if (read(target) != "new") {
        std::cerr << "Uncommitted child modified the existing target\n";
        return 1;
    }

    {
        stonky::AtomicFileWriter recovered(target);
        if (!recovered.isOpen()) {
            std::cerr << "Kernel did not release advisory lock after process exit: "
                      << recovered.error() << '\n';
            return 1;
        }
        recovered.stream() << "recovered";
        std::string error;
        if (!recovered.commit(error)) {
            std::cerr << "Could not commit after crashed writer: " << error << '\n';
            return 1;
        }
    }
    if (read(target) != "recovered" || std::filesystem::exists(temporary)) {
        std::cerr << "Writer did not recover safely after a crashed process\n";
        return 1;
    }
#endif

    std::error_code cleanupError;
    std::filesystem::remove_all(dir, cleanupError);
    return 0;
}
