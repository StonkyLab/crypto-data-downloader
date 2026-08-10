#include "stonky/atomic_file.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

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
    if (std::filesystem::exists(temporary) || std::filesystem::exists(lock)) {
        std::cerr << "Abandoned transaction did not clean up its owned artifacts\n";
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
    if (std::filesystem::exists(temporary) || std::filesystem::exists(lock)) {
        std::cerr << "Committed transaction left temporary artifacts behind\n";
        return 1;
    }

    {
        stonky::AtomicFileWriter cleanupFailure(target);
        if (!cleanupFailure.isOpen()) {
            std::cerr << cleanupFailure.error() << '\n';
            return 1;
        }
        cleanupFailure.stream() << "committed despite lock cleanup failure";
        const auto blocker = lock / "blocker";
        {
            std::ofstream blockRemoval(blocker, std::ios::binary);
            blockRemoval << "x";
        }
        std::string error;
        if (cleanupFailure.commit(error) ||
            error.find("output was committed, but lock cleanup failed") == std::string::npos ||
            read(target) != "committed despite lock cleanup failure") {
            std::cerr << "Post-commit lock cleanup failure was not reported accurately\n";
            return 1;
        }
        std::filesystem::remove(blocker);
        // Destructor retries releaseLock after the transient blocker is gone.
    }
    if (std::filesystem::exists(lock)) {
        std::cerr << "Destructor did not retry a failed lock cleanup\n";
        return 1;
    }

    std::error_code cleanupError;
    std::filesystem::remove_all(dir, cleanupError);
    return 0;
}
