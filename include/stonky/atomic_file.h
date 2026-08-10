/**
Transactional sibling-file output with cross-process exclusion.

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_ATOMIC_FILE_H
#define INCLUDE_STONKY_ATOMIC_FILE_H

#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <utility>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace stonky {

class AtomicFileWriter {
public:
    explicit AtomicFileWriter(std::filesystem::path target,
                              const std::ios::openmode mode = std::ios::binary)
        : target_(std::move(target)), temporary_(target_), lock_(target_) {
        temporary_ += ".writing";
        lock_ += ".lock";

        std::error_code ec;
        locked_ = std::filesystem::create_directory(lock_, ec);
        if (!locked_) {
            error_ = ec ? "cannot create output lock: " + ec.message()
                        : "output lock already exists: " + lock_.string();
            return;
        }

        // Only the lock owner may create or remove the shared sibling.  In
        // particular, a second writer that failed to acquire the lock must not
        // delete the first writer's in-progress file from its destructor.
        ownsTemporary_ = true;
        stream_.open(temporary_, std::ios::trunc | mode);
        if (!stream_.is_open()) {
            error_ = "cannot create temporary output: " + temporary_.string();
            releaseLock();
        }
    }

    AtomicFileWriter(const AtomicFileWriter &) = delete;
    AtomicFileWriter &operator=(const AtomicFileWriter &) = delete;
    AtomicFileWriter(AtomicFileWriter &&) = delete;
    AtomicFileWriter &operator=(AtomicFileWriter &&) = delete;

    ~AtomicFileWriter() {
        if (stream_.is_open()) {
            stream_.close();
        }
        if (ownsTemporary_ && !committed_) {
            std::error_code ec;
            std::filesystem::remove(temporary_, ec);
        }
        releaseLock();
    }

    [[nodiscard]] bool isOpen() const { return stream_.is_open(); }
    [[nodiscard]] const std::string &error() const { return error_; }
    std::ofstream &stream() { return stream_; }

    bool commit(std::string &error) {
        if (!stream_.is_open()) {
            error = error_.empty() ? "temporary output is not open" : error_;
            return false;
        }
        stream_.flush();
        if (!stream_.good()) {
            error = "failed to flush temporary output: " + temporary_.string();
            return false;
        }
        stream_.close();
        if (!stream_.good()) {
            error = "failed to close temporary output: " + temporary_.string();
            return false;
        }

#ifdef _WIN32
        if (!::MoveFileExW(temporary_.c_str(), target_.c_str(),
                           MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
            error = "failed to replace output (Windows error " +
                    std::to_string(static_cast<unsigned long>(::GetLastError())) + ")";
            return false;
        }
#else
        std::error_code ec;
        std::filesystem::rename(temporary_, target_, ec);
        if (ec) {
            error = "failed to replace output: " + ec.message();
            return false;
        }
#endif
        committed_ = true;
        ownsTemporary_ = false;
        std::string lockError;
        if (!releaseLock(&lockError)) {
            error = "output was committed, but lock cleanup failed: " + lockError;
            return false;
        }
        return true;
    }

private:
    bool releaseLock(std::string *error = nullptr) noexcept {
        if (!locked_) {
            return true;
        }

        std::error_code ec;
        const bool removed = std::filesystem::remove(lock_, ec);
        if (removed || !ec) {
            locked_ = false;
            return true;
        }
        if (error) {
            *error = "cannot remove " + lock_.string() + ": " + ec.message();
        }
        // Keep ownership set so a later destructor call can retry a transient
        // cleanup failure instead of silently abandoning a stale lock.
        return false;
    }

    std::filesystem::path target_;
    std::filesystem::path temporary_;
    std::filesystem::path lock_;
    std::ofstream stream_;
    std::string error_;
    bool locked_{};
    bool ownsTemporary_{};
    bool committed_{};
};

} // namespace stonky

#endif // INCLUDE_STONKY_ATOMIC_FILE_H
