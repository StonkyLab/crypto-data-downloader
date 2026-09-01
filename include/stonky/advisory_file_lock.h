/**
Crash-safe cross-process advisory file lock.

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/
#ifndef INCLUDE_STONKY_ADVISORY_FILE_LOCK_H
#define INCLUDE_STONKY_ADVISORY_FILE_LOCK_H

#include <filesystem>
#include <string>
#include <system_error>
#include <utility>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace stonky {

/**
 * Non-blocking exclusive lock backed by the operating system.
 *
 * The lock file is deliberately persistent and must not be unlinked.  Lock
 * ownership lives in the kernel handle/file description, so it is released on
 * normal destruction and also after SIGKILL, abort or process termination. An
 * existing directory is rejected fail-closed, which safely handles migration
 * from the former create-directory locking scheme without guessing whether an
 * older downloader still owns it.
 */
class AdvisoryFileLock {
public:
    AdvisoryFileLock() = default;

    explicit AdvisoryFileLock(std::filesystem::path path) {
        acquire(std::move(path));
    }

    AdvisoryFileLock(const AdvisoryFileLock &) = delete;
    AdvisoryFileLock &operator=(const AdvisoryFileLock &) = delete;
    AdvisoryFileLock(AdvisoryFileLock &&) = delete;
    AdvisoryFileLock &operator=(AdvisoryFileLock &&) = delete;

    ~AdvisoryFileLock() {
        release();
    }

    bool acquire(std::filesystem::path path) {
        if (ownsLock_) {
            error_ = "advisory lock is already held";
            return false;
        }
        path_ = std::move(path);
        error_.clear();
        contended_ = false;

#ifdef _WIN32
        handle_ = ::CreateFileW(path_.c_str(), GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
                                OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) {
            error_ = windowsError("cannot open advisory lock file", ::GetLastError());
            return false;
        }

        OVERLAPPED overlapped{};
        if (!::LockFileEx(handle_, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                          0, 1, 0, &overlapped)) {
            const auto code = ::GetLastError();
            contended_ = code == ERROR_LOCK_VIOLATION;
            error_ = windowsError(
                contended_ ? "advisory lock is already held"
                           : "cannot acquire advisory lock",
                code);
            ::CloseHandle(handle_);
            handle_ = INVALID_HANDLE_VALUE;
            return false;
        }
#else
        int flags = O_RDWR | O_CREAT;
#ifdef O_CLOEXEC
        flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
        flags |= O_NOFOLLOW;
#endif
        descriptor_ = ::open(path_.c_str(), flags, 0666);
        if (descriptor_ < 0) {
            error_ = posixError("cannot open advisory lock file", errno);
            return false;
        }

        struct stat status {};
        if (::fstat(descriptor_, &status) != 0) {
            const auto code = errno;
            error_ = posixError("cannot inspect advisory lock file", code);
            ::close(descriptor_);
            descriptor_ = -1;
            return false;
        }
        if (!S_ISREG(status.st_mode)) {
            error_ = "advisory lock path is not a regular file: " + path_.string();
            ::close(descriptor_);
            descriptor_ = -1;
            return false;
        }

        if (::flock(descriptor_, LOCK_EX | LOCK_NB) != 0) {
            const auto code = errno;
            contended_ = code == EWOULDBLOCK || code == EAGAIN;
            error_ = posixError(
                contended_ ? "advisory lock is already held"
                           : "cannot acquire advisory lock",
                code);
            ::close(descriptor_);
            descriptor_ = -1;
            return false;
        }
#endif

        ownsLock_ = true;
        return true;
    }

    [[nodiscard]] bool ownsLock() const noexcept { return ownsLock_; }

    /// True only when the lock failed because another owner holds it. Every
    /// other failure — an unwritable directory, a path that is not a regular
    /// file, any other errno — leaves this false, so a caller can tell "someone
    /// else is running" from "this lock could not be established at all".
    [[nodiscard]] bool contended() const noexcept { return contended_; }
    [[nodiscard]] const std::string &error() const noexcept { return error_; }
    [[nodiscard]] const std::filesystem::path &path() const noexcept { return path_; }

    /**
     * Release ownership but retain the persistent lock file.  Removing it
     * would allow two processes to lock different inodes under the same name.
     */
    bool release(std::string *error = nullptr) noexcept {
        if (!ownsLock_) {
            return true;
        }

        bool ok = true;
        std::string releaseError;
#ifdef _WIN32
        OVERLAPPED overlapped{};
        if (!::UnlockFileEx(handle_, 0, 1, 0, &overlapped)) {
            releaseError = windowsError("cannot release advisory lock", ::GetLastError());
            ok = false;
        }
        if (!::CloseHandle(handle_)) {
            if (!releaseError.empty()) {
                releaseError += "; ";
            }
            releaseError += windowsError("cannot close advisory lock file", ::GetLastError());
            ok = false;
        }
        handle_ = INVALID_HANDLE_VALUE;
#else
        if (::flock(descriptor_, LOCK_UN) != 0) {
            releaseError = posixError("cannot release advisory lock", errno);
            ok = false;
        }
        if (::close(descriptor_) != 0) {
            if (!releaseError.empty()) {
                releaseError += "; ";
            }
            releaseError += posixError("cannot close advisory lock file", errno);
            ok = false;
        }
        descriptor_ = -1;
#endif
        // Closing the OS handle releases the kernel lock even if the explicit
        // unlock operation reported an error.
        ownsLock_ = false;
        if (!ok && error) {
            *error = std::move(releaseError);
        }
        return ok;
    }

private:
#ifdef _WIN32
    std::string windowsError(const std::string &prefix, const unsigned long code) const {
        return prefix + " " + path_.string() + " (Windows error " +
               std::to_string(code) + ")";
    }
    HANDLE handle_{INVALID_HANDLE_VALUE};
#else
    std::string posixError(const std::string &prefix, const int code) const {
        return prefix + " " + path_.string() + ": " + std::strerror(code);
    }
    int descriptor_{-1};
#endif
    std::filesystem::path path_;
    std::string error_;
    bool ownsLock_{};
    bool contended_{};
};

} // namespace stonky

#endif // INCLUDE_STONKY_ADVISORY_FILE_LOCK_H
