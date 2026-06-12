/**
CSV Data File Integrity Helpers

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/csv_data.h"
#include "stonky/utils/utils.h"
#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>

namespace stonky {

namespace {
bool parseTimestamp(const std::string& field, std::int64_t& out) {
    if (field.empty() || field.size() > 19) {
        return false;
    }
    std::size_t pos = 0;
    if (field[0] == '-') {
        return false;
    }
    try {
        out = std::stoll(field, &pos);
    } catch (const std::exception&) {
        return false;
    }
    return pos == field.size();
}

bool isValidRecord(const std::string& line, const std::size_t expectedFields, const bool allowMoreFields,
                   std::int64_t& tsOut) {
    const auto records = splitString(line, ',');
    if (allowMoreFields ? records.size() < expectedFields : records.size() != expectedFields) {
        return false;
    }
    return parseTimestamp(records[0], tsOut);
}
} // namespace

CsvData::TailCheck CsvData::lastValidRecord(const std::string& path, const std::size_t expectedFields,
                                            const std::int64_t fallback, const bool allowMoreFields) {
    TailCheck result;
    result.timestamp = fallback;

    std::error_code ec;
    const auto fileSize = std::filesystem::file_size(path, ec);
    if (ec || fileSize == 0) {
        return result;
    }

    // Read a tail window; expand if no valid record is found within it.
    std::size_t chunk = 256 * 1024;
    while (true) {
        const std::size_t readStart = fileSize > chunk ? fileSize - chunk : 0;
        const std::size_t readLen = fileSize - readStart;

        std::ifstream ifs(path, std::ios::binary);
        if (!ifs.is_open()) {
            spdlog::error(fmt::format("Couldn't open file: {}", path));
            return result;
        }
        ifs.seekg(static_cast<std::streamoff>(readStart));
        std::string buf(readLen, '\0');
        ifs.read(buf.data(), static_cast<std::streamsize>(readLen));
        ifs.close();

        // Walk lines backwards. `lineEnd` is one past the end of the current
        // line's content (exclusive of '\n').
        std::size_t searchEnd = buf.size(); // current line content ends here
        bool sawTrailingNewline = !buf.empty() && buf.back() == '\n';
        if (sawTrailingNewline) {
            searchEnd = buf.size() - 1;
        }

        while (true) {
            const auto nlPos = (searchEnd == 0) ? std::string::npos : buf.rfind('\n', searchEnd - 1);
            const std::size_t lineStart = (nlPos == std::string::npos) ? 0 : nlPos + 1;

            // Line spanning past the window start — expand the window.
            if (lineStart == 0 && readStart > 0) {
                break;
            }

            const std::string line = buf.substr(lineStart, searchEnd - lineStart);
            const bool isTerminated =
                (readStart + searchEnd < fileSize) || sawTrailingNewline;

            std::int64_t ts = 0;
            if (isTerminated && isValidRecord(line, expectedFields, allowMoreFields, ts)) {
                result.timestamp = ts;
                result.foundValid = true;

                // Truncate any trailing invalid bytes after this record's newline.
                const std::size_t validEnd = readStart + searchEnd + 1; // include '\n'
                if (validEnd < fileSize) {
                    std::filesystem::resize_file(path, validEnd, ec);
                    if (ec) {
                        spdlog::error(fmt::format("Failed to truncate torn tail of {}: {}", path, ec.message()));
                    } else {
                        result.repairedTail = true;
                        spdlog::warn(fmt::format("Repaired torn tail of {} ({} bytes truncated)",
                                                 path, fileSize - validEnd));
                    }
                }
                return result;
            }

            // Reached the first line of the file without finding a valid record.
            // Preserve it (the header) and truncate everything after it.
            if (lineStart == 0) {
                const auto headerNl = buf.find('\n');
                const std::size_t keep = (headerNl == std::string::npos) ? fileSize : headerNl + 1;
                if (keep < fileSize) {
                    std::filesystem::resize_file(path, keep, ec);
                    if (ec) {
                        spdlog::error(fmt::format("Failed to truncate corrupt {}: {}", path, ec.message()));
                    } else {
                        result.repairedTail = true;
                        spdlog::warn(fmt::format("No valid record in {}, truncated to header ({} bytes removed)",
                                                 path, fileSize - keep));
                    }
                }
                return result;
            }

            searchEnd = nlPos; // continue with the previous line
        }

        // Only reachable via the inner-loop break: a line spans past the window
        // start (which implies readStart > 0) — grow the window and rescan.
        chunk *= 4;
    }
}

} // namespace stonky
