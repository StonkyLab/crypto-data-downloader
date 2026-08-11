/**
CSV Archive File Naming Helpers

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_CSV_ARCHIVE_H
#define INCLUDE_STONKY_CSV_ARCHIVE_H

#include <array>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

namespace stonky {

/**
 * Return the original CSV stem for a plain CSV or an individually compressed
 * CSV whose filename still ends in `.csv.<codec>`. Container archives (tar,
 * arbitrary zip files) are intentionally not guessed because a filename does
 * not identify which symbols they contain.
 */
inline std::optional<std::string> csvStemFromArchivePath(const std::filesystem::path &path) {
    static constexpr std::array<std::string_view, 5> suffixes{
        ".csv", ".csv.gz", ".csv.xz", ".csv.bz2", ".csv.zst"
    };

    const std::string name = path.filename().string();
    for (const auto suffix : suffixes) {
        if (name.size() > suffix.size() && name.ends_with(suffix)) {
            return name.substr(0, name.size() - suffix.size());
        }
    }
    return std::nullopt;
}

} // namespace stonky

#endif // INCLUDE_STONKY_CSV_ARCHIVE_H
