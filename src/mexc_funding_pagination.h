/**
Fail-closed retry policy for MEXC funding-history page 1.

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/
#ifndef STONKY_MEXC_FUNDING_PAGINATION_H
#define STONKY_MEXC_FUNDING_PAGINATION_H

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace stonky::mexc_funding_pagination {

struct RetryPolicy {
    std::int32_t maxAttempts{3};
    std::chrono::milliseconds backoff{std::chrono::seconds{3}};
};

enum class EmptyFirstPageDecision {
    Retry,
    AcceptAuthoritativeNoOp,
    RejectInconsistentMetadata,
    RejectUnverifiedEmpty,
};

struct SnapshotMetadata {
    std::int32_t pageSize{};
    std::int32_t totalCount{};
    std::int32_t totalPage{};

    friend bool operator==(const SnapshotMetadata &, const SnapshotMetadata &) = default;
};

/** Validate one non-empty page and require stable metadata across the scan. */
inline bool validatePageMetadata(
    const std::int32_t requestedPage, const std::int32_t requestedPageSize,
    const std::size_t resultCount, const std::int32_t responsePage,
    const std::int32_t responsePageSize, const std::int32_t totalCount,
    const std::int32_t totalPage, std::optional<SnapshotMetadata> &snapshot,
    std::string &error) {
    if (requestedPage <= 0 || requestedPageSize <= 0 || responsePage != requestedPage) {
        error = "MEXC funding response currentPage does not match the requested page";
        return false;
    }
    if (responsePageSize <= 0 || responsePageSize > requestedPageSize ||
        totalCount <= 0 || totalPage <= 0) {
        error = "MEXC funding response has invalid page-size/count metadata";
        return false;
    }

    const auto count = static_cast<std::int64_t>(totalCount);
    const auto size = static_cast<std::int64_t>(responsePageSize);
    const auto expectedTotalPage = (count + size - 1) / size;
    if (expectedTotalPage != totalPage || requestedPage > totalPage) {
        error = "MEXC funding response totalPage is inconsistent with totalCount/pageSize";
        return false;
    }

    const auto offset = static_cast<std::int64_t>(requestedPage - 1) * size;
    const auto remaining = count - offset;
    const auto expectedRows = remaining < size ? remaining : size;
    if (remaining <= 0 || resultCount != static_cast<std::size_t>(expectedRows)) {
        error = "MEXC funding response row count is inconsistent with pagination metadata";
        return false;
    }

    const SnapshotMetadata current{responsePageSize, totalCount, totalPage};
    if (snapshot && *snapshot != current) {
        error = "MEXC funding pagination metadata changed during the scan";
        return false;
    }
    snapshot = current;
    return true;
}

enum class ScanDecision {
    Continue,
    Complete,
    RejectMissingBaseOverlap,
};

/**
 * A fully validated newest-first page proves that every later page is older
 * once its oldest row is below the inclusive download cutoff.  Equality must
 * remain part of the retained history, so it does not stop the scan by itself.
 */
inline bool validatedPageCrossesCutoff(const std::int64_t oldestPageTimestamp,
                                       const std::int64_t inclusiveCutoff) {
    return oldestPageTimestamp < inclusiveCutoff;
}

/**
 * An established append transaction is complete only after seeing its exact
 * local tail in the remote history.  Reaching the advertised final page is not
 * proof: totalPage may itself be transiently truncated.
 */
inline ScanDecision decideScanProgress(const bool establishedBase,
                                       const bool foundExactBaseTail,
                                       const std::int32_t currentPage,
                                       const std::int32_t totalPage) {
    if (establishedBase && foundExactBaseTail) {
        return ScanDecision::Complete;
    }
    if (currentPage < totalPage) {
        return ScanDecision::Continue;
    }
    return establishedBase ? ScanDecision::RejectMissingBaseOverlap
                           : ScanDecision::Complete;
}

/**
 * Decide what an empty page 1 means without performing I/O.  totalPage and
 * totalCount must both be zero for an empty result to be internally
 * consistent.  Even then it is only a retryable observation, not evidence
 * that an existing local tail is current.
 */
inline EmptyFirstPageDecision decideEmptyFirstPage(
    const std::int32_t totalPage, const std::int32_t totalCount,
    const std::int32_t attempt, const RetryPolicy &policy,
    const bool baseIsAuthoritativelyCurrent = false) {
    if (totalPage != 0 || totalCount != 0) {
        return EmptyFirstPageDecision::RejectInconsistentMetadata;
    }
    if (attempt < policy.maxAttempts) {
        return EmptyFirstPageDecision::Retry;
    }
    if (baseIsAuthoritativelyCurrent) {
        return EmptyFirstPageDecision::AcceptAuthoritativeNoOp;
    }
    return EmptyFirstPageDecision::RejectUnverifiedEmpty;
}

/**
 * Fetch page 1 with injected fetch/sleep functions so retry behaviour is
 * deterministic in tests.  Response must expose resultList, totalPage and
 * totalCount like MEXC HistoricalFundingRates.
 */
template<typename Fetch, typename Sleep>
auto fetchFirstPage(Fetch &&fetch, Sleep &&sleep, const RetryPolicy &policy = {},
                    const bool baseIsAuthoritativelyCurrent = false,
                    const std::int32_t expectedPageSize = 1000)
    -> std::invoke_result_t<Fetch &> {
    if (policy.maxAttempts <= 0 || policy.backoff.count() < 0 || expectedPageSize <= 0) {
        throw std::invalid_argument("invalid MEXC funding page-1 retry policy");
    }

    for (std::int32_t attempt = 1; attempt <= policy.maxAttempts; ++attempt) {
        auto response = std::invoke(fetch);
        const bool pageIdentityValid = response.currentPage == 1 && response.pageSize > 0 &&
                                       response.pageSize <= expectedPageSize;
        if (!response.resultList.empty() && !pageIdentityValid) {
            throw std::runtime_error(
                "MEXC funding page-1 identity/pageSize metadata is invalid for the request");
        }
        if (!response.resultList.empty()) {
            return response;
        }

        switch (decideEmptyFirstPage(response.totalPage, response.totalCount, attempt, policy,
                                     baseIsAuthoritativelyCurrent)) {
            case EmptyFirstPageDecision::Retry:
                std::invoke(sleep, policy.backoff, attempt + 1);
                break;
            case EmptyFirstPageDecision::AcceptAuthoritativeNoOp:
                if (!pageIdentityValid) {
                    throw std::runtime_error(
                        "MEXC funding empty page-1 identity/pageSize metadata is invalid");
                }
                return response;
            case EmptyFirstPageDecision::RejectInconsistentMetadata:
                throw std::runtime_error(
                    "MEXC returned empty funding page 1 with inconsistent pagination metadata "
                    "(totalPage=" + std::to_string(response.totalPage) +
                    ", totalCount=" + std::to_string(response.totalCount) + ")");
            case EmptyFirstPageDecision::RejectUnverifiedEmpty:
                throw std::runtime_error(
                    "MEXC funding page 1 remained empty after " +
                    std::to_string(policy.maxAttempts) +
                    " attempts; refusing to report unverified data as current");
        }
    }

    throw std::logic_error("unreachable MEXC funding page-1 retry state");
}

} // namespace stonky::mexc_funding_pagination

#endif // STONKY_MEXC_FUNDING_PAGINATION_H
