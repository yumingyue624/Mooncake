#include "external_store_operation.h"

#include <unordered_set>
#include <glog/logging.h>

namespace mooncake::external_detail {
void CheckResults(Results& results, size_t count) {
    if (results.size() != count) {
        results = Results(count, tl::unexpected(ErrorCode::INTERNAL_ERROR));
    }
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i])
            LOG(WARNING) << "External operation failed at index " << i << ": "
                         << results[i].error();
    }
}

Results MergePutResults(Results native, const Results& external) {
    if (native.size() != external.size()) {
        native = Results(external.size(), tl::unexpected(ErrorCode::INTERNAL_ERROR));
    }
    for (size_t i = 0; i < external.size(); ++i) {
        if (external[i]) native[i] = {};
    }
    return native;
}

bool ValidWrite(const std::vector<ObjectKey>& keys,
                const std::vector<std::vector<Slice>>& slices) {
    if (keys.empty() || keys.size() != slices.size()) return false;
    std::unordered_set<ObjectKey> seen;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (keys[i].empty() || slices[i].empty() ||
            !seen.insert(keys[i]).second)
            return false;
        size_t total = 0;
        for (const auto& slice : slices[i]) {
            if (!slice.ptr || !slice.size || slice.size > SIZE_MAX - total ||
                slice.size >
                    UINTPTR_MAX - reinterpret_cast<uintptr_t>(slice.ptr))
                return false;
            total += slice.size;
        }
    }
    return true;
}
}  // namespace mooncake::external_detail
