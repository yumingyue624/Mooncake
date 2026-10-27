#pragma once

#include "external_store_backend.h"

namespace mooncake::external_detail {
using Results = std::vector<tl::expected<void, ErrorCode>>;

void CheckResults(Results& results, size_t count);
Results MergePutResults(Results native, const Results& external);
bool ValidWrite(const std::vector<ObjectKey>&,
                const std::vector<std::vector<Slice>>&);
}  // namespace mooncake::external_detail
