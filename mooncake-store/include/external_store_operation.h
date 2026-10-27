#pragma once

#include <functional>
#include "external_store_backend.h"

namespace mooncake::external_detail {
using Results = std::vector<tl::expected<void, ErrorCode>>;

Results Write(ExternalStoreBackend& backend, const std::vector<ObjectKey>& keys,
              const std::vector<std::vector<Slice>>& slices,
              uint64_t timeout_ms, bool single,
              const std::function<Results()>& native);

Results Remove(ExternalStoreBackend& backend,
               const std::vector<ObjectKey>& keys, uint64_t timeout_ms,
               bool single, const std::function<Results()>& native);
}  // namespace mooncake::external_detail
