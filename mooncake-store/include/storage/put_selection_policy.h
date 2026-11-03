#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct ReplicateConfig;

enum class PutKind { PUT, UPSERT, RANGE };

struct PutSelectionContext {
    const ObjectKey& key;
    const TenantId& tenant_id;
    PutKind kind;
    uint64_t write_bytes;
    size_t slice_count;
    // Range writes do not carry a ReplicateConfig.
    const ReplicateConfig* config;
    std::span<const size_t> destination_offsets;
};

// true selects an external write; native writes always run. An empty policy or
// an error keeps dual writing. Called concurrently, once per input key, before
// submission. Implementations must not throw or retain references to context.
// All fragments of an aggregated object must receive consistent decisions.
using PutSelectionPolicy =
    std::function<tl::expected<bool, ErrorCode>(const PutSelectionContext&)>;

}  // namespace mooncake
