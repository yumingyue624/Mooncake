#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct MemoryRegion {
    const void* base = nullptr;
    uint64_t capacity = 0;
    std::string memory_domain;
    int32_t device_id = -1;
    std::shared_ptr<const void> access_lease;
};

struct BufferInfo {
    uint64_t capacity = 0;
    std::string memory_domain;
    int32_t device_id = -1;
};

class StorageBuffer {
   public:
    virtual ~StorageBuffer() = default;
    virtual BufferInfo GetInfo() const = 0;
};

class ExternalStoreBackend {
   public:
    virtual ~ExternalStoreBackend() = default;
    virtual tl::expected<void*, ErrorCode> Put(const ObjectKey&,
                                               const std::vector<Slice>&) = 0;
    virtual tl::expected<void*, ErrorCode> BatchPut(
        const std::vector<ObjectKey>&,
        const std::vector<std::vector<Slice>>&) = 0;
    virtual tl::expected<void*, ErrorCode> Remove(const ObjectKey&) = 0;
    virtual tl::expected<void*, ErrorCode> BatchRemove(
        const std::vector<ObjectKey>&) = 0;
    // Consumes a task returned by this backend exactly once. Results retain
    // input order. Return only once the source is no longer being accessed.
    virtual std::vector<tl::expected<void, ErrorCode>> Wait(
        void* task, uint64_t timeout_ms) = 0;
};

// Empty path selects native-only. An unknown/unavailable backend fails startup.
tl::expected<std::shared_ptr<ExternalStoreBackend>, ErrorCode>
CreateExternalStoreBackend(const std::string& path, const std::string& tenant,
                           uint64_t& wait_timeout_ms);

}  // namespace mooncake
