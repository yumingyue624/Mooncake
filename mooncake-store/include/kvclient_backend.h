#pragma once

#include <memory>

#include "external_store_backend.h"

namespace kv {
class KvClient;
}

namespace mooncake {

// Built only with MOONCAKE_WITH_KVCLIENT. SDK types stay in the implementation.
class KVClientBackend final : public ExternalStoreBackend {
   public:
    explicit KVClientBackend(std::shared_ptr<kv::KvClient> sdk);
    ~KVClientBackend() override;
    tl::expected<void*, ErrorCode> Put(
        const std::string& tenant, const ObjectKey& key,
        const std::vector<Slice>& slices) override;
    tl::expected<void*, ErrorCode> BatchPut(
        const std::string& tenant, const std::vector<ObjectKey>& keys,
        const std::vector<std::vector<Slice>>& batched_slices) override;
    tl::expected<void*, ErrorCode> Remove(const std::string& tenant,
                                          const ObjectKey& key) override;
    tl::expected<void*, ErrorCode> BatchRemove(
        const std::string& tenant, const std::vector<ObjectKey>& keys) override;
    std::vector<tl::expected<void, ErrorCode>> Wait(
        void* task, uint64_t timeout_ms) override;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
