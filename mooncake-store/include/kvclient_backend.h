#pragma once

#include <functional>
#include <memory>

#include "external_store_backend.h"

namespace kv {
class KvClient;
}

namespace mooncake {

// Built only with MOONCAKE_WITH_KVCLIENT. The SDK remains outside the public
// ExternalStoreBackend interface. Quiesce must synchronously stop all SDK DMA;
// a failed barrier is fatal rather than returning an unsafe caller buffer.
class KVClientBackend final : public ExternalStoreBackend {
   public:
    using Quiesce = std::function<bool()>;
    KVClientBackend(std::shared_ptr<kv::KvClient> sdk, Quiesce quiesce);
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
