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
    tl::expected<void*, ErrorCode> Put(const std::string& tenant,
                                       const ObjectKey&,
                                       const std::vector<Slice>&) override;
    tl::expected<void*, ErrorCode> BatchPut(
        const std::string& tenant, const std::vector<ObjectKey>&,
        const std::vector<std::vector<Slice>>&) override;
    tl::expected<void*, ErrorCode> Remove(const std::string& tenant,
                                          const ObjectKey&) override;
    tl::expected<void*, ErrorCode> BatchRemove(
        const std::string& tenant, const std::vector<ObjectKey>&) override;
    std::vector<tl::expected<void, ErrorCode>> Wait(void*, uint64_t) override;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
