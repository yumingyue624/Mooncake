#pragma once

#include "external_store_backend.h"

namespace mooncake::external_detail {
using Results = std::vector<tl::expected<void, ErrorCode>>;

// Scope-local cleanup for an optional external request. The native algorithm
// stays in Client; every exit drains an already-submitted external request.
class PendingExternal {
   public:
    PendingExternal(ExternalStoreBackend* backend, uint64_t timeout_ms);
    ~PendingExternal();
    PendingExternal(const PendingExternal&) = delete;
    PendingExternal& operator=(const PendingExternal&) = delete;

    void Put(const ObjectKey&, const std::vector<Slice>&);
    void BatchPut(const std::vector<ObjectKey>&,
                  const std::vector<std::vector<Slice>>&);
    void Remove(const ObjectKey&);
    void BatchRemove(const std::vector<ObjectKey>&);
    Results Finish(Results native);
    tl::expected<void, ErrorCode> Finish(tl::expected<void, ErrorCode> native);

   private:
    void Submit(tl::expected<void*, ErrorCode>, size_t count, bool write);
    Results Wait();
    ExternalStoreBackend* backend_;
    void* task_ = nullptr;
    uint64_t timeout_ms_;
    size_t count_ = 0;
    bool write_ = false;
    ErrorCode error_ = ErrorCode::INTERNAL_ERROR;
};

bool ValidWrite(const std::vector<ObjectKey>&,
                const std::vector<std::vector<Slice>>&);
}  // namespace mooncake::external_detail
