#include "external_store_operation.h"

#include <exception>
#include <unordered_set>
#include <glog/logging.h>

namespace mooncake::external_detail {
PendingExternal::PendingExternal(ExternalStoreBackend* backend,
                                 uint64_t timeout_ms)
    : backend_(backend), timeout_ms_(timeout_ms) {}

PendingExternal::~PendingExternal() {
    if (!task_) return;
    try {
        Wait();
    } catch (...) {
        std::terminate();
    }
}

void PendingExternal::Submit(tl::expected<void*, ErrorCode> submitted,
                             size_t count, bool write) {
    count_ = count;
    write_ = write;
    if (submitted && *submitted) {
        task_ = *submitted;
    } else {
        error_ = submitted ? ErrorCode::INTERNAL_ERROR : submitted.error();
        LOG(WARNING) << "External submission failed: " << error_;
    }
}

void PendingExternal::Put(const ObjectKey& key,
                          const std::vector<Slice>& slices) {
    if (backend_) Submit(backend_->Put(key, slices), 1, true);
}

void PendingExternal::BatchPut(const std::vector<ObjectKey>& keys,
                               const std::vector<std::vector<Slice>>& slices) {
    if (backend_) Submit(backend_->BatchPut(keys, slices), keys.size(), true);
}

void PendingExternal::Remove(const ObjectKey& key) {
    if (backend_) Submit(backend_->Remove(key), 1, false);
}

void PendingExternal::BatchRemove(const std::vector<ObjectKey>& keys) {
    if (backend_) Submit(backend_->BatchRemove(keys), keys.size(), false);
}

Results PendingExternal::Wait() {
    if (!task_) return Results(count_, tl::unexpected(error_));
    auto results = backend_->Wait(task_, timeout_ms_);
    task_ = nullptr;
    if (results.size() != count_) {
        return Results(count_, tl::unexpected(ErrorCode::INTERNAL_ERROR));
    }
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i])
            LOG(WARNING) << "External operation failed at index " << i << ": "
                         << results[i].error();
    }
    return results;
}

Results PendingExternal::Finish(Results native) {
    if (!backend_ || !count_) return native;
    auto external = Wait();
    if (!write_) return native;
    if (native.size() != count_) {
        native = Results(count_, tl::unexpected(ErrorCode::INTERNAL_ERROR));
    }
    for (size_t i = 0; i < count_; ++i) {
        if (external[i]) native[i] = {};
    }
    return native;
}

tl::expected<void, ErrorCode> PendingExternal::Finish(
    tl::expected<void, ErrorCode> native) {
    if (!backend_ || !count_) return native;
    return Finish(Results{std::move(native)})[0];
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
