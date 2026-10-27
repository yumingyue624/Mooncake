#include "kvclient_backend.h"

#include <algorithm>
#include <map>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <glog/logging.h>
#include <xxhash.h>

#include "kv_client.h"
#include "tenant_id.h"

namespace mooncake {
namespace {
using Results = std::vector<tl::expected<void, ErrorCode>>;

ErrorCode ConvertStatus(const kv::Status& status) {
    switch (status.code) {
        case kv::StatusCode::OK:
            return ErrorCode::OK;
        case kv::StatusCode::INVALID_ARGUMENT:
        case kv::StatusCode::BUFFER_NOT_REGISTERED:
        case kv::StatusCode::BUFFER_NOT_SUPPORTED:
        case kv::StatusCode::UNSUPPORTED:
            return ErrorCode::INVALID_PARAMS;
        case kv::StatusCode::NOT_FOUND:
        case kv::StatusCode::CQE_KEY_NOT_EXIST:
        case kv::StatusCode::ENTRY_KEY_NOT_FOUND:
        case kv::StatusCode::ENTRY_DATA_NOT_EXIST:
            return ErrorCode::OBJECT_NOT_FOUND;
        case kv::StatusCode::TIMEOUT:
        case kv::StatusCode::CQE_IO_TIMEOUT:
            return ErrorCode::RPC_TIMEOUT;
        default:
            return ErrorCode::TRANSFER_FAIL;
    }
}

bool Contains(uintptr_t base, size_t capacity, uintptr_t ptr, size_t size) {
    return ptr >= base && size <= capacity && ptr - base <= capacity - size;
}
}  // namespace

struct KVClientBackend::Impl {
    struct Region {
        kv::RegisteredMemory memory;
        std::shared_ptr<const void> access_lease;
        size_t inflight = 0;
    };
    struct Task {
        kv::TaskId id = kv::kInvalidTaskId;
        bool remove = false;
        Results results;
        std::vector<size_t> indices;
        std::vector<std::shared_ptr<Region>> regions;
    };

    std::shared_ptr<kv::KvClient> sdk;
    std::mutex mutex;
    bool disabled = false;
    std::map<uintptr_t, std::shared_ptr<Region>> registered;
    std::unordered_map<void*, std::unique_ptr<Task>> tasks;
    // Prototype collision detection is process-local. It does not establish a
    // globally collision-free, persistent mapping for arbitrary string keys.
    std::map<kv::CacheKey, std::string> key_owners;

    tl::expected<kv::CacheKey, ErrorCode> Key(const std::string& tenant,
                                              const std::string& key) {
        if (key.empty()) return tl::unexpected(ErrorCode::INVALID_KEY);
        const auto identity = TenantId(tenant).MakeScopedKey(key);
        const auto digest = XXH3_64bits(identity.data(), identity.size());
        kv::CacheKey result;
        for (size_t i = 0; i < result.size(); ++i) {
            result[i] = static_cast<std::byte>((digest >> (i * 8)) & 0xff);
        }
        auto [it, inserted] = key_owners.emplace(result, identity);
        if (!inserted && it->second != identity) {
            return tl::unexpected(ErrorCode::INVALID_KEY);
        }
        return result;
    }

    void StopAccess() {
        disabled = true;
        bool stopped = false;
        try {
            stopped = sdk->Shutdown().ok();
        } catch (...) {
            stopped = false;
        }
        if (!stopped) {
            // Continuing would return a buffer potentially still targeted by
            // DMA. Do not turn an unverified stop into an ordinary error.
            LOG(FATAL) << "External SDK could not establish quiescence";
            std::terminate();
        }
    }

    tl::expected<void*, ErrorCode> Submit(
        const std::string& tenant, const std::vector<ObjectKey>& keys,
        const std::vector<std::vector<Slice>>* slices, bool batch) {
        std::lock_guard<std::mutex> lock(mutex);
        if (disabled)
            return tl::unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        if (keys.empty() || (slices && slices->size() != keys.size())) {
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto task = std::make_unique<Task>();
        task->remove = slices == nullptr;
        task->results =
            Results(keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        std::vector<kv::KVBuffer> entries;
        std::vector<kv::CacheKey> delete_keys;
        entries.reserve(keys.size());
        delete_keys.reserve(keys.size());
        task->indices.reserve(keys.size());
        task->regions.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            auto key = Key(tenant, keys[i]);
            if (!key) {
                task->results[i] = tl::unexpected(key.error());
                continue;
            }
            if (!slices) {
                delete_keys.push_back(*key);
                task->indices.push_back(i);
                continue;
            }
            // Offset zero is deliberately limited to one Slice per object.
            if ((*slices)[i].size() != 1) continue;
            const auto& slice = (*slices)[i][0];
            const auto ptr = reinterpret_cast<uintptr_t>(slice.ptr);
            constexpr size_t kMaxProtocolBufferBytes = 0xffffff;
            if (!slice.ptr || !slice.size ||
                slice.size > kMaxProtocolBufferBytes ||
                slice.size % kv::kAlignmentBytes)
                continue;
            auto it = registered.upper_bound(ptr);
            if (it == registered.begin()) continue;
            auto region = std::prev(it)->second;
            const auto& memory = region->memory;
            if (!Contains(memory.region.addr, memory.region.size, ptr,
                          slice.size))
                continue;
            if (memory.region.memoryType == kv::MemoryType::DEVICE &&
                ptr % kv::kAlignmentBytes)
                continue;
            kv::KVBuffer entry;
            entry.key = *key;
            entry.buffer = {memory.region, memory.handle};
            entry.buffer.region.addr = ptr;
            entry.buffer.region.size = slice.size;
            entry.offset = 0;
            // mrKey is resolved by the SDK from the registered MRHandle.
            entries.push_back(entry);
            task->indices.push_back(i);
            task->regions.push_back(region);
        }
        // Publish ownership before SDK submission: allocation of the task map
        // must not fail after the SDK has started reading caller memory.
        void* token = &task->id;
        auto inserted = tasks.emplace(token, std::move(task)).first;
        auto& submitted = *inserted->second;
        for (auto& region : submitted.regions) ++region->inflight;
        if (!submitted.indices.empty()) {
            kv::Status status;
            try {
                if (submitted.remove) {
                    status = sdk->DeleteAsync(delete_keys, submitted.id);
                } else if (batch) {
                    status = sdk->BatchStoreAsync(entries, submitted.id);
                } else {
                    status = sdk->StoreAsync(entries, submitted.id);
                }
            } catch (...) {
                StopAccess();
                status = kv::Status::Error(kv::StatusCode::INTERNAL_ERROR,
                                           "SDK submission threw");
            }
            if (!status.ok() && submitted.id == kv::kInvalidTaskId) {
                for (auto i : submitted.indices) {
                    submitted.results[i] =
                        tl::unexpected(ConvertStatus(status));
                }
                for (auto& region : submitted.regions) --region->inflight;
                submitted.regions.clear();
                submitted.indices.clear();
            }
        }
        return token;
    }
};

KVClientBackend::KVClientBackend(std::shared_ptr<kv::KvClient> sdk)
    : impl_(std::make_unique<Impl>()) {
    if (!sdk) throw std::invalid_argument("KVClient SDK is required");
    impl_->sdk = std::move(sdk);
}

KVClientBackend::~KVClientBackend() {
    // SDK teardown precedes destruction of registration/task bookkeeping.
    if (!impl_->disabled) impl_->StopAccess();
}

tl::expected<void*, ErrorCode> KVClientBackend::Put(
    const std::string& tenant, const ObjectKey& key,
    const std::vector<Slice>& slices) {
    const std::vector<std::vector<Slice>> batch{slices};
    return impl_->Submit(tenant, {key}, &batch, false);
}

tl::expected<void*, ErrorCode> KVClientBackend::BatchPut(
    const std::string& tenant, const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& slices) {
    return impl_->Submit(tenant, keys, &slices, true);
}

tl::expected<void*, ErrorCode> KVClientBackend::Remove(
    const std::string& tenant, const ObjectKey& key) {
    return impl_->Submit(tenant, {key}, nullptr, false);
}

tl::expected<void*, ErrorCode> KVClientBackend::BatchRemove(
    const std::string& tenant, const std::vector<ObjectKey>& keys) {
    return impl_->Submit(tenant, keys, nullptr, true);
}

std::vector<tl::expected<void, ErrorCode>> KVClientBackend::Wait(
    void* token, uint64_t timeout_ms) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    auto it = impl_->tasks.find(token);
    if (it == impl_->tasks.end())
        return {tl::unexpected(ErrorCode::INVALID_PARAMS)};
    auto& task = *it->second;
    if (task.id != kv::kInvalidTaskId) {
        kv::TaskResult result;
        auto status =
            impl_->disabled
                ? kv::Status::Error(kv::StatusCode::CANCELED, "SDK stopped")
                : impl_->sdk->Wait(task.id, timeout_ms, result);
        if (status.ok() && !result.status.ok()) status = result.status;
        // The SDK's timeout removes the task record before its asynchronous
        // channel drain finishes. Close all SDK channels before releasing MRs.
        if (!status.ok() && !task.remove && !impl_->disabled)
            impl_->StopAccess();
        for (size_t i = 0; i < task.indices.size(); ++i) {
            auto entry_status =
                i < result.entryStatus.size()
                    ? result.entryStatus[i]
                    : kv::Status::Error(kv::StatusCode::INTERNAL_ERROR,
                                        "missing SDK entry result");
            if (!status.ok() && (result.entryStatus.empty() ||
                                 status.code == kv::StatusCode::TIMEOUT ||
                                 status.code == kv::StatusCode::CANCELED))
                entry_status = status;
            const auto error = ConvertStatus(entry_status);
            task.results[task.indices[i]] =
                error == ErrorCode::OK
                    ? tl::expected<void, ErrorCode>{}
                    : tl::expected<void, ErrorCode>{tl::unexpected(error)};
        }
    }
    for (auto& region : task.regions) --region->inflight;
    auto results = std::move(task.results);
    impl_->tasks.erase(it);
    return results;
}

}  // namespace mooncake
