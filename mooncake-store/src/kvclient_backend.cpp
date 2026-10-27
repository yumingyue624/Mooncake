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

    tl::expected<kv::CacheKey, ErrorCode> EncodeKey(const std::string& tenant,
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

    std::shared_ptr<Region> FindRegion(const Slice& slice) {
        const auto ptr = reinterpret_cast<uintptr_t>(slice.ptr);
        constexpr size_t kMaxProtocolBufferBytes = 0xffffff;
        if (!slice.ptr || !slice.size || slice.size > kMaxProtocolBufferBytes ||
            slice.size % kv::kAlignmentBytes)
            return nullptr;
        auto it = registered.upper_bound(ptr);
        if (it == registered.begin()) return nullptr;
        auto region = std::prev(it)->second;
        const auto& memory = region->memory;
        if (!Contains(memory.region.addr, memory.region.size, ptr,
                      slice.size) ||
            (memory.region.memoryType == kv::MemoryType::DEVICE &&
             ptr % kv::kAlignmentBytes))
            return nullptr;
        return region;
    }

    Task& TrackTask(std::unique_ptr<Task> task) {
        // Establish ownership before the SDK can access caller memory.
        void* token = &task->id;
        auto& tracked = *tasks.emplace(token, std::move(task)).first->second;
        for (auto& region : tracked.regions) ++region->inflight;
        return tracked;
    }

    void* FinishSubmission(Task& task, const kv::Status& status) {
        if (!status.ok() && task.id == kv::kInvalidTaskId) {
            for (auto index : task.indices)
                task.results[index] = tl::unexpected(ConvertStatus(status));
            for (auto& region : task.regions) --region->inflight;
            task.regions.clear();
            task.indices.clear();
        }
        return &task.id;
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
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->disabled)
        return tl::unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    auto external_key = impl_->EncodeKey(tenant, key);
    if (!external_key) return tl::unexpected(external_key.error());
    if (slices.size() != 1) return tl::unexpected(ErrorCode::INVALID_PARAMS);
    auto region = impl_->FindRegion(slices[0]);
    if (!region) return tl::unexpected(ErrorCode::INVALID_PARAMS);

    kv::KVBuffer entry;
    entry.key = *external_key;
    entry.buffer = {region->memory.region, region->memory.handle};
    entry.buffer.region.addr = reinterpret_cast<uintptr_t>(slices[0].ptr);
    entry.buffer.region.size = slices[0].size;
    entry.offset = 0;
    // The SDK resolves mrKey from the pre-registered MRHandle.
    std::vector<kv::KVBuffer> entries{entry};
    auto pending = std::make_unique<Impl::Task>();
    pending->results = Results(1, tl::unexpected(ErrorCode::INVALID_PARAMS));
    pending->indices = {0};
    pending->regions = {region};
    auto& task = impl_->TrackTask(std::move(pending));
    kv::Status status;
    try {
        status = impl_->sdk->StoreAsync(entries, task.id);
    } catch (...) {
        impl_->StopAccess();
        status = kv::Status::Error(kv::StatusCode::INTERNAL_ERROR,
                                   "SDK StoreAsync threw");
    }
    return impl_->FinishSubmission(task, status);
}

tl::expected<void*, ErrorCode> KVClientBackend::BatchPut(
    const std::string& tenant, const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->disabled)
        return tl::unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    if (keys.empty() || keys.size() != batched_slices.size())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);

    auto pending = std::make_unique<Impl::Task>();
    pending->results =
        Results(keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    std::vector<kv::KVBuffer> entries;
    entries.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto external_key = impl_->EncodeKey(tenant, keys[i]);
        if (!external_key) {
            pending->results[i] = tl::unexpected(external_key.error());
            continue;
        }
        if (batched_slices[i].size() != 1) continue;
        const auto& slice = batched_slices[i][0];
        auto region = impl_->FindRegion(slice);
        if (!region) continue;
        kv::KVBuffer entry;
        entry.key = *external_key;
        entry.buffer = {region->memory.region, region->memory.handle};
        entry.buffer.region.addr = reinterpret_cast<uintptr_t>(slice.ptr);
        entry.buffer.region.size = slice.size;
        entry.offset = 0;
        entries.push_back(entry);
        pending->indices.push_back(i);
        pending->regions.push_back(region);
    }
    auto& task = impl_->TrackTask(std::move(pending));
    kv::Status status;
    if (!entries.empty()) {
        try {
            status = impl_->sdk->BatchStoreAsync(entries, task.id);
        } catch (...) {
            impl_->StopAccess();
            status = kv::Status::Error(kv::StatusCode::INTERNAL_ERROR,
                                       "SDK BatchStoreAsync threw");
        }
    }
    return impl_->FinishSubmission(task, status);
}

tl::expected<void*, ErrorCode> KVClientBackend::Remove(
    const std::string& tenant, const ObjectKey& key) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->disabled)
        return tl::unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    auto external_key = impl_->EncodeKey(tenant, key);
    if (!external_key) return tl::unexpected(external_key.error());
    std::vector<kv::CacheKey> keys{*external_key};
    auto pending = std::make_unique<Impl::Task>();
    pending->remove = true;
    pending->results = Results(1, tl::unexpected(ErrorCode::INVALID_PARAMS));
    pending->indices = {0};
    auto& task = impl_->TrackTask(std::move(pending));
    kv::Status status;
    try {
        status = impl_->sdk->DeleteAsync(keys, task.id);
    } catch (...) {
        impl_->StopAccess();
        status = kv::Status::Error(kv::StatusCode::INTERNAL_ERROR,
                                   "SDK DeleteAsync threw");
    }
    return impl_->FinishSubmission(task, status);
}

tl::expected<void*, ErrorCode> KVClientBackend::BatchRemove(
    const std::string& tenant, const std::vector<ObjectKey>& keys) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->disabled)
        return tl::unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    if (keys.empty()) return tl::unexpected(ErrorCode::INVALID_PARAMS);
    auto pending = std::make_unique<Impl::Task>();
    pending->remove = true;
    pending->results =
        Results(keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    std::vector<kv::CacheKey> external_keys;
    external_keys.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto external_key = impl_->EncodeKey(tenant, keys[i]);
        if (!external_key) {
            pending->results[i] = tl::unexpected(external_key.error());
            continue;
        }
        external_keys.push_back(*external_key);
        pending->indices.push_back(i);
    }
    auto& task = impl_->TrackTask(std::move(pending));
    kv::Status status;
    if (!external_keys.empty()) {
        try {
            status = impl_->sdk->DeleteAsync(external_keys, task.id);
        } catch (...) {
            impl_->StopAccess();
            status = kv::Status::Error(kv::StatusCode::INTERNAL_ERROR,
                                       "SDK DeleteAsync threw");
        }
    }
    return impl_->FinishSubmission(task, status);
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
