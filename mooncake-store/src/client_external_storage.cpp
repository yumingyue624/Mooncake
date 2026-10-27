#include "client_service.h"
#include "external_store_operation.h"

#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <glog/logging.h>
#include "device/accelerator_registry.h"

namespace mooncake {
namespace {
using Result = tl::expected<void, ErrorCode>;
using Results = std::vector<Result>;

bool Valid(const std::vector<ObjectKey>& keys,
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
}  // namespace

void Client::SetExternalStoreBackend(
    std::shared_ptr<ExternalStoreBackend> backend, uint64_t wait_timeout_ms) {
    std::lock_guard<std::recursive_mutex> lock(external_store_mutex_);
    if (!external_regions_.empty() || !wait_timeout_ms) {
        throw std::invalid_argument(
            "Configure external backend before registration");
    }
    external_store_backend_ = std::move(backend);
    external_wait_timeout_ms_ = wait_timeout_ms;
}

tl::expected<void, ErrorCode> Client::InitExternalStoreBackend() {
    const char* path = std::getenv("MOONCAKE_EXTERNAL_STORE_CONFIG");
    if (!path || !*path) return {};
    auto backend = CreateExternalStoreBackend(path, tenant_id(),
                                              external_wait_timeout_ms_);
    if (!backend) return tl::unexpected(backend.error());
    external_store_backend_ = std::move(*backend);
    return {};
}

tl::expected<void, ErrorCode> Client::RegisterExternalMemory(void* addr,
                                                             size_t size) {
    std::lock_guard<std::recursive_mutex> lock(external_store_mutex_);
    if (!external_store_backend_) return {};
    MemoryRegion region{addr, size, "host", -1, {}};
    device::PointerInfo info;
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators(true);
    if (runtime.FindDeviceForPointer(addr, &info)) {
        region.memory_domain =
            info.kind == device::MemoryKind::kDevice ? "device" : "host";
        region.device_id = info.device_id;
    }
    auto it = external_regions_.find(addr);
    if (it != external_regions_.end()) {
        const auto registered_info = it->second->GetInfo();
        return registered_info.capacity == size &&
                       registered_info.device_id == region.device_id &&
                       registered_info.memory_domain == region.memory_domain
                   ? Result{}
                   : Result{tl::unexpected(ErrorCode::INVALID_PARAMS)};
    }
    std::vector<std::shared_ptr<const StorageBuffer>> registered;
    auto result =
        external_store_backend_->RegisterRegions({region}, registered);
    if (!result) return result;
    if (registered.size() != 1)
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    external_regions_.emplace(addr, std::move(registered[0]));
    return {};
}

tl::expected<void, ErrorCode> Client::Put(const ObjectKey& key,
                                          std::vector<Slice>& slices,
                                          const ReplicateConfig& config) {
    if (!external_store_backend_) return PutNative(key, slices, config);
    std::lock_guard<std::recursive_mutex> lock(external_store_mutex_);
    if (!Valid({key}, {slices}))
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    return external_detail::Write(
        *external_store_backend_, {key}, {slices}, external_wait_timeout_ms_,
        true, [&] { return Results{PutNative(key, slices, config)}; })[0];
}

tl::expected<void, ErrorCode> Client::Upsert(const ObjectKey& key,
                                             std::vector<Slice>& slices,
                                             const ReplicateConfig& config) {
    if (!external_store_backend_) return UpsertNative(key, slices, config);
    std::lock_guard<std::recursive_mutex> lock(external_store_mutex_);
    if (!Valid({key}, {slices}))
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    return external_detail::Write(
        *external_store_backend_, {key}, {slices}, external_wait_timeout_ms_,
        true, [&] { return Results{UpsertNative(key, slices, config)}; })[0];
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys, std::vector<std::vector<Slice>>& slices,
    const ReplicateConfig& config, const WriteBufferStager& stager) {
    if (!external_store_backend_)
        return BatchPutNative(keys, slices, config, stager);
    std::lock_guard<std::recursive_mutex> lock(external_store_mutex_);
    if (stager || !Valid(keys, slices)) {
        return Results(keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    return external_detail::Write(
        *external_store_backend_, keys, slices, external_wait_timeout_ms_,
        false, [&] { return BatchPutNative(keys, slices, config, {}); });
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchUpsert(
    const std::vector<ObjectKey>& keys, std::vector<std::vector<Slice>>& slices,
    const ReplicateConfig& config, const WriteBufferStager& stager) {
    if (!external_store_backend_)
        return BatchUpsertNative(keys, slices, config, stager);
    std::lock_guard<std::recursive_mutex> lock(external_store_mutex_);
    if (stager || !Valid(keys, slices)) {
        return Results(keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    return external_detail::Write(
        *external_store_backend_, keys, slices, external_wait_timeout_ms_,
        false, [&] { return BatchUpsertNative(keys, slices, config, {}); });
}

tl::expected<void, ErrorCode> Client::Remove(const ObjectKey& key, bool force) {
    if (!external_store_backend_) return RemoveNative(key, force);
    std::lock_guard<std::recursive_mutex> lock(external_store_mutex_);
    if (key.empty()) return tl::unexpected(ErrorCode::INVALID_KEY);
    return external_detail::Remove(
        *external_store_backend_, {key}, external_wait_timeout_ms_, true,
        [&] { return Results{RemoveNative(key, force)}; })[0];
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchRemove(
    const std::vector<ObjectKey>& keys, bool force) {
    if (!external_store_backend_) return BatchRemoveNative(keys, force);
    std::lock_guard<std::recursive_mutex> lock(external_store_mutex_);
    return external_detail::Remove(
        *external_store_backend_, keys, external_wait_timeout_ms_, false,
        [&] { return BatchRemoveNative(keys, force); });
}

}  // namespace mooncake
