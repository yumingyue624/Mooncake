#include "client_service.h"

#include <cstdlib>
#include <stdexcept>
#include <glog/logging.h>
#include "device/accelerator_registry.h"

namespace mooncake {
namespace {
using Result = tl::expected<void, ErrorCode>;

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

}  // namespace mooncake
