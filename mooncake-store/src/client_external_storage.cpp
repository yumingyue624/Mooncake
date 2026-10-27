#include "client_service.h"

#include <cstdlib>
#include <stdexcept>
#include <glog/logging.h>

namespace mooncake {
void Client::SetExternalStoreBackend(
    std::shared_ptr<ExternalStoreBackend> backend, uint64_t wait_timeout_ms) {
    std::lock_guard<std::recursive_mutex> lock(external_store_mutex_);
    if (!wait_timeout_ms) {
        throw std::invalid_argument("External wait timeout must be positive");
    }
    external_store_backend_ = std::move(backend);
    external_wait_timeout_ms_ = wait_timeout_ms;
}

tl::expected<void, ErrorCode> Client::InitExternalStoreBackend() {
    const char* path = std::getenv("MOONCAKE_EXTERNAL_STORE_CONFIG");
    if (!path || !*path) return {};
    auto backend = CreateExternalStoreBackend(path, external_wait_timeout_ms_);
    if (!backend) return tl::unexpected(backend.error());
    external_store_backend_ = std::move(*backend);
    return {};
}

}  // namespace mooncake
