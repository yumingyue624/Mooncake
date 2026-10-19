#include "client_service.h"

#ifdef USE_KVCLIENT
#include <glog/logging.h>
#include <kv_client.h>
#endif

namespace mooncake {

tl::expected<void, ErrorCode> Client::InitKvClient(
    const std::string& config_path) {
#ifdef USE_KVCLIENT
    if (config_path.empty()) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto client = kv::CreateKvClient();
    if (!client) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const auto status = client->Init(config_path);
    if (!status.ok()) {
        LOG(ERROR) << "KvClient initialization failed: " << status.message;
        return tl::unexpected(status.code == kv::StatusCode::INVALID_ARGUMENT
                                  ? ErrorCode::INVALID_PARAMS
                                  : ErrorCode::INTERNAL_ERROR);
    }
    SetKvClient(std::move(client));
    return {};
#else
    (void)config_path;
    return tl::unexpected(ErrorCode::NOT_SUPPORTED);
#endif
}

void Client::SetKvClient(std::shared_ptr<kv::KvClient> client) {
    std::atomic_store(&kv_client_, std::move(client));
}

std::shared_ptr<kv::KvClient> Client::GetKvClient() const {
    return std::atomic_load(&kv_client_);
}

}  // namespace mooncake
