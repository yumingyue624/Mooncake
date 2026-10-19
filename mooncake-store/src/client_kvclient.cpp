#include "client_service.h"

#include <glog/logging.h>

#ifdef USE_KVCLIENT
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
    return SetKvClient(std::move(client));
#else
    if (!config_path.empty())
        LOG(ERROR) << "KvClient is configured but Mooncake was built without "
                      "USE_KVCLIENT";
    return tl::unexpected(ErrorCode::NOT_SUPPORTED);
#endif
}

tl::expected<void, ErrorCode> Client::SetKvClient(
    std::shared_ptr<kv::KvClient> client) {
    if (std::atomic_load(&kv_client_) == client) return {};
    auto io = MakeKvClientIO(client, tenant_id());
    if (client && !io) return tl::unexpected(ErrorCode::NOT_SUPPORTED);
    std::atomic_store(&kv_io_, std::move(io));
    std::atomic_store(&kv_client_, std::move(client));
    return {};
}

bool Client::HasKvClient() const { return bool(std::atomic_load(&kv_io_)); }

std::shared_ptr<kv::KvClient> Client::GetKvClient() const {
    return std::atomic_load(&kv_client_);
}

}  // namespace mooncake
