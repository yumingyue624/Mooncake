#include "external_store_backend.h"

#include <fstream>
#include <json/json.h>
#include <glog/logging.h>

#ifdef MOONCAKE_WITH_KVCLIENT
#include "kv_client.h"
#include "kvclient_backend.h"
#endif

namespace mooncake {

tl::expected<std::shared_ptr<ExternalStoreBackend>, ErrorCode>
CreateExternalStoreBackend(const std::string& path, uint64_t& wait_timeout_ms) {
    if (path.empty()) return std::shared_ptr<ExternalStoreBackend>{};
    Json::Value config;
    Json::CharReaderBuilder reader;
    std::string errors;
    std::ifstream input(path);
    if (!input || !Json::parseFromStream(reader, input, &config, &errors) ||
        !config.isObject() || !config["backend"].isString()) {
        LOG(ERROR) << "Invalid external store config: " << path;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto name = config["backend"].asString();
    if (name == "none") return std::shared_ptr<ExternalStoreBackend>{};
    const auto timeout = config.get("wait_timeout_ms", Json::UInt64(30000));
    if (!timeout.isUInt64() || timeout.asUInt64() == 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    wait_timeout_ms = timeout.asUInt64();
#ifdef MOONCAKE_WITH_KVCLIENT
    if (name == "kvclient" && config["sdk_config_path"].isString()) {
        std::shared_ptr<kv::KvClient> sdk = kv::CreateKvClient();
        auto status = sdk->Init(config["sdk_config_path"].asString());
        if (!status.ok()) {
            LOG(ERROR) << "KVClient initialization failed: " << status.message;
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        // A task timeout is not a DMA barrier. Conservatively close the whole
        // SDK rather than relying on the asynchronous maxErrorCount drain.
        return std::shared_ptr<ExternalStoreBackend>(
            std::make_shared<KVClientBackend>(
                sdk, [sdk] { return sdk->Shutdown().ok(); }));
    }
#endif
    LOG(ERROR) << "External store backend unavailable: " << name;
    return tl::unexpected(ErrorCode::INVALID_PARAMS);
}

}  // namespace mooncake
