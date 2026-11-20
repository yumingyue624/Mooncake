#include "client_service.h"

#include <utility>
#include <glog/logging.h>
#include <ylt/struct_json/json_reader.h>

#include "environ.h"
#include "environment_variables.h"
#include "storage/external/external_store_backend_factory.h"

namespace mooncake {

tl::expected<void, ErrorCode>
Client::InitExternalStoreBackendFromEnvironment() {
    using Variables = ExternalStoreEnvironmentVariables;
    const auto backend_type =
        Environ::ReadOr(Variables::MC_EXTERNAL_STORE_BACKEND, std::string{});
    if (backend_type.empty()) return {};
    std::unordered_map<std::string, std::string> options;
    try {
        struct_json::from_json(
            options, Environ::ReadOr(Variables::MC_EXTERNAL_STORE_OPTIONS,
                                     std::string{"{}"}));
    } catch (const std::exception& error) {
        LOG(ERROR) << "Invalid MC_EXTERNAL_STORE_OPTIONS: " << error.what();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto backend = CreateExternalStoreBackend(backend_type);
    if (!backend) {
        LOG(ERROR) << "Failed to create external store backend: "
                   << toString(backend.error());
        return tl::unexpected(backend.error());
    }
    auto initialized =
        (*backend)->Init(options, TenantId(master_client_.tenant_id()));
    if (!initialized) {
        LOG(ERROR) << "Failed to initialize external store backend: "
                   << toString(initialized.error());
        return tl::unexpected(initialized.error());
    }
    external_store_backend_ = std::move(*backend);
    LOG(INFO) << "External store backend initialized successfully: "
              << backend_type;
    return {};
}

bool Client::ShouldGetFromExternalStore(ErrorCode query_error) const {
    return external_store_backend_ &&
           (query_error == ErrorCode::OBJECT_NOT_FOUND ||
            query_error == ErrorCode::REPLICA_IS_NOT_READY);
}

tl::expected<ExternalStoreTaskGuard, ErrorCode> Client::BatchGetExternalAsync(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& targets) {
    if (keys.empty()) return ExternalStoreTaskGuard{};
    if (!external_store_backend_)
        return tl::unexpected(ErrorCode::NOT_SUPPORTED);
    auto submitted = external_store_backend_->BatchGetAsync(keys, targets);
    if (!submitted) return tl::unexpected(submitted.error());
    return ExternalStoreTaskGuard(external_store_backend_.get(), *submitted);
}

tl::expected<ExternalStoreTaskGuard, ErrorCode>
Client::BatchGetExternalRangesAsync(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& targets,
    const std::vector<std::vector<size_t>>& source_offsets) {
    if (keys.empty()) return ExternalStoreTaskGuard{};
    if (!external_store_backend_)
        return tl::unexpected(ErrorCode::NOT_SUPPORTED);
    auto submitted = external_store_backend_->BatchGetRangesAsync(
        keys, targets, source_offsets);
    if (!submitted) return tl::unexpected(submitted.error());
    return ExternalStoreTaskGuard(external_store_backend_.get(), *submitted);
}

tl::expected<ExternalStoreTaskGuard, ErrorCode>
Client::BatchExistsExternalAsync(const std::vector<ObjectKey>& keys) {
    if (keys.empty()) return ExternalStoreTaskGuard{};
    if (!external_store_backend_)
        return tl::unexpected(ErrorCode::NOT_SUPPORTED);
    auto submitted = external_store_backend_->BatchExistsAsync(keys);
    if (!submitted) return tl::unexpected(submitted.error());
    return ExternalStoreTaskGuard(external_store_backend_.get(), *submitted);
}

std::vector<tl::expected<bool, ErrorCode>> Client::BatchExistsExternal(
    const std::vector<ObjectKey>& keys) {
    if (keys.empty()) return {};
    auto submitted = BatchExistsExternalAsync(keys);
    if (!submitted) {
        return std::vector<tl::expected<bool, ErrorCode>>(
            keys.size(), tl::unexpected(submitted.error()));
    }
    ExternalStoreBackend::TaskOutput output;
    auto statuses = submitted->Wait(output);
    std::vector<tl::expected<bool, ErrorCode>> results(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (statuses[i]) {
            results[i] = output.exists[i] != 0;
        } else {
            results[i] = tl::unexpected(statuses[i].error());
        }
    }
    return results;
}

tl::expected<ExternalStoreTaskGuard, ErrorCode> Client::BatchPutExternalAsync(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& sources) {
    if (keys.empty() || !external_store_backend_)
        return ExternalStoreTaskGuard{};
    auto submitted = external_store_backend_->BatchPutAsync(keys, sources);
    if (!submitted) return tl::unexpected(submitted.error());
    return ExternalStoreTaskGuard(external_store_backend_.get(), *submitted);
}

tl::expected<ExternalStoreTaskGuard, ErrorCode>
Client::BatchPutExternalRangesAsync(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& sources,
    const std::vector<std::vector<size_t>>& destination_offsets) {
    if (keys.empty() || !external_store_backend_)
        return ExternalStoreTaskGuard{};
    auto submitted = external_store_backend_->BatchPutRangesAsync(
        keys, sources, destination_offsets);
    if (!submitted) return tl::unexpected(submitted.error());
    return ExternalStoreTaskGuard(external_store_backend_.get(), *submitted);
}

tl::expected<ExternalStoreTaskGuard, ErrorCode>
Client::BatchRemoveExternalAsync(const std::vector<ObjectKey>& keys) {
    if (keys.empty() || !external_store_backend_)
        return ExternalStoreTaskGuard{};
    auto submitted = external_store_backend_->BatchRemoveAsync(keys);
    if (!submitted) return tl::unexpected(submitted.error());
    return ExternalStoreTaskGuard(external_store_backend_.get(), *submitted);
}

tl::expected<void, ErrorCode> Client::CompleteExternalPut(
    tl::expected<ExternalStoreTaskGuard, ErrorCode>& submission,
    tl::expected<void, ErrorCode> native_result) {
    CompleteExternalPut(submission, std::span(&native_result, 1));
    return native_result;
}

// A key succeeds if either storage path succeeds; always wait for submitted
// work.
void Client::CompleteExternalPut(
    tl::expected<ExternalStoreTaskGuard, ErrorCode>& submission,
    std::span<tl::expected<void, ErrorCode>> native_results) {
    if (!submission) {
        LOG(WARNING) << "External store submission failed: "
                     << submission.error();
        return;
    }
    if (!*submission) return;
    ExternalStoreBackend::TaskOutput output;
    auto external = submission->Wait(output);
    if (external.size() != native_results.size()) {
        LOG(ERROR) << "External store result count mismatch: expected="
                   << native_results.size() << ", actual=" << external.size();
        return;
    }
    for (size_t i = 0; i < external.size(); ++i) {
        if (!external[i]) {
            LOG(WARNING) << "External store operation failed at index=" << i
                         << ": " << external[i].error();
        } else {
            native_results[i] = {};
        }
    }
}

std::vector<tl::expected<void, ErrorCode>> Client::CompleteExternalRangePut(
    tl::expected<ExternalStoreTaskGuard, ErrorCode>& submission,
    const std::vector<ObjectKey>& keys) {
    if (!submission) {
        LOG(WARNING) << "External range write submission failed, keys="
                     << keys.size() << ", error=" << submission.error();
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(submission.error()));
    }
    if (*submission) {
        ExternalStoreBackend::TaskOutput output;
        auto results = submission->Wait(output);
        if (results.size() != keys.size()) {
            LOG(ERROR)
                << "External range write result count mismatch: expected="
                << keys.size() << ", actual=" << results.size();
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::RPC_FAIL));
        }
        for (size_t i = 0; i < results.size(); ++i) {
            if (!results[i]) {
                LOG(WARNING)
                    << "External range write failed, key=" << keys[i]
                    << ", index=" << i << ", error=" << results[i].error();
            }
        }
        return results;
    }
    return std::vector<tl::expected<void, ErrorCode>>(
        keys.size(), tl::unexpected(ErrorCode::NOT_SUPPORTED));
}

// Drain external deletes for diagnostics; callers return the native result.
void Client::CompleteExternalRemove(
    tl::expected<ExternalStoreTaskGuard, ErrorCode>& submission) {
    if (!submission) {
        LOG(WARNING) << "External delete submission failed: "
                     << submission.error();
    } else if (*submission) {
        ExternalStoreBackend::TaskOutput output;
        auto results = submission->Wait(output);
        for (size_t i = 0; i < results.size(); ++i) {
            if (!results[i]) {
                LOG(WARNING) << "External delete failed at index=" << i << ": "
                             << results[i].error();
            }
        }
    }
}

}  // namespace mooncake
