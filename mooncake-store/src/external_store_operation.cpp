#include "external_store_operation.h"

#include <exception>
#include <glog/logging.h>

namespace mooncake::external_detail {
namespace {
class PendingExternal {
   public:
    PendingExternal(ExternalStoreBackend& backend,
                    tl::expected<void*, ErrorCode> submitted,
                    uint64_t timeout_ms, size_t count)
        : backend_(backend), timeout_ms_(timeout_ms), count_(count) {
        if (submitted && *submitted) {
            task_ = *submitted;
        } else {
            error_ = submitted ? ErrorCode::INTERNAL_ERROR : submitted.error();
            LOG(WARNING) << "External submission failed: " << error_;
        }
    }
    ~PendingExternal() {
        if (!task_) return;
        try {
            Finish();
        } catch (...) {
            std::terminate();
        }
    }
    Results Finish() {
        if (!task_) return Results(count_, tl::unexpected(error_));
        auto results = backend_.Wait(task_, timeout_ms_);
        task_ = nullptr;
        if (results.size() != count_) {
            return Results(count_, tl::unexpected(ErrorCode::INTERNAL_ERROR));
        }
        for (size_t i = 0; i < results.size(); ++i) {
            if (!results[i])
                LOG(WARNING) << "External operation failed at index " << i
                             << ": " << results[i].error();
        }
        return results;
    }

   private:
    ExternalStoreBackend& backend_;
    void* task_ = nullptr;
    uint64_t timeout_ms_;
    size_t count_;
    ErrorCode error_ = ErrorCode::INTERNAL_ERROR;
};
}  // namespace

Results Write(ExternalStoreBackend& backend, const std::vector<ObjectKey>& keys,
              const std::vector<std::vector<Slice>>& slices,
              uint64_t timeout_ms, bool single,
              const std::function<Results()>& native) {
    PendingExternal pending(backend,
                            single ? backend.Put(keys.at(0), slices.at(0))
                                   : backend.BatchPut(keys, slices),
                            timeout_ms, keys.size());
    auto native_results = native();
    auto external_results = pending.Finish();
    if (native_results.size() != keys.size()) {
        native_results =
            Results(keys.size(), tl::unexpected(ErrorCode::INTERNAL_ERROR));
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        if (external_results[i]) native_results[i] = {};
    }
    return native_results;
}

Results Remove(ExternalStoreBackend& backend,
               const std::vector<ObjectKey>& keys, uint64_t timeout_ms,
               bool single, const std::function<Results()>& native) {
    PendingExternal pending(
        backend,
        single ? backend.Remove(keys.at(0)) : backend.BatchRemove(keys),
        timeout_ms, keys.size());
    auto native_results = native();
    (void)pending.Finish();
    return native_results;
}
}  // namespace mooncake::external_detail
