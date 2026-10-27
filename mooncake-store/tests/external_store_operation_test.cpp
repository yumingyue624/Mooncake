#include "external_store_operation.h"

#include <iostream>
#include <functional>
#include <stdexcept>
#include <string>

using namespace mooncake;
using mooncake::external_detail::Results;

static void Require(bool ok) {
    if (!ok) throw std::runtime_error("external operation assertion failed");
}

class FakeBackend final : public ExternalStoreBackend {
   public:
    std::string events;
    Results results;
    bool reject = false;
    int token = 1;
    tl::expected<void*, ErrorCode> Put(const std::string& tenant,
                                       const ObjectKey&,
                                       const std::vector<Slice>&) override {
        Require(tenant == "tenant-a");
        return Submit();
    }
    tl::expected<void*, ErrorCode> BatchPut(
        const std::string& tenant, const std::vector<ObjectKey>&,
        const std::vector<std::vector<Slice>>&) override {
        Require(tenant == "tenant-a");
        return Submit();
    }
    tl::expected<void*, ErrorCode> Remove(const std::string& tenant,
                                          const ObjectKey&) override {
        Require(tenant == "tenant-a");
        return Submit();
    }
    tl::expected<void*, ErrorCode> BatchRemove(
        const std::string& tenant, const std::vector<ObjectKey>&) override {
        Require(tenant == "tenant-a");
        return Submit();
    }
    Results Wait(void* task, uint64_t) override {
        Require(task == &token);
        events += "W";
        return results;
    }

   private:
    tl::expected<void*, ErrorCode> Submit() {
        events += "S";
        if (reject) return tl::unexpected(ErrorCode::TRANSFER_FAIL);
        return static_cast<void*>(&token);
    }
};

static Results RunWrite(ExternalStoreBackend& backend,
                        const std::vector<ObjectKey>& keys,
                        const std::vector<std::vector<Slice>>& slices,
                        uint64_t timeout, bool single,
                        const std::function<Results()>& native) {
    external_detail::PendingExternal pending(&backend, timeout, "tenant-a");
    if (single)
        pending.Put(keys[0], slices[0]);
    else
        pending.BatchPut(keys, slices);
    return pending.Finish(native());
}

static Results RunRemove(ExternalStoreBackend& backend,
                         const std::vector<ObjectKey>& keys, uint64_t timeout,
                         bool single, const std::function<Results()>& native) {
    external_detail::PendingExternal pending(&backend, timeout, "tenant-a");
    if (single)
        pending.Remove(keys[0]);
    else
        pending.BatchRemove(keys);
    return pending.Finish(native());
}

int main() {
    int data = 0;
    const std::vector<ObjectKey> keys{"A", "B", "C", "D"};
    const std::vector<std::vector<Slice>> slices(4, {{&data, sizeof(data)}});
    auto error =
        tl::expected<void, ErrorCode>{tl::unexpected(ErrorCode::WRITE_FAIL)};
    FakeBackend backend;
    backend.results = {{}, error, {}, error};
    auto result = RunWrite(backend, keys, slices, 1, false, [&] {
        backend.events += "N";
        return Results{{}, {}, error, error};
    });
    Require(backend.events == "SNW");
    Require(result.size() == 4 && result[0] && result[1] && result[2] &&
            !result[3]);

    backend.events.clear();
    auto removed = RunRemove(backend, keys, 1, false, [&] {
        backend.events += "N";
        return Results{error, {}, error, {}};
    });
    Require(backend.events == "SNW");
    Require(!removed[0] && removed[1] && !removed[2] && removed[3]);

    backend.events.clear();
    try {
        RunWrite(backend, keys, slices, 1, false, [&]() -> Results {
            backend.events += "N";
            throw std::runtime_error("native exception");
        });
        Require(false);
    } catch (const std::runtime_error&) {
        Require(backend.events == "SNW");
    }

    backend.events.clear();
    backend.reject = true;
    result = RunWrite(backend, keys, slices, 1, false, [&] {
        backend.events += "N";
        return Results{{}, error, {}, error};
    });
    Require(backend.events == "SN");
    Require(result[0] && !result[1] && result[2] && !result[3]);

    backend.reject = false;
    backend
        .results = {};  // Malformed backend response must not invent success.
    result = RunWrite(backend, keys, slices, 1, false,
                      [&] { return Results(4, error); });
    for (const auto& entry : result) Require(!entry);
    backend.results = {{}};
    {
        external_detail::PendingExternal pending(&backend, 1, "tenant-a");
        pending.Put("single", {{&data, sizeof(data)}});
        Require(bool(pending.Finish(tl::unexpected(ErrorCode::WRITE_FAIL))));
    }
    {
        external_detail::PendingExternal pending(nullptr, 1);
        pending.Put("native-only", {{&data, sizeof(data)}});
        Require(!pending.Finish(tl::unexpected(ErrorCode::WRITE_FAIL)));
    }
    std::cout << "external operation tests passed\n";
}
