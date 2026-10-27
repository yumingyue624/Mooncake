#include "external_store_operation.h"

#include <iostream>
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
    tl::expected<void*, ErrorCode> Put(const ObjectKey&,
                                       const std::vector<Slice>&) override {
        return Submit();
    }
    tl::expected<void*, ErrorCode> BatchPut(
        const std::vector<ObjectKey>&,
        const std::vector<std::vector<Slice>>&) override {
        return Submit();
    }
    tl::expected<void*, ErrorCode> Remove(const ObjectKey&) override {
        return Submit();
    }
    tl::expected<void*, ErrorCode> BatchRemove(
        const std::vector<ObjectKey>&) override {
        return Submit();
    }
    Results Wait(void* task, uint64_t) override {
        Require(task == &token);
        events += "W";
        return results;
    }
    tl::expected<void, ErrorCode> RegisterRegions(
        const std::vector<MemoryRegion>&,
        std::vector<std::shared_ptr<const StorageBuffer>>&) override {
        return {};
    }
    tl::expected<void, ErrorCode> UnregisterRegions(
        const std::vector<std::shared_ptr<const StorageBuffer>>&) override {
        return {};
    }

   private:
    tl::expected<void*, ErrorCode> Submit() {
        events += "S";
        if (reject) return tl::unexpected(ErrorCode::TRANSFER_FAIL);
        return static_cast<void*>(&token);
    }
};

int main() {
    int data = 0;
    const std::vector<ObjectKey> keys{"A", "B", "C", "D"};
    const std::vector<std::vector<Slice>> slices(4, {{&data, sizeof(data)}});
    auto error =
        tl::expected<void, ErrorCode>{tl::unexpected(ErrorCode::WRITE_FAIL)};
    FakeBackend backend;
    backend.results = {{}, error, {}, error};
    auto result = external_detail::Write(backend, keys, slices, 1, false, [&] {
        backend.events += "N";
        return Results{{}, {}, error, error};
    });
    Require(backend.events == "SNW");
    Require(result.size() == 4 && result[0] && result[1] && result[2] &&
            !result[3]);

    backend.events.clear();
    auto removed = external_detail::Remove(backend, keys, 1, false, [&] {
        backend.events += "N";
        return Results{error, {}, error, {}};
    });
    Require(backend.events == "SNW");
    Require(!removed[0] && removed[1] && !removed[2] && removed[3]);

    backend.events.clear();
    try {
        external_detail::Write(backend, keys, slices, 1, false,
                               [&]() -> Results {
                                   backend.events += "N";
                                   throw std::runtime_error("native exception");
                               });
        Require(false);
    } catch (const std::runtime_error&) {
        Require(backend.events == "SNW");
    }

    backend.events.clear();
    backend.reject = true;
    result = external_detail::Write(backend, keys, slices, 1, false, [&] {
        backend.events += "N";
        return Results{{}, error, {}, error};
    });
    Require(backend.events == "SN");
    Require(result[0] && !result[1] && result[2] && !result[3]);

    backend.reject = false;
    backend
        .results = {};  // Malformed backend response must not invent success.
    result = external_detail::Write(backend, keys, slices, 1, false,
                                    [&] { return Results(4, error); });
    for (const auto& entry : result) Require(!entry);
    std::cout << "external operation tests passed\n";
}
