#include "kvclient_backend.h"
#include "kv_client.h"

#include <iostream>
#include <stdexcept>

using namespace mooncake;
static void Require(bool ok) {
    if (!ok) throw std::runtime_error("KVClient backend assertion failed");
}

class FakeSdk final : public kv::KvClient {
   public:
    int registers = 0, stores = 0, waits = 0, deletes = 0, stops = 0;
    bool reject = false, timeout = false;
    std::vector<kv::KVBuffer> entries;
    std::vector<kv::CacheKey> removed;
    kv::Status Init(const kv::KvClientConfig&) override { return {}; }
    kv::Status Init(const std::string&) override { return {}; }
    kv::Status Shutdown() override {
        ++stops;
        return {};
    }
    kv::Status QueryAsync(const std::vector<kv::CacheKey>&,
                          kv::TaskId&) override {
        return {};
    }
    kv::Status LoadAsync(const std::vector<kv::KVBuffer>&,
                         kv::TaskId&) override {
        return {};
    }
    kv::Status BatchLoadAsync(const std::vector<kv::KVBuffer>&,
                              kv::TaskId&) override {
        return {};
    }
    kv::Status StoreAsync(const std::vector<kv::KVBuffer>& e,
                          kv::TaskId& id) override {
        ++stores;
        entries = e;
        if (reject)
            return kv::Status::Error(kv::StatusCode::RESOURCE_BUSY, "busy");
        id = 7;
        return {};
    }
    kv::Status StoreAsync(const std::vector<kv::KVBuffer>& e, kv::TaskId& id,
                          uintptr_t) override {
        return StoreAsync(e, id);
    }
    kv::Status BatchStoreAsync(const std::vector<kv::KVBuffer>& e,
                               kv::TaskId& id) override {
        return StoreAsync(e, id);
    }
    kv::Status BatchStoreAsync(const std::vector<kv::KVBuffer>& e,
                               kv::TaskId& id, uintptr_t) override {
        return StoreAsync(e, id);
    }
    kv::Status DeleteAsync(const std::vector<kv::CacheKey>& keys,
                           kv::TaskId& id) override {
        ++deletes;
        removed = keys;
        id = 8;
        return {};
    }
    bool Check(kv::TaskId) override { return true; }
    kv::Status Wait(kv::TaskId id, uint64_t, kv::TaskResult& result) override {
        ++waits;
        if (timeout)
            return kv::Status::Error(kv::StatusCode::TIMEOUT, "timeout");
        result.entryStatus.assign(id == 8 ? removed.size() : entries.size(),
                                  kv::Status::OK());
        return {};
    }
    kv::Status RegisterRegions(
        const std::vector<kv::MemoryRegion>& regions,
        std::vector<kv::RegisteredMemory>& out) override {
        ++registers;
        for (const auto& region : regions) out.push_back({region, 42, 11});
        return {};
    }
    kv::Status UnregisterRegions(const std::vector<kv::MRHandle>&) override {
        return {};
    }
};

int main() {
    auto sdk = std::make_shared<FakeSdk>();
    int barriers = 0;
    auto barrier = [&] {
        ++barriers;
        return sdk->Shutdown().ok();
    };
    alignas(512) char data[2048]{};
    KVClientBackend backend(sdk, "tenant-a", barrier);
    std::vector<std::shared_ptr<const StorageBuffer>> regions;
    Require(bool(backend.RegisterRegions({{data, sizeof(data), "host", -1, {}}},
                                         regions)));
    auto pending =
        backend.BatchPut({"A", "B", "C"}, {{{data + 512, 512}},
                                           {{data, 512}, {data + 512, 512}},
                                           {{data + 1024, 512}}});
    Require(bool(pending));
    Require(sdk->stores == 1 && sdk->entries.size() == 2);
    Require(sdk->entries[0].offset == 0 && sdk->entries[0].buffer.handle == 42);
    Require(sdk->entries[0].buffer.region.addr ==
            reinterpret_cast<uintptr_t>(data + 512));
    Require(!sdk->entries[0].mrKey.has_value());
    Require(
        !backend.UnregisterRegions(regions));  // Inflight MR cannot be revoked.
    auto result = backend.Wait(*pending, 100);
    Require(result.size() == 3 && result[0] && !result[1] && result[2]);
    const auto stored_key = sdk->entries[0].key;
    pending = backend.Remove("A");
    Require(bool(pending) && sdk->removed[0] == stored_key);
    Require(bool(backend.Wait(*pending, 100)[0]));
    Require(sdk->registers == 1);  // No registration in Put.

    pending = backend.Put("outside", {{data + 1900, 512}});
    Require(bool(pending) && !backend.Wait(*pending, 100)[0]);
    Require(sdk->stores ==
            1);  // Reject an out-of-bounds range before SDK submission.
    Require(bool(backend.UnregisterRegions(regions)));
    pending = backend.Put("unregistered", {{data, 512}});
    Require(bool(pending) && !backend.Wait(*pending, 100)[0]);

    Require(bool(backend.RegisterRegions({{data, sizeof(data), "host", -1, {}}},
                                         regions)));
    sdk->reject = true;
    pending = backend.Put("busy", {{data, 512}});
    Require(bool(pending) && !backend.Wait(*pending, 100)[0]);
    Require(sdk->waits ==
            2);  // Submission failure did not invent a task to wait on.
    sdk->reject = false;
    sdk->timeout = true;
    pending = backend.Put("timeout", {{data, 512}});
    Require(bool(pending) && !backend.Wait(*pending, 1)[0]);
    Require(barriers == 1 && sdk->stops == 1);
    Require(!backend.Put("after-stop", {{data, 512}}));
    Require(bool(backend.UnregisterRegions(regions)));
    std::cout << "KVClient backend tests passed\n";
}
