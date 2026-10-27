#include "kvclient_backend.h"
#include "kv_client.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {
class FakeSdk final : public kv::KvClient {
   public:
    int registers = 0, stores = 0, waits = 0, deletes = 0, stops = 0;
    bool reject = false, timeout = false;
    kv::Status task_status;
    std::vector<kv::Status> completion;
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
        if (reject)
            return kv::Status::Error(kv::StatusCode::RESOURCE_BUSY, "busy");
        id = 8;
        return {};
    }
    bool Check(kv::TaskId) override { return true; }
    kv::Status Wait(kv::TaskId id, uint64_t, kv::TaskResult& result) override {
        ++waits;
        if (timeout)
            return kv::Status::Error(kv::StatusCode::TIMEOUT, "timeout");
        result.status = task_status;
        if (!completion.empty())
            result.entryStatus = completion;
        else
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

TEST(KVClientBackendTest, DeletePreservesInputOrderAndTenantIsolation) {
    auto sdk = std::make_shared<FakeSdk>();
    KVClientBackend backend(sdk);
    sdk->completion = {kv::Status::OK(),
                       kv::Status::Error(kv::StatusCode::NOT_FOUND, "missing")};
    auto task = backend.BatchRemove("tenant-a", {"A", "", "B"});
    ASSERT_TRUE(task);
    ASSERT_EQ(sdk->removed.size(), 2);
    const auto first_key = sdk->removed[0];
    auto results = backend.Wait(*task, 100);
    ASSERT_EQ(results.size(), 3);
    EXPECT_TRUE(results[0]);
    ASSERT_FALSE(results[1]);
    EXPECT_EQ(results[1].error(), ErrorCode::INVALID_KEY);
    ASSERT_FALSE(results[2]);
    EXPECT_EQ(results[2].error(), ErrorCode::OBJECT_NOT_FOUND);

    sdk->completion.clear();
    task = backend.Remove("tenant-a", "A");
    ASSERT_TRUE(task);
    EXPECT_EQ(sdk->removed[0], first_key);
    EXPECT_TRUE(backend.Wait(*task, 100)[0]);
    task = backend.Remove("tenant-b", "A");
    ASSERT_TRUE(task);
    EXPECT_NE(sdk->removed[0], first_key);
    EXPECT_TRUE(backend.Wait(*task, 100)[0]);
    EXPECT_EQ(backend.Wait(*task, 100)[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST(KVClientBackendTest, RejectedSubmissionDoesNotWaitOnInvalidTaskId) {
    auto sdk = std::make_shared<FakeSdk>();
    KVClientBackend backend(sdk);
    sdk->reject = true;
    auto task = backend.Remove("tenant-a", "A");
    ASSERT_TRUE(task);
    EXPECT_FALSE(backend.Wait(*task, 100)[0]);
    EXPECT_EQ(sdk->waits, 0);
}

TEST(KVClientBackendTest, UnregisteredWritesNeverRegisterOrAccessPayload) {
    auto sdk = std::make_shared<FakeSdk>();
    KVClientBackend backend(sdk);
    alignas(512) char data[1024]{};
    auto task =
        backend.BatchPut("tenant-a", {"A", "B"},
                         {{{data, 512}}, {{data, 512}, {data + 512, 512}}});
    ASSERT_TRUE(task);
    auto results = backend.Wait(*task, 100);
    ASSERT_EQ(results.size(), 2);
    EXPECT_FALSE(results[0]);
    EXPECT_FALSE(results[1]);
    EXPECT_EQ(sdk->registers, 0);
    EXPECT_EQ(sdk->stores, 0);
    EXPECT_FALSE(backend.BatchPut("tenant-a", {"A"}, {}));
}

TEST(KVClientBackendTest, SinglePutRejectsInvalidInputBeforeSubmission) {
    auto sdk = std::make_shared<FakeSdk>();
    KVClientBackend backend(sdk);
    alignas(512) char data[512]{};
    auto result = backend.Put("tenant-a", "", {{data, sizeof(data)}});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_KEY);
    EXPECT_FALSE(backend.Put("tenant-a", "A", {}));
    EXPECT_FALSE(backend.Put("tenant-a", "A", {{data, sizeof(data)}}));
    EXPECT_FALSE(backend.Remove("tenant-a", ""));
    EXPECT_EQ(sdk->registers, 0);
    EXPECT_EQ(sdk->stores, 0);
    EXPECT_EQ(sdk->deletes, 0);
    EXPECT_EQ(sdk->waits, 0);
}

TEST(KVClientBackendTest, WaitAndTaskErrorsCannotBecomeSuccessfulEntries) {
    auto sdk = std::make_shared<FakeSdk>();
    {
        KVClientBackend backend(sdk);
        sdk->timeout = true;
        auto task = backend.Remove("tenant-a", "A");
        ASSERT_TRUE(task);
        EXPECT_EQ(backend.Wait(*task, 1)[0].error(), ErrorCode::TRANSFER_FAIL);
        sdk->timeout = false;
        sdk->task_status =
            kv::Status::Error(kv::StatusCode::TIMEOUT, "timeout");
        task = backend.Remove("tenant-a", "A");
        ASSERT_TRUE(task);
        EXPECT_EQ(backend.Wait(*task, 1)[0].error(), ErrorCode::TRANSFER_FAIL);
    }
    EXPECT_EQ(sdk->stops, 1);
}
}  // namespace
}  // namespace mooncake
