#include "storage/kvclient_backend.h"

#include <gtest/gtest.h>
#include <xxhash.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>

namespace mooncake {

class KvClientBackendTestPeer {
   public:
    static void SetSdk(KvClientBackend& backend,
                       std::unique_ptr<kv::KvClient> sdk) {
        backend.sdk_ = std::move(sdk);
    }
};

namespace {
class FakeSdk final : public kv::KvClient {
   public:
    int registers = 0, stores = 0, batches = 0, deletes = 0, waits = 0,
        stops = 0;
    uint64_t timeout = 0;
    kv::Status submit_status, wait_status, task_status;
    std::vector<kv::Status> entry_status;
    std::vector<kv::KVBuffer> entries;
    std::vector<kv::CacheKey> keys;
    std::vector<kv::CacheKey> query_keys;
    std::function<void()> wait_hook;

    kv::Status Init(const kv::KvClientConfig&) override { return {}; }
    kv::Status Init(const std::string&) override { return {}; }
    kv::Status Shutdown() override {
        ++stops;
        return {};
    }
    kv::Status QueryAsync(const std::vector<kv::CacheKey>& input,
                          kv::TaskId& id) override {
        query_keys = input;
        id = 9;
        return submit_status;
    }
    kv::Status LoadAsync(const std::vector<kv::KVBuffer>&,
                         kv::TaskId&) override {
        return {};
    }
    kv::Status BatchLoadAsync(const std::vector<kv::KVBuffer>&,
                              kv::TaskId&) override {
        return {};
    }
    kv::Status StoreAsync(const std::vector<kv::KVBuffer>& buffers,
                          kv::TaskId& id) override {
        ++stores;
        entries = buffers;
        id = 7;
        return submit_status;
    }
    kv::Status StoreAsync(const std::vector<kv::KVBuffer>& buffers,
                          kv::TaskId& id, uintptr_t) override {
        return StoreAsync(buffers, id);
    }
    kv::Status BatchStoreAsync(const std::vector<kv::KVBuffer>& buffers,
                               kv::TaskId& id) override {
        ++batches;
        entries = buffers;
        id = 7;
        return submit_status;
    }
    kv::Status BatchStoreAsync(const std::vector<kv::KVBuffer>& buffers,
                               kv::TaskId& id, uintptr_t) override {
        return BatchStoreAsync(buffers, id);
    }
    kv::Status DeleteAsync(const std::vector<kv::CacheKey>& input,
                           kv::TaskId& id) override {
        ++deletes;
        keys = input;
        id = 8;
        return submit_status;
    }
    bool Check(kv::TaskId) override { return true; }
    kv::Status Wait(kv::TaskId id, uint64_t timeout_ms,
                    kv::TaskResult& result) override {
        ++waits;
        timeout = timeout_ms;
        if (wait_hook) wait_hook();
        result.status = task_status;
        result.entryStatus = entry_status;
        if (id == 9) {
            result.queryResult = kv::QueryResult{};
            result.queryResult->exists.assign(query_keys.size(), 1);
        }
        return wait_status;
    }
    kv::Status RegisterRegions(
        const std::vector<kv::MemoryRegion>& regions,
        std::vector<kv::RegisteredMemory>& registered) override {
        ++registers;
        for (const auto& region : regions)
            registered.push_back({region, 42, 11});
        return {};
    }
    kv::Status UnregisterRegions(const std::vector<kv::MRHandle>&) override {
        return {};
    }
};

class KvClientWriteTest : public ::testing::Test {
   protected:
    KvClientBackend backend;
    FakeSdk* sdk;
    alignas(512) char data[2048]{};
    TenantId tenant{"team-a"};

    void SetUp() override {
        auto fake = std::make_unique<FakeSdk>();
        sdk = fake.get();
        KvClientBackendTestPeer::SetSdk(backend, std::move(fake));
        ASSERT_TRUE(backend.RegisterMemory(
            data, sizeof(data), "npu:3",
            {.kind = device::MemoryKind::kDevice, .device_id = 3}));
    }
};

TEST_F(KvClientWriteTest, SinglePutUsesStoreAndExistingRegistration) {
    auto submitted = backend.Put(tenant, "A", {{data + 512, 512}});
    ASSERT_TRUE(submitted);
    ASSERT_EQ(sdk->entries.size(), 1);
    EXPECT_EQ(sdk->stores, 1);
    EXPECT_EQ(sdk->batches, 0);
    EXPECT_EQ(sdk->registers, 1);
    EXPECT_EQ(sdk->entries[0].buffer.handle, 42);
    EXPECT_EQ(sdk->entries[0].buffer.region.addr,
              reinterpret_cast<uintptr_t>(data + 512));
    EXPECT_EQ(sdk->entries[0].buffer.region.size, 512);
    EXPECT_EQ(sdk->entries[0].buffer.region.deviceId, 3);
    EXPECT_EQ(sdk->entries[0].offset, 0);
    auto result = backend.Wait(*submitted, 123);
    ASSERT_EQ(result.size(), 1);
    EXPECT_TRUE(result[0]);
    EXPECT_EQ(sdk->timeout, 123);
}

TEST_F(KvClientWriteTest, InvalidInputDoesNotDiscardOtherKeysOrDuplicates) {
    auto submitted = backend.BatchPut(
        tenant, {"A", "bad", "A", ""},
        {{{data, 512}}, {{nullptr, 512}}, {{data + 512, 512}}, {{data, 512}}});
    ASSERT_TRUE(submitted);
    EXPECT_EQ(sdk->batches, 1);
    ASSERT_EQ(sdk->entries.size(), 3);
    EXPECT_EQ(sdk->entries[0].key, sdk->entries[1].key);
    auto result = backend.Wait(*submitted, 10);
    ASSERT_EQ(result.size(), 4);
    EXPECT_TRUE(result[0]);
    EXPECT_FALSE(result[1]);
    EXPECT_TRUE(result[2]);
    EXPECT_TRUE(result[3]);
}

TEST_F(KvClientWriteTest, ShapeMismatchIsRejectedBeforeSdkSubmission) {
    EXPECT_FALSE(backend.BatchPut(tenant, {"A"}, {}));
    EXPECT_EQ(sdk->batches, 0);
}

TEST_F(KvClientWriteTest, UnregisteredAndOutOfBoundsSlicesDoNotRegister) {
    EXPECT_FALSE(backend.Put(tenant, "A", {{nullptr, 512}}));
    EXPECT_FALSE(backend.Put(tenant, "A", {{data + 1536, 1024}}));
    EXPECT_FALSE(backend.Put(tenant, "A", {}));
    EXPECT_EQ(sdk->stores, 0);
    EXPECT_EQ(sdk->registers, 1);
}

TEST_F(KvClientWriteTest, SubmitFailureIsReturnedWithoutAWaitableTask) {
    sdk->submit_status =
        kv::Status::Error(kv::StatusCode::RESOURCE_BUSY, "busy");
    EXPECT_FALSE(backend.Put(tenant, "A", {{data, 512}}));
    EXPECT_FALSE(backend.BatchPut(tenant, {"A"}, {{{data, 512}}}));
    EXPECT_FALSE(backend.Remove(tenant, "A"));
    EXPECT_FALSE(backend.BatchRemove(tenant, {"A"}));
    EXPECT_EQ(sdk->waits, 0);
    EXPECT_TRUE(backend.UnregisterMemory(data));
}

TEST_F(KvClientWriteTest, EntryFailuresAreFoldedIntoOriginalKeyPositions) {
    sdk->wait_status =
        kv::Status::Error(kv::StatusCode::PARTIAL_FAILED, "partial");
    sdk->entry_status = {
        kv::Status::OK(),
        kv::Status::Error(kv::StatusCode::CQE_WRITE_FAULT, "write fault"),
        kv::Status::OK()};
    auto submitted = backend.BatchPut(
        tenant, {"A", "B"},
        {{{data, 512}, {data + 512, 512}}, {{data + 1024, 512}}});
    ASSERT_TRUE(submitted);
    auto result = backend.Wait(*submitted, 10);
    ASSERT_EQ(result.size(), 2);
    EXPECT_FALSE(result[0]);
    EXPECT_TRUE(result[1]);
}

TEST_F(KvClientWriteTest, TaskFailureIsHonoredWhenWaitCallSucceeds) {
    sdk->task_status =
        kv::Status::Error(kv::StatusCode::CQE_WRITE_FAULT, "failed");
    sdk->entry_status = {kv::Status::OK()};
    auto submitted = backend.Put(tenant, "A", {{data, 512}});
    ASSERT_TRUE(submitted);
    auto result = backend.Wait(*submitted, 10);
    EXPECT_FALSE(result[0]);
}

TEST_F(KvClientWriteTest, TimeoutIgnoresProvisionalEntrySuccess) {
    sdk->wait_status = kv::Status::Error(kv::StatusCode::TIMEOUT, "timeout");
    sdk->entry_status = {kv::Status::OK()};
    auto submitted = backend.Put(tenant, "A", {{data, 512}});
    ASSERT_TRUE(submitted);
    auto result = backend.Wait(*submitted, 10);
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(sdk->stops, 0);
}

TEST_F(KvClientWriteTest, DeleteUsesSameXxh64KeyAndKeepsDuplicatePositions) {
    auto put = backend.Put(tenant, "A", {{data, 512}});
    ASSERT_TRUE(put);
    const auto stored_key = sdk->entries[0].key;
    EXPECT_TRUE(backend.Wait(*put, 10)[0]);
    auto removed = backend.BatchRemove(tenant, {"A", "A", ""});
    ASSERT_TRUE(removed);
    ASSERT_EQ(sdk->keys.size(), 3);
    EXPECT_EQ(sdk->keys[0], stored_key);
    EXPECT_EQ(sdk->keys[1], stored_key);
    const auto scoped = tenant.MakeScopedKey("A");
    const uint64_t expected = XXH64(scoped.data(), scoped.size(), 0);
    for (size_t i = 0; i < stored_key.size(); ++i)
        EXPECT_EQ(std::to_integer<uint8_t>(stored_key[i]),
                  (expected >> ((7 - i) * 8)) & 0xff);
    EXPECT_EQ(backend.Wait(*removed, 10).size(), 3);
    auto other = backend.Remove(TenantId("team-b"), "A");
    ASSERT_TRUE(other);
    EXPECT_NE(sdk->keys[0], stored_key);
    EXPECT_TRUE(backend.Wait(*other, 10)[0]);
}

TEST_F(KvClientWriteTest, QueryAndWriteUseSameKeyEncoding) {
    auto put = backend.Put(tenant, "A", {{data, 512}});
    ASSERT_TRUE(put);
    const auto stored_key = sdk->entries[0].key;
    EXPECT_TRUE(backend.Wait(*put, 10)[0]);
    std::vector<uint8_t> exists;
    auto query = backend.BatchExistsAsync({"A"}, tenant, &exists);
    ASSERT_TRUE(query);
    ASSERT_EQ(sdk->query_keys.size(), 1);
    EXPECT_EQ(sdk->query_keys[0], stored_key);
    auto statuses = backend.Wait(*query);
    ASSERT_EQ(statuses.size(), 1);
    EXPECT_TRUE(statuses[0]);
    EXPECT_EQ(exists, (std::vector<uint8_t>{1}));
}

TEST_F(KvClientWriteTest, EmptyBatchesHaveNoSdkTask) {
    auto put = backend.BatchPut(tenant, {}, {});
    ASSERT_TRUE(put);
    EXPECT_TRUE(backend.Wait(*put, 10).empty());
    auto removed = backend.BatchRemove(tenant, {});
    ASSERT_TRUE(removed);
    EXPECT_TRUE(backend.Wait(*removed, 10).empty());
    EXPECT_EQ(sdk->batches, 0);
    EXPECT_EQ(sdk->deletes, 0);
    EXPECT_EQ(sdk->waits, 0);
}

TEST_F(KvClientWriteTest, InvalidTaskIsRejectedWithoutCallingSdk) {
    auto submitted = backend.Remove(tenant, "A");
    ASSERT_TRUE(submitted);
    EXPECT_TRUE(backend.Wait(*submitted, 10)[0]);
    auto repeated = backend.Wait(*submitted, 10);
    ASSERT_EQ(repeated.size(), 1);
    EXPECT_FALSE(repeated[0]);
    EXPECT_EQ(sdk->waits, 1);
}

TEST_F(KvClientWriteTest, WaitDoesNotHoldTaskOrMemoryMutexDuringSdkWait) {
    std::mutex mutex;
    std::condition_variable changed;
    bool entered = false, released = false;
    sdk->wait_hook = [&] {
        std::unique_lock lock(mutex);
        entered = true;
        changed.notify_all();
        changed.wait(lock, [&] { return released; });
    };
    auto submitted = backend.Put(tenant, "A", {{data, 512}});
    ASSERT_TRUE(submitted);
    auto waiting = std::async(std::launch::async,
                              [&] { return backend.Wait(*submitted, 10); });
    bool started;
    {
        std::unique_lock lock(mutex);
        started = changed.wait_for(lock, std::chrono::seconds(2),
                                   [&] { return entered; });
    }
    auto second = std::async(std::launch::async, [&] {
        return backend.Put(tenant, "B", {{data + 512, 512}});
    });
    const auto readiness = second.wait_for(std::chrono::seconds(2));
    {
        std::lock_guard lock(mutex);
        released = true;
    }
    changed.notify_all();
    EXPECT_TRUE(started);
    EXPECT_EQ(readiness, std::future_status::ready);
    EXPECT_TRUE(waiting.get()[0]);
    sdk->wait_hook = {};
    auto another = second.get();
    ASSERT_TRUE(another);
    EXPECT_TRUE(backend.Wait(*another, 10)[0]);
}

}  // namespace
}  // namespace mooncake
