#include <gtest/gtest.h>
#include <kv_client.h>

#include <cstring>
#include <algorithm>
#include <cstdlib>
#include <functional>
#include <map>
#include <mutex>

#include "client_service.h"
#include "client_buffer.h"
#include "common/client_buffer_allocation.h"
#include "test_server_helpers.h"

namespace mooncake::test {
namespace {
struct FakeStorage {
    std::mutex mutex;
    std::map<kv::CacheKey, std::vector<char>> objects;
};

// The fake implements the actual public SDK contract. The production Client,
// framing, task polling, memory registrations, Master RPCs and TCP transfers
// are exercised without requiring an ASU appliance.
class FakeSdk : public kv::KvClient {
   public:
    explicit FakeSdk(std::shared_ptr<FakeStorage> state)
        : state_(std::move(state)) {}
    kv::Status Init(const kv::KvClientConfig&) override {
        return kv::Status::OK();
    }
    kv::Status Init(const std::string&) override { return kv::Status::OK(); }
    kv::Status Shutdown() override {
        regions_.clear();
        return kv::Status::OK();
    }
    kv::Status QueryAsync(const std::vector<kv::CacheKey>& keys,
                          kv::TaskId& task) override {
        return Schedule(task, [this, keys] {
            kv::TaskResult result;
            result.queryResult.emplace();
            std::lock_guard<std::mutex> lock(state_->mutex);
            for (const auto& key : keys)
                result.queryResult->exists.push_back(
                    state_->objects.count(key) != 0);
            return result;
        });
    }
    kv::Status StoreAsync(const std::vector<kv::KVBuffer>& entries,
                          kv::TaskId& task) override {
        return BatchStoreAsync(entries, task);
    }
    kv::Status StoreAsync(const std::vector<kv::KVBuffer>& entries,
                          kv::TaskId& task, uintptr_t) override {
        return BatchStoreAsync(entries, task);
    }
    kv::Status LoadAsync(const std::vector<kv::KVBuffer>& entries,
                         kv::TaskId& task) override {
        return BatchLoadAsync(entries, task);
    }
    kv::Status BatchStoreAsync(const std::vector<kv::KVBuffer>& entries,
                               kv::TaskId& task, uintptr_t) override {
        return BatchStoreAsync(entries, task);
    }
    kv::Status BatchStoreAsync(const std::vector<kv::KVBuffer>& entries,
                               kv::TaskId& task) override {
        ++stores;
        return Submit(entries, task, false);
    }
    kv::Status BatchLoadAsync(const std::vector<kv::KVBuffer>& entries,
                              kv::TaskId& task) override {
        return Submit(entries, task, true);
    }
    kv::Status DeleteAsync(const std::vector<kv::CacheKey>& keys,
                           kv::TaskId& task) override {
        return Schedule(task, [this, keys] {
            std::lock_guard<std::mutex> lock(state_->mutex);
            for (const auto& key : keys) state_->objects.erase(key);
            kv::TaskResult result;
            result.entryStatus.assign(keys.size(), kv::Status::OK());
            return result;
        });
    }
    bool Check(kv::TaskId task) override {
        auto& pending = tasks_.at(task);
        if (pending.polls-- > 0) return false;
        if (!pending.ready) {
            pending.result = pending.work();
            pending.ready = true;
        }
        return true;
    }
    kv::Status Wait(kv::TaskId task, uint64_t,
                    kv::TaskResult& result) override {
        auto it = tasks_.find(task);
        if (it == tasks_.end() || !it->second.ready)
            return kv::Status::Error(kv::StatusCode::TIMEOUT,
                                     "Wait before completion");
        result = std::move(it->second.result);
        tasks_.erase(it);
        return result.status;
    }
    kv::Status RegisterRegions(
        const std::vector<kv::MemoryRegion>& regions,
        std::vector<kv::RegisteredMemory>& registered) override {
        for (const auto& region : regions) {
            if (region.addr % 512 || region.size % 512)
                return kv::Status::Error(kv::StatusCode::INVALID_ARGUMENT,
                                         "unaligned");
            const auto handle = ++next_region_;
            regions_[handle] = region;
            registered.push_back({region, handle, 0});
        }
        return kv::Status::OK();
    }
    kv::Status UnregisterRegions(
        const std::vector<kv::MRHandle>& handles) override {
        for (auto handle : handles) regions_.erase(handle);
        return kv::Status::OK();
    }
    bool fail_store = false;
    size_t fail_on_store = 0;
    size_t stores = 0;

   private:
    struct Task {
        std::function<kv::TaskResult()> work;
        kv::TaskResult result;
        int polls = 2;
        bool ready = false;
    };
    kv::Status Schedule(kv::TaskId& task,
                        std::function<kv::TaskResult()> work) {
        task = ++next_task_;
        tasks_.emplace(task, Task{std::move(work), {}});
        return kv::Status::OK();
    }
    kv::Status Submit(const std::vector<kv::KVBuffer>& entries,
                      kv::TaskId& task, bool read) {
        return Schedule(task, [this, entries, read, store_number = stores] {
            kv::TaskResult result;
            std::lock_guard<std::mutex> lock(state_->mutex);
            for (const auto& entry : entries) {
                kv::Status status;
                const auto region = regions_.find(entry.buffer.handle);
                auto stored = state_->objects.find(entry.key);
                if (region == regions_.end())
                    status =
                        kv::Status::Error(kv::StatusCode::BUFFER_NOT_REGISTERED,
                                          "released before completion");
                else if (!read &&
                         (fail_store ||
                          (fail_on_store && store_number == fail_on_store)))
                    status = kv::Status::Error(kv::StatusCode::IO_ERROR,
                                               "injected store failure");
                else if (read && stored == state_->objects.end())
                    status = kv::Status::Error(
                        kv::StatusCode::ENTRY_KEY_NOT_FOUND, "missing");
                else {
                    auto* bytes =
                        reinterpret_cast<char*>(entry.buffer.region.addr);
                    const auto length = entry.buffer.region.size;
                    if (read) {
                        if (entry.offset + length > stored->second.size())
                            status = kv::Status::Error(kv::StatusCode::IO_ERROR,
                                                       "short read");
                        else
                            std::memcpy(bytes,
                                        stored->second.data() + entry.offset,
                                        length);
                    } else {
                        auto& value = state_->objects[entry.key];
                        value.resize(std::max<uint64_t>(value.size(),
                                                        entry.offset + length));
                        std::memcpy(value.data() + entry.offset, bytes, length);
                    }
                }
                result.entryStatus.push_back(status);
                if (!status.ok())
                    result.status = kv::Status::Error(
                        kv::StatusCode::PARTIAL_FAILED, "entry failed");
            }
            return result;
        });
    }
    std::shared_ptr<FakeStorage> state_;
    std::map<kv::MRHandle, kv::MemoryRegion> regions_;
    std::map<kv::TaskId, Task> tasks_;
    kv::TaskId next_task_ = 0;
    kv::MRHandle next_region_ = 0;
};

class KvDualWriteTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
        writer = Create("127.0.0.1:18301");
        provider = Create("127.0.0.1:18302");
        reader = Create("127.0.0.1:18303");
        ASSERT_TRUE(writer && provider && reader);
        segment = allocate_buffer_allocator_memory(segment_size);
        source = allocate_buffer_allocator_memory(4096);
        ASSERT_TRUE(segment && source);
        ASSERT_TRUE(provider->MountSegment(segment, segment_size, "tcp"));
        ASSERT_TRUE(writer->RegisterLocalMemory(source, 4096, "cpu:0"));
        std::memset(source, 0x5a, 4096);
        state = std::make_shared<FakeStorage>();
        sdk = std::make_shared<FakeSdk>(state);
        ASSERT_TRUE(writer->SetKvClient(sdk));
        ASSERT_TRUE(reader->SetKvClient(std::make_shared<FakeSdk>(state)));
    }
    void TearDown() override {
        if (writer && source) (void)writer->unregisterLocalMemory(source);
        if (provider && segment)
            (void)provider->UnmountSegment(segment, segment_size);
        reader.reset();
        writer.reset();
        provider.reset();
        master.Stop();
        std::free(source);
        std::free(segment);
    }
    std::shared_ptr<Client> Create(const std::string& address) {
        auto value = Client::Create(address, "P2PHANDSHAKE", "tcp",
                                    std::nullopt, master.master_address());
        return value ? *value : nullptr;
    }
    std::vector<Slice> Slices() {
        return {{source, 123}, {static_cast<char*>(source) + 123, 901}};
    }
    KvObjectInfo Info(const std::string& key) {
        auto query = writer->Query(key);
        EXPECT_TRUE(query);
        if (query)
            for (const auto& replica : query->replicas)
                if (replica.is_kv_replica()) return replica.get_kv_descriptor();
        ADD_FAILURE() << "Missing ASU descriptor";
        return {};
    }
    testing::InProcMaster master;
    std::shared_ptr<Client> writer, reader, provider;
    std::shared_ptr<FakeStorage> state;
    std::shared_ptr<FakeSdk> sdk;
    void* segment = nullptr;
    void* source = nullptr;
    static constexpr size_t segment_size = 16 * 1024 * 1024;
};

TEST_F(KvDualWriteTest, PutActuallyWritesAsuAndReadsAfterMemoryUnmount) {
    auto slices = Slices();
    ASSERT_TRUE(writer->Put("kv/one", slices, {}));
    ASSERT_EQ(sdk->stores, 1);
    ASSERT_EQ(Info("kv/one").object_size, 1024);
    ASSERT_TRUE(provider->UnmountSegment(segment, segment_size));
    provider.reset();
    ASSERT_TRUE(writer->unregisterLocalMemory(source));
    writer.reset();
    std::vector<char> output(1024);
    std::vector<Slice> destination{{output.data(), output.size()}};
    ASSERT_TRUE(reader->Get("kv/one", destination));
    EXPECT_EQ(std::memcmp(source, output.data(), output.size()), 0);
    auto query = reader->Query("kv/one");
    ASSERT_TRUE(query);
    std::fill(output.begin(), output.end(), 0);
    std::unordered_map<std::string, std::vector<Slice>> destinations{
        {"kv/one", destination}};
    auto batch = reader->BatchGet({"kv/one"}, {*query}, destinations, true);
    ASSERT_EQ(batch.size(), 1);
    ASSERT_TRUE(batch[0]);
    EXPECT_EQ(std::memcmp(source, output.data(), output.size()), 0);
}

TEST_F(KvDualWriteTest, BatchAndSameNodePathWriteEachObject) {
    for (bool same_node : {false, true}) {
        const std::string prefix = same_node ? "same/" : "batch/";
        std::vector<std::string> keys{prefix + "a", prefix + "b"};
        std::vector<std::vector<Slice>> batches{Slices(), Slices()};
        ReplicateConfig config;
        config.prefer_alloc_in_same_node = same_node;
        auto result = writer->BatchPut(keys, batches, config);
        ASSERT_EQ(result.size(), 2);
        EXPECT_TRUE(result[0]);
        EXPECT_TRUE(result[1]);
        EXPECT_EQ(Info(keys[0]).object_size, 1024);
        EXPECT_EQ(Info(keys[1]).object_size, 1024);
    }
    EXPECT_EQ(sdk->stores, 4);
}

TEST_F(KvDualWriteTest, AsuFailureRevokesSingleAndBatchMemoryWrites) {
    sdk->fail_store = true;
    auto slices = Slices();
    EXPECT_FALSE(writer->Put("failed/one", slices, {}));
    EXPECT_FALSE(writer->Query("failed/one"));
    std::vector<std::vector<Slice>> batch{Slices(), Slices()};
    auto result = writer->BatchPut({"failed/a", "failed/b"}, batch, {});
    ASSERT_EQ(result.size(), 2);
    EXPECT_FALSE(result[0]);
    EXPECT_FALSE(result[1]);
    EXPECT_FALSE(writer->Query("failed/a"));
    EXPECT_FALSE(writer->Query("failed/b"));
}

TEST_F(KvDualWriteTest,
       DuplicatePutIsIdempotentAndRecreationUsesNewGeneration) {
    auto slices = Slices();
    ASSERT_TRUE(writer->Put("immutable", slices, {}));
    const auto first = Info("immutable");
    ASSERT_TRUE(writer->Put("immutable", slices, {}));
    EXPECT_EQ(sdk->stores, 1);
    EXPECT_EQ(Info("immutable").generation, first.generation);
    ASSERT_TRUE(writer->Remove("immutable", true));
    EXPECT_TRUE(state->objects.empty());
    ASSERT_TRUE(writer->Put("immutable", slices, {}));
    EXPECT_NE(Info("immutable").generation, first.generation);
}

TEST_F(KvDualWriteTest, BatchPreservesPartialFailures) {
    sdk->fail_on_store = 2;
    std::vector<std::vector<Slice>> batch{Slices(), Slices()};
    auto results = writer->BatchPut({"partial/a", "partial/b"}, batch, {});
    ASSERT_EQ(results.size(), 2);
    EXPECT_TRUE(results[0]);
    EXPECT_FALSE(results[1]);
    EXPECT_TRUE(writer->Query("partial/a"));
    EXPECT_FALSE(writer->Query("partial/b"));
}

TEST_F(KvDualWriteTest, MemoryAllocationFailureDoesNotPublishAnAsuOnlyWrite) {
    ASSERT_TRUE(provider->UnmountSegment(segment, segment_size));
    auto slices = Slices();
    EXPECT_FALSE(writer->Put("memory-failed", slices, {}));
    EXPECT_FALSE(writer->Query("memory-failed"));
    EXPECT_EQ(sdk->stores, 0);
    EXPECT_TRUE(state->objects.empty());
}

TEST_F(KvDualWriteTest, CorruptionIsDetectedBeforeCopyingToUserBuffer) {
    auto slices = Slices();
    ASSERT_TRUE(writer->Put("corrupt", slices, {}));
    auto query = writer->Query("corrupt");
    ASSERT_TRUE(query);
    auto replica =
        std::find_if(query->replicas.begin(), query->replicas.end(),
                     [](const auto& r) { return r.is_kv_replica(); });
    ASSERT_NE(replica, query->replicas.end());
    QueryResult asu_only({*replica}, query->lease_timeout,
                         query->object_checksum);
    ASSERT_EQ(state->objects.size(), 1);
    state->objects.begin()->second[512] ^= 1;
    std::vector<char> output(1024, 0);
    std::vector<Slice> destination{{output.data(), output.size()}};
    auto result = reader->Get("corrupt", asu_only, destination);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::CHECKSUM_MISMATCH);
    EXPECT_EQ(output[0], 0);
}

TEST_F(KvDualWriteTest, RejectsMutableAndUnenumerableOperations) {
    auto slices = Slices();
    EXPECT_FALSE(writer->Upsert("mutable", slices, {}));
    EXPECT_FALSE(writer->RemoveAll());
    EXPECT_FALSE(writer->RemoveByRegex(".*"));
}
}  // namespace
}  // namespace mooncake::test
