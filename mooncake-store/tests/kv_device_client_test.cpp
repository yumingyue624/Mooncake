// Unit tests for the KV device client port (kv_device_client.h): the
// deterministic key mapping, the port contract on an in-memory fake, and
// the Client::SetKvDeviceClient wiring.

#include <gtest/gtest.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "client_service.h"
#include "kv_device_client.h"

namespace mooncake {
namespace {

// In-memory fake device: maps the 8-byte device key to stored bytes.
// Tasks complete immediately; Check/Wait model the async contract.
class FakeKvDeviceClient : public KvDeviceClient {
   public:
    tl::expected<std::vector<RegionHandle>, ErrorCode> RegisterRegions(
        const std::vector<Region>& regions) override {
        std::vector<RegionHandle> handles;
        handles.reserve(regions.size());
        for (size_t i = 0; i < regions.size(); ++i) {
            handles.push_back(next_region_handle_++);
        }
        return handles;
    }

    tl::expected<void, ErrorCode> UnregisterRegions(
        const std::vector<RegionHandle>& handles) override {
        for (auto handle : handles) {
            (void)handle;  // Registration is a no-op in the fake.
        }
        return {};
    }

    tl::expected<TaskHandle, ErrorCode> StoreAsync(
        const std::vector<Entry>& entries) override {
        TaskResult result;
        for (const auto& entry : entries) {
            const uint8_t* base = static_cast<const uint8_t*>(entry.buffer) +
                                  entry.offset;
            objects_[MakeKvCacheKey(entry.key)].assign(base,
                                                       base + entry.length);
            result.entry_status.push_back(ErrorCode::OK);
        }
        return Finish(std::move(result));
    }

    tl::expected<TaskHandle, ErrorCode> LoadAsync(
        const std::vector<Entry>& entries) override {
        TaskResult result;
        for (const auto& entry : entries) {
            auto it = objects_.find(MakeKvCacheKey(entry.key));
            if (it == objects_.end()) {
                result.entry_status.push_back(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }
            if (entry.length < it->second.size()) {
                result.entry_status.push_back(ErrorCode::INVALID_PARAMS);
                continue;
            }
            uint8_t* base =
                static_cast<uint8_t*>(entry.buffer) + entry.offset;
            std::memcpy(base, it->second.data(), it->second.size());
            result.entry_status.push_back(ErrorCode::OK);
        }
        return Finish(std::move(result));
    }

    tl::expected<TaskHandle, ErrorCode> QueryAsync(
        const std::vector<std::string>& keys) override {
        TaskResult result;
        for (const auto& key : keys) {
            result.exists.push_back(
                objects_.count(MakeKvCacheKey(key)) ? 1 : 0);
        }
        return Finish(std::move(result));
    }

    tl::expected<TaskHandle, ErrorCode> DeleteAsync(
        const std::vector<std::string>& keys) override {
        TaskResult result;
        for (const auto& key : keys) {
            result.entry_status.push_back(
                objects_.erase(MakeKvCacheKey(key))
                    ? ErrorCode::OK
                    : ErrorCode::OBJECT_NOT_FOUND);
        }
        return Finish(std::move(result));
    }

    bool Check(TaskHandle handle) override {
        return tasks_.count(handle) > 0;
    }

    tl::expected<TaskResult, ErrorCode> Wait(
        TaskHandle handle, uint64_t /*timeout_ms*/) override {
        auto it = tasks_.find(handle);
        if (it == tasks_.end()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        TaskResult result = std::move(it->second);
        tasks_.erase(it);
        return result;
    }

   private:
    TaskHandle Finish(TaskResult&& result) {
        const TaskHandle handle = ++next_task_handle_;
        tasks_.emplace(handle, std::move(result));
        return handle;
    }

    std::map<KvCacheKey, std::vector<uint8_t>> objects_;
    std::map<TaskHandle, TaskResult> tasks_;
    TaskHandle next_task_handle_{0};
    RegionHandle next_region_handle_{1};
};

// Subclassing is the supported way to construct a Client in unit tests.
class TestableClient : public Client {
   public:
    TestableClient()
        : Client(/*local_hostname=*/"localhost:9011",
                 /*metadata_connstring=*/"",
                 /*protocol=*/"tcp",
                 /*labels=*/{}) {}
};

std::vector<KvDeviceClient::Entry> MakeEntries(const std::string& key,
                                               void* buffer, uint32_t offset,
                                               uint64_t length) {
    return {{key, buffer, offset, length}};
}

TEST(KvCacheKeyTest, MappingIsDeterministic) {
    const auto first = MakeKvCacheKey("model/layer0/block42");
    const auto second = MakeKvCacheKey("model/layer0/block42");
    EXPECT_EQ(first, second);
}

TEST(KvCacheKeyTest, DistinctKeysMapApart) {
    const std::vector<std::string> keys = {
        "",       "a",                    "b",
        "same-prefix-1", "same-prefix-2", "model/layer0/block42",
        "model/layer1/block42"};
    std::map<KvCacheKey, size_t> seen;
    for (const auto& key : keys) {
        const auto device_key = MakeKvCacheKey(key);
        ASSERT_NE(device_key, KvCacheKey{});
        ASSERT_TRUE(seen.emplace(device_key, keys.size()).second)
            << "unexpected collision for key " << key;
    }
}

TEST(KvDeviceClientTest, StoreQueryLoadDeleteRoundtrip) {
    FakeKvDeviceClient device;
    uint8_t backing[16];
    const std::string key = "kv-device/port-test";

    std::memcpy(backing, "hello-asu", 9);
    auto store = device.StoreAsync(MakeEntries(key, backing, 0, 9));
    ASSERT_TRUE(store.has_value());
    EXPECT_NE(*store, KvDeviceClient::kInvalidTaskHandle);
    EXPECT_TRUE(device.Check(*store));
    auto store_result = device.Wait(*store, 1000);
    ASSERT_TRUE(store_result.has_value());
    EXPECT_EQ(store_result->status, ErrorCode::OK);

    auto query = device.QueryAsync({key, "missing"});
    ASSERT_TRUE(query.has_value());
    auto query_result = device.Wait(*query, 1000);
    ASSERT_TRUE(query_result.has_value());
    ASSERT_EQ(query_result->exists.size(), 2u);
    EXPECT_EQ(query_result->exists[0], 1);
    EXPECT_EQ(query_result->exists[1], 0);

    uint8_t loaded[16] = {0};
    auto load = device.LoadAsync(MakeEntries(key, loaded, 0, sizeof(loaded)));
    ASSERT_TRUE(load.has_value());
    auto load_result = device.Wait(*load, 1000);
    ASSERT_TRUE(load_result.has_value());
    ASSERT_EQ(load_result->entry_status.size(), 1u);
    EXPECT_EQ(load_result->entry_status[0], ErrorCode::OK);
    EXPECT_EQ(std::memcmp(loaded, "hello-asu", 9), 0);

    auto remove = device.DeleteAsync({key});
    ASSERT_TRUE(remove.has_value());
    auto remove_result = device.Wait(*remove, 1000);
    ASSERT_TRUE(remove_result.has_value());
    ASSERT_EQ(remove_result->entry_status.size(), 1u);
    EXPECT_EQ(remove_result->entry_status[0], ErrorCode::OK);

    auto missing =
        device.LoadAsync(MakeEntries(key, loaded, 0, sizeof(loaded)));
    ASSERT_TRUE(missing.has_value());
    auto missing_result = device.Wait(*missing, 1000);
    ASSERT_TRUE(missing_result.has_value());
    ASSERT_EQ(missing_result->entry_status.size(), 1u);
    EXPECT_EQ(missing_result->entry_status[0], ErrorCode::OBJECT_NOT_FOUND);
}

TEST(KvDeviceClientTest, RegisterAndUnregisterRegions) {
    FakeKvDeviceClient device;
    uint8_t buffer[8];

    auto handles = device.RegisterRegions(
        {{KvDeviceClient::Region::MemoryType::HOST_PINNED, buffer,
          sizeof(buffer), -1, -1}});
    ASSERT_TRUE(handles.has_value());
    ASSERT_EQ(handles->size(), 1u);
    EXPECT_NE((*handles)[0], KvDeviceClient::kInvalidRegionHandle);

    EXPECT_TRUE(device.UnregisterRegions(*handles).has_value());
}

TEST(ClientKvDeviceClientTest, AttachesAndDrivesPortThroughClient) {
    TestableClient client;
    EXPECT_EQ(client.kv_device_client(), nullptr);

    auto device = std::make_shared<FakeKvDeviceClient>();
    client.SetKvDeviceClient(device);
    EXPECT_EQ(client.kv_device_client(), device);

    uint8_t backing[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    auto store = client.kv_device_client()->StoreAsync(
        MakeEntries("attached", backing, 0, sizeof(backing)));
    ASSERT_TRUE(store.has_value());
    auto result = client.kv_device_client()->Wait(*store, 1000);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->status, ErrorCode::OK);

    uint8_t loaded[8] = {0};
    auto load = client.kv_device_client()->LoadAsync(
        MakeEntries("attached", loaded, 0, sizeof(loaded)));
    ASSERT_TRUE(load.has_value());
    auto load_result = client.kv_device_client()->Wait(*load, 1000);
    ASSERT_TRUE(load_result.has_value());
    EXPECT_EQ(load_result->entry_status.at(0), ErrorCode::OK);
    EXPECT_EQ(std::memcmp(loaded, backing, sizeof(backing)), 0);

    client.SetKvDeviceClient(nullptr);
    EXPECT_EQ(client.kv_device_client(), nullptr);
}

}  // namespace
}  // namespace mooncake
