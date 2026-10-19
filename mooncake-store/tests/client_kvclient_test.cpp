#include <gtest/gtest.h>

#include "client_service.h"

#ifdef USE_KVCLIENT
#include <kv_client.h>
#endif

namespace mooncake {
namespace {

TEST(KvClientFactoryTest, ConfiguredSdkFailureFailsProductionClientCreation) {
    const char* value = std::getenv("MOONCAKE_KVCLIENT_CONFIG");
    std::optional<std::string> saved;
    if (value) saved = value;
    setenv("MOONCAKE_KVCLIENT_CONFIG",
           "/nonexistent/mooncake-kvclient-test.conf", 1);
    // Initialization must fail before attempting to connect to a Master.
    EXPECT_FALSE(Client::Create("localhost:9012", "P2PHANDSHAKE", "tcp"));
    if (saved)
        setenv("MOONCAKE_KVCLIENT_CONFIG", saved->c_str(), 1);
    else
        unsetenv("MOONCAKE_KVCLIENT_CONFIG");
}

class KvClientTest : public ::testing::Test {
   protected:
    class StoreClient : public Client {
       public:
        StoreClient() : Client("localhost:9011", "", "tcp", {}) {}
    };

    StoreClient client;
};

TEST_F(KvClientTest, DefaultClientDoesNotCreateSdkOrConnectToAsu) {
    EXPECT_EQ(client.GetKvClient(), nullptr);
    client.SetKvClient(nullptr);
    EXPECT_EQ(client.GetKvClient(), nullptr);
}

#ifdef USE_KVCLIENT
TEST_F(KvClientTest, NativeApiAndTaskErrorsArePreserved) {
    auto sdk = std::shared_ptr<kv::KvClient>(kv::CreateKvClient());
    ASSERT_NE(sdk, nullptr);
    client.SetKvClient(sdk);
    auto snapshot = client.GetKvClient();
    ASSERT_EQ(snapshot, sdk);

    // No SDK Init or ASU is needed: this also verifies that attachment does
    // not hide the native SDK's initialization state or status codes.
    kv::TaskId task = kv::kInvalidTaskId;
    kv::CacheKey key{};
    key[0] = std::byte{1};
    const auto status = snapshot->QueryAsync({key}, task);
    EXPECT_EQ(status.code, kv::StatusCode::NOT_INITIALIZED);
}

TEST_F(KvClientTest, SnapshotOutlivesReplacementAndDetach) {
    auto first = std::shared_ptr<kv::KvClient>(kv::CreateKvClient());
    auto second = std::shared_ptr<kv::KvClient>(kv::CreateKvClient());
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    std::weak_ptr<kv::KvClient> weak_first = first;
    client.SetKvClient(first);
    auto snapshot = client.GetKvClient();
    first.reset();

    client.SetKvClient(second);
    EXPECT_EQ(client.GetKvClient(), second);
    EXPECT_FALSE(weak_first.expired());
    client.SetKvClient(nullptr);
    EXPECT_EQ(client.GetKvClient(), nullptr);
    EXPECT_FALSE(weak_first.expired());
    snapshot.reset();
    EXPECT_TRUE(weak_first.expired());
}

TEST_F(KvClientTest, FailedInitializationPreservesAttachment) {
    auto sdk = std::shared_ptr<kv::KvClient>(kv::CreateKvClient());
    ASSERT_NE(sdk, nullptr);
    client.SetKvClient(sdk);
    auto empty = client.InitKvClient("");
    ASSERT_FALSE(empty);
    EXPECT_EQ(empty.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(client.GetKvClient(), sdk);

    auto missing = client.InitKvClient(
        "/nonexistent/mooncake-client-kvclient-test/config.conf");
    EXPECT_FALSE(missing);
    EXPECT_EQ(client.GetKvClient(), sdk);
}
#else
TEST_F(KvClientTest, DisabledBuildReportsNotSupported) {
    auto result = client.InitKvClient("kvclient.conf");
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NOT_SUPPORTED);
    EXPECT_EQ(client.GetKvClient(), nullptr);
}
#endif

}  // namespace
}  // namespace mooncake
