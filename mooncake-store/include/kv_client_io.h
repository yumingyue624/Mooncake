#pragma once

#include <memory>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace kv {
class KvClient;
}

namespace mooncake {

// An ASU object is immutable. The generation lets a reader reject a cached
// descriptor after removal/recreation rather than returning another version.
struct KvObjectInfo {
    uint64_t object_size = 0;
    UUID generation{};
    YLT_REFL(KvObjectInfo, object_size, generation);
};

class KvWrite {
   public:
    virtual ~KvWrite() = default;
    // Also called by the concrete destructor, so every exit drains the IO.
    virtual ErrorCode Wait() = 0;
    // Disarm rollback before Master finalization, whose RPC response may be
    // lost even after publication. Master alone controls object visibility.
    virtual ErrorCode Commit() = 0;
};

// Mooncake object framing and staging over the native SDK. This is not a
// replacement SDK interface: keys, registrations and task handles stay inside.
class KvClientIO {
   public:
    virtual ~KvClientIO() = default;
    virtual tl::expected<std::unique_ptr<KvWrite>, ErrorCode> StartWrite(
        const std::string& key, const KvObjectInfo& info,
        const std::vector<Slice>& slices) = 0;
    virtual ErrorCode Read(const std::string& key, const KvObjectInfo& info,
                           const std::vector<Slice>& slices) = 0;
    virtual ErrorCode Delete(const std::string& key,
                             const KvObjectInfo& info) = 0;
};

std::shared_ptr<KvClientIO> MakeKvClientIO(std::shared_ptr<kv::KvClient> sdk,
                                           const std::string& tenant);

}  // namespace mooncake
