#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

/**
 * @brief Fixed-width device key used by external KV-semantics storage
 * devices. Matches the 8-byte CacheKey of the UCM kvclient so an adapter
 * can forward it without re-hashing.
 */
using KvCacheKey = std::array<uint8_t, 8>;

/**
 * @brief Deterministic mapping from a Store object key to a device key.
 *
 * FNV-1a 64 keeps the mapping stable across processes and machines;
 * hash-seed variance would silently split the key space. Collisions are
 * possible in principle and remain the device's concern: it owns the
 * authoritative key table.
 */
inline KvCacheKey MakeKvCacheKey(const std::string& key) {
    constexpr uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
    constexpr uint64_t kFnvPrime = 1099511628211ULL;
    uint64_t hash = kFnvOffsetBasis;
    for (unsigned char c : key) {
        hash ^= c;
        hash *= kFnvPrime;
    }
    KvCacheKey device_key{};
    for (size_t i = 0; i < device_key.size(); ++i) {
        device_key[i] = static_cast<uint8_t>(hash >> (8 * i));
    }
    return device_key;
}

/**
 * @brief Port interface for an external KV-semantics storage device client,
 * e.g. an ASU appliance reached through the UCM kvclient.
 *
 * The interface mirrors the async surface of the kvclient: batched
 * operations return a task handle that is completed through Check/Wait.
 * Mooncake treats the device as opaque; an adapter outside this repository
 * implements this interface on top of the concrete device SDK.
 */
class KvDeviceClient {
   public:
    /// @brief Handle of an async task. Zero is never a valid handle.
    using TaskHandle = uint64_t;
    static constexpr TaskHandle kInvalidTaskHandle = 0;

    /// @brief Handle of a registered memory region.
    using RegionHandle = uint64_t;
    static constexpr RegionHandle kInvalidRegionHandle = 0;

    /// @brief Memory types a device buffer can be backed by.
    enum class MemoryType : uint8_t {
        HOST = 0,
        HOST_PINNED = 1,
        DEVICE = 2,
    };

    /// @brief A memory region offered to the device for remote access.
    struct Region {
        MemoryType type{MemoryType::HOST};
        void* addr{nullptr};
        uint64_t size{0};
        int32_t device_id{-1};
        int32_t numa_node{-1};
    };

    /// @brief One object placement inside a registered region.
    struct Entry {
        std::string key;        ///< Store object key
        void* buffer{nullptr};  ///< Address inside a registered region
        uint32_t offset{0};     ///< Offset within the buffer
        uint64_t length{0};     ///< Bytes to store or load
    };

    /// @brief Final result of a task, in submission order.
    struct TaskResult {
        ErrorCode status{ErrorCode::OK};
        std::vector<ErrorCode> entry_status;
        /// @brief QueryAsync only: 1 = key present on the device.
        std::vector<uint8_t> exists;
    };

    virtual ~KvDeviceClient() = default;

    /**
     * @brief Register memory regions with the device.
     * @note Regions referenced by queued or in-flight tasks must remain
     * registered until those tasks complete.
     */
    virtual tl::expected<std::vector<RegionHandle>, ErrorCode>
    RegisterRegions(const std::vector<Region>& regions) = 0;

    /// @brief Unregister previously registered regions.
    virtual tl::expected<void, ErrorCode> UnregisterRegions(
        const std::vector<RegionHandle>& handles) = 0;

    /// @brief Store object bytes from the entry buffers to the device.
    virtual tl::expected<TaskHandle, ErrorCode> StoreAsync(
        const std::vector<Entry>& entries) = 0;

    /// @brief Load object bytes from the device into the entry buffers.
    virtual tl::expected<TaskHandle, ErrorCode> LoadAsync(
        const std::vector<Entry>& entries) = 0;

    /// @brief Check which keys exist on the device.
    virtual tl::expected<TaskHandle, ErrorCode> QueryAsync(
        const std::vector<std::string>& keys) = 0;

    /// @brief Delete keys from the device.
    virtual tl::expected<TaskHandle, ErrorCode> DeleteAsync(
        const std::vector<std::string>& keys) = 0;

    /// @brief Poll a task; false while it is still in progress.
    virtual bool Check(TaskHandle handle) = 0;

    /// @brief Wait for a task and return its final result.
    virtual tl::expected<TaskResult, ErrorCode> Wait(TaskHandle handle,
                                                     uint64_t timeout_ms) = 0;
};

}  // namespace mooncake
