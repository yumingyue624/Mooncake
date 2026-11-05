#include "storage/kvclient_backend.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <limits>
#include <mutex>
#include <utility>

#include "device/accelerator_registry.h"

namespace mooncake {

namespace {

ErrorCode ToError(const kv::Status& status) {
    switch (status.code) {
        case kv::StatusCode::OK:
            return ErrorCode::OK;
        case kv::StatusCode::ENTRY_KEY_NOT_FOUND:
        case kv::StatusCode::ENTRY_KEY_NOT_EXIST:
        case kv::StatusCode::ENTRY_DATA_NOT_EXIST:
        case kv::StatusCode::CQE_KEY_NOT_EXIST:
            return ErrorCode::OBJECT_NOT_FOUND;
        case kv::StatusCode::INVALID_ARGUMENT:
        case kv::StatusCode::BUFFER_NOT_REGISTERED:
        case kv::StatusCode::TASK_NOT_FOUND:
        case kv::StatusCode::CQE_INVALID_FIELD_IN_COMMAND:
            return ErrorCode::INVALID_PARAMS;
        case kv::StatusCode::UNSUPPORTED:
        case kv::StatusCode::BUFFER_NOT_SUPPORTED:
        case kv::StatusCode::CQE_INVALID_COMMAND_OPCODE:
            return ErrorCode::NOT_SUPPORTED;
        case kv::StatusCode::NOT_INITIALIZED:
        case kv::StatusCode::RESOURCE_BUSY:
        case kv::StatusCode::CQE_RESOURCE_BUSY:
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        case kv::StatusCode::INTERNAL_ERROR:
        case kv::StatusCode::CQE_INTERNAL_ERROR:
            return ErrorCode::INTERNAL_ERROR;
        default:
            // Generic NOT_FOUND is not proof that the object is missing.
            return ErrorCode::TRANSFER_FAIL;
    }
}

}  // namespace

tl::expected<kv::ResolvedKey, ErrorCode> KvClientBackend::ResolveKey(
    const ObjectKey& key, const TenantId& tenant_id) const {
    kv::ResolvedKey resolved;
    const auto status = sdk_->ResolveKey(key, tenant_id.value(), resolved);
    if (status.code == kv::StatusCode::INVALID_ARGUMENT)
        return tl::unexpected(ErrorCode::INVALID_KEY);
    if (!status.ok()) return tl::unexpected(ToError(status));
    return resolved;
}

KvClientBackend::~KvClientBackend() { (void)Shutdown(); }

tl::expected<void, ErrorCode> KvClientBackend::Shutdown() {
    // Collect outstanding tasks before shutting down the SDK.
    while (!tasks_.empty()) {
        (void)Wait(*tasks_.begin());
    }
    if (!sdk_) return {};
    // SDK shutdown also releases registered memory.
    auto status = sdk_->Shutdown();
    sdk_.reset();
    regions_.clear();
    if (!status.ok()) return tl::unexpected(ToError(status));
    return {};
}

ExternalStoreBackend::TaskHandle KvClientBackend::SaveTask(
    std::function<std::vector<tl::expected<void, ErrorCode>>()> completion) {
    auto* task = new KvTaskContext(std::move(completion));
    std::lock_guard lock(tasks_mutex_);
    tasks_.insert(task);
    return task;
}

std::vector<tl::expected<void, ErrorCode>> KvClientBackend::Wait(
    TaskHandle task) {
    std::unique_ptr<KvTaskContext> context(static_cast<KvTaskContext*>(task));
    {
        std::lock_guard lock(tasks_mutex_);
        tasks_.erase(task);
    }
    return context->completion();
}

tl::expected<void, ErrorCode> KvClientBackend::Init(
    const std::unordered_map<std::string, std::string>& options) {
    const auto config = options.find("config_path");
    if (config == options.end()) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    sdk_ = kv::CreateKvClient();
    auto status = sdk_->Init(config->second);
    if (!status.ok()) return tl::unexpected(ToError(status));
    return {};
}

bool KvClientBackend::SupportsMemory(
    const device::PointerInfo& pointer_info) const {
    // ASU supports device memory for business data.
    return pointer_info.kind == device::MemoryKind::kDevice;
}

tl::expected<void, ErrorCode> KvClientBackend::RegisterMemory(
    void* addr, size_t length, [[maybe_unused]] const std::string& location,
    const device::PointerInfo& pointer_info) {
    std::unique_lock lock(memory_mutex_);
    if (!SupportsMemory(pointer_info)) {
        return tl::unexpected(ErrorCode::NOT_SUPPORTED);
    }
    kv::MemoryRegion region;
    region.memoryType = kv::MemoryType::DEVICE;
    region.addr = reinterpret_cast<uintptr_t>(addr);
    region.size = length;
    region.deviceId = pointer_info.device_id;
    if (regions_.find(region.addr) != regions_.end()) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::vector<kv::RegisteredMemory> registered;
    auto status = sdk_->RegisterRegions({region}, registered);
    if (!status.ok()) return tl::unexpected(ToError(status));
    regions_.emplace(region.addr, registered.front());
    return {};
}

tl::expected<void, ErrorCode> KvClientBackend::UnregisterMemory(void* addr) {
    std::unique_lock lock(memory_mutex_);
    auto iter = regions_.find(reinterpret_cast<uintptr_t>(addr));
    // Host buffers may be registered only with TE.
    if (iter == regions_.end()) {
        return {};
    }
    auto status = sdk_->UnregisterRegions({iter->second.handle});
    if (!status.ok()) return tl::unexpected(ToError(status));
    regions_.erase(iter);
    return {};
}

kv::TaskResult KvClientBackend::WaitForCompletion(kv::TaskId task) {
    kv::TaskResult result;
    // The SDK must drain timed-out work before returning.
    auto status = sdk_->Wait(task, 0, result);  // Use the SDK default timeout.
    result.status = status;
    if (!status.ok() && status.code != kv::StatusCode::PARTIAL_FAILED) {
        // Ignore provisional entry results on a task-level failure.
        result.entryStatus.clear();
        result.queryResult.reset();
    }
    return result;
}

tl::expected<ExternalStoreBackend::TaskHandle, ErrorCode>
KvClientBackend::BatchExistsAsync(const std::vector<ObjectKey>& keys,
                                  const TenantId& tenant_id,
                                  std::vector<uint8_t>* exists) {
    if (keys.empty()) {
        return SaveTask([exists] {
            exists->clear();
            return std::vector<tl::expected<void, ErrorCode>>{};
        });
    }
    std::vector<kv::CacheKey> sdk_keys;
    std::vector<size_t> indices;
    std::vector<tl::expected<void, ErrorCode>> results(keys.size());
    sdk_keys.reserve(keys.size());
    indices.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto key = ResolveKey(keys[i], tenant_id);
        if (!key) {
            results[i] = tl::unexpected(key.error());
            continue;
        }
        sdk_keys.push_back(key->key);
        indices.push_back(i);
    }
    if (sdk_keys.empty()) {
        return SaveTask([exists, results = std::move(results)]() mutable {
            exists->assign(results.size(), 0);
            return std::move(results);
        });
    }
    kv::TaskId task;
    auto submitted = sdk_->QueryAsync(sdk_keys, task);
    if (!submitted.ok()) return tl::unexpected(ToError(submitted));
    return SaveTask([this, task, exists, indices = std::move(indices),
                     results = std::move(results)]() mutable {
        auto completed = WaitForCompletion(task);
        exists->assign(results.size(), 0);
        for (size_t j = 0; j < indices.size(); ++j) {
            const auto index = indices[j];
            const auto& status = completed.entryStatus.empty()
                                     ? completed.status
                                     : completed.entryStatus[j];
            if (status.code == kv::StatusCode::ENTRY_KEY_EXIST) {
                (*exists)[index] = 1;
            } else if (ToError(status) == ErrorCode::OBJECT_NOT_FOUND &&
                       !status.ok()) {
                (*exists)[index] = 0;
            } else if (!status.ok()) {
                results[index] = tl::unexpected(ToError(status));
            } else {
                (*exists)[index] = completed.queryResult->exists[j] != 0;
            }
        }
        return std::move(results);
    });
}

tl::expected<ExternalStoreBackend::TaskHandle, ErrorCode>
KvClientBackend::GetBatchAsync(const std::vector<ObjectKey>& keys,
                               const std::vector<std::vector<Slice>>& targets,
                               const TenantId& tenant_id,
                               std::vector<size_t>* actual_sizes) {
    std::vector<std::vector<size_t>> offsets(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        size_t offset = 0;
        for (const auto& slice : targets[i]) {
            offsets[i].push_back(offset);
            offset += slice.size;
        }
    }
    // MC supplies capacities; this adapter currently requires exact lengths.
    return SubmitLoads(keys, targets, offsets, tenant_id, actual_sizes);
}

tl::expected<ExternalStoreBackend::TaskHandle, ErrorCode>
KvClientBackend::GetRangesAsync(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& targets,
    const std::vector<std::vector<size_t>>& source_offsets,
    const TenantId& tenant_id, std::vector<size_t>* actual_sizes) {
    return SubmitLoads(keys, targets, source_offsets, tenant_id, actual_sizes);
}

tl::expected<ExternalStoreBackend::TaskHandle, ErrorCode>
KvClientBackend::SubmitLoads(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& targets,
    const std::vector<std::vector<size_t>>& source_offsets,
    const TenantId& tenant_id, std::vector<size_t>* actual_sizes) {
    if (keys.empty()) {
        return SaveTask([actual_sizes] {
            actual_sizes->clear();
            return std::vector<tl::expected<void, ErrorCode>>{};
        });
    }
    std::vector<tl::expected<void, ErrorCode>> results(keys.size());
    std::vector<kv::KVBuffer> entries;
    std::vector<size_t> indices;
    std::vector<size_t> lengths;
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators();
    // Keep registrations stable until the SDK accepts the descriptors.
    std::shared_lock lock(memory_mutex_);
    for (size_t i = 0; i < keys.size(); ++i) {
        auto key = ResolveKey(keys[i], tenant_id);
        if (!key) {
            results[i] = tl::unexpected(key.error());
            continue;
        }
        const auto base_offset = key->base_offset;
        std::vector<kv::KVBuffer> object_entries;
        if (targets[i].empty()) {
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        for (size_t j = 0; j < targets[i].size(); ++j) {
            const auto& slice = targets[i][j];
            // Range offsets are relative to this key's slot, not the SDK
            // object.
            const auto source_offset = source_offsets[i][j];
            if (source_offset > std::numeric_limits<uint32_t>::max() ||
                base_offset >
                    std::numeric_limits<uint32_t>::max() - source_offset) {
                results[i] = tl::unexpected(ErrorCode::NOT_SUPPORTED);
                break;
            }
            const auto object_offset = base_offset + source_offset;
            device::PointerInfo info{.kind = device::MemoryKind::kHost,
                                     .device_id = -1};
            runtime.FindDeviceForPointer(slice.ptr, &info);
            if (!SupportsMemory(info)) {
                results[i] = tl::unexpected(ErrorCode::NOT_SUPPORTED);
                break;
            }
            const auto addr = reinterpret_cast<uintptr_t>(slice.ptr);
            auto registered = FindRegisteredMemory(slice);
            if (!registered) {
                results[i] = tl::unexpected(registered.error());
                break;
            }
            // SDK offsets and lengths are limited to uint32.
            if (slice.size > std::numeric_limits<uint32_t>::max()) {
                results[i] = tl::unexpected(ErrorCode::NOT_SUPPORTED);
                break;
            }
            kv::KVBuffer entry{};
            entry.key = key->key;
            entry.buffer.region = (*registered)->region;
            entry.buffer.handle = (*registered)->handle;
            entry.buffer.region.addr = addr;
            entry.buffer.region.size = slice.size;
            // Offset is relative to the object, not the registered region.
            entry.offset = static_cast<uint32_t>(object_offset);
            object_entries.push_back(entry);
        }
        // Submit an input key only when all its slices are valid.
        if (!results[i]) continue;
        for (const auto& entry : object_entries) {
            entries.push_back(entry);
            indices.push_back(i);
            lengths.push_back(entry.buffer.region.size);
        }
    }
    if (entries.empty()) {
        lock.unlock();
        return SaveTask([actual_sizes, results = std::move(results)]() mutable {
            actual_sizes->assign(results.size(), 0);
            return std::move(results);
        });
    }
    kv::TaskId sdk_task;
    auto submitted = sdk_->BatchLoadAsync(entries, sdk_task);
    lock.unlock();
    if (!submitted.ok()) return tl::unexpected(ToError(submitted));
    return SaveTask([this, sdk_task, actual_sizes, indices = std::move(indices),
                     lengths = std::move(lengths),
                     results = std::move(results)]() mutable {
        auto completed = WaitForCompletion(sdk_task);
        actual_sizes->assign(results.size(), 0);
        for (size_t j = 0; j < indices.size(); ++j) {
            const auto& status = completed.entryStatus.empty()
                                     ? completed.status
                                     : completed.entryStatus[j];
            auto& result = results[indices[j]];
            if (!status.ok()) {
                if (result) result = tl::unexpected(ToError(status));
            } else if (result) {
                (*actual_sizes)[indices[j]] += lengths[j];
            }
        }
        return std::move(results);
    });
}

tl::expected<const kv::RegisteredMemory*, ErrorCode>
KvClientBackend::FindRegisteredMemory(const Slice& slice) const {
    const auto addr = reinterpret_cast<uintptr_t>(slice.ptr);
    auto region = regions_.upper_bound(addr);
    if (region == regions_.begin())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    const auto& registered = std::prev(region)->second;
    const auto offset = addr - registered.region.addr;
    if (offset > registered.region.size || slice.size == 0 ||
        slice.size > registered.region.size - offset)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    if (registered.region.memoryType != kv::MemoryType::DEVICE)
        return tl::unexpected(ErrorCode::NOT_SUPPORTED);
    return &registered;
}

tl::expected<void, ErrorCode> KvClientBackend::AppendStoreEntries(
    const kv::CacheKey& key, const std::vector<Slice>& slices,
    uint64_t base_offset, std::vector<kv::KVBuffer>& entries,
    const std::vector<size_t>* destination_offsets) {
    if (slices.empty()) return tl::unexpected(ErrorCode::INVALID_PARAMS);
    if (destination_offsets && destination_offsets->size() != slices.size())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    // Preserve slice order even when the source addresses are noncontiguous.
    uint64_t logical_offset = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (destination_offsets) logical_offset = (*destination_offsets)[i];
        kv::RegisteredMemory registered;
        {
            std::shared_lock lock(memory_mutex_);
            auto region = FindRegisteredMemory(slice);
            if (!region) return tl::unexpected(region.error());
            // Keep a snapshot; the map entry may be erased after unlocking.
            registered = **region;
        }
        const auto addr = reinterpret_cast<uintptr_t>(slice.ptr);
        uint64_t consumed = 0;
        while (consumed < slice.size) {
            if (logical_offset > std::numeric_limits<uint32_t>::max() ||
                base_offset >
                    std::numeric_limits<uint32_t>::max() - logical_offset)
                return tl::unexpected(ErrorCode::NOT_SUPPORTED);
            const uint64_t object_offset = base_offset + logical_offset;
            // Split descriptors, never pad or copy source payload.
            const uint64_t length =
                std::min(slice.size - consumed, uint64_t{0x00fffe00});
            kv::KVBuffer entry{};
            entry.key = key;
            entry.buffer.region = registered.region;
            entry.buffer.region.addr = addr + consumed;
            entry.buffer.region.size = length;
            entry.buffer.handle = registered.handle;
            entry.offset = static_cast<uint32_t>(object_offset);
            entries.push_back(entry);
            consumed += length;
            logical_offset += length;
        }
    }
    return {};
}

std::vector<kv::KVBuffer> KvClientBackend::BuildStoreEntries(
    const TenantId& tenant_id, const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices,
    std::vector<size_t>& indices,
    std::vector<tl::expected<void, ErrorCode>>& results,
    const std::vector<std::vector<size_t>>* destination_offsets) {
    std::vector<kv::KVBuffer> entries;
    results.resize(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto key = ResolveKey(keys[i], tenant_id);
        if (!key) {
            results[i] = tl::unexpected(key.error());
            continue;
        }
        const auto base_offset = key->base_offset;
        const auto begin = entries.size();
        results[i] = AppendStoreEntries(
            key->key, batched_slices[i], base_offset, entries,
            destination_offsets ? &(*destination_offsets)[i] : nullptr);
        if (!results[i]) {
            // Do not submit a partial set of descriptors for this input key.
            entries.resize(begin);
            continue;
        }
        indices.insert(indices.end(), entries.size() - begin, i);
    }
    return entries;
}

std::vector<tl::expected<void, ErrorCode>> KvClientBackend::WaitForKeyResults(
    kv::TaskId task, const std::vector<size_t>& indices,
    std::vector<tl::expected<void, ErrorCode>> results) {
    const auto completed = WaitForCompletion(task);
    const auto& status = completed.status;
    const bool has_entries =
        (status.ok() || status.code == kv::StatusCode::PARTIAL_FAILED) &&
        completed.entryStatus.size() == indices.size();
    // Multiple SDK entries can belong to one input key; any failure wins.
    for (size_t j = 0; j < indices.size(); ++j) {
        const auto error =
            ToError(has_entries ? completed.entryStatus[j] : status);
        auto& item = results[indices[j]];
        if (item && error != ErrorCode::OK) item = tl::unexpected(error);
    }
    return results;
}

tl::expected<ExternalStoreBackend::TaskHandle, ErrorCode>
KvClientBackend::BatchPutAsync(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices,
    const TenantId& tenant_id) {
    if (keys.size() != batched_slices.size())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    std::vector<size_t> indices;
    std::vector<tl::expected<void, ErrorCode>> results;
    auto entries =
        BuildStoreEntries(tenant_id, keys, batched_slices, indices, results);
    if (entries.empty()) {
        return SaveTask([results = std::move(results)]() mutable {
            return std::move(results);
        });
    }
    kv::TaskId task;
    auto submitted = sdk_->BatchStoreAsync(entries, task);
    if (!submitted.ok()) return tl::unexpected(ToError(submitted));
    return SaveTask([this, task, indices = std::move(indices),
                     results = std::move(results)]() mutable {
        return WaitForKeyResults(task, indices, std::move(results));
    });
}

tl::expected<ExternalStoreBackend::TaskHandle, ErrorCode>
KvClientBackend::PutRangesAsync(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& sources,
    const std::vector<std::vector<size_t>>& destination_offsets,
    const TenantId& tenant_id) {
    if (keys.size() != sources.size() ||
        keys.size() != destination_offsets.size())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    std::vector<size_t> indices;
    std::vector<tl::expected<void, ErrorCode>> results;
    auto entries = BuildStoreEntries(tenant_id, keys, sources, indices, results,
                                     &destination_offsets);
    if (entries.empty()) {
        return SaveTask([results = std::move(results)]() mutable {
            return std::move(results);
        });
    }
    kv::TaskId task;
    auto submitted = sdk_->BatchStoreAsync(entries, task);
    if (!submitted.ok()) return tl::unexpected(ToError(submitted));
    return SaveTask([this, task, indices = std::move(indices),
                     results = std::move(results)]() mutable {
        return WaitForKeyResults(task, indices, std::move(results));
    });
}

tl::expected<ExternalStoreBackend::TaskHandle, ErrorCode>
KvClientBackend::BatchRemoveAsync(const std::vector<ObjectKey>& keys,
                                  const TenantId& tenant_id) {
    if (keys.empty()) {
        return SaveTask(
            [] { return std::vector<tl::expected<void, ErrorCode>>{}; });
    }
    std::vector<kv::CacheKey> sdk_keys;
    std::vector<size_t> indices;
    std::vector<tl::expected<void, ErrorCode>> results(keys.size());
    sdk_keys.reserve(keys.size());
    indices.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto key = ResolveKey(keys[i], tenant_id);
        if (!key) {
            results[i] = tl::unexpected(key.error());
            continue;
        }
        sdk_keys.push_back(key->key);
        indices.push_back(i);
    }
    if (sdk_keys.empty()) {
        return SaveTask([results = std::move(results)]() mutable {
            return std::move(results);
        });
    }
    kv::TaskId task;
    auto submitted = sdk_->DeleteAsync(sdk_keys, task);
    if (!submitted.ok()) return tl::unexpected(ToError(submitted));
    return SaveTask([this, task, indices = std::move(indices),
                     results = std::move(results)]() mutable {
        return WaitForKeyResults(task, indices, std::move(results));
    });
}

}  // namespace mooncake
