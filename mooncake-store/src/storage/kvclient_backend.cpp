#include "storage/kvclient_backend.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <mutex>
#include <string_view>
#include <xxhash.h>
#include <utility>

#include "device/accelerator_registry.h"

namespace mooncake {
namespace {

struct VllmKey {
    std::string_view model_name;
    std::string_view chunk_hash;
    uint64_t pp_rank = 0;
    uint64_t pcp_rank = 0;
    uint64_t dcp_rank = 0;
    uint64_t head_or_tp_rank = 0;
    std::optional<uint64_t> layer;
};

bool ParseRank(std::string_view field, std::string_view prefix,
               uint64_t& value) {
    if (!field.starts_with(prefix)) return false;
    field.remove_prefix(prefix.size());
    const auto [end, error] =
        std::from_chars(field.data(), field.data() + field.size(), value);
    return error == std::errc{} && end == field.data() + field.size();
}

tl::expected<VllmKey, ErrorCode> ParseVllmKey(std::string_view key) {
    std::array<std::string_view, 6> fields;
    for (size_t i = 0; i < fields.size() - 1; ++i) {
        const auto end = key.find('@');
        if (end == std::string_view::npos)
            return tl::unexpected(ErrorCode::INVALID_KEY);
        fields[i] = key.substr(0, end);
        key.remove_prefix(end + 1);
    }
    fields.back() = key;
    if (key.find('@') != std::string_view::npos || fields[0].empty())
        return tl::unexpected(ErrorCode::INVALID_KEY);
    VllmKey parsed;
    parsed.model_name = fields[0];
    if (!ParseRank(fields[1], "pcp", parsed.pcp_rank) ||
        !ParseRank(fields[2], "dcp", parsed.dcp_rank) ||
        !ParseRank(fields[3], "head_or_tp_rank:", parsed.head_or_tp_rank))
        return tl::unexpected(ErrorCode::INVALID_KEY);
    if (fields[4].starts_with("pp_rank:")) {
        if (!ParseRank(fields[4], "pp_rank:", parsed.pp_rank))
            return tl::unexpected(ErrorCode::INVALID_KEY);
        parsed.chunk_hash = fields[5];
    } else {
        // Current LayerPoolKey omits PP rank; only PP=1 is supported.
        uint64_t layer;
        if (!ParseRank(fields[5], "", layer))
            return tl::unexpected(ErrorCode::INVALID_KEY);
        parsed.layer = layer;
        parsed.chunk_hash = fields[4];
    }
    if (parsed.chunk_hash.empty())
        return tl::unexpected(ErrorCode::INVALID_KEY);
    return parsed;
}

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

kv::CacheKey KvClientBackend::EncodeKey(const ObjectKey& key,
                                        const TenantId& tenant_id) {
    std::string identity = key;
    if (layout_) {
        auto parsed = ParseVllmKey(key);
        if (parsed) {
            identity.assign(parsed->model_name);
            identity.push_back('\0');
            identity.append(parsed->chunk_hash);
        }
    }
    const auto scoped_key = tenant_id.MakeScopedKey(identity);
    const uint64_t hash = XXH64(scoped_key.data(), scoped_key.size(), 0);
    XXH64_canonical_t canonical;
    XXH64_canonicalFromHash(&canonical, hash);
    kv::CacheKey encoded;
    static_assert(kv::kCacheKeySizeBytes == sizeof(canonical.digest));
    std::memcpy(encoded.data(), canonical.digest, encoded.size());
    return encoded;
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
    auto layout = ParseLayoutConfig(options);
    if (!layout) return tl::unexpected(layout.error());
    sdk_ = kv::CreateKvClient();
    auto status = sdk_->Init(config->second);
    if (!status.ok()) return tl::unexpected(ToError(status));
    layout_ = *layout;
    return {};
}

tl::expected<std::optional<KvClientBackend::LayoutConfig>, ErrorCode>
KvClientBackend::ParseLayoutConfig(
    const std::unordered_map<std::string, std::string>& options) {
    LayoutConfig layout;
    const std::pair<const char*, uint64_t*> fields[] = {
        {"pp_size", &layout.pp_size},
        {"pcp_size", &layout.pcp_size},
        {"dcp_size", &layout.dcp_size},
        {"layer_slots", &layout.layer_slots},
        {"kv_group_slots", &layout.kv_group_slots},
        {"group_slot_bytes", &layout.group_slot_bytes},
    };
    // Existing SDK-only configurations remain valid. A layout, when supplied,
    // must specify every dimension shared by all writers of an object.
    const bool has_layout = std::any_of(
        std::begin(fields), std::end(fields),
        [&](const auto& field) { return options.contains(field.first); });
    if (has_layout) {
        layout.object_capacity = 1;
        for (const auto& [name, value] : fields) {
            const auto it = options.find(name);
            if (it == options.end())
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            const auto& text = it->second;
            const auto [end, error] =
                std::from_chars(text.data(), text.data() + text.size(), *value);
            if (error != std::errc{} || end != text.data() + text.size() ||
                *value == 0 ||
                layout.object_capacity >
                    std::numeric_limits<uint64_t>::max() / *value)
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            layout.object_capacity *= *value;
        }
        if (layout.group_slot_bytes % 512 != 0)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        if (layout.pp_size != 1 ||
            layout.object_capacity >
                uint64_t{std::numeric_limits<uint32_t>::max()} + 1)
            return tl::unexpected(ErrorCode::NOT_SUPPORTED);
    }
    return has_layout ? std::optional<LayoutConfig>{layout} : std::nullopt;
}

bool KvClientBackend::SupportsMemory(
    const device::PointerInfo& pointer_info) const {
    // ASU supports device memory for business data.
    return pointer_info.kind == device::MemoryKind::kDevice;
}

tl::expected<void, ErrorCode> KvClientBackend::RegisterMemory(
    void* addr, size_t length, [[maybe_unused]] const std::string& location,
    const device::PointerInfo& pointer_info) {
    std::lock_guard lock(memory_mutex_);
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
    std::lock_guard lock(memory_mutex_);
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
    for (const auto& key : keys) sdk_keys.push_back(EncodeKey(key, tenant_id));
    kv::TaskId task;
    auto submitted = sdk_->QueryAsync(sdk_keys, task);
    if (!submitted.ok()) return tl::unexpected(ToError(submitted));
    return SaveTask([this, task, exists, count = keys.size()] {
        auto completed = WaitForCompletion(task);
        std::vector<tl::expected<void, ErrorCode>> results(count);
        exists->assign(count, 0);
        for (size_t j = 0; j < count; ++j) {
            const auto& status = completed.entryStatus.empty()
                                     ? completed.status
                                     : completed.entryStatus[j];
            if (status.code == kv::StatusCode::ENTRY_KEY_EXIST) {
                (*exists)[j] = 1;
            } else if (ToError(status) == ErrorCode::OBJECT_NOT_FOUND &&
                       !status.ok()) {
                (*exists)[j] = 0;
            } else if (!status.ok()) {
                results[j] = tl::unexpected(ToError(status));
            } else {
                (*exists)[j] = completed.queryResult->exists[j] != 0;
            }
        }
        return results;
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
    // Resolve registrations while holding the memory lock.
    std::unique_lock lock(memory_mutex_);
    std::vector<tl::expected<void, ErrorCode>> results(keys.size());
    std::vector<kv::KVBuffer> entries;
    std::vector<size_t> indices;
    std::vector<size_t> lengths;
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators();
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto key = EncodeKey(keys[i], tenant_id);
        std::vector<kv::KVBuffer> object_entries;
        if (targets[i].empty()) {
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        for (size_t j = 0; j < targets[i].size(); ++j) {
            const auto& slice = targets[i][j];
            const auto object_offset = source_offsets[i][j];
            device::PointerInfo info{.kind = device::MemoryKind::kHost,
                                     .device_id = -1};
            runtime.FindDeviceForPointer(slice.ptr, &info);
            if (!SupportsMemory(info)) {
                results[i] = tl::unexpected(ErrorCode::NOT_SUPPORTED);
                break;
            }
            const auto addr = reinterpret_cast<uintptr_t>(slice.ptr);
            auto region = regions_.upper_bound(addr);
            if (region == regions_.begin()) {
                results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                break;
            }
            const auto& registered = std::prev(region)->second;
            const auto region_offset = addr - registered.region.addr;
            if (region_offset > registered.region.size ||
                slice.size > registered.region.size - region_offset) {
                results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                break;
            }
            // SDK offsets and lengths are limited to uint32.
            if (object_offset > std::numeric_limits<uint32_t>::max() ||
                slice.size > std::numeric_limits<uint32_t>::max()) {
                results[i] = tl::unexpected(ErrorCode::NOT_SUPPORTED);
                break;
            }
            kv::KVBuffer entry{};
            entry.key = key;
            entry.buffer.region = registered.region;
            entry.buffer.handle = registered.handle;
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
        return SaveTask([actual_sizes, results = std::move(results)]() mutable {
            actual_sizes->assign(results.size(), 0);
            return std::move(results);
        });
    }
    kv::TaskId sdk_task;
    auto submitted = sdk_->BatchLoadAsync(entries, sdk_task);
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

tl::expected<std::optional<KvClientBackend::StoreLayout>, ErrorCode>
KvClientBackend::ResolveStoreLayout(const ObjectKey& key,
                                    const std::vector<Slice>& slices) const {
    if (!layout_) return std::optional<StoreLayout>{};
    auto parsed = ParseVllmKey(key);
    if (!parsed) return tl::unexpected(parsed.error());
    if (parsed->pp_rank >= layout_->pp_size ||
        parsed->pcp_rank >= layout_->pcp_size ||
        parsed->dcp_rank >= layout_->dcp_size ||
        parsed->head_or_tp_rank >= layout_->kv_group_slots ||
        (parsed->layer && *parsed->layer >= layout_->layer_slots))
        return tl::unexpected(ErrorCode::INVALID_KEY);
    uint64_t layer_bytes = 0;
    for (const auto& slice : slices) {
        if (slice.size > std::numeric_limits<uint64_t>::max() - layer_bytes)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        layer_bytes += slice.size;
    }
    // PP=1: non-layerwise inputs contain layer_slots equal-sized layers.
    const auto layers = parsed->layer ? uint64_t{1} : layout_->layer_slots;
    if (layer_bytes == 0 || layer_bytes % layers != 0)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    layer_bytes /= layers;
    if (layer_bytes > layout_->group_slot_bytes || layer_bytes % 512 != 0)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    const auto base_offset =
        ((((parsed->pp_rank * layout_->pcp_size + parsed->pcp_rank) *
               layout_->dcp_size +
           parsed->dcp_rank) *
              layout_->layer_slots +
          parsed->layer.value_or(0)) *
             layout_->kv_group_slots +
         parsed->head_or_tp_rank) *
        layout_->group_slot_bytes;
    return std::optional<StoreLayout>{
        StoreLayout{base_offset, layer_bytes,
                    layout_->kv_group_slots * layout_->group_slot_bytes}};
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
    const std::optional<StoreLayout>& layout,
    std::vector<kv::KVBuffer>& entries) {
    if (slices.empty()) return tl::unexpected(ErrorCode::INVALID_PARAMS);
    uint64_t logical_offset = 0;
    for (const auto& slice : slices) {
        auto registered = FindRegisteredMemory(slice);
        if (!registered) return tl::unexpected(registered.error());
        const auto addr = reinterpret_cast<uintptr_t>(slice.ptr);
        uint64_t consumed = 0;
        while (consumed < slice.size) {
            uint64_t object_offset = 0;
            uint64_t length = slice.size - consumed;
            if (layout) {
                const auto layer = logical_offset / layout->layer_bytes;
                const auto within_layer = logical_offset % layout->layer_bytes;
                object_offset = layout->base_offset +
                                layer * layout->layer_stride + within_layer;
                // Split descriptors, never pad or copy source payload.
                length = std::min({length, layout->layer_bytes - within_layer,
                                   uint64_t{0x00fffe00}});
                if (object_offset % 512 != 0 || length % 512 != 0)
                    return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
            kv::KVBuffer entry{};
            entry.key = key;
            entry.buffer.region = (*registered)->region;
            entry.buffer.region.addr = addr + consumed;
            entry.buffer.region.size = length;
            entry.buffer.handle = (*registered)->handle;
            entry.offset = static_cast<uint32_t>(object_offset);
            entries.push_back(entry);
            consumed += length;
            if (layout) logical_offset += length;
        }
    }
    return {};
}

std::vector<kv::KVBuffer> KvClientBackend::BuildStoreEntries(
    const TenantId& tenant_id, const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices,
    std::vector<size_t>& indices,
    std::vector<tl::expected<void, ErrorCode>>& results) {
    // The caller holds memory_mutex_ throughout layout/registration resolution.
    std::vector<kv::KVBuffer> entries;
    results.resize(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto layout = ResolveStoreLayout(keys[i], batched_slices[i]);
        if (!layout) {
            results[i] = tl::unexpected(layout.error());
            continue;
        }
        const auto begin = entries.size();
        results[i] = AppendStoreEntries(EncodeKey(keys[i], tenant_id),
                                        batched_slices[i], *layout, entries);
        if (!results[i]) {
            entries.resize(begin);
            continue;
        }
        indices.insert(indices.end(), entries.size() - begin, i);
    }
    return entries;
}

std::vector<tl::expected<void, ErrorCode>> KvClientBackend::WaitForWrite(
    kv::TaskId task, const std::vector<size_t>& indices,
    std::vector<tl::expected<void, ErrorCode>> results) {
    const auto completed = WaitForCompletion(task);
    const auto& status = completed.status;
    const bool has_entries =
        (status.ok() || status.code == kv::StatusCode::PARTIAL_FAILED) &&
        completed.entryStatus.size() == indices.size();
    for (size_t j = 0; j < indices.size(); ++j) {
        const auto error =
            ToError(has_entries ? completed.entryStatus[j] : status);
        auto& item = results[indices[j]];
        if (item && error != ErrorCode::OK) item = tl::unexpected(error);
    }
    return results;
}

tl::expected<void*, ErrorCode> KvClientBackend::Put(
    const TenantId& tenant_id, const ObjectKey& key,
    const std::vector<Slice>& slices) {
    std::unique_lock memory_lock(memory_mutex_);
    std::vector<size_t> indices;
    std::vector<tl::expected<void, ErrorCode>> results;
    auto entries =
        BuildStoreEntries(tenant_id, {key}, {slices}, indices, results);
    if (!results.front()) return tl::unexpected(results.front().error());
    kv::TaskId task;
    auto submitted = sdk_->StoreAsync(entries, task);
    if (!submitted.ok()) return tl::unexpected(ToError(submitted));
    return SaveTask([this, task, indices = std::move(indices),
                     results = std::move(results)]() mutable {
        return WaitForWrite(task, indices, std::move(results));
    });
}

tl::expected<void*, ErrorCode> KvClientBackend::BatchPut(
    const TenantId& tenant_id, const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    if (keys.size() != batched_slices.size())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    std::unique_lock memory_lock(memory_mutex_);
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
        return WaitForWrite(task, indices, std::move(results));
    });
}

tl::expected<void*, ErrorCode> KvClientBackend::Remove(
    const TenantId& tenant_id, const ObjectKey& key) {
    kv::TaskId task;
    auto submitted = sdk_->DeleteAsync({EncodeKey(key, tenant_id)}, task);
    if (!submitted.ok()) return tl::unexpected(ToError(submitted));
    return SaveTask([this, task] {
        return WaitForWrite(task, {0},
                            std::vector<tl::expected<void, ErrorCode>>(1));
    });
}

tl::expected<void*, ErrorCode> KvClientBackend::BatchRemove(
    const TenantId& tenant_id, const std::vector<ObjectKey>& keys) {
    if (keys.empty()) {
        return SaveTask(
            [] { return std::vector<tl::expected<void, ErrorCode>>{}; });
    }
    std::vector<kv::CacheKey> sdk_keys;
    std::vector<size_t> indices;
    sdk_keys.reserve(keys.size());
    indices.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        sdk_keys.push_back(EncodeKey(keys[i], tenant_id));
        indices.push_back(i);
    }
    kv::TaskId task;
    auto submitted = sdk_->DeleteAsync(sdk_keys, task);
    if (!submitted.ok()) return tl::unexpected(ToError(submitted));
    return SaveTask([this, task, indices = std::move(indices)] {
        return WaitForWrite(
            task, indices,
            std::vector<tl::expected<void, ErrorCode>>(indices.size()));
    });
}

}  // namespace mooncake
