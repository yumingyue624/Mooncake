#include "kv_client_io.h"

#ifdef USE_KVCLIENT
#include <kv_client.h>
#include <glog/logging.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <mutex>
#include <thread>
#include "crc_checksum.h"
#include "device/accelerator_registry.h"

namespace mooncake {
namespace {
constexpr uint64_t kMagic = 0x31554d43564b434dULL;
constexpr size_t kHeader = 64;
constexpr size_t kAlignment = 512;
size_t Align(size_t n) { return (n + kAlignment - 1) & ~(kAlignment - 1); }
void Put64(char* p, uint64_t n) {
    for (size_t i = 0; i != 8; ++i) p[i] = static_cast<char>(n >> (8 * i));
}
uint64_t Get64(const char* p) {
    uint64_t n = 0;
    for (size_t i = 0; i != 8; ++i)
        n |= uint64_t(static_cast<unsigned char>(p[i])) << (8 * i);
    return n;
}
kv::CacheKey PhysicalKey(const std::string& identity) {
    // Stable across processes. The full identity is stored and checked on
    // every read; a 64-bit key collision is an error, never a false cache hit.
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : identity) hash = (hash ^ c) * 1099511628211ULL;
    if (!hash) hash = 1;
    kv::CacheKey key{};
    for (size_t i = 0; i != key.size(); ++i)
        key[i] = std::byte(hash >> (8 * i));
    return key;
}
bool CopyBytes(void* destination, const void* source, size_t bytes,
               bool to_host) {
    if (!bytes) return true;
    auto devices = device::GetAcceleratorRegistry().RuntimeAccelerators();
    device::PointerInfo info{};
    auto* accelerator =
        devices.FindDeviceForPointer(to_host ? source : destination, &info);
    if (!accelerator) {
        std::memcpy(destination, source, bytes);
        return true;
    }
    const auto previous = accelerator->CurrentDeviceId();
    accelerator->SetContext(info.device_id);
    bool ok = accelerator->Copy(destination, source, bytes,
                                to_host ? device::CopyDirection::kDeviceToHost
                                        : device::CopyDirection::kHostToDevice);
    if (previous >= 0) accelerator->SetContext(previous);
    return ok;
}
ErrorCode Convert(const kv::Status& status) {
    using S = kv::StatusCode;
    if (status.ok()) return ErrorCode::OK;
    if (status.code == S::NOT_FOUND || status.code == S::ENTRY_KEY_NOT_FOUND ||
        status.code == S::ENTRY_DATA_NOT_EXIST ||
        status.code == S::CQE_KEY_NOT_EXIST)
        return ErrorCode::OBJECT_NOT_FOUND;
    if (status.code == S::INVALID_ARGUMENT) return ErrorCode::INVALID_PARAMS;
    return ErrorCode::TRANSFER_FAIL;
}
struct Buffer {
    explicit Buffer(size_t n) : allocation(new char[n + kAlignment]), size(n) {
        data = reinterpret_cast<char*>(
            Align(reinterpret_cast<uintptr_t>(allocation.get())));
        std::memset(data, 0, size);
    }
    ~Buffer() {
        // An exception must not free a still-registered DMA region. Normal
        // release clears the handle only after unregister/shutdown succeeds.
        if (handle != kv::kInvalidMRHandle) (void)allocation.release();
    }
    std::unique_ptr<char[]> allocation;
    char* data;
    size_t size;
    kv::MRHandle handle = kv::kInvalidMRHandle;
    std::vector<kv::MRHandle> handles;
};
class NativeIO;
class NativeWrite final : public KvWrite {
   public:
    NativeWrite(std::shared_ptr<NativeIO> owner, std::string key,
                std::unique_ptr<Buffer> buffer, kv::TaskId task);
    ~NativeWrite() override;
    ErrorCode Start();
    ErrorCode Wait() override;
    ErrorCode Commit() override;

   private:
    std::shared_ptr<NativeIO> owner_;
    std::string key_;
    std::unique_ptr<Buffer> buffer_;
    KvObjectInfo info_;
    kv::TaskId task_;
    ErrorCode result_ = ErrorCode::TRANSFER_FAIL;
    bool waited_ = false;
    bool committed_ = false;
};

class NativeIO final : public KvClientIO,
                       public std::enable_shared_from_this<NativeIO> {
   public:
    NativeIO(std::shared_ptr<kv::KvClient> sdk, std::string tenant)
        : sdk_(std::move(sdk)) {
        (void)tenant;
    }
    ~NativeIO() override {
        // Pending task objects retain this owner. All normal IO has drained
        // before destruction; shutdown fences registrations quarantined after
        // timeout/transport errors. Never free DMA buffers on failed shutdown.
        bool stopped = false;
        try {
            stopped = sdk_->Shutdown().ok();
        } catch (...) {
        }
        if (stopped) {
            for (auto& buffer : quarantine_)
                buffer->handle = kv::kInvalidMRHandle;
        } else {
            LOG(ERROR)
                << "KvClient shutdown failed; retaining unsafe IO buffers";
        }
    }
    tl::expected<std::unique_ptr<KvWrite>, ErrorCode> StartWrite(
        const std::string& key, const KvObjectInfo& info,
        const std::vector<Slice>& slices) override try {
        const auto identity = prefix_ + key;
        if (identity.size() > std::numeric_limits<uint32_t>::max() - 1024)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        size_t bytes = 0;
        const size_t metadata = Align(kHeader + identity.size());
        for (const auto& slice : slices) {
            if ((!slice.ptr && slice.size) ||
                slice.size > UINT32_MAX - metadata - bytes - 511)
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            bytes += slice.size;
        }
        if (!bytes || bytes != info.object_size)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        // Reject an existing colliding identity before overwriting it. An
        // independent full-key/CRC validation also protects every later read.
        auto existing = Metadata(key, info);
        if (!existing && existing.error() != ErrorCode::OBJECT_NOT_FOUND)
            return tl::unexpected(existing.error());
        auto buffer = std::make_unique<Buffer>(Align(metadata + bytes));
        char* payload = buffer->data + metadata;
        size_t offset = 0;
        for (const auto& slice : slices) {
            if (!CopyBytes(payload + offset, slice.ptr, slice.size, true))
                return tl::unexpected(ErrorCode::TRANSFER_FAIL);
            offset += slice.size;
        }
        const auto generation = info.generation;
        Put64(buffer->data, kMagic);
        Put64(buffer->data + 8, bytes);
        Put64(buffer->data + 16, identity.size());
        Put64(buffer->data + 24, ComputeCrcChecksum(payload, bytes));
        Put64(buffer->data + 32, generation.first);
        Put64(buffer->data + 40, generation.second);
        // Reserved header bytes stay zero; publication is controlled by Master.
        std::memcpy(buffer->data + kHeader, identity.data(), identity.size());
        auto pending = std::make_unique<NativeWrite>(
            shared_from_this(), key, std::move(buffer), kv::kInvalidTaskId);
        auto status = pending->Start();
        if (status != ErrorCode::OK) return tl::unexpected(status);
        return std::unique_ptr<KvWrite>(std::move(pending));
    } catch (const std::bad_alloc&) {
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    ErrorCode Read(const std::string& key, const KvObjectInfo& info,
                   const std::vector<Slice>& slices) override {
        size_t capacity = 0;
        for (const auto& slice : slices) {
            if (!slice.ptr && slice.size) return ErrorCode::INVALID_PARAMS;
            if (slice.size > SIZE_MAX - capacity)
                return ErrorCode::INVALID_PARAMS;
            capacity += slice.size;
        }
        const auto identity = prefix_ + key;
        const auto metadata = Align(kHeader + identity.size());
        if (capacity < info.object_size ||
            info.object_size > UINT32_MAX - metadata - 511)
            return ErrorCode::INVALID_PARAMS;
        auto buffer =
            std::make_unique<Buffer>(Align(metadata + info.object_size));
        auto status = Transfer(*buffer, key, info, true, buffer->size);
        if (status == ErrorCode::OK) {
            auto actual = Validate(*buffer, key, info);
            if (!actual)
                status = actual.error();
            else if (actual->generation != info.generation ||
                     actual->object_size != info.object_size)
                status = ErrorCode::INVALID_VERSION;
            else if (Get64(buffer->data + 24) !=
                     ComputeCrcChecksum(buffer->data + metadata,
                                        info.object_size))
                status = ErrorCode::CHECKSUM_MISMATCH;
        }
        if (status == ErrorCode::OK) {
            size_t offset = 0;
            for (const auto& slice : slices) {
                auto bytes =
                    std::min<uint64_t>(slice.size, info.object_size - offset);
                if (!CopyBytes(slice.ptr, buffer->data + metadata + offset,
                               bytes, false)) {
                    status = ErrorCode::TRANSFER_FAIL;
                    break;
                }
                offset += bytes;
                if (offset == info.object_size) break;
            }
        }
        Release(std::move(buffer), status != ErrorCode::TRANSFER_FAIL);
        return status;
    }
    ErrorCode Delete(const std::string& key,
                     const KvObjectInfo& info) override {
        auto current = Metadata(key, info);
        if (!current)
            return current.error() == ErrorCode::OBJECT_NOT_FOUND
                       ? ErrorCode::OK
                       : current.error();
        kv::TaskId task = kv::kInvalidTaskId;
        auto submitted = sdk_->DeleteAsync({DeviceKey(key, info)}, task);
        if (!submitted.ok()) return Convert(submitted);
        return WaitTask(task);
    }
    kv::CacheKey DeviceKey(const std::string& key,
                           const KvObjectInfo& info) const {
        auto physical_identity = prefix_ + key;
        char generation[16];
        Put64(generation, info.generation.first);
        Put64(generation + 8, info.generation.second);
        physical_identity.append(generation, sizeof(generation));
        return PhysicalKey(physical_identity);
    }
    ErrorCode WaitTask(kv::TaskId task) {
        // Wait(timeout) consumes the SDK task even on timeout. Poll to terminal
        // state first, then retrieve once. Provider timeouts are still errors,
        // and their registered buffers are quarantined until shutdown.
        while (!sdk_->Check(task))
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        kv::TaskResult result;
        auto status = sdk_->Wait(task, 1000, result);
        if (!status.ok() && status.code != kv::StatusCode::PARTIAL_FAILED)
            return Convert(status);
        if (result.entryStatus.size() != 1) return ErrorCode::TRANSFER_FAIL;
        for (const auto& entry : result.entryStatus)
            if (!entry.ok()) return Convert(entry);
        if (!status.ok()) return Convert(status);
        return Convert(result.status);
    }
    ErrorCode Submit(Buffer& buffer, const std::string& key,
                     const KvObjectInfo& info, bool read, size_t length,
                     kv::TaskId& task) {
        if (buffer.handle == kv::kInvalidMRHandle) {
            kv::MemoryRegion region{kv::MemoryType::HOST,
                                    reinterpret_cast<uint64_t>(buffer.data),
                                    buffer.size};
            std::vector<kv::RegisteredMemory> registered;
            auto status = sdk_->RegisterRegions({region}, registered);
            if (!registered.empty()) buffer.handle = registered.front().handle;
            for (const auto& region : registered)
                buffer.handles.push_back(region.handle);
            if (!buffer.handles.empty()) buffer.handle = buffer.handles.front();
            if (!status.ok() || registered.size() != 1 ||
                buffer.handle == kv::kInvalidMRHandle)
                return ErrorCode::TRANSFER_FAIL;
            buffer.handle = registered.front().handle;
        }
        kv::KVBuffer entry{};
        entry.key = DeviceKey(key, info);
        entry.buffer.region = {kv::MemoryType::HOST,
                               reinterpret_cast<uint64_t>(buffer.data), length};
        entry.buffer.handle = buffer.handle;
        auto status = read ? sdk_->BatchLoadAsync({entry}, task)
                           : sdk_->BatchStoreAsync({entry}, task);
        if (status.ok() && task == kv::kInvalidTaskId)
            return ErrorCode::TRANSFER_FAIL;
        if (!status.ok()) {
            if (task != kv::kInvalidTaskId && WaitTask(task) != ErrorCode::OK) {
                task = kv::kInvalidTaskId;
                return ErrorCode::TRANSFER_FAIL;
            }
            task = kv::kInvalidTaskId;
            return Convert(status);
        }
        return ErrorCode::OK;
    }
    ErrorCode Transfer(Buffer& buffer, const std::string& key,
                       const KvObjectInfo& info, bool read, size_t length) {
        kv::TaskId task = kv::kInvalidTaskId;
        auto status = Submit(buffer, key, info, read, length, task);
        return status == ErrorCode::OK ? WaitTask(task) : status;
    }
    void Release(std::unique_ptr<Buffer> buffer, bool safe) {
        if (!buffer) return;
        if (buffer->handle == kv::kInvalidMRHandle) return;
        if (safe && sdk_->UnregisterRegions(buffer->handles).ok()) {
            buffer->handle = kv::kInvalidMRHandle;
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        quarantine_.push_back(std::move(buffer));
    }

   private:
    tl::expected<KvObjectInfo, ErrorCode> Validate(const Buffer& buffer,
                                                   const std::string& key,
                                                   const KvObjectInfo& info) {
        const auto identity = prefix_ + key;
        if (Get64(buffer.data) != kMagic ||
            Get64(buffer.data + 16) != identity.size() ||
            std::memcmp(buffer.data + kHeader, identity.data(),
                        identity.size()) != 0)
            return tl::unexpected(ErrorCode::INVALID_KEY);
        if (Get64(buffer.data + 32) != info.generation.first ||
            Get64(buffer.data + 40) != info.generation.second)
            return tl::unexpected(ErrorCode::INVALID_VERSION);
        const auto bytes = Get64(buffer.data + 8);
        if (!bytes ||
            bytes > UINT32_MAX - Align(kHeader + identity.size()) - 511)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        return KvObjectInfo{bytes,
                            {Get64(buffer.data + 32), Get64(buffer.data + 40)}};
    }
    tl::expected<KvObjectInfo, ErrorCode> Metadata(const std::string& key,
                                                   const KvObjectInfo& info) {
        const auto identity = prefix_ + key;
        if (identity.size() > UINT32_MAX - 1024)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        auto buffer =
            std::make_unique<Buffer>(Align(kHeader + identity.size()));
        auto status = Transfer(*buffer, key, info, true, buffer->size);
        auto actual =
            status == ErrorCode::OK
                ? Validate(*buffer, key, info)
                : tl::expected<KvObjectInfo, ErrorCode>(tl::unexpected(status));
        Release(std::move(buffer), status != ErrorCode::TRANSFER_FAIL);
        return actual;
    }
    std::shared_ptr<kv::KvClient> sdk_;
    std::string prefix_;
    std::mutex mutex_;
    std::vector<std::unique_ptr<Buffer>> quarantine_;
};
NativeWrite::NativeWrite(std::shared_ptr<NativeIO> owner, std::string key,
                         std::unique_ptr<Buffer> buffer, kv::TaskId task)
    : owner_(std::move(owner)),
      key_(std::move(key)),
      buffer_(std::move(buffer)),
      info_{Get64(buffer_->data + 8),
            {Get64(buffer_->data + 32), Get64(buffer_->data + 40)}},
      task_(task) {}
ErrorCode NativeWrite::Start() {
    result_ =
        owner_->Submit(*buffer_, key_, info_, false, buffer_->size, task_);
    return result_;
}
NativeWrite::~NativeWrite() {
    try {
        (void)Wait();
        if (!committed_ && owner_->Delete(key_, info_) != ErrorCode::OK)
            LOG(WARNING) << "Failed to clean up unpublished ASU generation for "
                         << key_;
        owner_->Release(std::move(buffer_),
                        result_ != ErrorCode::TRANSFER_FAIL);
    } catch (...) {
        // Buffer's destructor conservatively retains registered storage.
        LOG(ERROR) << "Exception while draining ASU IO for " << key_;
    }
}
ErrorCode NativeWrite::Wait() {
    if (!waited_) {
        if (task_ != kv::kInvalidTaskId) result_ = owner_->WaitTask(task_);
        waited_ = true;
    }
    return result_;
}
ErrorCode NativeWrite::Commit() {
    if (committed_) return ErrorCode::OK;
    if (Wait() != ErrorCode::OK) return result_;
    committed_ = true;
    return result_;
}
}  // namespace
std::shared_ptr<KvClientIO> MakeKvClientIO(std::shared_ptr<kv::KvClient> sdk,
                                           const std::string& tenant) {
    return sdk ? std::make_shared<NativeIO>(std::move(sdk), tenant) : nullptr;
}
}  // namespace mooncake
#else
namespace mooncake {
std::shared_ptr<KvClientIO> MakeKvClientIO(std::shared_ptr<kv::KvClient>,
                                           const std::string&) {
    return nullptr;
}
}  // namespace mooncake
#endif
