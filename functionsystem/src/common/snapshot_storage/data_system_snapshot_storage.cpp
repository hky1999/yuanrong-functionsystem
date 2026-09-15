/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#include "common/snapshot_storage/data_system_snapshot_storage.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <functional>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/kv_client/kv_client.h"
#include "openssl/sha.h"

namespace functionsystem::snapshot_storage {
namespace {

constexpr char ENVELOPE_MAGIC_V1[] = "YRSNAP01";
constexpr char ENVELOPE_MAGIC[] = "YRSNAP02";
constexpr size_t MAGIC_SIZE = sizeof(ENVELOPE_MAGIC) - 1;
constexpr size_t ENVELOPE_UINT64_FIELD_COUNT = 5;

using CreateDataSystemBuffer = std::function<datasystem::Status(
    const std::string &, uint64_t, const datasystem::SetParam &, std::shared_ptr<datasystem::Buffer> &)>;
using SetDataSystemBuffer = std::function<datasystem::Status(const std::shared_ptr<datasystem::Buffer> &)>;

Status PutFileToDataSystemBuffer(const std::string &key, const std::string &sourceFile,
                                 const SnapshotObjectMetadata &metadata,
                                 const CreateDataSystemBuffer &createBuffer,
                                 const SetDataSystemBuffer &setBuffer);
SnapshotStat StatDataSystemBuffer(const void *data, uint64_t size);
Status DownloadDataSystemBuffer(const void *data, uint64_t size, const std::string &destinationFile,
                                SnapshotObjectMetadata &metadata);

class KVDataSystemSnapshotClient final : public DataSystemSnapshotClient {
public:
    Status Init(const DataSystemSnapshotConfig &) override
    {
        return Status::OK();
    }

    Status Put(const std::string &key, const std::string &value) override
    {
        return KVClient::GetInstance().Put(key, value);
    }

    DataSystemGetResult Get(const std::string &key) override
    {
        auto [rawStatus, buffer] = KVClient::GetInstance().GetRaw(key);
        auto status = detail::MapDataSystemStatus(rawStatus);
        if (status.IsError()) {
            return { std::move(status), {} };
        }
        return { Status::OK(), std::string(static_cast<const char *>(buffer.ImmutableData()), buffer.GetSize()) };
    }

    Status Delete(const std::string &key) override
    {
        return detail::MapDataSystemStatus(KVClient::GetInstance().DeleteRaw(key));
    }

    Status PutFile(const std::string &key, const std::string &sourceFile,
                   const SnapshotObjectMetadata &metadata) override
    {
        return PutFileToDataSystemBuffer(
            key, sourceFile, metadata,
            [](const std::string &objectKey, uint64_t size, const datasystem::SetParam &param,
               std::shared_ptr<datasystem::Buffer> &buffer) {
                return KVClient::GetInstance().CreateRaw(objectKey, size, param, buffer);
            },
            [](const std::shared_ptr<datasystem::Buffer> &buffer) {
                return KVClient::GetInstance().SetRaw(buffer);
            });
    }

    SnapshotStat StatObject(const std::string &key) override
    {
        auto [status, buffer] = KVClient::GetInstance().GetRaw(key);
        if (status.IsError()) {
            return { detail::MapDataSystemStatus(status), {} };
        }
        return StatDataSystemBuffer(buffer.ImmutableData(), static_cast<uint64_t>(buffer.GetSize()));
    }

    Status DownloadFile(const std::string &key, const std::string &destinationFile,
                        SnapshotObjectMetadata &metadata) override
    {
        auto [status, buffer] = KVClient::GetInstance().GetRaw(key);
        if (status.IsError()) {
            return detail::MapDataSystemStatus(status);
        }
        return DownloadDataSystemBuffer(buffer.ImmutableData(), static_cast<uint64_t>(buffer.GetSize()),
                                        destinationFile, metadata);
    }
};

class DedicatedKVDataSystemSnapshotClient final : public DataSystemSnapshotClient {
public:
    Status Init(const DataSystemSnapshotConfig &config) override
    {
        datasystem::ConnectOptions connectOptions{};
        connectOptions.host = config.host;
        connectOptions.port = config.port;
        auto client = std::make_unique<datasystem::KVClient>(connectOptions);
        auto status = client->Init();
        if (status.IsError()) {
            return detail::MapDataSystemStatus(status);
        }
        client_ = std::move(client);
        return Status::OK();
    }

    Status Put(const std::string &key, const std::string &value) override
    {
        return detail::MapDataSystemStatus(client_->Set(key, value));
    }

    DataSystemGetResult Get(const std::string &key) override
    {
        datasystem::Optional<datasystem::ReadOnlyBuffer> buffer;
        auto rawStatus = client_->Get(key, buffer);
        auto status = detail::MapDataSystemStatus(rawStatus);
        if (status.IsError()) {
            return { std::move(status), {} };
        }
        return { Status::OK(), std::string(static_cast<const char *>((*buffer).ImmutableData()), (*buffer).GetSize()) };
    }

    Status Delete(const std::string &key) override
    {
        return detail::MapDataSystemStatus(client_->Del(key));
    }

    Status PutFile(const std::string &key, const std::string &sourceFile,
                   const SnapshotObjectMetadata &metadata) override
    {
        return PutFileToDataSystemBuffer(
            key, sourceFile, metadata,
            [this](const std::string &objectKey, uint64_t size, const datasystem::SetParam &param,
                   std::shared_ptr<datasystem::Buffer> &buffer) {
                return client_->Create(objectKey, size, param, buffer);
            },
            [this](const std::shared_ptr<datasystem::Buffer> &buffer) {
                return client_->Set(buffer);
            });
    }

    SnapshotStat StatObject(const std::string &key) override
    {
        datasystem::Optional<datasystem::ReadOnlyBuffer> buffer;
        auto status = client_->Get(key, buffer);
        if (status.IsError()) {
            return { detail::MapDataSystemStatus(status), {} };
        }
        return StatDataSystemBuffer((*buffer).ImmutableData(), static_cast<uint64_t>((*buffer).GetSize()));
    }

    Status DownloadFile(const std::string &key, const std::string &destinationFile,
                        SnapshotObjectMetadata &metadata) override
    {
        datasystem::Optional<datasystem::ReadOnlyBuffer> buffer;
        auto status = client_->Get(key, buffer);
        if (status.IsError()) {
            return detail::MapDataSystemStatus(status);
        }
        return DownloadDataSystemBuffer((*buffer).ImmutableData(), static_cast<uint64_t>((*buffer).GetSize()),
                                        destinationFile, metadata);
    }

private:
    std::unique_ptr<datasystem::KVClient> client_;
};

void AppendUint64(std::string &output, uint64_t value)
{
    for (int shift = 56; shift >= 0; shift -= 8) {
        output.push_back(static_cast<char>((value >> shift) & 0xffU));
    }
}

bool ReadUint64(const unsigned char *input, size_t size, size_t &offset, uint64_t &value)
{
    if (input == nullptr || offset > size || size - offset < sizeof(uint64_t)) {
        return false;
    }
    value = 0;
    for (size_t index = 0; index < sizeof(uint64_t); ++index) {
        value = (value << 8U) | input[offset++];
    }
    return true;
}

std::string EncodeEnvelopeHeader(const SnapshotObjectMetadata &metadata)
{
    std::string output;
    output.reserve(MAGIC_SIZE + metadata.snapshotID.size() + metadata.sha256.size()
                   + ENVELOPE_UINT64_FIELD_COUNT * sizeof(uint64_t) + 1);
    output.assign(ENVELOPE_MAGIC, MAGIC_SIZE);
    AppendUint64(output, metadata.snapshotID.size());
    output.append(metadata.snapshotID);
    AppendUint64(output, static_cast<uint64_t>(metadata.sourceInstanceVersion));
    AppendUint64(output, metadata.size);
    AppendUint64(output, metadata.sha256.size());
    output.append(metadata.sha256);
    output.push_back(metadata.complete ? '\1' : '\0');
    AppendUint64(output, static_cast<uint64_t>(metadata.expiresAtUnixSeconds));
    return output;
}

std::string ReadFile(const std::string &path)
{
    std::ifstream input(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

Status EncodeEnvelope(const std::string &sourceFile, const SnapshotObjectMetadata &metadata, std::string &output)
{
    auto status = detail::ValidateFile(sourceFile, metadata);
    if (status.IsError()) {
        return status;
    }
    auto payload = ReadFile(sourceFile);
    output = EncodeEnvelopeHeader(metadata);
    output.append(payload);
    return Status::OK();
}

struct EnvelopeView {
    Status status;
    SnapshotObjectMetadata metadata;
    const unsigned char *payload{ nullptr };
    uint64_t payloadSize{ 0 };
};

EnvelopeView DecodeEnvelopeView(const void *rawData, uint64_t rawSize)
{
    if (rawData == nullptr || rawSize > std::numeric_limits<size_t>::max()) {
        return { Status(StatusCode::FAILED, "invalid snapshot envelope buffer"), {}, nullptr, 0 };
    }
    const auto *data = static_cast<const unsigned char *>(rawData);
    const auto size = static_cast<size_t>(rawSize);
    const bool version2 = size >= MAGIC_SIZE && std::memcmp(data, ENVELOPE_MAGIC, MAGIC_SIZE) == 0;
    const bool version1 = size >= MAGIC_SIZE && std::memcmp(data, ENVELOPE_MAGIC_V1, MAGIC_SIZE) == 0;
    if (!version1 && !version2) {
        return { Status(StatusCode::FAILED, "invalid snapshot envelope magic"), {}, nullptr, 0 };
    }
    size_t offset = MAGIC_SIZE;
    uint64_t snapshotIDSize = 0;
    uint64_t version = 0;
    uint64_t payloadSize = 0;
    uint64_t shaSize = 0;
    if (!ReadUint64(data, size, offset, snapshotIDSize) || snapshotIDSize > size - offset) {
        return { Status(StatusCode::FAILED, "invalid snapshot envelope id"), {}, nullptr, 0 };
    }
    SnapshotObjectMetadata metadata;
    metadata.snapshotID.assign(reinterpret_cast<const char *>(data + offset), snapshotIDSize);
    offset += static_cast<size_t>(snapshotIDSize);
    if (!ReadUint64(data, size, offset, version) || !ReadUint64(data, size, offset, payloadSize) ||
        !ReadUint64(data, size, offset, shaSize) || shaSize > size - offset) {
        return { Status(StatusCode::FAILED, "invalid snapshot envelope metadata"), {}, nullptr, 0 };
    }
    metadata.sourceInstanceVersion = static_cast<int64_t>(version);
    metadata.size = payloadSize;
    metadata.sha256.assign(reinterpret_cast<const char *>(data + offset), shaSize);
    offset += static_cast<size_t>(shaSize);
    if (offset >= size) {
        return { Status(StatusCode::FAILED, "invalid snapshot envelope completion marker"), {}, nullptr, 0 };
    }
    const auto complete = data[offset++];
    if (complete != 0 && complete != 1) {
        return { Status(StatusCode::FAILED, "invalid snapshot envelope completion marker"), {}, nullptr, 0 };
    }
    metadata.complete = complete == 1;
    if (version2) {
        uint64_t expiresAt = 0;
        if (!ReadUint64(data, size, offset, expiresAt)) {
            return { Status(StatusCode::FAILED, "invalid snapshot envelope expiry metadata"), {}, nullptr, 0 };
        }
        metadata.expiresAtUnixSeconds = static_cast<int64_t>(expiresAt);
    }
    if (payloadSize != size - offset) {
        return { Status(StatusCode::FAILED, "invalid snapshot envelope payload size"), {}, nullptr, 0 };
    }
    return { Status::OK(), std::move(metadata), data + offset, payloadSize };
}

struct DecodedEnvelope {
    Status status;
    SnapshotObjectMetadata metadata;
    std::string payload;
};

std::string Sha256(const std::string &value)
{
    std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
    SHA256(reinterpret_cast<const unsigned char *>(value.data()), value.size(), digest.data());
    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for (auto byte : digest) {
        output << std::setw(2) << static_cast<unsigned int>(byte);
    }
    return output.str();
}

DecodedEnvelope DecodeEnvelope(const std::string &value)
{
    auto view = DecodeEnvelopeView(value.data(), value.size());
    if (view.status.IsError()) {
        return { view.status, {}, {} };
    }
    std::string payload(reinterpret_cast<const char *>(view.payload), static_cast<size_t>(view.payloadSize));
    if (Sha256(payload) != view.metadata.sha256) {
        return { Status(StatusCode::FAILED, "snapshot envelope payload sha256 mismatch"), {}, {} };
    }
    return { Status::OK(), std::move(view.metadata), std::move(payload) };
}

Status WritePayload(const std::string &path, const std::string &payload)
{
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(payload.data(), payload.size());
    return output.good() ? Status::OK() : Status(StatusCode::FAILED, "failed to write staging snapshot");
}

bool SameFileIdentity(const struct stat &left, const struct stat &right)
{
    return left.st_dev == right.st_dev && left.st_ino == right.st_ino && left.st_size == right.st_size &&
           left.st_mtim.tv_sec == right.st_mtim.tv_sec && left.st_mtim.tv_nsec == right.st_mtim.tv_nsec &&
           left.st_ctim.tv_sec == right.st_ctim.tv_sec && left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
}

void ReleaseConsumedSharedPages(const unsigned char *base, uint64_t consumed, uint64_t &released)
{
    if (base == nullptr || consumed == 0) {
        return;
    }
    const auto pageSize = sysconf(_SC_PAGESIZE);
    if (pageSize <= 0) {
        return;
    }
    const auto page = static_cast<uintptr_t>(pageSize);
    const auto begin = reinterpret_cast<uintptr_t>(base);
    const auto firstPage = (begin + page - 1) & ~(page - 1);
    const auto consumedEnd = begin + static_cast<uintptr_t>(consumed);
    const auto completeEnd = consumedEnd & ~(page - 1);
    const auto releasedEnd = begin + static_cast<uintptr_t>(released);
    const auto adviseBegin = std::max(firstPage, releasedEnd);
    if (completeEnd <= adviseBegin) {
        return;
    }
    if (madvise(reinterpret_cast<void *>(adviseBegin), completeEnd - adviseBegin, MADV_DONTNEED) == 0) {
        released = static_cast<uint64_t>(completeEnd - begin);
    }
}

Status PutFileToDataSystemBuffer(const std::string &key, const std::string &sourceFile,
                                 const SnapshotObjectMetadata &metadata,
                                 const CreateDataSystemBuffer &createBuffer,
                                 const SetDataSystemBuffer &setBuffer)
{
    const auto header = EncodeEnvelopeHeader(metadata);
    if (metadata.size > std::numeric_limits<uint64_t>::max() - header.size() ||
        metadata.size > std::numeric_limits<size_t>::max() - header.size()) {
        return Status(StatusCode::ERR_PARAM_INVALID, "snapshot artifact is too large");
    }
    const auto totalSize = metadata.size + header.size();
    const int source = open(sourceFile.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (source < 0) {
        return Status(StatusCode::FAILED, "failed to open snapshot artifact for DataSystem upload");
    }
    struct stat initial {};
    if (fstat(source, &initial) != 0 || !S_ISREG(initial.st_mode) || initial.st_size < 0 ||
        static_cast<uint64_t>(initial.st_size) != metadata.size) {
        (void)close(source);
        return Status(StatusCode::FAILED, "snapshot artifact size or type changed before DataSystem upload");
    }

    datasystem::SetParam parameters;
    parameters.existence = datasystem::ExistenceOpt::NX;
    std::shared_ptr<datasystem::Buffer> buffer;
    auto create = createBuffer(key, totalSize, parameters, buffer);
    if (create.IsError()) {
        (void)close(source);
        return detail::MapDataSystemStatus(create);
    }
    if (buffer == nullptr || buffer->GetSize() < 0 || static_cast<uint64_t>(buffer->GetSize()) != totalSize ||
        buffer->MutableData() == nullptr) {
        (void)close(source);
        return Status(StatusCode::BP_DATASYSTEM_ERROR, "DataSystem returned an invalid snapshot buffer");
    }

    auto *destination = static_cast<unsigned char *>(buffer->MutableData());
    std::memcpy(destination, header.data(), header.size());
    uint64_t offset = 0;
    Status status = Status::OK();
    constexpr size_t chunkSize = 1024 * 1024;
    while (offset < metadata.size) {
        const auto chunk = static_cast<size_t>(std::min<uint64_t>(chunkSize, metadata.size - offset));
        const auto count = read(source, destination + header.size() + static_cast<size_t>(offset), chunk);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            status = Status(StatusCode::FAILED, "failed to stream snapshot artifact into DataSystem buffer");
            break;
        }
        offset += static_cast<uint64_t>(count);
    }
    struct stat finalInfo {};
    if (status.IsOk() && (fstat(source, &finalInfo) != 0 || !SameFileIdentity(initial, finalInfo))) {
        status = Status(StatusCode::SCHEDULE_CONFLICTED,
                        "snapshot artifact changed during DataSystem upload");
    }
    if (close(source) != 0 && status.IsOk()) {
        status = Status(StatusCode::FAILED, "failed to close snapshot artifact after DataSystem upload");
    }
    if (status.IsError()) {
        return status;
    }
    return detail::MapDataSystemStatus(setBuffer(buffer));
}

SnapshotStat StatDataSystemBuffer(const void *data, uint64_t size)
{
    auto view = DecodeEnvelopeView(data, size);
    return { view.status, std::move(view.metadata) };
}

Status DownloadDataSystemBuffer(const void *data, uint64_t size, const std::string &destinationFile,
                                SnapshotObjectMetadata &metadata)
{
    auto view = DecodeEnvelopeView(data, size);
    if (view.status.IsError()) {
        return view.status;
    }
    const int output = open(destinationFile.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC);
    if (output < 0) {
        return Status(StatusCode::FAILED, "failed to open DataSystem snapshot download staging file");
    }
    uint64_t offset = 0;
    uint64_t released = 0;
    Status status = Status::OK();
    constexpr size_t chunkSize = 1024 * 1024;
    while (offset < view.payloadSize) {
        const auto chunk = static_cast<size_t>(std::min<uint64_t>(chunkSize, view.payloadSize - offset));
        const auto count = write(output, view.payload + static_cast<size_t>(offset), chunk);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            status = Status(StatusCode::FAILED, "failed to stream DataSystem snapshot to staging file");
            break;
        }
        offset += static_cast<uint64_t>(count);
        ReleaseConsumedSharedPages(view.payload, offset, released);
    }
    if (close(output) != 0 && status.IsOk()) {
        status = Status(StatusCode::FAILED, "failed to close DataSystem snapshot staging file");
    }
    if (status.IsOk()) {
        metadata = std::move(view.metadata);
    }
    return status;
}

bool LooksNotFound(const Status &status)
{
    return status.StatusCode() == StatusCode::FILE_NOT_FOUND;
}

}  // namespace

namespace detail {

std::string DataSystemObjectKey(const std::string &logicalKey)
{
    return "yr-snapshot:" + Sha256(logicalKey);
}

Status MapDataSystemStatus(const datasystem::Status &status)
{
    if (status.IsOk()) {
        return Status::OK();
    }
    return status.GetCode() == datasystem::K_NOT_FOUND
               ? Status(StatusCode::FILE_NOT_FOUND, status.ToString())
               : Status(StatusCode::BP_DATASYSTEM_ERROR, status.ToString());
}

}  // namespace detail

Status DataSystemSnapshotClient::PutFile(const std::string &key, const std::string &sourceFile,
                                         const SnapshotObjectMetadata &metadata)
{
    std::string envelope;
    auto status = EncodeEnvelope(sourceFile, metadata, envelope);
    return status.IsError() ? status : Put(key, envelope);
}

SnapshotStat DataSystemSnapshotClient::StatObject(const std::string &key)
{
    auto result = Get(key);
    if (result.status.IsError()) {
        return { result.status, {} };
    }
    auto decoded = DecodeEnvelope(result.value);
    return { decoded.status, std::move(decoded.metadata) };
}

Status DataSystemSnapshotClient::DownloadFile(const std::string &key, const std::string &destinationFile,
                                              SnapshotObjectMetadata &metadata)
{
    auto result = Get(key);
    if (result.status.IsError()) {
        return result.status;
    }
    auto decoded = DecodeEnvelope(result.value);
    if (decoded.status.IsError()) {
        return decoded.status;
    }
    auto status = WritePayload(destinationFile, decoded.payload);
    if (status.IsOk()) {
        metadata = std::move(decoded.metadata);
    }
    return status;
}

DataSystemSnapshotStorage::DataSystemSnapshotStorage()
    : DataSystemSnapshotStorage(std::make_shared<KVDataSystemSnapshotClient>())
{
}

DataSystemSnapshotStorage::DataSystemSnapshotStorage(std::shared_ptr<DataSystemSnapshotClient> client,
                                                     std::shared_ptr<ActorWorker> worker)
    : client_(std::move(client)), worker_(worker == nullptr ? std::make_shared<ActorWorker>() : std::move(worker))
{
}

Status DataSystemSnapshotStorage::Create(const DataSystemSnapshotConfig &config,
                                         std::shared_ptr<DataSystemSnapshotStorage> &storage,
                                         std::shared_ptr<DataSystemSnapshotClient> client)
{
    storage.reset();
    constexpr int32_t MAX_PORT = 65535;
    if (config.host.empty() || config.port <= 0 || config.port > MAX_PORT) {
        return Status(StatusCode::ERR_PARAM_INVALID, "invalid DataSystem snapshot endpoint");
    }
    if (client == nullptr) {
        client = std::make_shared<DedicatedKVDataSystemSnapshotClient>();
    }
    auto status = client->Init(config);
    if (status.IsError()) {
        return status;
    }
    storage = std::make_shared<DataSystemSnapshotStorage>(std::move(client));
    return Status::OK();
}

litebus::Future<Status> DataSystemSnapshotStorage::PutTemporary(const std::string &temporaryKey,
                                                                const std::string &sourceFile,
                                                                const SnapshotObjectMetadata &metadata)
{
    return detail::RunOnWorker<Status>(worker_, [=, client = client_]() {
        const auto objectKey = detail::DataSystemObjectKey(temporaryKey);
        auto current = client->StatObject(objectKey);
        if (current.status.IsOk()) {
            return detail::MetadataEqual(current.metadata, metadata)
                       ? Status::OK()
                       : detail::Conflict("temporary snapshot metadata conflicts");
        }
        if (!LooksNotFound(current.status)) {
            return current.status;
        }
        auto status = client->PutFile(objectKey, sourceFile, metadata);
        auto postcondition = client->StatObject(objectKey);
        if (postcondition.status.IsError()) {
            return status.IsError() ? status : postcondition.status;
        }
        return detail::MetadataEqual(postcondition.metadata, metadata)
                   ? Status::OK()
                   : detail::Conflict("temporary snapshot failed postcondition verification");
    });
}

litebus::Future<SnapshotStat> DataSystemSnapshotStorage::Stat(const std::string &key)
{
    return detail::RunOnWorker<SnapshotStat>(worker_, [key, client = client_]() {
        const auto objectKey = detail::DataSystemObjectKey(key);
        auto result = client->StatObject(objectKey);
        if (result.status.IsError()) {
            const auto status = LooksNotFound(result.status) ? Status(StatusCode::FILE_NOT_FOUND) : result.status;
            return SnapshotStat{ status, {} };
        }
        if (detail::IsExpired(result.metadata)) {
            auto deleted = client->Delete(objectKey);
            if (deleted.IsError() && !LooksNotFound(deleted)) {
                return SnapshotStat{ deleted, {} };
            }
            return SnapshotStat{ Status(StatusCode::FILE_NOT_FOUND), {} };
        }
        return result;
    });
}

litebus::Future<Status> DataSystemSnapshotStorage::Publish(const std::string &temporaryKey, const std::string &finalKey,
                                                           const SnapshotObjectMetadata &expected)
{
    return detail::RunOnWorker<Status>(worker_, [=, client = client_]() {
        const auto temporaryObjectKey = detail::DataSystemObjectKey(temporaryKey);
        const auto finalObjectKey = detail::DataSystemObjectKey(finalKey);
        auto final = client->Get(finalObjectKey);
        if (final.status.IsOk()) {
            auto decoded = DecodeEnvelope(final.value);
            return decoded.status.IsOk() && detail::MetadataEqual(decoded.metadata, expected) && expected.complete
                       ? Status::OK()
                       : detail::Conflict("published snapshot metadata conflicts");
        }
        if (!LooksNotFound(final.status)) {
            return final.status;
        }
        auto temporary = client->Get(temporaryObjectKey);
        if (temporary.status.IsError()) {
            return temporary.status;
        }
        auto decoded = DecodeEnvelope(temporary.value);
        if (decoded.status.IsError() || !expected.complete ||
            !detail::MetadataIdentityEqual(decoded.metadata, expected)) {
            return detail::Conflict("temporary snapshot does not match publish metadata");
        }
        std::string envelope;
        envelope.assign(ENVELOPE_MAGIC, MAGIC_SIZE);
        AppendUint64(envelope, expected.snapshotID.size());
        envelope.append(expected.snapshotID);
        AppendUint64(envelope, static_cast<uint64_t>(expected.sourceInstanceVersion));
        AppendUint64(envelope, expected.size);
        AppendUint64(envelope, expected.sha256.size());
        envelope.append(expected.sha256);
        envelope.push_back('\1');
        AppendUint64(envelope, static_cast<uint64_t>(expected.expiresAtUnixSeconds));
        envelope.append(decoded.payload);
        auto status = client->Put(finalObjectKey, envelope);
        if (status.IsError()) {
            return status;
        }
        auto postcondition = client->Get(finalObjectKey);
        if (postcondition.status.IsError()) {
            return postcondition.status;
        }
        auto verified = DecodeEnvelope(postcondition.value);
        return verified.status.IsOk() && detail::MetadataEqual(verified.metadata, expected)
                   ? Status::OK()
                   : detail::Conflict("published snapshot failed postcondition verification");
    });
}

litebus::Future<Status> DataSystemSnapshotStorage::PutFinal(const std::string &finalKey,
                                                            const std::string &sourceFile,
                                                            const SnapshotObjectMetadata &metadata)
{
    return detail::RunOnWorker<Status>(worker_, [=, client = client_]() {
        if (!metadata.complete) {
            return Status(StatusCode::ERR_PARAM_INVALID, "direct final snapshot metadata must be complete");
        }
        const auto objectKey = detail::DataSystemObjectKey(finalKey);
        auto current = client->StatObject(objectKey);
        if (current.status.IsOk()) {
            return detail::MetadataEqual(current.metadata, metadata)
                       ? Status::OK()
                       : detail::Conflict("published snapshot metadata conflicts");
        }
        if (!LooksNotFound(current.status)) {
            return current.status;
        }

        const auto put = client->PutFile(objectKey, sourceFile, metadata);
        auto postcondition = client->StatObject(objectKey);
        if (postcondition.status.IsOk()) {
            return detail::MetadataEqual(postcondition.metadata, metadata)
                       ? Status::OK()
                       : detail::Conflict("published snapshot metadata conflicts");
        }
        return put.IsError() ? put : postcondition.status;
    });
}

litebus::Future<Status> DataSystemSnapshotStorage::Get(const std::string &finalKey,
                                                       const std::string &destinationFile)
{
    return detail::RunOnWorker<Status>(worker_, [=, client = client_]() {
        const auto objectKey = detail::DataSystemObjectKey(finalKey);
        detail::SecureDownloadTarget target;
        auto prepare = target.Prepare(destinationFile);
        if (prepare.IsError()) {
            return prepare;
        }
        SnapshotObjectMetadata metadata;
        auto download = client->DownloadFile(objectKey, target.StagingPath(), metadata);
        if (download.IsError()) {
            target.Cleanup();
            return download;
        }
        if (!metadata.complete) {
            target.Cleanup();
            return Status(StatusCode::FAILED, "snapshot is incomplete or corrupt");
        }
        if (detail::IsExpired(metadata)) {
            target.Cleanup();
            auto deleted = client->Delete(objectKey);
            return deleted.IsError() && !LooksNotFound(deleted)
                       ? deleted
                       : Status(StatusCode::FILE_NOT_FOUND);
        }
        return target.Commit(metadata);
    });
}

litebus::Future<Status> DataSystemSnapshotStorage::Delete(const std::string &key)
{
    return detail::RunOnWorker<Status>(worker_, [key, client = client_]() {
        auto status = client->Delete(detail::DataSystemObjectKey(key));
        return LooksNotFound(status) ? Status::OK() : status;
    });
}

}  // namespace functionsystem::snapshot_storage
