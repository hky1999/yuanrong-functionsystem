/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "tcp_tunnel_server.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cerrno>
#include <cstring>
#include <optional>
#include <sstream>
#include <string_view>
#include <system_error>

#include <nlohmann/json.hpp>
#include <openssl/err.h>
#include <openssl/pem.h>

#include "common/logs/logging.h"
#include "common/state_machine/instance_control_view.h"
#include "common/types/instance_state.h"
#include "function_proxy/local_scheduler/instance_control/idle/idle_mgr.h"

namespace functionsystem::local_scheduler {
namespace {
constexpr uint32_t MAX_HEADER_SIZE = 16 * 1024;
constexpr int TUNNEL_VERSION = 1;
constexpr int IO_BUFFER_SIZE = 32 * 1024;
constexpr int POLL_TIMEOUT_MS = 1000;
constexpr int LISTEN_BACKLOG = 128;
constexpr int CLIENT_IO_TIMEOUT_SECONDS = 30;
constexpr size_t PORT_FORWARD_FIELD_COUNT = 3;
constexpr size_t PORT_FORWARD_HOST_INDEX = 1;
constexpr size_t PORT_FORWARD_CONTAINER_INDEX = 2;
constexpr size_t RELAY_DESCRIPTOR_COUNT = 2;

struct TCPPortMapping {
    int hostPort;
    int containerPort;
};

std::optional<int> ParsePort(std::string_view value)
{
    uint16_t port = 0;
    const auto result = std::from_chars(value.data(), value.data() + value.size(), port);
    if (value.empty() || result.ec != std::errc() || result.ptr != value.data() + value.size() || port == 0) {
        return std::nullopt;
    }
    return port;
}

bool SetSocketTimeouts(int fd)
{
    const timeval timeout{ CLIENT_IO_TIMEOUT_SECONDS, 0 };
    return setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0 &&
           setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0;
}

bool SSLReadAll(SSL *ssl, std::string &buffer)
{
    auto *current = buffer.data();
    size_t length = buffer.size();
    while (length > 0) {
        const int count = SSL_read(ssl, current, static_cast<int>(length));
        if (count <= 0) {
            return false;
        }
        current += count;
        length -= static_cast<size_t>(count);
    }
    return true;
}

bool SSLWriteAll(SSL *ssl, const unsigned char *buffer, size_t length)
{
    const auto *current = buffer;
    while (length > 0) {
        const int count = SSL_write(ssl, current, static_cast<int>(length));
        if (count <= 0) {
            return false;
        }
        current += count;
        length -= static_cast<size_t>(count);
    }
    return true;
}

bool SendAll(int fd, const unsigned char *buffer, size_t length)
{
    const auto *current = buffer;
    while (length > 0) {
        const ssize_t count = send(fd, current, length, MSG_NOSIGNAL);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return false;
        }
        current += count;
        length -= static_cast<size_t>(count);
    }
    return true;
}

bool ReadAll(SSL *ssl, int fd, std::string &buffer)
{
    if (ssl != nullptr) {
        return SSLReadAll(ssl, buffer);
    }
    auto *current = buffer.data();
    size_t length = buffer.size();
    while (length > 0) {
        const ssize_t count = recv(fd, current, length, 0);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return false;
        }
        current += count;
        length -= static_cast<size_t>(count);
    }
    return true;
}

bool WriteAll(SSL *ssl, int fd, const unsigned char *buffer, size_t length)
{
    return ssl != nullptr ? SSLWriteAll(ssl, buffer, length) : SendAll(fd, buffer, length);
}

bool SendResponse(SSL *ssl, int fd, bool ok, const std::string &message)
{
    const auto payload = nlohmann::json{ { "ok", ok }, { "message", message } }.dump();
    const uint32_t networkSize = htonl(static_cast<uint32_t>(payload.size()));
    std::array<unsigned char, sizeof(networkSize)> sizeBytes{};
    std::memcpy(sizeBytes.data(), &networkSize, sizeBytes.size());
    const std::vector<unsigned char> payloadBytes(payload.begin(), payload.end());
    return WriteAll(ssl, fd, sizeBytes.data(), sizeBytes.size()) &&
           WriteAll(ssl, fd, payloadBytes.data(), payloadBytes.size());
}

bool ReadHeader(SSL *ssl, int fd, nlohmann::json &header, std::string &error)
{
    std::string sizeBytes(sizeof(uint32_t), '\0');
    if (!ReadAll(ssl, fd, sizeBytes)) {
        error = "failed to read tunnel header size";
        return false;
    }
    uint32_t networkSize = 0;
    std::memcpy(&networkSize, sizeBytes.data(), sizeBytes.size());
    const uint32_t size = ntohl(networkSize);
    if (size == 0 || size > MAX_HEADER_SIZE) {
        error = "invalid tunnel header size";
        return false;
    }
    std::string payload(size, '\0');
    if (!ReadAll(ssl, fd, payload)) {
        error = "failed to read tunnel header";
        return false;
    }
    try {
        header = nlohmann::json::parse(payload);
    } catch (const std::exception &exception) {
        error = std::string("invalid tunnel header JSON: ") + exception.what();
        return false;
    }
    return true;
}

int ConnectLocalPort(const std::string &host, int port)
{
    const int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return -1;
    }
    union SocketAddress {
        sockaddr generic;
        sockaddr_in ipv4;
    } address{};
    address.ipv4.sin_family = AF_INET;
    address.ipv4.sin_port = htons(static_cast<uint16_t>(port));
    if (host.empty()) {
        address.ipv4.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    } else if (inet_pton(AF_INET, host.c_str(), &address.ipv4.sin_addr) != 1) {
        close(fd);
        return -1;
    }
    if (connect(fd, &address.generic, sizeof(address.ipv4)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int CreateListener(const std::string &host, uint16_t port, std::string &error)
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = host.empty() ? AI_PASSIVE : 0;
    addrinfo *addresses = nullptr;
    const std::string service = std::to_string(port);
    const int result = getaddrinfo(host.empty() ? nullptr : host.c_str(), service.c_str(), &hints, &addresses);
    if (result != 0) {
        error = gai_strerror(result);
        return -1;
    }
    int listener = -1;
    for (auto *address = addresses; address != nullptr; address = address->ai_next) {
        listener = socket(address->ai_family, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (listener < 0) {
            continue;
        }
        int reuse = 1;
        (void)setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        if (address->ai_family == AF_INET6) {
            int ipv6Only = 0;
            (void)setsockopt(listener, IPPROTO_IPV6, IPV6_V6ONLY, &ipv6Only, sizeof(ipv6Only));
        }
        if (bind(listener, address->ai_addr, address->ai_addrlen) == 0 &&
            listen(listener, LISTEN_BACKLOG) == 0) {
            break;
        }
        error = std::error_code(errno, std::generic_category()).message();
        close(listener);
        listener = -1;
    }
    freeaddrinfo(addresses);
    return listener;
}

bool LoadCertificateFromMemory(SSL_CTX *context, const std::string &certificate, const std::string &privateKey,
                               const std::string &rootCertificate)
{
    BIO *certBio = BIO_new_mem_buf(certificate.data(), static_cast<int>(certificate.size()));
    BIO *keyBio = BIO_new_mem_buf(privateKey.data(), static_cast<int>(privateKey.size()));
    BIO *rootBio = BIO_new_mem_buf(rootCertificate.data(), static_cast<int>(rootCertificate.size()));
    if (certBio == nullptr || keyBio == nullptr || rootBio == nullptr) {
        BIO_free(certBio);
        BIO_free(keyBio);
        BIO_free(rootBio);
        return false;
    }
    X509 *cert = PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr);
    EVP_PKEY *key = PEM_read_bio_PrivateKey(keyBio, nullptr, nullptr, nullptr);
    BIO_free(certBio);
    BIO_free(keyBio);
    if (cert == nullptr || key == nullptr) {
        X509_free(cert);
        EVP_PKEY_free(key);
        BIO_free(rootBio);
        return false;
    }
    bool loadedRoot = false;
    while (X509 *root = PEM_read_bio_X509(rootBio, nullptr, nullptr, nullptr)) {
        if (X509_STORE_add_cert(SSL_CTX_get_cert_store(context), root) == 1) {
            loadedRoot = true;
        }
        X509_free(root);
    }
    ERR_clear_error();
    BIO_free(rootBio);
    const bool ok = loadedRoot && SSL_CTX_use_certificate(context, cert) == 1 &&
                    SSL_CTX_use_PrivateKey(context, key) == 1 && SSL_CTX_check_private_key(context) == 1;
    X509_free(cert);
    EVP_PKEY_free(key);
    return ok;
}

std::optional<TCPPortMapping> ParseTCPPortMapping(const nlohmann::json &value)
{
    if (!value.is_string()) {
        return std::nullopt;
    }
    std::vector<std::string> parts;
    std::stringstream stream(value.get<std::string>());
    std::string part;
    while (std::getline(stream, part, ':')) {
        parts.push_back(part);
    }
    if (parts.size() != PORT_FORWARD_FIELD_COUNT) {
        return std::nullopt;
    }
    std::transform(parts.front().begin(), parts.front().end(), parts.front().begin(),
                   [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
    if (parts.front() != "tcp") {
        return std::nullopt;
    }
    const auto hostPort = ParsePort(parts[PORT_FORWARD_HOST_INDEX]);
    const auto containerPort = ParsePort(parts[PORT_FORWARD_CONTAINER_INDEX]);
    if (!hostPort.has_value() || !containerPort.has_value()) {
        return std::nullopt;
    }
    return TCPPortMapping{ hostPort.value(), containerPort.value() };
}

struct TunnelRequest {
    std::string instanceID;
    std::string requestID;
    int targetPort{ 0 };
};

bool AuthenticateClient(SSL *ssl, int clientFd, bool enableTLS)
{
    if (!enableTLS) {
        return true;
    }
    if (ssl == nullptr || SSL_set_fd(ssl, clientFd) != 1 || SSL_accept(ssl) != 1 ||
        SSL_get_verify_result(ssl) != X509_V_OK) {
        YRLOG_WARN("rejected TCP tunnel connection with invalid component certificate");
        return false;
    }
    X509 *peer = SSL_get_peer_certificate(ssl);
    if (peer == nullptr) {
        return false;
    }
    X509_free(peer);
    return true;
}

bool ReadTunnelRequest(SSL *ssl, int clientFd, TunnelRequest &request, std::string &error)
{
    nlohmann::json header;
    if (!ReadHeader(ssl, clientFd, header, error)) {
        return false;
    }
    try {
        request.instanceID = header.value("instanceID", std::string{});
        request.requestID = header.value("requestID", std::string{});
        request.targetPort = header.value("targetPort", 0);
        const bool invalid = header.value("tunnelVersion", 0) != TUNNEL_VERSION || request.instanceID.empty() ||
                             request.requestID.empty() || header.value("protocol", std::string{}).empty() ||
                             request.targetPort < 0 || request.targetPort > UINT16_MAX;
        if (invalid) {
            error = "unsupported tunnel request";
            return false;
        }
    } catch (const std::exception &exception) {
        error = std::string("invalid tunnel header fields: ") + exception.what();
        return false;
    }
    return true;
}
}  // namespace

int ResolvePublishedTCPPort(const std::string &portForwardMetadata, int targetPort, std::string &error)
{
    try {
        const auto values = nlohmann::json::parse(portForwardMetadata);
        if (!values.is_array()) {
            error = "port forward metadata must be an array";
            return -1;
        }
        std::vector<TCPPortMapping> mappings;
        for (const auto &value : values) {
            auto mapping = ParseTCPPortMapping(value);
            if (mapping.has_value()) {
                mappings.push_back(mapping.value());
            }
        }
        if (targetPort > 0) {
            for (const auto &mapping : mappings) {
                if (mapping.containerPort == targetPort) {
                    return mapping.hostPort;
                }
            }
            error = "requested container port is not published as TCP";
            return -1;
        }
        if (mappings.size() == 1) {
            return mappings.front().hostPort;
        }
    } catch (const std::exception &exception) {
        error = std::string("invalid port forward metadata: ") + exception.what();
        return -1;
    }
    error = targetPort > 0 ? "requested container port is not published as TCP"
                           : "target port is required when multiple TCP ports are published";
    return -1;
}

TcpTunnelServer::TcpTunnelServer(TcpTunnelServerConfig config, std::shared_ptr<InstanceControlView> instanceView,
                                 std::shared_ptr<IdleMgr> idleMgr)
    : config_(std::move(config)), instanceView_(std::move(instanceView)), idleMgr_(std::move(idleMgr))
{
}

TcpTunnelServer::~TcpTunnelServer()
{
    Stop();
}

bool TcpTunnelServer::ConfigureTLS()
{
    if (config_.rootCert.empty() || config_.moduleCert.empty() || config_.moduleKey.empty()) {
        YRLOG_ERROR("TCP tunnel mTLS certificate configuration is incomplete");
        return false;
    }
    sslContext_ = SSL_CTX_new(TLS_server_method());
    if (sslContext_ == nullptr) {
        return false;
    }
    SSL_CTX_set_min_proto_version(sslContext_, TLS1_2_VERSION);
    SSL_CTX_set_verify(sslContext_, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    return LoadCertificateFromMemory(sslContext_, config_.moduleCert, config_.moduleKey, config_.rootCert);
}

bool TcpTunnelServer::Start()
{
    if (config_.listenPort == 0 || instanceView_ == nullptr || idleMgr_ == nullptr ||
        (config_.enableTLS && !ConfigureTLS())) {
        YRLOG_ERROR("invalid TCP tunnel server configuration");
        SSL_CTX_free(sslContext_);
        sslContext_ = nullptr;
        return false;
    }
    std::string listenError;
    const std::string listenHost = config_.listenIP == "0.0.0.0" ? "" : config_.listenIP;
    listenFd_ = CreateListener(listenHost, config_.listenPort, listenError);
    if (listenFd_ < 0) {
        YRLOG_ERROR("failed to bind TCP tunnel listener {}:{}, error {}", config_.listenIP, config_.listenPort,
                    listenError);
        SSL_CTX_free(sslContext_);
        sslContext_ = nullptr;
        return false;
    }
    running_.store(true);
    acceptThread_ = std::thread(&TcpTunnelServer::AcceptLoop, this);
    YRLOG_INFO("TCP tunnel listening on {}:{}, mTLS: {}", config_.listenIP, config_.listenPort, config_.enableTLS);
    return true;
}

void TcpTunnelServer::Stop()
{
    if (!running_.exchange(false)) {
        return;
    }
    if (listenFd_ >= 0) {
        shutdown(listenFd_, SHUT_RDWR);
        close(listenFd_);
        listenFd_ = -1;
    }
    {
        std::lock_guard<std::mutex> lock(clientsMutex_);
        for (const int fd : clients_) {
            shutdown(fd, SHUT_RDWR);
        }
    }
    if (acceptThread_.joinable()) {
        acceptThread_.join();
    }
    ReapWorkers(true);
    SSL_CTX_free(sslContext_);
    sslContext_ = nullptr;
}

void TcpTunnelServer::AcceptLoop()
{
    while (running_.load()) {
        const int clientFd = accept4(listenFd_, nullptr, nullptr, SOCK_CLOEXEC);
        if (clientFd < 0) {
            if (running_.load()) {
                YRLOG_WARN("accept TCP tunnel connection failed: {}",
                           std::error_code(errno, std::generic_category()).message());
            }
            continue;
        }
        if (!SetSocketTimeouts(clientFd)) {
            YRLOG_WARN("failed to set TCP tunnel client timeout: {}",
                       std::error_code(errno, std::generic_category()).message());
            close(clientFd);
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(clientsMutex_);
            if (!running_.load()) {
                close(clientFd);
                break;
            }
            if (clients_.size() >= config_.maxConnections) {
                YRLOG_WARN("rejected TCP tunnel connection: concurrent client limit {} reached",
                           config_.maxConnections);
                close(clientFd);
                continue;
            }
            clients_.insert(clientFd);
        }
        ReapWorkers();
        auto completed = std::make_shared<std::atomic<bool>>(false);
        std::lock_guard<std::mutex> lock(workersMutex_);
        workers_.push_back(Worker{
            std::thread([this, clientFd, completed] {
                HandleClient(clientFd);
                completed->store(true, std::memory_order_release);
            }),
            completed
        });
    }
}

void TcpTunnelServer::ReapWorkers(bool waitForAll)
{
    std::lock_guard<std::mutex> lock(workersMutex_);
    auto worker = workers_.begin();
    while (worker != workers_.end()) {
        if (!waitForAll && !worker->completed->load(std::memory_order_acquire)) {
            ++worker;
            continue;
        }
        if (worker->thread.joinable()) {
            worker->thread.join();
        }
        worker = workers_.erase(worker);
    }
}

int TcpTunnelServer::ResolveHostPort(const std::string &instanceID, int targetPort, std::string &error) const
{
    auto machine = instanceView_->GetInstance(instanceID);
    if (machine == nullptr) {
        error = "instance is not managed by this proxy";
        return -1;
    }
    const auto &instance = machine->GetInstanceInfo();
    if (instance.functionproxyid() != config_.nodeID ||
        instance.instancestatus().code() != static_cast<int32_t>(InstanceState::RUNNING)) {
        error = "instance is not running on this proxy";
        return -1;
    }
    const auto mapping = instance.extensions().find("portForward");
    if (mapping == instance.extensions().end()) {
        error = "instance has no port forward metadata";
        return -1;
    }
    return ResolvePublishedTCPPort(mapping->second, targetPort, error);
}

std::string TcpTunnelServer::ResolveContainerIP(const std::string &instanceID, std::string &error) const
{
    auto machine = instanceView_->GetInstance(instanceID);
    if (machine == nullptr) {
        error = "instance is not managed by this proxy";
        return {};
    }
    const auto &instance = machine->GetInstanceInfo();
    if (instance.functionproxyid() != config_.nodeID ||
        instance.instancestatus().code() != static_cast<int32_t>(InstanceState::RUNNING)) {
        error = "instance is not running on this proxy";
        return {};
    }
    return instance.containerip();
}

void TcpTunnelServer::HandleClient(int clientFd)
{
    ClientSession session;
    session.ssl = config_.enableTLS ? SSL_new(sslContext_) : nullptr;
    session.clientFd = clientFd;
    (void)ServeClient(session);
    CloseClient(session);
}

bool TcpTunnelServer::ServeClient(ClientSession &session)
{
    if (!AuthenticateClient(session.ssl, session.clientFd, config_.enableTLS)) {
        return false;
    }
    TunnelRequest request;
    std::string error;
    if (!ReadTunnelRequest(session.ssl, session.clientFd, request, error)) {
        (void)SendResponse(session.ssl, session.clientFd, false, error);
        return false;
    }
    session.instanceID = request.instanceID;
    session.requestID = request.requestID;
    std::string containerIP = ResolveContainerIP(request.instanceID, error);
    int backendPort = 0;
    std::string connectHost;
    if (!containerIP.empty()) {
        // Network isolation mode: sandbox has its own netns, loopback can't reach AgentServer.
        // Connect directly to sandboxIP:containerPort (targetPort semantics == containerPort).
        connectHost = containerIP;
        backendPort = request.targetPort;
    } else if (!error.empty()) {
        // Instance not managed here / not RUNNING — fail fast.
        (void)SendResponse(session.ssl, session.clientFd, false, error);
        return false;
    } else {
        // Host network mode: no containerIP, fall back to loopback:hostPort.
        const int hostPort = ResolveHostPort(request.instanceID, request.targetPort, error);
        if (hostPort <= 0) {
            (void)SendResponse(session.ssl, session.clientFd, false, error);
            return false;
        }
        backendPort = hostPort;
    }
    session.backendFd = ConnectLocalPort(connectHost, backendPort);
    if (session.backendFd < 0) {
        (void)SendResponse(session.ssl, session.clientFd, false, "failed to connect instance target port");
        return false;
    }
    if (!SetSocketTimeouts(session.backendFd)) {
        (void)SendResponse(session.ssl, session.clientFd, false, "failed to configure instance target socket");
        return false;
    }
    idleMgr_->SessionCountDelta(session.instanceID, 1);
    session.counted = true;
    YRLOG_INFO("{}|accepted TCP tunnel for instance {}", session.requestID, session.instanceID);
    if (!SendResponse(session.ssl, session.clientFd, true, "")) {
        return false;
    }
    return Relay(session.ssl, session.clientFd, session.backendFd);
}

void TcpTunnelServer::CloseClient(ClientSession &session)
{
    if (session.counted) {
        idleMgr_->SessionCountDelta(session.instanceID, -1);
        YRLOG_INFO("{}|closed TCP tunnel for instance {}", session.requestID, session.instanceID);
    }
    if (session.backendFd >= 0) {
        close(session.backendFd);
    }
    if (session.ssl != nullptr) {
        (void)SSL_shutdown(session.ssl);
        SSL_free(session.ssl);
    }
    {
        std::lock_guard<std::mutex> lock(clientsMutex_);
        clients_.erase(session.clientFd);
    }
    close(session.clientFd);
}

bool TcpTunnelServer::Relay(SSL *ssl, int clientFd, int backendFd) const
{
    std::array<unsigned char, IO_BUFFER_SIZE> buffer{};
    std::array<pollfd, RELAY_DESCRIPTOR_COUNT> descriptors{
        pollfd{ clientFd, POLLIN, 0 }, pollfd{ backendFd, POLLIN, 0 }
    };
    while (running_.load()) {
        descriptors[0].revents = 0;
        descriptors[1].revents = 0;
        const int pending = ssl == nullptr ? 0 : SSL_pending(ssl);
        const int ready = pending > 0 ? 1 : poll(descriptors.data(), descriptors.size(), POLL_TIMEOUT_MS);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (ready == 0) {
            continue;
        }
        if ((descriptors[0].revents & (POLLERR | POLLNVAL)) != 0 ||
            (descriptors[1].revents & (POLLERR | POLLNVAL)) != 0) {
            return false;
        }
        if (pending > 0 || (descriptors[0].revents & POLLIN) != 0) {
            const int count = ssl == nullptr ? recv(clientFd, buffer.data(), buffer.size(), 0)
                                             : SSL_read(ssl, buffer.data(), buffer.size());
            if (count <= 0 || !SendAll(backendFd, buffer.data(), static_cast<size_t>(count))) {
                return false;
            }
        }
        if ((descriptors[1].revents & POLLIN) != 0) {
            const ssize_t count = recv(backendFd, buffer.data(), buffer.size(), 0);
            if (count <= 0 || !WriteAll(ssl, clientFd, buffer.data(), static_cast<size_t>(count))) {
                return false;
            }
        }
        const bool clientDrained =
            (descriptors[0].revents & POLLHUP) != 0 && (descriptors[0].revents & POLLIN) == 0;
        const bool backendDrained =
            (descriptors[1].revents & POLLHUP) != 0 && (descriptors[1].revents & POLLIN) == 0;
        if (clientDrained || backendDrained) {
            return true;
        }
    }
    return true;
}

}  // namespace functionsystem::local_scheduler
