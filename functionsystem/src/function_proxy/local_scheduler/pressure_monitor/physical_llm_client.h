#ifndef LOCAL_SCHEDULER_PHYSICAL_LLM_CLIENT_H
#define LOCAL_SCHEDULER_PHYSICAL_LLM_CLIENT_H

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <functional>
#include <nlohmann/json.hpp>
#include "httpd/http_connect.hpp"

namespace functionsystem::local_scheduler {
struct PhysicalLLMIdentity {
    std::string instanceID, sandboxID, runtimeID, generation;
    bool Valid() const {
        for (const auto &value : {instanceID, sandboxID, runtimeID, generation}) {
            if (value.empty() || value.size() > 256 || value.find_first_not_of(" \t\r\n") == std::string::npos) { return false; }
        }
        return true;
    }
    nlohmann::json JSON() const { return {{"instance_id", instanceID}, {"sandbox_id", sandboxID},
        {"runtime_id", runtimeID}, {"resource_generation", generation}}; }
};

enum class PhysicalHoldOutcome { UNKNOWN, LOCAL_REJECTED, UNCHANGED, HELD, READY, RELEASED };
struct PhysicalHoldResult {
    PhysicalHoldOutcome outcome = PhysicalHoldOutcome::UNKNOWN;
    std::string token;
    std::string epoch;
    int64_t sequence = 0;
    int64_t observedAtMs = 0;
};

inline int64_t PhysicalLLMTimeMs(const std::string &value)
{
    // Go time.Time JSON is RFC3339 with optional subsecond digits, UTC here.
    if (value.size() < 20 || value.size() > 30 || value.back() != 'Z') { return 0; }
    std::tm time{};
    std::istringstream input(value.substr(0, 19));
    input.imbue(std::locale::classic());
    input >> std::get_time(&time, "%Y-%m-%dT%H:%M:%S");
    if (input.fail()) { return 0; }
    const auto seconds = timegm(&time);
    char normalized[32];
    if (seconds <= 0 || std::strftime(normalized, sizeof(normalized), "%Y-%m-%dT%H:%M:%S", &time) == 0
        || value.substr(0,19) != normalized) { return 0; }
    int millis = 0;
    if (value.size() != 20) {
        if (value[19] != '.' || value.size() < 22) { return 0; }
        for (size_t i = 20; i + 1 < value.size(); ++i) {
            if (value[i] < '0' || value[i] > '9') { return 0; }
            if (i < 23) { millis += (value[i]-'0') * (i == 20 ? 100 : i == 21 ? 10 : 1); }
        }
    }
    return static_cast<int64_t>(seconds) * 1000 + millis;
}

inline PhysicalHoldResult ParsePhysicalHold(const litebus::http::Response &response,
    const PhysicalLLMIdentity &identity, const std::string &requestedToken, bool release,
    int64_t nowMs, int64_t maxAgeMs)
{
    PhysicalHoldResult result;
    if (!identity.Valid() || response.body.size() > 16384) { return result; }
    try {
        const auto body = nlohmann::json::parse(response.body);
        if (response.retCode == litebus::http::CONFLICT && body.value("hold_state", "") == "unchanged") {
            result.outcome = PhysicalHoldOutcome::UNCHANGED;
            result.token = requestedToken;
            return result;
        }
        if (!body.contains("identity") || body.at("identity") != identity.JSON()) { return result; }
        if (release) {
            if (response.retCode == litebus::http::OK && body.at("held").is_boolean()
                && !body.at("held").get<bool>() && !requestedToken.empty()) {
                result.outcome = PhysicalHoldOutcome::RELEASED;
                result.token = requestedToken;
            }
            return result;
        }
        if (response.retCode != litebus::http::OK && response.retCode != litebus::http::CONFLICT) { return result; }
        const auto token = body.at("hold_token").get<std::string>();
        if (token.empty() || token.size() > 128 || (!requestedToken.empty() && token != requestedToken)
            || !body.at("held").get<bool>()) { return result; }
        result.token = token;
        result.outcome = PhysicalHoldOutcome::HELD;
        // A valid token remains useful for rollback even if LLM evidence is bad.
        if (response.retCode != litebus::http::OK || !body.at("active_deliveries").is_number_integer()
            || !body.at("delivery_quiescent").get<bool>()
            || body.at("active_deliveries").get<int64_t>() != 0) { return result; }
        const auto &view = body.at("llm_observation");
        const auto &session = view.at("session");
        for (const auto *name : {"seq", "unbound_active_requests"}) {
            if (!view.at(name).is_number_integer()) { return result; }
        }
        for (const auto *name : {"active_requests", "waiting_requests", "ready_requests"}) {
            if (!session.at(name).is_number_integer()) { return result; }
        }
        result.epoch = view.at("epoch").get<std::string>();
        result.sequence = view.at("seq").get<int64_t>();
        result.observedAtMs = PhysicalLLMTimeMs(view.at("observed_at").get<std::string>());
        if (result.epoch.empty() || result.epoch.size() > 128 || result.sequence < 0
            || result.observedAtMs <= 0 || maxAgeMs <= 0 || result.observedAtMs > nowMs
            || nowMs - result.observedAtMs > maxAgeMs || !view.at("physical_complete").get<bool>()
            || view.at("unbound_active_requests").get<int64_t>() != 0 || !view.at("waiting_only").get<bool>()
            || session.at("identity") != identity.JSON() || session.at("streaming").get<bool>()
            || !session.at("waiting_only").get<bool>() || session.at("ready_requests").get<int64_t>() != 0
            || session.at("active_requests").get<int64_t>() <= 0
            || session.at("active_requests") != session.at("waiting_requests")) { return result; }
        result.outcome = PhysicalHoldOutcome::READY;
    } catch (const nlohmann::json::exception &) {
        // Keep independently validated ownership for cleanup, never readiness.
    }
    return result;
}

class PhysicalLLMClient {
public:
    using Sender = std::function<litebus::Future<litebus::http::Response>(const litebus::http::Request &)>;
    PhysicalLLMClient(std::string baseURL, std::string token, uint64_t timeoutMs = 2000,
        Sender sender = [](const litebus::http::Request &request) { return litebus::http::LaunchRequest(request); })
        : baseURL_(std::move(baseURL)), token_(std::move(token)), timeoutMs_(timeoutMs), sender_(std::move(sender)) {}

    litebus::Future<PhysicalHoldResult> Prepare(const PhysicalLLMIdentity &identity, const std::string &token = "") const {
        return Call(identity, token, false);
    }
    litebus::Future<PhysicalHoldResult> Release(const PhysicalLLMIdentity &identity, const std::string &token) const {
        return Call(identity, token, true);
    }
private:
    litebus::Future<PhysicalHoldResult> Call(const PhysicalLLMIdentity &identity, const std::string &token, bool release) const {
        PhysicalHoldResult rejected;
        rejected.outcome = PhysicalHoldOutcome::LOCAL_REJECTED;
        if (!identity.Valid() || token.size() > 128 || (release && token.empty()) || !sender_
            || token_.size() < 32 || token_.size() > 4096 || token_.find_first_of("\r\n") != std::string::npos
            || timeoutMs_ == 0 || timeoutMs_ > 5000 || baseURL_.size() > 2048
            || (baseURL_.rfind("http://", 0) != 0 && baseURL_.rfind("https://", 0) != 0)) { return rejected; }
        auto url = litebus::http::URL::Decode(baseURL_ + (release ? "/hold/physical/off" : "/hold/physical/prepare"));
        if (url.IsError() || !url.Get().query.empty() || !litebus::http::CheckReqUrl(url.Get())) { return rejected; }
        litebus::http::Request request;
        request.method = "POST";
        request.url = url.Get();
        request.timeout = litebus::Option<uint64_t>(timeoutMs_);
        request.headers["Authorization"] = "Bearer " + token_;
        request.headers["Content-Type"] = "application/json";
        request.body = nlohmann::json{{"identity", identity.JSON()}, {"hold_token", token}}.dump();
        const auto started = std::chrono::steady_clock::now();
        auto completion = std::make_shared<litebus::Promise<PhysicalHoldResult>>();
        auto future = completion->GetFuture();
        sender_(request).OnComplete([completion, identity, token, release, started, timeout = timeoutMs_](
            const litebus::Future<litebus::http::Response> &response) {
            PhysicalHoldResult result;
            if (response.IsOK()) {
                const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                result = ParsePhysicalHold(response.Get(), identity, token, release, nowMs, 3000);
                if (result.outcome == PhysicalHoldOutcome::READY
                    && std::chrono::steady_clock::now() - started > std::chrono::milliseconds(timeout)) {
                    result.outcome = PhysicalHoldOutcome::HELD;
                }
            }
            completion->SetValue(result);
        });
        return future;
    }
    std::string baseURL_, token_;
    uint64_t timeoutMs_;
    Sender sender_;
};
} // namespace functionsystem::local_scheduler
#endif
