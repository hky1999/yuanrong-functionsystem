#ifndef FUNCTION_PROXY_HOLD_ATTEMPT_POLICY_H
#define FUNCTION_PROXY_HOLD_ATTEMPT_POLICY_H

#include <string>
#include <nlohmann/json.hpp>

namespace functionsystem::local_scheduler {
inline bool ValidParkHoldToken(const std::string &token, bool leaveRunning)
{
    return token.empty() || (!leaveRunning && token.size() >= 16 && token.size() <= 128);
}
inline bool WakeMatchesHoldAttempt(const nlohmann::json &command, const std::string &expected)
{
    if (!command.is_object()) { return false; }
    const auto field = command.find("hold_token");
    if (field == command.end()) { return expected.empty(); }
    if (!field->is_string()) { return false; }
    const auto &provided = field->get_ref<const std::string &>();
    return ValidParkHoldToken(provided, false) && provided == expected;
}
inline bool WakeSchemaMatchesHoldAttempt(const nlohmann::json &command, const std::string &expected)
{
    if (!command.is_object() || !command.contains("schema") || !command["schema"].is_number_integer()) {
        return false;
    }
    const bool schemaMatches = (command["schema"] == 1 && expected.empty())
        || (command["schema"] == 2 && !expected.empty());
    return schemaMatches && WakeMatchesHoldAttempt(command, expected);
}
} // namespace functionsystem::local_scheduler
#endif
