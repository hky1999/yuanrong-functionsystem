#ifndef FUNCTION_PROXY_SNAPSHOT_ATTEMPT_GATE_H
#define FUNCTION_PROXY_SNAPSHOT_ATTEMPT_GATE_H
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
namespace functionsystem::local_scheduler {
// Actor-confined ownership. Unknown dispatched outcomes stay fenced until
// authoritative reconciliation; elapsed time alone never admits another park.
class SnapshotAttemptGate {
public:
    std::optional<uint64_t> Begin(const std::string &instance)
    {
        if (instance.empty() || entries_.count(instance) != 0 || entries_.size() >= 4096
            || next_ == std::numeric_limits<uint64_t>::max()) { return std::nullopt; }
        const auto attempt = ++next_;
        entries_[instance] = {attempt, false};
        return attempt;
    }
    bool Busy(const std::string &instance) const { return entries_.count(instance) != 0; }
    bool Matches(const std::string &instance, uint64_t attempt) const
    {
        const auto it = entries_.find(instance);
        return it != entries_.end() && attempt != 0 && it->second.attempt == attempt;
    }
    bool Finish(const std::string &instance, uint64_t attempt, bool outcomeKnown)
    {
        if (!Matches(instance, attempt)) { return false; }
        if (outcomeKnown) { entries_.erase(instance); }
        else { entries_.at(instance).unknown = true; }
        return true;
    }
    bool Unknown(const std::string &instance) const
    {
        const auto it = entries_.find(instance);
        return it != entries_.end() && it->second.unknown;
    }
private:
    struct Entry { uint64_t attempt; bool unknown; };
    std::unordered_map<std::string, Entry> entries_;
    uint64_t next_ = 0;
};
} // namespace functionsystem::local_scheduler
#endif
