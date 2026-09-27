/*
 * Pure routing/rejection policy for core_service::SnapOptions payloads.
 * Kept free of actor dependencies so wire-level behavior is unit-testable
 * against the REAL generated message (see snap_options_wire_test.cpp and
 * snap_options_policy_gate_test.cpp).
 */
#ifndef LOCAL_SCHEDULER_SNAP_OPTIONS_POLICY_H
#define LOCAL_SCHEDULER_SNAP_OPTIONS_POLICY_H

#include "common/proto/pb/posix/core_service.pb.h"

namespace functionsystem::local_scheduler {

// A pre-migration PRIVATE park payload (hold_token@5, expected_runtime_id@6,
// expected_sandbox_id@7, expected_resource_generation@8) parses under the
// fixed schema as name=<old token> plus UNKNOWN fields 6/7/8 (the tag-6
// string cannot satisfy the uint64 field). Such a packet must be REJECTED,
// never silently degraded to the no-identity legacy path.
inline bool CarriesLegacyPrivateWire(const core_service::SnapOptions &options)
{
    // (a) any old expected_* string (tags 6-8) survives as an UNKNOWN field
    // (proto3 omits empty strings, so a full/partial old tuple always
    // leaves this footprint when any expected field was non-empty)
    const auto &unknown =
        options.GetReflection()->GetUnknownFields(options);
    for (int i = 0; i < unknown.field_count(); ++i) {
        const int number = unknown.field(i).number();
        if (number >= 6 && number <= 8) {
            return true;
        }
    }
    // (b) an old TOKEN-ONLY packet serialized just tag 5, which under the
    // fixed schema is indistinguishable from a public name BYTES-WISE —
    // but a public name only ever travels with type=SNAPSHOT (the only
    // upstream consumer of name). A name on any non-SNAPSHOT type is the
    // legacy hold-token footprint and is rejected.
    return !options.name().empty() && options.type() != common::SnapType::SNAPSHOT;
}

inline bool HasPrivateParkFields(const core_service::SnapOptions &options)
{
    return !options.hold_token().empty() || !options.expected_runtime_id().empty()
        || !options.expected_sandbox_id().empty()
        || !options.expected_resource_generation().empty();
}

// A PUBLIC type=SNAPSHOT request must be PURELY public: any private park
// field (or legacy private wire footprint) mixed in is a protocol error,
// so the public routing can never be used to bypass the private identity
// fence, and the private runtime comparison is never skipped for a
// mixed packet.
inline bool PublicSnapshotRequestIsPure(const core_service::SnapOptions &options)
{
    return !HasPrivateParkFields(options) && !CarriesLegacyPrivateWire(options);
}

}  // namespace functionsystem::local_scheduler
#endif
