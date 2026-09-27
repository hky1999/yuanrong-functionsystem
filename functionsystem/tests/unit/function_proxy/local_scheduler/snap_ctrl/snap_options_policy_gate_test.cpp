/*
 * Executable policy/gate regression for the SnapOptions tag migration and
 * the shared public/private single-flight. Self-contained main (no gtest
 * dependency) so it can be compiled and run directly against the REAL
 * generated protobuf code and the REAL SnapshotAttemptGate:
 *
 *   protoc the canonical proto/posix/*.proto, then
 *   g++ this file + core_service.pb.cc (+deps) + policy/gate headers.
 *
 * Every case prints PASS <name>; any failure prints FAIL <name> and exits 1.
 */
#include <cstdio>
#include <cstdlib>
#include <string>

#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/dynamic_message.h"

#include "common/proto/pb/posix/core_service.pb.h"
#include "local_scheduler/snap_ctrl/snapshot_attempt_gate.h"
#include "local_scheduler/snap_ctrl/snap_options_policy.h"

using functionsystem::local_scheduler::CarriesLegacyPrivateWire;
using functionsystem::local_scheduler::HasPrivateParkFields;
using functionsystem::local_scheduler::PublicSnapshotRequestIsPure;
using functionsystem::local_scheduler::SnapshotAttemptGate;

static int g_failed = 0;
#define CHECK(name, cond)                                        \
    do {                                                         \
        if (cond) { std::printf("PASS %s\n", name); }            \
        else { std::printf("FAIL %s\n", name); ++g_failed; }     \
    } while (0)

// dynamic descriptor replicating the PRE-migration private layout
// (hold_token=5, expected_runtime_id=6, expected_sandbox_id=7,
// expected_resource_generation=8) — the wire the old proxy sent.
static const google::protobuf::Descriptor *OldPrivateLayout()
{
    static google::protobuf::DescriptorPool pool;
    static const google::protobuf::Descriptor *d = nullptr;
    if (d == nullptr) {
        google::protobuf::FileDescriptorProto file;
        file.set_name("old_private_snap_options.proto");
        file.set_syntax("proto3");
        auto *msg = file.add_message_type();
        msg->set_name("OldPrivateSnapOptions");
        const char *names[4] = {"hold_token", "expected_runtime_id",
                                "expected_sandbox_id", "expected_resource_generation"};
        for (int i = 0; i < 4; ++i) {
            auto *f = msg->add_field();
            f->set_name(names[i]);
            f->set_number(5 + i);
            f->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
            f->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        }
        d = pool.BuildFile(file)->message_type(0);
    }
    return d;
}

static std::string SerializeOldPrivatePacket(const std::string &token,
                                              const std::string &runtime,
                                              const std::string &sandbox,
                                              const std::string &generation)
{
    google::protobuf::DynamicMessageFactory factory;
    const auto *prototype = factory.GetPrototype(OldPrivateLayout());
    auto *msg = prototype->New();
    const auto *refl = msg->GetReflection();
    const auto *d = msg->GetDescriptor();
    refl->SetString(msg, d->FindFieldByName("hold_token"), token);
    refl->SetString(msg, d->FindFieldByName("expected_runtime_id"), runtime);
    refl->SetString(msg, d->FindFieldByName("expected_sandbox_id"), sandbox);
    refl->SetString(msg, d->FindFieldByName("expected_resource_generation"), generation);
    const std::string bytes = msg->SerializeAsString();
    delete msg;
    return bytes;
}

int main()
{
    // 1) a REAL old private packet (full tuple) parses under the fixed
    //    schema but is DETECTED and must be rejected by policy
    core_service::SnapOptions legacy;
    CHECK("legacy-full-tuple-parses",
          legacy.ParseFromString(
              SerializeOldPrivatePacket("tok-1", "rt-1", "sbox-1", "gen-1")));
    CHECK("legacy-full-tuple-detected", CarriesLegacyPrivateWire(legacy));
    CHECK("legacy-full-tuple-not-public-pure", !PublicSnapshotRequestIsPure(legacy));

    // 2) old token-ONLY packet (the degraded case the review called out):
    //    hold_token@5 lands in name; policy must still reject it
    core_service::SnapOptions legacyTokenOnly;
    CHECK("legacy-token-only-parses",
          legacyTokenOnly.ParseFromString(
              SerializeOldPrivatePacket("tok-2", "", "", "")));
    CHECK("legacy-token-only-name-collided",
          legacyTokenOnly.name() == "tok-2");
    CHECK("legacy-token-only-no-new-park-fields", !HasPrivateParkFields(legacyTokenOnly));
    CHECK("legacy-token-only-still-detected", CarriesLegacyPrivateWire(legacyTokenOnly));

    // 3) a PURE public packet carries no legacy footprint
    core_service::SnapOptions pub;
    pub.set_type(common::SNAPSHOT);
    pub.set_name("pc-snapshot-ov23-1-snap");
    pub.set_checkpointtimeoutms(300000);
    CHECK("public-pure-parses", pub.ParseFromString(pub.SerializeAsString()));
    CHECK("public-pure-accepted", PublicSnapshotRequestIsPure(pub));

    // 4) public type with private fields mixed in must be refused
    core_service::SnapOptions mixed;
    mixed.set_type(common::SNAPSHOT);
    mixed.set_name("x");
    mixed.set_expected_runtime_id("rt-9");
    CHECK("public-mixed-refused", !PublicSnapshotRequestIsPure(mixed));

    // 5) a NEW private packet (fields 21-24) is not flagged legacy
    core_service::SnapOptions fresh;
    fresh.set_hold_token("tok");
    fresh.set_expected_runtime_id("rt");
    fresh.set_expected_sandbox_id("sb");
    fresh.set_expected_resource_generation("g");
    CHECK("new-private-not-legacy",
          fresh.ParseFromString(fresh.SerializeAsString())
          && !CarriesLegacyPrivateWire(fresh));
    CHECK("new-private-has-park-fields", HasPrivateParkFields(fresh));

    // 6) SHARED single flight on the REAL gate: public-in-flight blocks
    //    private, own-attempt release, foreign attempt cannot release
    SnapshotAttemptGate gate;
    const auto pubAttempt = gate.Begin("inst-1");
    CHECK("gate-public-begin", pubAttempt.has_value());
    CHECK("gate-public-blocks-private", gate.Busy("inst-1"));
    CHECK("gate-private-begin-refused", !gate.Begin("inst-1").has_value());
    CHECK("gate-foreign-attempt-cannot-release",
          !gate.Finish("inst-1", *pubAttempt + 999, true));
    CHECK("gate-still-busy-after-foreign-finish", gate.Busy("inst-1"));
    CHECK("gate-own-attempt-releases", gate.Finish("inst-1", *pubAttempt, true));
    CHECK("gate-free-after-release", !gate.Busy("inst-1"));
    const auto privAttempt = gate.Begin("inst-1");
    CHECK("gate-private-after-release", privAttempt.has_value());
    CHECK("gate-private-blocks-public", !gate.Begin("inst-1").has_value());
    CHECK("gate-unknown-outcome-marks-not-erases",
          gate.Finish("inst-1", *privAttempt, false) && gate.Busy("inst-1"));
    CHECK("gate-unknown-visible", gate.Unknown("inst-1"));
    CHECK("gate-known-finish-clears", gate.Finish("inst-1", *privAttempt, true)
          && !gate.Busy("inst-1"));

    if (g_failed != 0) {
        std::printf("%d FAILED\n", g_failed);
        return 1;
    }
    std::printf("ALL PASS\n");
    return 0;
}
