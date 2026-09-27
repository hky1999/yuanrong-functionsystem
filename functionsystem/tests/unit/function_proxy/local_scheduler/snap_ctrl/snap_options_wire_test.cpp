/*
 * Wire-contract regression for core_service.SnapOptions.
 *
 * Anchors the incident of 2026-09-27 (first physical Plan A case
 * pc-snapshot-ov23-1): the scheduler line had parked private hold
 * fields on tags 5-8, which are the PUBLIC upstream tags (name=5,
 * checkpointTimeoutMs=6) that the openYuanRong SDK and the frontend
 * (faasfrontend) encode. A public create-by-name request had its name
 * decoded as hold_token and was rejected by the identity guard
 * (snap_ctrl_actor.cpp "hold token requires the expected identity
 * tuple").
 *
 * This test pins, with REAL serialization:
 *   1. the public field numbers/types of the fixed layout;
 *   2. the private park fields live at 21-24, clear of every public tag;
 *   3. public bytes (name+timeout only) round-trip through the fixed
 *      layout and leave every private park field empty;
 *   4. a full private park payload round-trips through the fixed layout
 *      and leaves the public name empty (private wire never mints a
 *      public name);
 *   5. the COLLIDED layout (hold_token=5/expected_runtime_id=6, the
 *      pre-fix decoder) demonstrably misparses public bytes — kept as
 *      the negative control documenting why tags 5/6 are pinned.
 */
#include <gtest/gtest.h>

#include <string>

#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"

#include "common/proto/pb/posix/core_service.pb.h"

namespace functionsystem::local_scheduler {
namespace {

const google::protobuf::Descriptor *SnapOptionsDesc()
{
    const auto *d = core_service::SnapOptions::descriptor();
    return d;
}

TEST(SnapOptionsWire, PublicFieldsPinUpstreamTagsFiveAndSix)
{
    const auto *d = SnapOptionsDesc();
    ASSERT_NE(d, nullptr);
    const auto *name = d->FindFieldByName("name");
    ASSERT_NE(name, nullptr);
    EXPECT_EQ(name->number(), 5);
    EXPECT_EQ(name->type(), google::protobuf::FieldDescriptor::TYPE_STRING);
    const auto *timeout = d->FindFieldByName("checkpointTimeoutMs");
    ASSERT_NE(timeout, nullptr);
    EXPECT_EQ(timeout->number(), 6);
    EXPECT_EQ(timeout->type(), google::protobuf::FieldDescriptor::TYPE_UINT64);
}

TEST(SnapOptionsWire, PrivateParkFieldsLiveClearOfPublicTags)
{
    const auto *d = SnapOptionsDesc();
    for (const auto *field : d->fields()) {
        if (field->name() == "hold_token" || field->name() == "expected_runtime_id"
            || field->name() == "expected_sandbox_id"
            || field->name() == "expected_resource_generation") {
            EXPECT_GE(field->number(), 21) << field->name();
            EXPECT_LE(field->number(), 24) << field->name();
        } else {
            // every public/upstream-named field must stay in 1..6
            EXPECT_LE(field->number(), 6) << field->name();
        }
    }
    // no field may occupy a public tag other than its upstream meaning
    for (int number = 1; number <= 6; ++number) {
        const auto *field = d->FindFieldByNumber(number);
        ASSERT_NE(field, nullptr) << "tag " << number << " vacated";
    }
}

TEST(SnapOptionsWire, PublicBytesRoundTripAndLeaveParkFieldsEmpty)
{
    core_service::SnapOptions options;
    options.set_type(common::SNAPSHOT);
    options.set_name("pc-snapshot-ov23-1-snap");
    options.set_checkpointtimeoutms(300000);
    const auto bytes = options.SerializeAsString();
    ASSERT_FALSE(bytes.empty());

    core_service::SnapOptions parsed;
    ASSERT_TRUE(parsed.ParseFromString(bytes));
    EXPECT_EQ(parsed.type(), common::SNAPSHOT);
    EXPECT_EQ(parsed.name(), "pc-snapshot-ov23-1-snap");
    EXPECT_EQ(parsed.checkpointtimeoutms(), 300000U);
    EXPECT_TRUE(parsed.hold_token().empty());
    EXPECT_TRUE(parsed.expected_runtime_id().empty());
    EXPECT_TRUE(parsed.expected_sandbox_id().empty());
    EXPECT_TRUE(parsed.expected_resource_generation().empty());
}

TEST(SnapOptionsWire, PrivateParkPayloadNeverMintsPublicName)
{
    core_service::SnapOptions options;
    options.set_type(common::DUMPSTATE);
    options.set_leaverunning(false);
    options.set_hold_token("park-token-1");
    options.set_expected_runtime_id("runtime-1");
    options.set_expected_sandbox_id("sbox-1");
    options.set_expected_resource_generation("gen-1");
    const auto bytes = options.SerializeAsString();

    core_service::SnapOptions parsed;
    ASSERT_TRUE(parsed.ParseFromString(bytes));
    EXPECT_TRUE(parsed.name().empty());
    EXPECT_EQ(parsed.hold_token(), "park-token-1");
    EXPECT_EQ(parsed.expected_runtime_id(), "runtime-1");
    EXPECT_EQ(parsed.expected_sandbox_id(), "sbox-1");
    EXPECT_EQ(parsed.expected_resource_generation(), "gen-1");
}

// Build the PRE-FIX collided descriptor (park fields on 5/6) dynamically,
// exactly as the running 1bfe1bb9 proxy decoded public bytes.
google::protobuf::DescriptorPool *CollidedPool()
{
    static google::protobuf::DescriptorPool pool;
    static bool built = false;
    if (!built) {
        google::protobuf::FileDescriptorProto file;
        file.set_name("collided_core_service.proto");
        file.set_syntax("proto3");
        auto *msg = file.add_message_type();
        msg->set_name("CollidedSnapOptions");
        auto *hold = msg->add_field();
        hold->set_name("hold_token");
        hold->set_number(5);
        hold->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
        hold->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        auto *runtime = msg->add_field();
        runtime->set_name("expected_runtime_id");
        runtime->set_number(6);
        runtime->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
        runtime->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        pool.BuildFile(file);
        built = true;
    }
    return &pool;
}

TEST(SnapOptionsWire, CollidedLayoutWasTheFailureNegativeControl)
{
    // public bytes exactly as the SDK/frontend encode them
    core_service::SnapOptions options;
    options.set_type(common::SNAPSHOT);
    options.set_name("pc-snapshot-ov23-1-snap");
    options.set_checkpointtimeoutms(300000);
    const auto bytes = options.SerializeAsString();

    const auto *desc = CollidedPool()->FindMessageTypeByName("CollidedSnapOptions");
    ASSERT_NE(desc, nullptr);
    google::protobuf::DynamicMessageFactory factory;
    const auto *prototype = factory.GetPrototype(desc);
    ASSERT_NE(prototype, nullptr);
    const auto parsed = prototype->New();
    ASSERT_TRUE(parsed->ParseFromString(bytes));
    // the pre-fix decoder saw the public name as a hold token and an
    // empty expected tuple -> the identity-guard rejection of the
    // incident; keep this as the documented negative control
    const google::protobuf::Reflection *refl = parsed->GetReflection();
    const auto *holdField = desc->FindFieldByName("hold_token");
    const auto *runtimeField = desc->FindFieldByName("expected_runtime_id");
    EXPECT_EQ(refl->GetString(*parsed, holdField), "pc-snapshot-ov23-1-snap");
    EXPECT_TRUE(refl->GetString(*parsed, runtimeField).empty());
    delete parsed;
}

}  // namespace
}  // namespace functionsystem::local_scheduler
