/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include "meta_store_client/key_value/prefix_range_cursor.h"

namespace functionsystem::meta_store::test {
TEST(PrefixRangeCursorTest, SnapshotRevisionDoesNotFollowLaterHeaders)
{
    PrefixRangeCursor cursor("/instances/", "/instances0", 2);
    EXPECT_TRUE(cursor.Advance(10, 100, {"/instances/a", "/instances/b"}, true));
    EXPECT_EQ(cursor.Key(), std::string("/instances/b") + '\0');
    EXPECT_TRUE(cursor.Advance(12, 100, {"/instances/c"}, false));
    EXPECT_EQ(cursor.Revision(), 10);
}

TEST(PrefixRangeCursorTest, BinaryKeyContinuationIncludesExtensions)
{
    PrefixRangeCursor cursor("/p/", "/p0", 1);
    std::string key = std::string("/p/a") + '\0';
    EXPECT_TRUE(cursor.Advance(3, 1, {key}, true));
    EXPECT_EQ(cursor.Key(), key + '\0');
    EXPECT_TRUE(cursor.Advance(4, 1, {key + '\0' + "x"}, false));
}

TEST(PrefixRangeCursorTest, EmptyCompleteSnapshotIsValid)
{
    PrefixRangeCursor cursor("/p/", "/p0", 2);
    EXPECT_TRUE(cursor.Advance(3, 1, {}, false));
    EXPECT_EQ(cursor.Revision(), 3);
}

TEST(PrefixRangeCursorTest, RejectsZeroRevisionAndInvalidBudgets)
{
    PrefixRangeCursor cursor("/p/", "/p0", 2);
    EXPECT_FALSE(cursor.Advance(0, 1, {}, false));
    EXPECT_FALSE(cursor.Advance(-1, 1, {}, false));
    EXPECT_EQ(cursor.Revision(), 0);
    PrefixRangeCursor zero("/p/", "/p0", 0);
    EXPECT_FALSE(zero.Advance(1, 1, {}, false));
    EXPECT_FALSE(cursor.Advance(1, 1, {"/p/a", "/p/b", "/p/c"}, false));
}

TEST(PrefixRangeCursorTest, RejectsEmptyMorePage)
{
    PrefixRangeCursor cursor("/p/", "/p0", 2);
    EXPECT_FALSE(cursor.Advance(3, 1, {}, true));
    EXPECT_EQ(cursor.Revision(), 0);
}

TEST(PrefixRangeCursorTest, RejectsDuplicateAndDescendingKeys)
{
    PrefixRangeCursor cursor("/p/", "/p0", 2);
    EXPECT_FALSE(cursor.Advance(3, 1, {"/p/a", "/p/a"}, true));
    EXPECT_FALSE(cursor.Advance(3, 1, {"/p/b", "/p/a"}, true));
    EXPECT_EQ(cursor.Key(), "/p/");
}

TEST(PrefixRangeCursorTest, RejectsOutOfScopeAndOutOfRangeKeys)
{
    PrefixRangeCursor cursor("/p/", "/p0", 2);
    EXPECT_FALSE(cursor.Advance(3, 1, {"/q/a"}, false));
    EXPECT_FALSE(cursor.Advance(3, 1, {"/p0"}, false));
    EXPECT_FALSE(cursor.Advance(3, 1, {"/o/z"}, false));
}

TEST(PrefixRangeCursorTest, RejectsSourceChangeAndOlderHeaders)
{
    PrefixRangeCursor cursor("/p/", "/p0", 1);
    ASSERT_TRUE(cursor.Advance(3, 1, {"/p/a"}, true));
    EXPECT_FALSE(cursor.Advance(4, 2, {"/p/b"}, false));
    EXPECT_FALSE(cursor.Advance(2, 1, {"/p/b"}, false));
    EXPECT_EQ(cursor.Revision(), 3);
}

TEST(PrefixRangeCursorTest, RejectsReplayedPageWithoutAdvancing)
{
    PrefixRangeCursor cursor("/p/", "/p0", 1);
    ASSERT_TRUE(cursor.Advance(3, 1, {"/p/a"}, true));
    auto before = cursor.Key();
    EXPECT_FALSE(cursor.Advance(3, 1, {"/p/a"}, true));
    EXPECT_EQ(cursor.Key(), before);
}

TEST(PrefixRangeCursorTest, HandlesUnboundedRangeEndSentinel)
{
    std::string prefix(1, static_cast<char>(0xff));
    PrefixRangeCursor cursor(prefix, std::string(1, '\0'), 1);
    EXPECT_TRUE(cursor.Advance(3, 1, {prefix + "a"}, true));
    EXPECT_TRUE(cursor.Advance(3, 1, {prefix + "b"}, false));
}
}  // namespace functionsystem::meta_store::test
