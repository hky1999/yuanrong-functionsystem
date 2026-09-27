/* Plain gtest main for the pure managed-checkpoint target contract tests:
 * no litebus init, no observability — the suite must stay a pure-function
 * check (tests/unit/main.cpp drags in litebus/observability symbols that
 * would force a full link set for no coverage gain). */
#include "gtest/gtest.h"

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
