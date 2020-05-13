#include "gtest/gtest.h"
#include "src/common/utils.h"

using namespace gfs::common::utils;

class UtilsUnitTest : public ::testing::Test {
};

// Test different cases for checking the validity of a pathname
TEST_F(UtilsUnitTest, ValidPathnameTest) {
   auto emptyPathnameRes(checkPathnameValidity(""));
   EXPECT_EQ(emptyPathnameRes.error_message(), 
             "Empty pathname is not allowed");

   auto relativePathnameRes(checkPathnameValidity("a/b/c"));
   EXPECT_EQ(relativePathnameRes.error_message(), 
             "Relative path is not allowed");

   auto trailingSlashPathnameRes(checkPathnameValidity("/a/b/c/"));
   EXPECT_EQ(trailingSlashPathnameRes.error_message(), 
             "Trailing slash is not allowed");

   auto consecutiveSlashPathnameRes(checkPathnameValidity("//a/b//c"));
   EXPECT_EQ(consecutiveSlashPathnameRes.error_message(),
             "Consecutive slash is not allowed");

   auto validPathnameRes(checkPathnameValidity("/foo/bar/baz"));
   EXPECT_EQ(validPathnameRes.ok(), true);
}
