#include "gtest/gtest.h"
#include "src/common/utils.h"

using namespace gfs::common::utils;

class UtilsUnitTest : public ::testing::Test {
};

// Test different cases for checking the validity of a filename
TEST_F(UtilsUnitTest, ValidPathnameTest) {
   auto emptyPathnameRes(CheckFilenameValidity(""));
   EXPECT_EQ(emptyPathnameRes.error_code(), 
             google::protobuf::util::error::INVALID_ARGUMENT);

   auto relativePathnameRes(CheckFilenameValidity("a/b/c"));
   EXPECT_EQ(relativePathnameRes.error_code(), 
             google::protobuf::util::error::INVALID_ARGUMENT);

   auto trailingSlashPathnameRes(CheckFilenameValidity("/a/b/c/"));
   EXPECT_EQ(trailingSlashPathnameRes.error_code(), 
             google::protobuf::util::error::INVALID_ARGUMENT);

   auto consecutiveSlashPathnameRes(CheckFilenameValidity("//a/b//c"));
   EXPECT_EQ(consecutiveSlashPathnameRes.error_code(),
             google::protobuf::util::error::INVALID_ARGUMENT);

   auto validPathnameRes(CheckFilenameValidity("/foo/bar/baz"));
   EXPECT_TRUE(validPathnameRes.ok());
}
