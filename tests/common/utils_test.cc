#include "src/common/utils.h"

#include "gtest/gtest.h"

using namespace gfs::common::utils;

class UtilsUnitTest : public ::testing::Test {};

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

// A trivial test to ensure that we can compute the checksum reliably
TEST_F(UtilsUnitTest, SimpleChecksumTest) {
  std::string sample_data("Test is a sample data for testing checksum");
  auto check_sum(calc_checksum(sample_data));
  EXPECT_EQ(check_sum, calc_checksum(sample_data));
}
