#include <vector>

#include "src/common/system_logger.h"

int main(int argc, char* argv[]) {
  // Must do this first (most likely in your program entry/init code)
  gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);

  SYS_LOG(INFO) << "Hello, world!";

  // logging STL containers.
  std::vector<int> x;
  x.push_back(1);
  x.push_back(2);
  x.push_back(3);
  SYS_LOG(INFO) << "ABC, it's easy as " << x;

  SYS_LOG(WARNING) << "test warning";

  SYS_LOG(ERROR) << "test error";

  SYS_LOG(FATAL) << "test fatal, we will terminate here";

  return 0;
}