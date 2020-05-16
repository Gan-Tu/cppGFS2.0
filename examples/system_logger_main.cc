#include <vector>

#include "src/common/system_logger.h"

int main(int argc, char* argv[]) {
  // Must do this first (most likely in your program entry/init code)
  gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);

  // These logging macros are thread safe, already does synchronization for you.
  
  LOG(INFO) << "Hello, world!";

  // logging STL containers.
  std::vector<int> x;
  x.push_back(1);
  x.push_back(2);
  x.push_back(3);
  LOG(INFO) << "ABC, it's easy as " << x;

  LOG(WARNING) << "test warning";

  LOG(ERROR) << "test error";

  LOG(FATAL) << "test fatal, we will terminate here";

  return 0;
}