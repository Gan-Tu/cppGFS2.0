#include "src/common/system_logger.h"

namespace gfs {

namespace common {

// TODO(bmokutub): If needed, also configure minloglevel to filter out certain
// log levels. And log to files too.
void SystemLogger::Initialize(const std::string& program_name,
                              const bool use_failure_signal_handler) {
  if (this->is_initialized_) {
    // Already initialized. Should be done once.
    return;
  }

  // Log to console
  FLAGS_logtostderr = true;

  google::InitGoogleLogging(program_name.c_str());

  if (use_failure_signal_handler) {
    // Enables us to dump useful information when the program crashes on certain
    // signals such as SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGBUS, and SIGTERM.
    google::InstallFailureSignalHandler();
  }

  this->is_initialized_ = true;
}

}  // namespace common
}  // namespace gfs
