#ifndef GFS_COMMON_SYSTEM_LOGGER_H_
#define GFS_COMMON_SYSTEM_LOGGER_H_

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include <string>

// The system_logger wraps the logging library, which in this case is Google
// glog. The reason for having a wrapper is to ensure that we can easily change
// the underlying logging library if needed without having to change the
// system_logger clients code. Also this helps us to control the functionalities
// the underlying library that we want to expose.

namespace gfs {

namespace common {

// Singleton class for managing for configuring how we log. The system logger
// must be intialized in the application (ideally once) before using any of the
// logging macros. Please see examples/system_logger_main.cc on how to use.
// We currently only log to console.
class SystemLogger {
 public:
  static SystemLogger& GetInstance() {
    static SystemLogger instance;

    return instance;
  }

  // To prevent copying the SystemLogger
  SystemLogger(const SystemLogger&) = delete;

  void operator=(const SystemLogger&) = delete;

  // This initializes the logging framework and does the necessary
  // configurations. Currently logging to console only. Must be called before
  // using any of the logging macros. This isn't thread safe and must be done
  // from a single thread.
  //
  // If |use_failure_signal_handler| is true, also install the
  // "InstallFailureSignalHandler" provided by glog, which enables us to dump
  // useful information when the program crashes on certain signals such as
  // SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGBUS, and SIGTERM.
  //
  // However, the glibc built-in stack-unwinder on 64-bit systems has some
  // problem with the glog libraries, and is known to cause terminal to hang
  // and be in deadlock:
  //    https://github.com/google/glog/blob/master/INSTALL#L15
  // To solve this, you need to install libunwind first:
  //    https://www.nongnu.org/libunwind/
  // Note that it doesn't work with new MacOS systems yet.
  void Initialize(const std::string& program_name,
                  const bool use_failure_signal_handler = false);

 private:
  bool is_initialized_;

  SystemLogger() : is_initialized_(false) {}
};

}  // namespace common
}  // namespace gfs

// Macros for logging. Please see
// examples/system_logger_main.cc on how to use.
//
// SEVERITY LEVELS: You can specify one of the following severity levels (in
// increasing order of severity): INFO, WARNING, ERROR, and FATAL. Logging a
// FATAL message terminates the program (after the message is logged). Note that
// messages of a given severity are logged not only in the logfile for that
// severity, but also in all logfiles of lower severity. E.g., a message of
// severity FATAL will be logged to the logfiles of severity FATAL, ERROR,
// WARNING, and INFO. Currently logging to only console and the macros work
// both console and file logging. The macros are thread safe.
// Further reading about glog:
// https://github.com/google/glog/blob/master/doc/glog.html

// Log with the specified severity.
// LOG(severity)

// The *ASSERT* macro provides the ability to abort the application when a
// condition is not met.

// LOG_ASSERT(condition)
#define ASSERT_EQ(val1, val2) CHECK_EQ(val1, val2)
#define ASSERT_NE(val1, val2) CHECK_NE(val1, val2)
#define ASSERT_LE(val1, val2) CHECK_LE(val1, val2)
#define ASSERT_LT(val1, val2) CHECK_LT(val1, val2)
#define ASSERT_GE(val1, val2) CHECK_GE(val1, val2)
#define ASSERT_GT(val1, val2) CHECK_GT(val1, val2)

#define ASSERT_NOTNULL(val) CHECK_NOTNULL(val)
#define ASSERT_ERR(invocation) CHECK_ERR(invocation)

// Conditional / Occasional Logging. Log only when the condition is met.
// LOG_IF(severity, condition)
// LOG_FIRST_N(severity, n)
// LOG_IF_EVERY_N(severity, condition, n)
// LOG_EVERY_N(severity, n)

// STL logging
// LOG_STRING(severity, outvec)

#endif  // GFS_COMMON_SYSTEM_LOGGER_H_
