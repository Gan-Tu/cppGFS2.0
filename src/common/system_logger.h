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
  void Initialize(const std::string& program_name);

 private:
  bool is_initialized_;

  SystemLogger() : is_initialized_(false) {}
};

}  // namespace common
}  // namespace gfs

// The macros you should use for logging. only use the SYS_* macros. Please see
// examples/system_logger_main.cc on how to use.
//
// SEVERITY LEVELS: You can specify one of the following severity levels (in
// increasing order of severity): INFO, WARNING, ERROR, and FATAL. Logging a
// FATAL message terminates the program (after the message is logged). Note that
// messages of a given severity are logged not only in the logfile for that
// severity, but also in all logfiles of lower severity. E.g., a message of
// severity FATAL will be logged to the logfiles of severity FATAL, ERROR,
// WARNING, and INFO.
// Further reading about glog:
// https://github.com/google/glog/blob/master/doc/glog.html

// Log with the specified severity.
#define SYS_LOG(severity) LOG(severity)

// The *_ASSERT_* macro provides the ability to abort the application when a
// condition is not met.
#define SYS_LOG_ASSERT(condition) LOG_ASSERT(condition)

#define SYS_ASSERT_EQ(val1, val2) CHECK_EQ(val1, val2)
#define SYS_ASSERT_NE(val1, val2) CHECK_NE(val1, val2)
#define SYS_ASSERT_LE(val1, val2) CHECK_LE(val1, val2)
#define SYS_ASSERT_LT(val1, val2) CHECK_LT(val1, val2)
#define SYS_ASSERT_GE(val1, val2) CHECK_GE(val1, val2)
#define SYS_ASSERT_GT(val1, val2) CHECK_GT(val1, val2)

#define SYS_ASSERT_NOTNULL(val) CHECK_NOTNULL(val)
#define SYS_ASSERT_ERR(invocation) CHECK_ERR(invocation)

// Conditional / Occasional Logging. Log only when the condition is met.
#define SYS_LOG_IF(severity, condition) LOG_IF(severity, condition)
#define SYS_LOG_FIRST_N(severity, n) LOG_FIRST_N(severity, n)
#define SYS_LOG_IF_EVERY_N(severity, condition, n) \
  LOG_IF_EVERY_N(severity, condition, n)
#define SYS_LOG_EVERY_N(severity, n) LOG_EVERY_N(severity, n)

// STL logging
#define SYS_LOG_STRING_VECTOR(severity, outvec) LOG_STRING(severity, outvec)

#endif  // GFS_COMMON_SYSTEM_LOGGER_H_
