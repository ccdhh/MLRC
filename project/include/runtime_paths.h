#ifndef RUNTIME_PATHS_H
#define RUNTIME_PATHS_H

#include <limits.h>
#include <string>
#include <unistd.h>

/** Resolve a path relative to the running executable directory (project/cmake/build). */
inline std::string resolve_path_relative_to_executable(const char *argv0, const char *relative_path)
{
#if defined(__linux__)
  char exe_path[PATH_MAX];
  const ssize_t n = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
  if (n > 0)
  {
    exe_path[n] = '\0';
    std::string dir(exe_path);
    const size_t slash = dir.rfind('/');
    if (slash != std::string::npos)
      dir.resize(slash);
    return dir + "/" + relative_path;
  }
#endif
  std::string exe(argv0 ? argv0 : ".");
  const size_t slash = exe.rfind('/');
  const std::string dir = (slash == std::string::npos) ? "." : exe.substr(0, slash);
  return dir + "/" + relative_path;
}

#endif
