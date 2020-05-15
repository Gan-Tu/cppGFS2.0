#include <iostream>

#include "parallel_hashmap/phmap.h"

// Example Parallel Hashmap usage
// https://github.com/greg7mdp/parallel-hashmap
int main(int argc, char const* argv[]) {
  phmap::flat_hash_map<std::string, std::string> contacts = {
      {"tom", "tom@gmail.com"},
      {"jeff", "jk@gmail.com"},
      {"jim", "jimg@microsoft.com"}};

  for (const auto& name_email_pair : contacts) {
    std::cout << name_email_pair.first
              << "'s email is: " << name_email_pair.second << "\n";
  }
  contacts["bill"] = "bg@whatever.com";
  std::cout << "bill's email is: " << contacts["bill"] << "\n";

  return 0;
}
