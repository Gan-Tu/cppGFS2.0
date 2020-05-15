#include <iostream>
#include <thread>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "parallel_hashmap/phmap.h"

// Define an intrinsically thread-safe flat hash map by parallel hashmap
// The default definition assumes no internal lock and requires users
// to pragmatically synchronize concurrent read and write to the parallel
// hashmap. By passing a lock type, e.g. absl::Mutex, the parallel
// hashmap is intrinsically thread safe.
//
// The example below follows the pattern defined in:
// https://greg7mdp.github.io/parallel-hashmap/
// https://github.com/greg7mdp/parallel-hashmap/blob/master/examples/bench.cc
template <class K, class V>
class thread_safe_flat_hash_map
    : public phmap::parallel_flat_hash_map<
          K, V, phmap::container_internal::hash_default_hash<K>,
          phmap::container_internal::hash_default_eq<K>,
          std::allocator<std::pair<const K, V>>, /*submaps=*/4, absl::Mutex> {
};

// Example Parallel Hashmap usage
// https://github.com/greg7mdp/parallel-hashmap
int main(int argc, char const* argv[]) {
  // Default case, no internal lock protection.
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

  // Parallel insert with thread safe hash map.
  // Note that if you run the following code with the default non-thread-safe
  // phmap::flat_hash_map, you will run into race condition and likely encounter
  // a core-dump
  thread_safe_flat_hash_map<std::string, std::string> thread_safe_contact;
  int num_of_threads(50);
  std::vector<std::thread> threads;
  for (int i = 0; i < num_of_threads; i++) {
    threads.push_back(std::thread([&, i]() {
      std::string name(std::to_string(i));
      std::string contact(name + "@gmail.com");
      thread_safe_contact[name] = contact;
    }));
  }

  for (int i = 0; i < num_of_threads; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_of_threads; i++) {
    std::string name(std::to_string(i));
    std::cout << name << ", " << thread_safe_contact[name] << std::endl;
  }

  return 0;
}
