#include <iostream>
#include "leveldb/db.h"

// Example LevelDb usage
int main(int argc, char const *argv[]) {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;

  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());

  std::string key("var");
  status = db->Put(leveldb::WriteOptions(), key, "Hello World!");
  assert(status.ok());

  std::string value;
  status = db->Get(leveldb::ReadOptions(), key, &value);
  if (status.ok()) {
    std::cout << value << std::endl;
  } else {
    std::cout << status.ToString() << std::endl;
  }

  return 0;
}
