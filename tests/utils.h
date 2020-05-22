#ifndef GFS_TESTS_UTILS_H_
#define GFS_TESTS_UTILS_H_

#include <thread>
#include "src/protos/metadata.pb.h"

namespace tests {

// Helper function to join a colletion of threads and cleanup the container for
// these threads
void JoinAndClearThreads(std::vector<std::thread>& threads); 

// Helper function to compute a filename given a base string and a nested 
// level. The name scheme takes the following form:
// "/{base}0/{base}1/..../{base}{nested_level-1}"
std::string ComputeNestedFileName(const std::string& base, 
                                  const int nested_level);

// Helper function to easily construct a protos::ChunkServerLocation object
// by passing the host name and port number. In a sense, this function 
// serves as a constructor for the designated type with non-trivial
// parameters, since proto generates only a default constructor
protos::ChunkServerLocation ChunkServerLocationBuilder(
    const std::string& hostname, const uint32_t port);

// Helper function to initialize a FileChunkMetadata from data in 
// primitive std format. The location below is expressed as a 
// std::pair<string, uint32_t> where the first field is hostname 
// and the second the port number
void InitializeChunkMetadata(
         protos::FileChunkMetadata& chunk_metadata, 
         const std::string& chunk_handle, uint32_t version,
         const std::pair<std::string, uint32_t>& primary_location);

} // namespace tests

#endif // GFS_TESTS_UTILS_H_
