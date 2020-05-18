#include <memory>
#include <string>

#include "grpcpp/grpcpp.h"
#include "src/common/system_logger.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"
#include "src/server/chunk_server/chunk_server_lease_service_impl.h"

using gfs::service::ChunkServerFileServiceImpl;
using gfs::service::ChunkServerLeaseServiceImpl;
using grpc::Server;
using grpc::ServerBuilder;

int main(int argc, char** argv) {
  gfs::common::SystemLogger::GetInstance().Initialize(/*program_name=*/argv[0]);

  ServerBuilder builder;

  // TODO(tugan): add support to listen on host:port based on configuration
  std::string server_address("0.0.0.0:50052");

  // Listen on the given address without any authentication mechanism for now.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  // Register synchronous services for handling clients' metadata requests
  // Note that gRPC only support providing services through via a single port.
  ChunkServerLeaseServiceImpl lease_service;
  builder.RegisterService(&lease_service);

  ChunkServerFileServiceImpl file_service;
  builder.RegisterService(&file_service);

  // Assemble and start the server
  std::unique_ptr<Server> server(builder.BuildAndStart());
  LOG(INFO) << "Server listening on " << server_address;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();

  return 0;
}