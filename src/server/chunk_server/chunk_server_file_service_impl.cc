#include "src/server/chunk_server/chunk_server_file_service_impl.h"

#include "grpcpp/grpcpp.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/server/chunk_server/file_chunk_manager.h"

using grpc::ServerContext;
using protos::grpc::AdvanceFileChunkVersionReply;
using protos::grpc::AdvanceFileChunkVersionRequest;
using protos::grpc::ApplyMutationsReply;
using protos::grpc::ApplyMutationsRequest;
using protos::grpc::InitFileChunkReply;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::ReadFileChunkReply;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::SendChunkDataReply;
using protos::grpc::SendChunkDataRequest;
using protos::grpc::WriteFileChunkReply;
using protos::grpc::WriteFileChunkRequest;

namespace gfs {
namespace service {

grpc::Status ChunkServerFileServiceImpl::InitFileChunk(
    ServerContext* context, const InitFileChunkRequest* request,
    InitFileChunkReply* reply) {
  // TODO(someone): implement the GFS chunk server logic here
  *reply->mutable_request() = *request;
  reply->set_status(InitFileChunkReply::ALREADY_EXISTS);
  return grpc::Status::OK;
}

grpc::Status ChunkServerFileServiceImpl::ReadFileChunk(
    ServerContext* context, const ReadFileChunkRequest* request,
    ReadFileChunkReply* reply) {
  // TODO(Gan): implement the GFS chunk server logic here
  // The following impl may not be as complete as it should be as Xi needs
  // to get a minimal impl here so that the client's read can be tested
  auto file_chunk_manager = server::FileChunkManager::GetInstance();
  auto read_from_chunk_or(file_chunk_manager->ReadFromChunk(
                              request->chunk_handle(), request->chunk_version(),
                              request->offset_start(), request->length()));
  if (!read_from_chunk_or.ok()) {
    return common::utils::ConvertProtobufStatusToGrpcStatus(
               read_from_chunk_or.status()); 
  }
  
  auto read_data(read_from_chunk_or.ValueOrDie());
  reply->set_status(ReadFileChunkReply::OK);
  reply->set_data(read_data);
  reply->set_bytes_read(read_data.size());

  return grpc::Status::OK;
}

grpc::Status ChunkServerFileServiceImpl::WriteFileChunk(
    ServerContext* context, const WriteFileChunkRequest* request,
    WriteFileChunkReply* reply) {
  // TODO(someone): implement the GFS chunk server logic here
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
}

grpc::Status ChunkServerFileServiceImpl::AdvanceFileChunkVersion(
    ServerContext* context, const AdvanceFileChunkVersionRequest* request,
    AdvanceFileChunkVersionReply* reply) {
  // TODO(someone): implement the GFS chunk server logic here
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
}

grpc::Status ChunkServerFileServiceImpl::ApplyMutations(
    grpc::ServerContext* context,
    const protos::grpc::ApplyMutationsRequest* request,
    protos::grpc::ApplyMutationsReply* reply) {
  // TODO(someone): implement the GFS chunk server logic here
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
}

grpc::Status ChunkServerFileServiceImpl::SendChunkData(
    grpc::ServerContext* context,
    const protos::grpc::SendChunkDataRequest* request,
    protos::grpc::SendChunkDataReply* reply) {
  // TODO(someone): implement the GFS chunk server logic here
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
}

}  // namespace service
}  // namespace gfs
