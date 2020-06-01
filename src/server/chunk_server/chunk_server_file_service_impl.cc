#include "src/server/chunk_server/chunk_server_file_service_impl.h"

#include "grpcpp/grpcpp.h"
#include "src/common/system_logger.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"

using gfs::common::utils::ConvertProtobufStatusToGrpcStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using StatusCode = google::protobuf::util::error::Code;
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
  LOG(INFO) << "Received InitFileChunkRequest:" << (*request).DebugString();
  *reply->mutable_request() = *request;

  LOG(INFO) << "Preparing redo logs for InitFileChunk of file handle: "
            << request->chunk_handle();
  // TODO: may not be necessary depending on how crash-resistent we want to be
  LOG(INFO) << "InitFileChunk redo logs prepared: " << request->chunk_handle();

  LOG(INFO) << "Trying to create the file chunk with initial version of 1: "
            << request->chunk_handle();
  Status status = file_manager_->CreateChunk(request->chunk_handle(), 1);
  if (status.ok()) {
    LOG(INFO) << "Initial empty file chunk successfully created: "
              << request->chunk_handle();
    reply->set_status(InitFileChunkReply::CREATED);
    return grpc::Status::OK;
  } else if (status.error_code() == StatusCode::ALREADY_EXISTS) {
    LOG(ERROR) << "Cannot initialize file chunk because it already exits: "
               << request->chunk_handle();
    reply->set_status(InitFileChunkReply::ALREADY_EXISTS);
    return grpc::Status::OK;
  } else {
    LOG(ERROR) << "Unexpected error when initializing file chunk: " << status;
    return ConvertProtobufStatusToGrpcStatus(status);
  }
}

grpc::Status ChunkServerFileServiceImpl::ReadFileChunk(
    ServerContext* context, const ReadFileChunkRequest* request,
    ReadFileChunkReply* reply) {
  // TODO(someone): implement the GFS chunk server logic here
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "needs implementation");
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
