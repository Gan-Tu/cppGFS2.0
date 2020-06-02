#include "src/server/chunk_server/chunk_server_file_service_impl.h"

#include "grpcpp/grpcpp.h"
#include "src/common/system_logger.h"
#include "src/protos/grpc/chunk_server_file_service.grpc.pb.h"
#include "src/server/chunk_server/file_chunk_manager.h"

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

  LOG(INFO) << "Preparing redo/undo logs for InitFileChunk of file handle: "
            << request->chunk_handle();
  // TODO: may not be necessary depending on how crash-resistent we want to be
  LOG(INFO) << "InitFileChunk redo/undo logs prepared: "
            << request->chunk_handle();

  LOG(INFO) << "Trying to create the file chunk with initial version of 1: "
            << request->chunk_handle();
  Status status = file_manager_->CreateChunk(request->chunk_handle(), 1);
  if (status.ok()) {
    // SUCCESS
    LOG(INFO) << "Initial empty file chunk successfully created: "
              << request->chunk_handle();
    reply->set_status(InitFileChunkReply::CREATED);
    return grpc::Status::OK;
  } else if (status.error_code() == StatusCode::ALREADY_EXISTS) {
    // ALREADY EXISTS
    LOG(ERROR) << "Cannot initialize file chunk because it already exits: "
               << request->chunk_handle();
    reply->set_status(InitFileChunkReply::ALREADY_EXISTS);
    return grpc::Status::OK;
  } else {
    // INTERNAL ERROR
    LOG(ERROR) << "Unexpected error when initializing file chunk: " << status;
    return ConvertProtobufStatusToGrpcStatus(status);
  }
}

grpc::Status ChunkServerFileServiceImpl::ReadFileChunk(
    ServerContext* context, const ReadFileChunkRequest* request,
    ReadFileChunkReply* reply) {
  LOG(INFO) << "Received ReadFileChunkRequest:" << (*request).DebugString();
  *reply->mutable_request() = *request;

  // No redo/undo logs needed for reads, since there are no state changes

  LOG(INFO) << "Trying to read the file chunk " << request->chunk_handle()
            << " of version " << request->chunk_version() << " from offset "
            << request->offset_start() << " for " << request->length()
            << " bytes";

  // Read file chunk
  StatusOr<std::string> data_or = file_manager_->ReadFromChunk(
      request->chunk_handle(), request->chunk_version(),
      request->offset_start(), request->length());

  if (data_or.ok()) {
    // SUCCESS
    reply->set_status(ReadFileChunkReply::OK);
    reply->set_data(data_or.ValueOrDie());
    reply->set_bytes_read(reply->data().length());
    LOG(INFO) << "Successfully read " << reply->bytes_read() << " bytes of "
              << request->chunk_handle();
    return grpc::Status::OK;
  } else if (data_or.status().error_code() == StatusCode::OUT_OF_RANGE) {
    // OUT OF RANGE
    LOG(ERROR) << "Cannot read file chunk because the requested offset is out"
               << " of range for " << request->chunk_handle() << ": "
               << data_or.status().ToString();
    reply->set_status(ReadFileChunkReply::FAILED_OUT_OF_RANGE);
    return grpc::Status::OK;
  } else if (data_or.status().error_code() == StatusCode::NOT_FOUND) {
    StatusOr<uint32_t> version_or =
        file_manager_->GetChunkVersion(request->chunk_handle());
    if (!version_or.ok()) {
      // NOT FOUND: file handle
      if (version_or.status().error_code() == StatusCode::NOT_FOUND) {
        LOG(ERROR) << "Cannot read file chunk because it is not found: "
                   << request->chunk_handle();
        reply->set_status(ReadFileChunkReply::FAILED_NOT_FOUND);
        return grpc::Status::OK;
      } else {
        // INTERNAL ERROR
        LOG(ERROR) << "Unexpected error when reading the file chunk "
                   << request->chunk_handle()
                   << " of version: " << version_or.status();
        return ConvertProtobufStatusToGrpcStatus(version_or.status());
      }
    } else {
      // NOT FOUND: stale version
      LOG(ERROR) << "Cannot read file chunk because it is stale: "
                 << request->chunk_handle();
      LOG(ERROR) << "Chunk server has version " << version_or.ValueOrDie()
                 << " but a different version is requested: "
                 << request->chunk_version();
      reply->set_status(ReadFileChunkReply::FAILED_VERSION_OUT_OF_SYNC);
      return grpc::Status::OK;
    }
  } else {
    // INTERNAL ERROR
    LOG(ERROR) << "Unexpected error when initializing file chunk: "
               << data_or.status();
    return ConvertProtobufStatusToGrpcStatus(data_or.status());
  }
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
  LOG(INFO) << "Received AdvanceFileChunkVersion:" << (*request).DebugString();
  *reply->mutable_request() = *request;

  // Per AdvanceFileVersion gRPC definition, we only advance version by one
  const uint32_t from_version = request->new_chunk_version() - 1;
  LOG(INFO) << "Trying to advance the version of file chunk "
            << request->chunk_handle() << " from version " << from_version
            << " to version " << request->new_chunk_version();
  Status status = file_manager_->UpdateChunkVersion(
      request->chunk_handle(), from_version, request->new_chunk_version());

  if (status.ok()) {
    // SUCCESS
    LOG(INFO) << "Successfully updated file chunk " << request->chunk_handle()
              << " to version " << request->new_chunk_version();
    reply->set_status(AdvanceFileChunkVersionReply::OK);
    reply->set_chunk_version(request->new_chunk_version());
    return grpc::Status::OK;
  } else if (status.error_code() == StatusCode::NOT_FOUND) {
    StatusOr<uint32_t> version_or =
        file_manager_->GetChunkVersion(request->chunk_handle());
    if (!version_or.ok()) {
      // NOT FOUND: file handle
      if (version_or.status().error_code() == StatusCode::NOT_FOUND) {
        LOG(ERROR)
            << "Cannot advance file chunk version because it is not found: "
            << request->chunk_handle();
        reply->set_status(AdvanceFileChunkVersionReply::FAILED_NOT_FOUND);
        return grpc::Status::OK;
      } else {
        // INTERNAL ERROR
        LOG(ERROR) << "Unexpected error when reading the file chunk "
                   << request->chunk_handle()
                   << " of version: " << version_or.status();
        return ConvertProtobufStatusToGrpcStatus(version_or.status());
      }
    } else {
      // NOT FOUND: stale version
      LOG(ERROR) << "Cannot advance file chunk, because version is out of sync "
                 << request->chunk_handle();
      LOG(ERROR) << "Chunk server has version " << version_or.ValueOrDie()
                 << " but the request tries to update version from "
                 << from_version << " to " << request->new_chunk_version();
      reply->set_status(
          AdvanceFileChunkVersionReply::FAILED_VERSION_OUT_OF_SYNC);
      return grpc::Status::OK;
    }
  } else {
    // INTERNAL ERROR
    LOG(ERROR) << "Unexpected error when advancing chunk version: " << status;
    return ConvertProtobufStatusToGrpcStatus(status);
  }
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
