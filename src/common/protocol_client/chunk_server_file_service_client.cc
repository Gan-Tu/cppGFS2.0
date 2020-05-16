#include "src/common/protocol_client/chunk_server_file_service_client.h"

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ConvertGrpcStatusToProtobufStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ClientContext;
using protos::grpc::AdvanceFileChunkVersionReply;
using protos::grpc::AdvanceFileChunkVersionRequest;
using protos::grpc::InitFileChunkReply;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::ReadFileChunkReply;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::WriteFileChunkReply;
using protos::grpc::WriteFileChunkRequest;

namespace gfs {
namespace service {

StatusOr<InitFileChunkReply> ChunkServerFileServiceClient::SendRequest(
    const InitFileChunkRequest& request, ClientContext& context) {
  InitFileChunkReply reply;
  grpc::Status status = stub_->InitFileChunk(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}

StatusOr<InitFileChunkReply> ChunkServerFileServiceClient::SendRequest(
    const InitFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<ReadFileChunkReply> ChunkServerFileServiceClient::SendRequest(
    const ReadFileChunkRequest& request, ClientContext& context) {
  ReadFileChunkReply reply;
  grpc::Status status = stub_->ReadFileChunk(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}

StatusOr<ReadFileChunkReply> ChunkServerFileServiceClient::SendRequest(
    const ReadFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<AdvanceFileChunkVersionReply>
ChunkServerFileServiceClient::SendRequest(
    const AdvanceFileChunkVersionRequest& request, ClientContext& context) {
  AdvanceFileChunkVersionReply reply;
  grpc::Status status =
      stub_->AdvanceFileChunkVersion(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}

StatusOr<AdvanceFileChunkVersionReply>
ChunkServerFileServiceClient::SendRequest(
    const AdvanceFileChunkVersionRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<WriteFileChunkReply> ChunkServerFileServiceClient::SendRequest(
    const WriteFileChunkRequest& request, ClientContext& context) {
  WriteFileChunkReply reply;
  grpc::Status status = stub_->WriteFileChunk(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}

StatusOr<WriteFileChunkReply> ChunkServerFileServiceClient::SendRequest(
    const WriteFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs