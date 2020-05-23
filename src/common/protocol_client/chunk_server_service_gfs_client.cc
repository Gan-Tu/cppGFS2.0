#include "src/common/protocol_client/chunk_server_service_gfs_client.h"

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ReturnStatusOrFromGrpcStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ClientContext;
using protos::grpc::ReadFileChunkReply;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::SendChunkDataReply;
using protos::grpc::SendChunkDataRequest;
using protos::grpc::WriteFileChunkReply;
using protos::grpc::WriteFileChunkRequest;

namespace gfs {
namespace service {

StatusOr<ReadFileChunkReply> ChunkServerServiceGfsClient::SendRequest(
    const ReadFileChunkRequest& request, ClientContext& context) {
  ReadFileChunkReply reply;
  grpc::Status status = file_stub_->ReadFileChunk(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<SendChunkDataReply> ChunkServerServiceGfsClient::SendRequest(
    const SendChunkDataRequest& request, ClientContext& context) {
  SendChunkDataReply reply;
  grpc::Status status = file_stub_->SendChunkData(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<WriteFileChunkReply> ChunkServerServiceGfsClient::SendRequest(
    const WriteFileChunkRequest& request, ClientContext& context) {
  WriteFileChunkReply reply;
  grpc::Status status = file_stub_->WriteFileChunk(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

//
// Overloaded functions using default mutable client context
//

StatusOr<ReadFileChunkReply> ChunkServerServiceGfsClient::SendRequest(
    const ReadFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<SendChunkDataReply> ChunkServerServiceGfsClient::SendRequest(
    const SendChunkDataRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<WriteFileChunkReply> ChunkServerServiceGfsClient::SendRequest(
    const WriteFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs
