#include "src/common/protocol_client/chunk_server_service_client_client.h"

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ReturnStatusOrFromGrpcStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ClientContext;
using protos::grpc::ReadFileChunkReply;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::WriteFileChunkReply;
using protos::grpc::WriteFileChunkRequest;

namespace gfs {
namespace service {

StatusOr<ReadFileChunkReply> ChunkServerServiceClientClient::SendRequest(
    const ReadFileChunkRequest& request, ClientContext& context) {
  ReadFileChunkReply reply;
  grpc::Status status = file_stub_->ReadFileChunk(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<WriteFileChunkReply> ChunkServerServiceClientClient::SendRequest(
    const WriteFileChunkRequest& request, ClientContext& context) {
  WriteFileChunkReply reply;
  grpc::Status status = file_stub_->WriteFileChunk(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

//
// Overloaded functions using default mutable client context
//

StatusOr<ReadFileChunkReply> ChunkServerServiceClientClient::SendRequest(
    const ReadFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<WriteFileChunkReply> ChunkServerServiceClientClient::SendRequest(
    const WriteFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs
