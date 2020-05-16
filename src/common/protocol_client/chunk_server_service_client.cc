#include "src/common/protocol_client/chunk_server_service_client.h"

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ReturnStatusOrFromGrpcStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ClientContext;
using protos::grpc::AdvanceFileChunkVersionReply;
using protos::grpc::AdvanceFileChunkVersionRequest;
using protos::grpc::GrantLeaseReply;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::InitFileChunkReply;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::ReadFileChunkReply;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::RevokeLeaseReply;
using protos::grpc::RevokeLeaseRequest;
using protos::grpc::WriteFileChunkReply;
using protos::grpc::WriteFileChunkRequest;

namespace gfs {
namespace service {

StatusOr<GrantLeaseReply> ChunkServerServiceClient::SendRequest(
    const GrantLeaseRequest& request, ClientContext& context) {
  GrantLeaseReply reply;
  grpc::Status status = lease_stub_->GrantLease(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<GrantLeaseReply> ChunkServerServiceClient::SendRequest(
    const GrantLeaseRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<RevokeLeaseReply> ChunkServerServiceClient::SendRequest(
    const RevokeLeaseRequest& request, ClientContext& context) {
  RevokeLeaseReply reply;
  grpc::Status status = lease_stub_->RevokeLease(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<RevokeLeaseReply> ChunkServerServiceClient::SendRequest(
    const RevokeLeaseRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<InitFileChunkReply> ChunkServerServiceClient::SendRequest(
    const InitFileChunkRequest& request, ClientContext& context) {
  InitFileChunkReply reply;
  grpc::Status status = file_stub_->InitFileChunk(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<InitFileChunkReply> ChunkServerServiceClient::SendRequest(
    const InitFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<ReadFileChunkReply> ChunkServerServiceClient::SendRequest(
    const ReadFileChunkRequest& request, ClientContext& context) {
  ReadFileChunkReply reply;
  grpc::Status status = file_stub_->ReadFileChunk(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<ReadFileChunkReply> ChunkServerServiceClient::SendRequest(
    const ReadFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<AdvanceFileChunkVersionReply> ChunkServerServiceClient::SendRequest(
    const AdvanceFileChunkVersionRequest& request, ClientContext& context) {
  AdvanceFileChunkVersionReply reply;
  grpc::Status status =
      file_stub_->AdvanceFileChunkVersion(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<AdvanceFileChunkVersionReply> ChunkServerServiceClient::SendRequest(
    const AdvanceFileChunkVersionRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<WriteFileChunkReply> ChunkServerServiceClient::SendRequest(
    const WriteFileChunkRequest& request, ClientContext& context) {
  WriteFileChunkReply reply;
  grpc::Status status = file_stub_->WriteFileChunk(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<WriteFileChunkReply> ChunkServerServiceClient::SendRequest(
    const WriteFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs