#include "src/common/protocol_client/chunk_server_service_master_client.h"

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
using protos::grpc::RevokeLeaseReply;
using protos::grpc::RevokeLeaseRequest;

namespace gfs {
namespace service {

StatusOr<GrantLeaseReply> ChunkServerServiceMasterClient::SendRequest(
    const GrantLeaseRequest& request, ClientContext& context) {
  GrantLeaseReply reply;
  grpc::Status status = lease_stub_->GrantLease(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<RevokeLeaseReply> ChunkServerServiceMasterClient::SendRequest(
    const RevokeLeaseRequest& request, ClientContext& context) {
  RevokeLeaseReply reply;
  grpc::Status status = lease_stub_->RevokeLease(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<InitFileChunkReply> ChunkServerServiceMasterClient::SendRequest(
    const InitFileChunkRequest& request, ClientContext& context) {
  InitFileChunkReply reply;
  grpc::Status status = file_stub_->InitFileChunk(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<AdvanceFileChunkVersionReply> ChunkServerServiceMasterClient::SendRequest(
    const AdvanceFileChunkVersionRequest& request, ClientContext& context) {
  AdvanceFileChunkVersionReply reply;
  grpc::Status status =
      file_stub_->AdvanceFileChunkVersion(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

//
// Overloaded functions using default mutable client context
//

StatusOr<GrantLeaseReply> ChunkServerServiceMasterClient::SendRequest(
    const GrantLeaseRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<RevokeLeaseReply> ChunkServerServiceMasterClient::SendRequest(
    const RevokeLeaseRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<InitFileChunkReply> ChunkServerServiceMasterClient::SendRequest(
    const InitFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<AdvanceFileChunkVersionReply> ChunkServerServiceMasterClient::SendRequest(
    const AdvanceFileChunkVersionRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs
