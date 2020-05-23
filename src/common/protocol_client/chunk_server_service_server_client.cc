#include "src/common/protocol_client/chunk_server_service_server_client.h"

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ReturnStatusOrFromGrpcStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ClientContext;
using protos::grpc::AdvanceFileChunkVersionReply;
using protos::grpc::AdvanceFileChunkVersionRequest;
using protos::grpc::ApplyMutationsReply;
using protos::grpc::ApplyMutationsRequest;
using protos::grpc::GrantLeaseReply;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::InitFileChunkReply;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::RevokeLeaseReply;
using protos::grpc::RevokeLeaseRequest;

namespace gfs {
namespace service {

StatusOr<GrantLeaseReply> ChunkServerServiceMasterServerClient::SendRequest(
    const GrantLeaseRequest& request, ClientContext& context) {
  GrantLeaseReply reply;
  grpc::Status status = lease_stub_->GrantLease(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<RevokeLeaseReply> ChunkServerServiceMasterServerClient::SendRequest(
    const RevokeLeaseRequest& request, ClientContext& context) {
  RevokeLeaseReply reply;
  grpc::Status status = lease_stub_->RevokeLease(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<InitFileChunkReply> ChunkServerServiceMasterServerClient::SendRequest(
    const InitFileChunkRequest& request, ClientContext& context) {
  InitFileChunkReply reply;
  grpc::Status status = file_stub_->InitFileChunk(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<AdvanceFileChunkVersionReply>
ChunkServerServiceMasterServerClient::SendRequest(
    const AdvanceFileChunkVersionRequest& request, ClientContext& context) {
  AdvanceFileChunkVersionReply reply;
  grpc::Status status =
      file_stub_->AdvanceFileChunkVersion(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<ApplyMutationsReply> ChunkServerServiceChunkServerClient::SendRequest(
    const ApplyMutationsRequest& request, ClientContext& context) {
  ApplyMutationsReply reply;
  grpc::Status status = file_stub_->ApplyMutations(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

//
// Overloaded functions using default mutable client context
//

StatusOr<GrantLeaseReply> ChunkServerServiceMasterServerClient::SendRequest(
    const GrantLeaseRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<RevokeLeaseReply> ChunkServerServiceMasterServerClient::SendRequest(
    const RevokeLeaseRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<InitFileChunkReply> ChunkServerServiceMasterServerClient::SendRequest(
    const InitFileChunkRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<AdvanceFileChunkVersionReply>
ChunkServerServiceMasterServerClient::SendRequest(
    const AdvanceFileChunkVersionRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs
