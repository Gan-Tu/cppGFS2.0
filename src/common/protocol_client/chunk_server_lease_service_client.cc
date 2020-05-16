#include "src/common/protocol_client/chunk_server_lease_service_client.h"

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ConvertGrpcStatusToProtobufStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ClientContext;
using protos::grpc::GrantLeaseReply;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::RevokeLeaseReply;
using protos::grpc::RevokeLeaseRequest;

namespace gfs {
namespace service {

StatusOr<GrantLeaseReply> ChunkServerLeaseServiceClient::SendRequest(
    const GrantLeaseRequest& request, ClientContext& context) {
  GrantLeaseReply reply;
  grpc::Status status = stub_->GrantLease(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}
StatusOr<GrantLeaseReply> ChunkServerLeaseServiceClient::SendRequest(
    const GrantLeaseRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<RevokeLeaseReply> ChunkServerLeaseServiceClient::SendRequest(
    const RevokeLeaseRequest& request, ClientContext& context) {
  RevokeLeaseReply reply;
  grpc::Status status = stub_->RevokeLease(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}

StatusOr<RevokeLeaseReply> ChunkServerLeaseServiceClient::SendRequest(
    const RevokeLeaseRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs