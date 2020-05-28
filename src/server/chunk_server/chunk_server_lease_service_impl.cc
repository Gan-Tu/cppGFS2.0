#include "src/server/chunk_server/chunk_server_lease_service_impl.h"

#include "grpcpp/grpcpp.h"
#include "src/common/utils.h"
#include "src/protos/grpc/chunk_server_lease_service.grpc.pb.h"

using gfs::common::utils::ConvertProtobufStatusToGrpcStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ServerContext;
using protos::grpc::GrantLeaseReply;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::RevokeLeaseReply;
using protos::grpc::RevokeLeaseRequest;

namespace gfs {
namespace service {

grpc::Status ChunkServerLeaseServiceImpl::GrantLease(
    ServerContext* context, const GrantLeaseRequest* request,
    GrantLeaseReply* reply) {
  // TODO(someone): implement the GFS chunk server logic here
  *reply->mutable_request() = *request;
  reply->set_status(GrantLeaseReply::ACCEPTED);
  return grpc::Status::OK;
}

grpc::Status ChunkServerLeaseServiceImpl::RevokeLease(
    ServerContext* context, const RevokeLeaseRequest* request,
    RevokeLeaseReply* reply) {
  *reply->mutable_request() = *request;

  StatusOr<absl::Time> current_lease_expiration_time_or =
      chunk_server_impl_->GetLeaseExpirationTime(request->chunk_handle());
  if (!current_lease_expiration_time_or.ok()) {
    if (current_lease_expiration_time_or.status().code() ==
        google::protobuf::util::error::Code::NOT_FOUND) {
      reply->set_status(RevokeLeaseReply::REJECTED_NOT_FOUND);
      return grpc::Status::OK;
    } else {
      return ConvertProtobufStatusToGrpcStatus(
          current_lease_expiration_time_or.status());
    }
  }
  absl::Time original_lease_expiration_time =
      absl::FromUnixNanos(request->original_lease_expiration_time().nanos());
  if (original_lease_expiration_time <
      current_lease_expiration_time_or.ValueOrDie()) {
    reply->set_status(RevokeLeaseReply::IGNORED_HAS_NEWER_LEASE);
  } else {
    chunk_server_impl_->RemoveLease(request->chunk_handle());
    reply->set_status(RevokeLeaseReply::REVOKED);
  }
  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gfs
