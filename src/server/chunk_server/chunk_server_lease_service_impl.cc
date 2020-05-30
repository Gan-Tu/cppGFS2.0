#include "src/server/chunk_server/chunk_server_lease_service_impl.h"

#include "absl/time/time.h"
#include "grpcpp/grpcpp.h"
#include "src/common/system_logger.h"
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
  LOG(INFO) << "Received GrantLeaseRequest:" << (*request).DebugString();
  *reply->mutable_request() = *request;

  LOG(INFO) << "Preparing redo logs for GrantLease of file handle: "
            << request->chunk_handle();
  // TODO: may not be necessary depending on how crash-resistent we want to be
  LOG(INFO) << "GrantLease redo logs prepared: " << request->chunk_handle();

  LOG(INFO) << "Validating GrantLeaseRequest for: " << request->chunk_handle();
  StatusOr<uint32_t> owned_version_or =
      chunk_server_impl_->GetChunkVersion(request->chunk_handle());
  if (!owned_version_or.ok()) {
    if (owned_version_or.status().code() ==
        google::protobuf::util::error::Code::NOT_FOUND) {
      LOG(INFO) << "Cannot accept lease because " << request->chunk_handle()
                << " doesn't exit on this chunk server";
      reply->set_status(GrantLeaseReply::REJECTED_NOT_FOUND);
      return grpc::Status::OK;
    } else {
      LOG(ERROR) << "Unexpected error when receiving lease: "
                 << owned_version_or.status();
      return ConvertProtobufStatusToGrpcStatus(owned_version_or.status());
    }
  } else if (owned_version_or.ValueOrDie() < request->chunk_version()) {
    LOG(INFO) << "Cannot accept lease because " << request->chunk_handle()
              << " is stale on this chunk server";
    reply->set_status(GrantLeaseReply::REJECTED_STALE_VERSION);
    return grpc::Status::OK;
  } else if (owned_version_or.ValueOrDie() > request->chunk_version()) {
    LOG(INFO) << "Cannot accept lease because " << request->chunk_handle()
              << " has a newer version on this chunk server";
    reply->set_status(GrantLeaseReply::UNKNOWN);
    return grpc::Status::OK;
  } else if (absl::Now() > absl::FromUnixSeconds(
                               request->lease_expiration_time().seconds())) {
    LOG(INFO) << "Cannot accept lease for " << request->chunk_handle()
              << " because the given lease is already expired";
    reply->set_status(GrantLeaseReply::IGNORED_EXPIRED_LEASE);
    return grpc::Status::OK;
  } else {
    chunk_server_impl_->AddOrUpdateLease(
        request->chunk_handle(), request->lease_expiration_time().seconds());
    LOG(INFO) << "New lease accepted for " << request->chunk_handle();
    reply->set_status(GrantLeaseReply::ACCEPTED);
    return grpc::Status::OK;
  }
}

grpc::Status ChunkServerLeaseServiceImpl::RevokeLease(
    ServerContext* context, const RevokeLeaseRequest* request,
    RevokeLeaseReply* reply) {
  LOG(INFO) << "Received RevokeLeaseRequest:" << (*request).DebugString();
  *reply->mutable_request() = *request;

  LOG(INFO) << "Preparing redo logs for RevokeLease of file handle: "
            << request->chunk_handle();
  // TODO: may not be necessary depending on how crash-resistent we want to be
  LOG(INFO) << "RevokeLease redo logs prepared: " << request->chunk_handle();

  LOG(INFO) << "Validating RevokeLeaseRequest for: " << request->chunk_handle();
  StatusOr<absl::Time> current_lease_expiration_time_or =
      chunk_server_impl_->GetLeaseExpirationTime(request->chunk_handle());
  if (!current_lease_expiration_time_or.ok()) {
    if (current_lease_expiration_time_or.status().code() ==
        google::protobuf::util::error::Code::NOT_FOUND) {
      LOG(INFO) << "No lease to revoke for " << request->chunk_handle();
      reply->set_status(RevokeLeaseReply::REJECTED_NOT_FOUND);
      return grpc::Status::OK;
    } else {
      LOG(ERROR) << "Unexpected error when revoking lease: "
                 << current_lease_expiration_time_or.status();
      return ConvertProtobufStatusToGrpcStatus(
          current_lease_expiration_time_or.status());
    }
  }
  absl::Time original_lease_expiration_time = absl::FromUnixSeconds(
      request->original_lease_expiration_time().seconds());
  if (original_lease_expiration_time <
      current_lease_expiration_time_or.ValueOrDie()) {
    LOG(INFO) << "Server already holds a newer lease that expires in future: "
              << request->chunk_handle();
    reply->set_status(RevokeLeaseReply::IGNORED_HAS_NEWER_LEASE);
  } else {
    chunk_server_impl_->RemoveLease(request->chunk_handle());
    LOG(INFO) << "Lease successfully revoked for " << request->chunk_handle();
    reply->set_status(RevokeLeaseReply::REVOKED);
  }
  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gfs
