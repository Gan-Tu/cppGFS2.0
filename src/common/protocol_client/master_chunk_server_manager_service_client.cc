#include "src/common/protocol_client/master_chunk_server_manager_service_client.h"

#include <memory>

#include "absl/status/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ConvertGrpcStatusToProtobufStatus;
using absl::Status;
using absl::StatusOr;
using grpc::ClientContext;
using protos::grpc::ReportChunkServerReply;
using protos::grpc::ReportChunkServerRequest;

namespace gfs {
namespace service {

StatusOr<ReportChunkServerReply>
MasterChunkServerManagerServiceClient::SendRequest(
    const ReportChunkServerRequest& request, ClientContext& context) {
  ReportChunkServerReply reply;
  grpc::Status status = stub_->ReportChunkServer(&context, request, &reply);

  // TODO(tugan): Replace this with ReturnStatusOrFromGrpcStatus.
  if (status.ok()) {
    return reply;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}

StatusOr<ReportChunkServerReply>
MasterChunkServerManagerServiceClient::SendRequest(
    const ReportChunkServerRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs