#include "src/common/protocol_client/chunk_server_control_service_client.h"

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ReturnStatusOrFromGrpcStatus;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ClientContext;
using protos::grpc::CheckHeartBeatReply;
using protos::grpc::CheckHeartBeatRequest;
using protos::grpc::TriggerReportChunkServerReply;
using protos::grpc::TriggerReportChunkServerRequest;

namespace gfs {
namespace service {

StatusOr<CheckHeartBeatReply> ChunkServerControlServiceClient::SendRequest(
    const CheckHeartBeatRequest& request, ClientContext& context) {
  CheckHeartBeatReply reply;
  grpc::Status status = stub_->CheckHeartBeat(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<TriggerReportChunkServerReply>
ChunkServerControlServiceClient::SendRequest(
    const TriggerReportChunkServerRequest& request, ClientContext& context) {
  TriggerReportChunkServerReply reply;
  grpc::Status status =
      stub_->TriggerReportChunkServer(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

//
// Overloaded functions using default mutable client context
//

StatusOr<CheckHeartBeatReply> ChunkServerControlServiceClient::SendRequest(
    const CheckHeartBeatRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

StatusOr<TriggerReportChunkServerReply>
ChunkServerControlServiceClient::SendRequest(
    const TriggerReportChunkServerRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs
