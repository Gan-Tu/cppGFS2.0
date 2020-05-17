#include "src/common/protocol_client/master_metadata_service_client.h"

#include <memory>

#include "google/protobuf/empty.pb.h"
#include "google/protobuf/stubs/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ConvertGrpcStatusToProtobufStatus;
using gfs::common::utils::ReturnStatusOrFromGrpcStatus;
using google::protobuf::Empty;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ClientContext;
using protos::grpc::DeleteFileRequest;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;

namespace gfs {
namespace service {

StatusOr<OpenFileReply> MasterMetadataServiceClient::SendRequest(
    const OpenFileRequest& request, ClientContext& context) {
  OpenFileReply reply;
  grpc::Status status = stub_->OpenFile(&context, request, &reply);
  return ReturnStatusOrFromGrpcStatus(reply, status);
}

StatusOr<OpenFileReply> MasterMetadataServiceClient::SendRequest(
    const OpenFileRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

Status MasterMetadataServiceClient::SendRequest(
    const DeleteFileRequest& request, ClientContext& context) {
  google::protobuf::Empty reply;
  grpc::Status status = stub_->DeleteFile(&context, request, &reply);
  return ConvertGrpcStatusToProtobufStatus(status);
}

Status MasterMetadataServiceClient::SendRequest(
    const DeleteFileRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs