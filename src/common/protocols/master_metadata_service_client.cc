#include "src/common/protocols/master_metadata_service_client.h"

#include <memory>

#include "google/protobuf/empty.pb.h"
#include "google/protobuf/stubs/status.h"
#include "src/common/utils.h"

using gfs::common::utils::ConvertGrpcStatusToProtobufStatus;
using google::protobuf::Empty;
using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using grpc::ClientContext;
using protos::grpc::DeleteFileRequest;
using protos::grpc::MasterMetadataService;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;

namespace gfs {
namespace service {

StatusOr<OpenFileReply> MasterMetadataServiceClient::SendRequest(
    const OpenFileRequest& request, ClientContext& context) {
  OpenFileReply reply;
  grpc::Status status = stub_->OpenFile(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
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

  if (status.ok()) {
    return Status::OK;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}

Status MasterMetadataServiceClient::SendRequest(
    const DeleteFileRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace service
}  // namespace gfs