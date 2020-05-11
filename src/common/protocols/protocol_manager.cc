#include "src/common/protocols/protocol_manager.h"

#include <memory>

#include "google/protobuf/stubs/status.h"
#include "google/protobuf/empty.pb.h"
#include "src/common/protocols/utils.h"

using grpc::ClientContext;
using protos::grpc::MasterMetadataService;
using protos::grpc::OpenFileRequest;
using protos::grpc::OpenFileReply;
using protos::grpc::DeleteFileRequest;
using google::protobuf::Empty;
using google::protobuf::util::StatusOr;
using google::protobuf::util::Status;
using gfs::protocols::utils::ConvertGrpcStatusToProtobufStatus;

namespace gfs {
namespace protocols {

StatusOr<OpenFileReply> MasterMetadataServiceRpcProtocols::SendRequest(
    const OpenFileRequest& request, ClientContext& context) {
  OpenFileReply reply;
  grpc::Status status = stub_->OpenFile(&context, request, &reply);

  if (status.ok()) {
    return reply;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}

StatusOr<OpenFileReply> MasterMetadataServiceRpcProtocols::SendRequest(
    const OpenFileRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

Status MasterMetadataServiceRpcProtocols::SendRequest(
    const DeleteFileRequest& request, ClientContext& context) {
  google::protobuf::Empty reply;
  grpc::Status status = stub_->DeleteFile(&context, request, &reply);

  if (status.ok()) {
    return Status::OK;
  } else {
    return ConvertGrpcStatusToProtobufStatus(status);
  }
}

Status MasterMetadataServiceRpcProtocols::SendRequest(
    const DeleteFileRequest& request) {
  ClientContext default_context;
  return SendRequest(request, default_context);
}

}  // namespace protocols
}  // namespace gfs