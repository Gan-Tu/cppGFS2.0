#include "src/client/client_impl.h"
#include "src/protos/grpc/master_metadata_service.grpc.pb.h"

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::OpenFileRequest;
using protos::grpc::OpenFileReply;

namespace gfs {
namespace client {

inline void ClientImpl::SetClientContextDeadline(
    grpc::ClientContext& client_context) {
  absl::Duration grpc_timeout(config_manager_->GetGrpcDeadline());
  client_context.set_deadline(std::chrono::system_clock::now() + 
      std::chrono::milliseconds(absl::ToInt64Milliseconds(grpc_timeout)));
}

google::protobuf::util::Status ClientImpl::CreateFile(
    const std::string& filename) {
  OpenFileRequest open_file_request;
  // For create mode, we just set filename and mode and leave other fields
  // default
  open_file_request.set_filename(filename);
  open_file_request.set_mode(OpenFileRequest::CREATE);
 
  // Define a client context and set its deadline using the timeout value 
  // obtained from the config manager
  grpc::ClientContext client_context;  
  SetClientContextDeadline(client_context); 

  // Issue OpenFileReply rpc and check status
  StatusOr<OpenFileReply> status_or(
      master_metadata_service_client_->SendRequest(
          open_file_request, client_context));

  if (!status_or.ok()) {
    return status_or.status();
  }

  // Optionally we could have the master create the first chunk for this 
  // file, that case the client should cache the FileChunkMetadata 
  return google::protobuf::util::Status::OK;
}

ClientImpl::ClientImpl(common::ConfigManager* config_manager, 
    const std::string& master_name, const bool resolve_hostname) 
       : config_manager_(config_manager) {
  // Instantiate the CacheManager with the configured timeout
  cache_manager_ = CacheManager::ConstructCacheManager(
                       config_manager_->GetClientCacheTimeout());
   
  // Instantiate the master service client
  auto master_address(config_manager_->GetServerAddress(
                          master_name, resolve_hostname));
  auto credentials = grpc::InsecureChannelCredentials();
  auto master_channel(grpc::CreateChannel(master_address, credentials));
  master_metadata_service_client_ = new service::MasterMetadataServiceClient(
                                        master_channel); 

  // Instantiate the list of chunk service clients
  auto chunk_server_names(config_manager_->GetAllChunkServers());
  for(auto const& chunk_server_name : chunk_server_names) {
    auto chunk_server_address(config_manager_->GetServerAddress(
                                  chunk_server_name, resolve_hostname));
    auto chunk_server_channel(grpc::CreateChannel(
                                  chunk_server_address, credentials));
    chunk_server_service_client_.push_back(
        new service::ChunkServerServiceGfsClient(chunk_server_channel));
  }
}

google::protobuf::util::StatusOr<ClientImpl*> ClientImpl::ConstructClientImpl(
    const std::string& config_filename, const std::string& master_name,
    const bool resolve_hostname) {
  // Instantiate a ConfigManager with the given filename
  auto config_manager_or(common::ConfigManager::GetConfig(config_filename));
  if (!config_manager_or.ok()) {
    return config_manager_or.status();
  }
  common::ConfigManager* config_manager = config_manager_or.ValueOrDie();
  
  return new ClientImpl(config_manager, master_name, resolve_hostname);
}


} // namespace client
} // namespace gfs
