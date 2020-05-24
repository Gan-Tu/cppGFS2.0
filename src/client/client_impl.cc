#include "src/client/client_impl.h"

namespace gfs {
namespace client {

google::protobuf::util::Status ClientImpl::CreateFile(
    const std::string& filename) {
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
