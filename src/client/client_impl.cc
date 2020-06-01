#include "src/common/protocol_client/grpc_client_utils.h"
#include "src/common/system_logger.h"
#include "src/client/client_impl.h"

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::OpenFileRequest;
using protos::grpc::OpenFileReply;

namespace gfs {
namespace client {

void ClientImpl::cache_file_chunk_metadata(
    const std::string& filename,
    const uint32_t chunk_index,
    const OpenFileReply& open_file_reply) {
  const std::string& chunk_handle(open_file_reply.metadata().chunk_handle());
  
  auto set_chunk_handle_status(cache_manager_->SetChunkHandle(
      filename, chunk_index, chunk_handle));
  
  if (!set_chunk_handle_status.ok()) {
    LOG(ERROR) << "CacheManager failed to set chunk handle mapping for " 
               << chunk_handle << "due to: " 
               << set_chunk_handle_status.error_message();
    return;
  }

  auto chunk_version_or(cache_manager_->GetChunkVersion(chunk_handle));
  // If this chunk version has not been cached, or the replied version is 
  // higher than the current one, we cache the version
  auto new_version(open_file_reply.metadata().version());
  if (!chunk_version_or.ok() || new_version > chunk_version_or.ValueOrDie()) {
    cache_manager_->SetChunkVersion(chunk_handle, new_version); 
  } else {
    // Falling into this block means chnk_version_or.ok() is true and the 
    // new version is less or equal than the current value
    auto cur_version(chunk_version_or.ValueOrDie());
    LOG(ERROR) << "Skip updating the version number for " << chunk_handle
               << "because the current version " << cur_version 
               << " >= " << "received " << new_version;
    return;
  }

  // Cache the chunk server location 
  CacheManager::ChunkServerLocationEntry cache_entry;
  cache_entry.primary_location = open_file_reply.metadata().primary_location();
  for (auto location : open_file_reply.metadata().locations()) {
    cache_entry.locations.emplace_back(location);
  }
  cache_manager_->SetChunkServerLocation(chunk_handle, cache_entry);
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
  common::SetClientContextDeadline(client_context, config_manager_); 

  // Issue OpenFileReply rpc and check status
  StatusOr<OpenFileReply> open_file_or(
      master_metadata_service_client_->SendRequest(
          open_file_request, client_context));

  if (!open_file_or.ok()) {
    return open_file_or.status();
  }

  // The master creates the first chunk for this file, and the client 
  // should cache the FileChunkMetadata for this chunk
  cache_file_chunk_metadata(filename, 0, open_file_or.ValueOrDie());
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
    chunk_server_service_client_[chunk_server_address] = 
        new service::ChunkServerServiceGfsClient(chunk_server_channel);
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
