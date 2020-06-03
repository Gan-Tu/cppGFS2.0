#include <thread>
#include <vector>
#include "src/client/client_impl.h"

#include "src/common/protocol_client/grpc_client_utils.h"
#include "src/common/system_logger.h"
#include "src/common/utils.h"

using google::protobuf::util::Status;
using google::protobuf::util::StatusOr;
using protos::grpc::FileChunkMutationStatus;
using protos::grpc::OpenFileReply;
using protos::grpc::OpenFileRequest;
using protos::grpc::ReadFileChunkReply;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::SendChunkDataReply;
using protos::grpc::SendChunkDataRequest;
using protos::grpc::WriteFileChunkReply;
using protos::grpc::WriteFileChunkRequest;

namespace gfs {
namespace client {

void ClientImpl::cache_file_chunk_metadata(
    const std::string& filename, const uint32_t chunk_index,
    const OpenFileReply& open_file_reply) {
  const std::string& chunk_handle(open_file_reply.metadata().chunk_handle());

  auto set_chunk_handle_status(
      cache_manager_->SetChunkHandle(filename, chunk_index, chunk_handle));

  if (!set_chunk_handle_status.ok()) {
    LOG(ERROR) << "CacheManager failed to set chunk handle mapping for "
               << chunk_handle
               << "due to: " << set_chunk_handle_status.error_message();
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
               << "because the current version " << cur_version << " >= "
               << "received " << new_version;
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
      master_metadata_service_client_->SendRequest(open_file_request,
                                                   client_context));

  if (!open_file_or.ok()) {
    return open_file_or.status();
  }

  // The master creates the first chunk for this file, and the client
  // should cache the FileChunkMetadata for this chunk
  cache_file_chunk_metadata(filename, 0, open_file_or.ValueOrDie());
  return google::protobuf::util::Status::OK;
}

google::protobuf::util::Status ClientImpl::GetMetadataForChunk(
    const char* filename, size_t chunk_index, 
    OpenFileRequest::OpenMode file_open_mode, std::string& chunk_handle, 
    uint32_t& chunk_version, 
    CacheManager::ChunkServerLocationEntry& chunk_server_location_entry,
    bool force_get) {
  // First check if the cache manager has file chunk metadata for this chunk
  bool file_chunk_metadata_in_cache(true);

  // Make sure that chunk_handle, version and location info are all there,
  // otherwise we'd have to call OpenFileRequest
  auto chunk_handle_or(cache_manager_->GetChunkHandle(filename, chunk_index));
  if (chunk_handle_or.ok()) {
    chunk_handle = chunk_handle_or.ValueOrDie();
    auto chunk_version_or(cache_manager_->GetChunkVersion(chunk_handle));
    if (chunk_version_or.ok()) {
      chunk_version = chunk_version_or.ValueOrDie();
      auto chunk_server_location_entry_or(
          cache_manager_->GetChunkServerLocation(chunk_handle));
      if (chunk_server_location_entry_or.ok()) {
        chunk_server_location_entry =
            chunk_server_location_entry_or.ValueOrDie();
      } else {
        file_chunk_metadata_in_cache = false;
      }
    } else {
      file_chunk_metadata_in_cache = false;
    }
  } else {
    file_chunk_metadata_in_cache = false;
  }

  // If file chunk metadata is not available, issue an OpenFileRequest to get
  // all information needed
  if (!file_chunk_metadata_in_cache || force_get) {
    // TODO(Xi): refactor this block into a helper function
    // Prepare the open file request
    OpenFileRequest open_file_request;
    open_file_request.set_filename(filename);
    open_file_request.set_chunk_index(chunk_index);
    open_file_request.set_mode(file_open_mode);
    // For WRITE request, we set create_if_not_exists to be true so we would
    // create chunk for write if it does not exist
    open_file_request.set_create_if_not_exists(
        file_open_mode==OpenFileRequest::WRITE);                                       
    grpc::ClientContext client_context;
    common::SetClientContextDeadline(client_context, config_manager_);

    LOG(INFO) << "Issuing OpenFilRequest to file " << filename
              << " at chunk index " << chunk_index << " with open mode "
              << file_open_mode;

    // Issue OpenFileReply rpc and check stastus
    StatusOr<OpenFileReply> open_file_or(
        master_metadata_service_client_->SendRequest(open_file_request,
                                                     client_context));

    // Handle error
    if (!open_file_or.ok()) {
      LOG(ERROR) << "OpenFileRequest failed due to "
                 << open_file_or.status().error_message();
      return open_file_or.status();
    }

    // Cache file chunk metadata
    auto open_file_reply(open_file_or.ValueOrDie());
    cache_file_chunk_metadata(filename, chunk_index, open_file_reply);

    chunk_handle =
        cache_manager_->GetChunkHandle(filename, chunk_index).ValueOrDie();
    chunk_version = cache_manager_->GetChunkVersion(chunk_handle).ValueOrDie();
    chunk_server_location_entry =
        cache_manager_->GetChunkServerLocation(chunk_handle).ValueOrDie();
  }

  // If no locations are there, we need to return error
  if (chunk_server_location_entry.locations.empty()) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::UNAVAILABLE,
        "No chunk server has been found");
  }

  return google::protobuf::util::Status::OK;
}

google::protobuf::util::StatusOr<ReadFileChunkReply> ClientImpl::ReadFileChunk(
    const char* filename, size_t chunk_index, size_t offset, size_t nbytes) {
  std::string chunk_handle;
  uint32_t chunk_version;
  CacheManager::ChunkServerLocationEntry chunk_server_location_entry;
  
  // Access the metadata first
  auto get_metadata_status(
      GetMetadataForChunk(filename, chunk_index, OpenFileRequest::READ,
                          chunk_handle, chunk_version, 
                          chunk_server_location_entry));

  if (!get_metadata_status.ok()) {
    return get_metadata_status;
  }

  // Loop over all server locations but return immediately as long as one
  // read request is successful
  for (auto& location : chunk_server_location_entry.locations) {
    // Prepare for the read file chunk request
    ReadFileChunkRequest read_file_chunk_request;
    read_file_chunk_request.set_chunk_handle(chunk_handle);
    read_file_chunk_request.set_chunk_version(chunk_version);
    read_file_chunk_request.set_offset_start(offset);
    read_file_chunk_request.set_length(nbytes);
    // Access the client-end-point for contacting the chunk server
    auto server_address(location.server_hostname() + ":" +
                        std::to_string(location.server_port()));
    // Prepare the client context
    grpc::ClientContext client_context;
    common::SetClientContextDeadline(client_context, config_manager_);
    // Create a client end point if it does not exist, this can happen if
    // a new chunk server joins
    auto chunk_server_service_client(
        GetChunkServerServiceClient(server_address));

    // Issue ReadFileChunkRequest and check status
    StatusOr<ReadFileChunkReply> read_file_chunk_reply_or(
        chunk_server_service_client->SendRequest(read_file_chunk_request,
                                                 client_context));

    // Handle grpc error, log and continue to try the next chunk server
    if (!read_file_chunk_reply_or.ok()) {
      LOG(ERROR) << "Read file chunk " << chunk_handle << " from "
                 << server_address << " failed due to "
                 << read_file_chunk_reply_or.status().error_message();
      continue;
    }

    // Handle chunk status error, log and continue
    auto read_file_chunk_reply(read_file_chunk_reply_or.ValueOrDie());
    switch (read_file_chunk_reply.status()) {
      case ReadFileChunkReply::UNKNOWN:
        LOG(ERROR) << "Unknown error while reading " + chunk_handle;
        continue;
      case ReadFileChunkReply::FAILED_NOT_FOUND:
        LOG(ERROR) << "Chunk not found: " + chunk_handle;
        continue;
      case ReadFileChunkReply::FAILED_VERSION_OUT_OF_SYNC:
        LOG(ERROR) << "Version is out of sync " << std::to_string(chunk_version)
                   << " for chunk " + chunk_handle;
        continue;
      case ReadFileChunkReply::FAILED_OUT_OF_RANGE:
        LOG(ERROR) << "Trying to read " << chunk_handle
                   << " out of range:  " << offset;
        continue;
      default:
        break;
    }

    // This read has been successful, return here. We expect the compiler to
    // execute copy elision here to "move" the read reply. (It is tempting
    // to explicitly write std::move(read_file_chunk_reply) but that is not
    // the right approach to do so).
    return read_file_chunk_reply;
  }

  // Failed to read from all chunk servers
  return google::protobuf::util::Status(
      google::protobuf::util::error::INTERNAL,
      "Failed to read from all chunk servers for " + chunk_handle);
}

google::protobuf::util::StatusOr<std::pair<size_t, void*>> ClientImpl::ReadFile(
    const char* filename, size_t offset, size_t nbytes) {
  const size_t chunk_block_size(config_manager_->GetFileChunkBlockSize() *
                                common::bytesPerMb);
  // Record the number of bytes that we already read
  size_t bytes_read(0);
  // Record the number of bytes to read
  size_t remain_bytes(nbytes);
  // A flag to detect EOF
  bool eof(false);
  // A variable that keeps track of the start_offset for each chunk. This is
  // equal to offset % chunk_block_size for the first chunk and 0 for the
  // following chunks
  size_t chunk_start_offset(offset % chunk_block_size);

  // We simply allocate the necessary size for the buffer. There is a
  // possibility that we may not read up to nbytes, but it is ok to pass
  // this optimization opportunity as it may cost more to realloc
  void* buffer(malloc(nbytes));
  if (!buffer) {
    return google::protobuf::util::Status(
        google::protobuf::util::error::RESOURCE_EXHAUSTED,
        "Not enough memory: malloc fails");
  }

  for (size_t chunk_index = offset / chunk_block_size; remain_bytes > 0 && !eof;
       chunk_index++) {
    // Calculate the bytes to be read, which is the min value of the remaining
    // bytes and the chunk size
    size_t bytes_to_read(std::min(remain_bytes, chunk_block_size));
    auto file_chunk_data_or(ReadFileChunk(filename, chunk_index,
                                          chunk_start_offset, bytes_to_read));
    // If one of the chunk's read fails, free the buffer and return
    // TODO(someone): there is a corner case here if the EOF happens to be
    // the last byte of a chunk, then if we read the next chunk we ended up
    // with reading pass EOF. A fix for this is to include a Status in
    // ReadFileChunkReply to indicate an EOF
    if (!file_chunk_data_or.ok()) {
      free(buffer);
      return file_chunk_data_or.status();
    }

    // Concatenate the read data to buffer
    size_t chunk_bytes_read(file_chunk_data_or.ValueOrDie().bytes_read());
    const void* chunk_buffer_read(
        file_chunk_data_or.ValueOrDie().data().c_str());
    memmove((char*)buffer + bytes_read, chunk_buffer_read, chunk_bytes_read);

    // Update chunk_start_offset,  bytes_read and remain_bytes counts
    // Starting from the second chunk, chunk_start_offset is zero
    chunk_start_offset = 0;
    bytes_read += chunk_bytes_read;
    remain_bytes -= chunk_bytes_read;

    // Detect EOF if the actual bytes read < bytes-to-read
    // TODO(someone): maybe we should include a Status in the
    // ReadFileChunkReply to indicate an EOF
    eof = chunk_bytes_read < bytes_to_read;
  }

  return std::make_pair(bytes_read, buffer);
}

google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkReply>
    ClientImpl::WriteFileChunk(const char* filename, void* buffer, 
                               size_t chunk_index, size_t offset, 
                               size_t nbytes) {
  std::string chunk_handle;
  uint32_t chunk_version;
  CacheManager::ChunkServerLocationEntry chunk_server_location_entry;
  
  // Access the metadata first
  auto get_metadata_status(
      GetMetadataForChunk(filename, chunk_index, OpenFileRequest::WRITE,
                          chunk_handle, chunk_version, 
                          chunk_server_location_entry));

  if (!get_metadata_status.ok()) {
    return get_metadata_status;
  }

  // By default we retry three times
  ushort retry(3);
  // Prepare the data to be sent
  const std::string data_to_send(std::string((const char*)buffer, nbytes));
  // Compute the checksum
  const std::string data_checksum(common::utils::calc_checksum(data_to_send));

  while (retry > 0) {
    // Push the data to all replica. For simplicity, we just push all data 
    // from client to replica instead of the way described in the paper. 
    // Furthermore, if any of the push request fails, we retry (and we retry
    // with all replica). In terms of retry policy, we only retry if the 
    // status says "FAILED", because "DATA_TOO_BIG" is not a retry-able error
    // and "RATE_LIMITED" is not used for now.
    std::atomic<bool> send_data_recoverable_error, 
                          send_data_irrecoverable_error; 
    std::vector<std::thread> send_data_threads;
    for (auto& location : chunk_server_location_entry.locations) {
      send_data_threads.push_back(std::thread([&, location]() {
        // Prepare the SendChunkDataRequest
        SendChunkDataRequest send_chunk_data_request;
        send_chunk_data_request.set_data(data_to_send);
        send_chunk_data_request.set_checksum(data_checksum);
        auto server_address(location.server_hostname() + ":" + 
                            std::to_string(location.server_port()));
        // Prepare the client context
        grpc::ClientContext client_context;
        common::SetClientContextDeadline(client_context, config_manager_);
        // Get the client end-point
        auto chunk_server_service_client(
            GetChunkServerServiceClient(server_address));
        // Issue SendChunkData request and check status
        StatusOr<SendChunkDataReply> send_chunk_reply_or(
            chunk_server_service_client->SendRequest(send_chunk_data_request,
                                                     client_context));
        // Handle grpc error, log and set retry flag
        if (!send_chunk_reply_or.ok()) {
          // TODO(Xi): Not sure if we should consider an grpc error as 
          // irrecoverable, but play safe here to retry
          LOG(ERROR) << "Send file chunk data to " << server_address
                     << "failed due to " << send_chunk_reply_or.status();
          send_data_recoverable_error.store(true);
        } else {
          auto send_chunk_reply(send_chunk_reply_or.ValueOrDie());
          switch (send_chunk_reply.status()) {
            case SendChunkDataReply::OK: 
              LOG(INFO) << "Send file chunk data to " << server_address 
                        << "succeeds";
              break; // Good
            case SendChunkDataReply::FAILED:
              // We consider this a retry-able condition
              send_data_recoverable_error.store(true);
              LOG(ERROR) << "Send file chunk data to " << server_address
                         << "encountered recoverable failure" 
                         << send_chunk_reply.status();
              break;
            default:
              // All others are not retryable 
              send_data_irrecoverable_error.store(true);
              LOG(ERROR) << "Send file chunk data to " << server_address
                         << "encountered irrecoverable failure" 
                         << send_chunk_reply.status();
          }
        }
      }));
    }

    // Join all threads above
    for (auto& send_data_thread : send_data_threads) {
      send_data_thread.join();
    }

    // If we countered irrecoverable failure, we have to abort here and return
    // error. If we encountered recoverable failure, we retry. Otherwise, we 
    // continue to the next phase of operation
    if (send_data_irrecoverable_error.load()) {
      // TODO(Xi): ideally return the actual status from send reply but as we
      // are using multi-threading to send data this is a little tricky. Due
      // to time constraint, simply use an internal error indicating that 
      return google::protobuf::util::Status(
          google::protobuf::util::error::INTERNAL,
          "Send data encountered irrecoverable failure");
    }

    if (send_data_recoverable_error.load()) {
      retry--;
      continue;
    }

    // Prepare WriteFileChunkRequest, WriteFileChunkRequestHeader
    WriteFileChunkRequest write_request;
    write_request.mutable_header()->set_chunk_handle(chunk_handle);
    write_request.mutable_header()->set_chunk_version(chunk_version);
    write_request.mutable_header()->set_offset_start(offset);
    write_request.mutable_header()->set_length(nbytes);
    write_request.mutable_header()->set_data_checksum(data_checksum);
    for (auto location : chunk_server_location_entry.locations) {
      write_request.mutable_replica_locations()->Add(std::move(location));
    }
    // Prepare the client context    
    grpc::ClientContext client_context;
    common::SetClientContextDeadline(client_context, config_manager_);
 
    // We send this WriteFileChunkRequest to the primary 
    auto primary_location(chunk_server_location_entry.primary_location);
    const std::string primary_server_address(
        primary_location.server_hostname() + ":" + 
            std::to_string(primary_location.server_port()));
    
    auto primary_server_service_client(GetChunkServerServiceClient(
                                           primary_server_address));
    // Issue WriteFileChunkRequest and check status
    StatusOr<WriteFileChunkReply> write_reply_or(
        primary_server_service_client->SendRequest(write_request, client_context));

    // Handle grpc error, and retry if possible
    if (!write_reply_or.ok()) {
      LOG(ERROR) << "Send write chunk data request to " 
                 << primary_server_address << "failed due to " 
                 << write_reply_or.status();
    } else {
      auto write_reply(write_reply_or.ValueOrDie());
      switch (write_reply.status()) {
        case FileChunkMutationStatus::OK:
          LOG(INFO) << "Write to file " << filename << " at chunk_index "
                    << chunk_index << " and offset " << offset << " for "
                    << nbytes << " bytes succeeds";
          return write_reply_or;
        case FileChunkMutationStatus::FAILED_NOT_LEASE_HOLDER:
          // Client found out that the primary it though is no longer a lease
          // holder, we need to force to get the chunk metadata and retry
          LOG(INFO) << "Retrying as primary server "
                    << primary_server_address << " is no longer lease holder "
                    << "refetching chunk metadata";
          get_metadata_status = 
              GetMetadataForChunk(filename, chunk_index, 
                                  OpenFileRequest::WRITE, chunk_handle, 
                                  chunk_version,
                                  chunk_server_location_entry, true);
          if (!get_metadata_status.ok()) {
            return get_metadata_status; 
          }
          break;
        default:
          // For other cases, we consider them to be irrecoverable and return
          // error
          LOG(ERROR) << "Write to file " << filename << " at chunk_index "
                     << chunk_index << " and offset " << offset << " for "
                     << nbytes << " byte failed due to " 
                     << write_reply.status();
          return write_reply_or;
      }
    }

    retry--;
  }

  LOG(ERROR) << "Write file chunk failed after 3 retries";
  return google::protobuf::util::Status(
             google::protobuf::util::error::INTERNAL,
                 "Write file chunk failed after 3 retries"); 
}

google::protobuf::util::Status ClientImpl::WriteFile(const char* filename, 
    void* buffer, size_t offset, size_t nbytes) {
  const size_t chunk_block_size(config_manager_->GetFileChunkBlockSize() *
                                common::bytesPerMb);
  // Record the number of bytes that we need to write
  size_t bytes_written(0);
  // Record the number of bytes to write
  size_t remain_bytes(nbytes);
  // A flag to detect errors that encountered during issuing writes
  size_t chunk_start_offset(offset % chunk_block_size);

  for (size_t chunk_index = offset / chunk_block_size; remain_bytes > 0; 
       chunk_index++) {
    // Calculate the byte length that we need to issue write for chunk_index
    // Within a chunk, the remaining space for a write, starting at offset
    // is (chunk_block_size - chunk_start_offset), globally there are 
    // remain_bytes to be written. Therefore, the byte length for this chunk is
    // the min of these two values
    size_t bytes_to_write(std::min(chunk_block_size - chunk_start_offset, 
                                   remain_bytes));
    // The buffer offset that we will start writing in this batch.
    void* buffer_start((char*)buffer + bytes_written);
    auto write_data_or(WriteFileChunk(filename, buffer_start, chunk_index,
                                      chunk_start_offset, bytes_to_write));
    // Return error if any write-to-chunk operation fails. This leaves the file
    // in a partial state (and that is ok). 
    if (!write_data_or.ok()) {
      return write_data_or.status();
    }

    // Currently, we consier the case of bytes_written < bytes requested to be 
    // an error, so chunk_bytes_written should be equal to bytes_to_write
    size_t chunk_bytes_written(write_data_or.ValueOrDie().bytes_written());

    // Update chunk_start_offset, bytes_written and remain_bytes counts. 
    // Starting from the second chunk, chunk_start_offset is zero
    chunk_start_offset = 0;
    bytes_written += chunk_bytes_written;
    remain_bytes -= chunk_bytes_written;
  }

  return google::protobuf::util::Status::OK;
}

void ClientImpl::RegisterChunkServerServiceClient(
    const std::string& server_address) {
  LOG(INFO) << "Establishing new connection to chunk server:" << server_address;
  chunk_server_service_client_.TryInsert(server_address,
      std::make_shared<service::ChunkServerServiceGfsClient>(
          grpc::CreateChannel(server_address,
                              grpc::InsecureChannelCredentials())));
}

std::shared_ptr<service::ChunkServerServiceGfsClient>
ClientImpl::GetChunkServerServiceClient(const std::string& server_address) {
  auto try_get_client(
      chunk_server_service_client_.TryGetValue(server_address));

  if (try_get_client.second) {
    // Client exist, return
    return try_get_client.first;
  }
   
  RegisterChunkServerServiceClient(server_address);
 
  // We are not removing client ent-point, so this line must suceed
  try_get_client = chunk_server_service_client_.TryGetValue(server_address);
  return try_get_client.first;
}

ClientImpl::ClientImpl(common::ConfigManager* config_manager,
                       const std::string& master_name,
                       const bool resolve_hostname)
    : config_manager_(config_manager) {
  // Instantiate the CacheManager with the configured timeout
  cache_manager_ = CacheManager::ConstructCacheManager(
      config_manager_->GetClientCacheTimeout());

  // Instantiate the master service client
  auto master_address(
      config_manager_->GetServerAddress(master_name, resolve_hostname));
  auto credentials = grpc::InsecureChannelCredentials();
  auto master_channel(grpc::CreateChannel(master_address, credentials));
  master_metadata_service_client_ =
      std::make_shared<service::MasterMetadataServiceClient>(master_channel);

  // Instantiate the list of chunk service clients
  auto chunk_server_names(config_manager_->GetAllChunkServers());
  for (auto const& chunk_server_name : chunk_server_names) {
    auto chunk_server_address(
        config_manager_->GetServerAddress(chunk_server_name, resolve_hostname));
    RegisterChunkServerServiceClient(chunk_server_address);
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

}  // namespace client
}  // namespace gfs
